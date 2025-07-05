package apix

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/oklog/ulid"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/natefinch/lumberjack.v2"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// --------- 常量与内部结构体 ---------
const (
	defaultValueNow       = "{{now}}"
	defaultValueSnowflake = "{{snowflake}}"
	defaultValueULID      = "{{ulid}}"
	defaultValueUUIDv4    = "{{uuidv4}}"
	defaultValueUUIDv7    = "{{uuidv7}}"

	softDeleteTypeTimestamp = "timestamp"
	softDeleteTypeBoolean   = "boolean"
	softDeleteTypeInt       = "int"

	queryParamPage     = "page"
	queryParamPageSize = "page_size"
	queryParamFields   = "fields"
	queryParamOrder    = "order"
	queryParamKey      = "key"
)

type dmConfig struct {
	DefaultPage      int                       `mapstructure:"default_page"`
	DefaultPageSize  int                       `mapstructure:"default_page_size"`
	MaxPageSize      int                       `mapstructure:"max_page_size"`
	SnowflakeNodeID  int64                     `mapstructure:"snowflake_node_id"`
	TotalCntInterval int64                     `mapstructure:"total_cnt_interval"`
	GormLog          gormLogConfig             `mapstructure:"gorm_log"`
	Databases        map[string]databaseConfig `mapstructure:"databases"`
}

type gormLogConfig struct {
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`

	SlowThreshold             string `mapstructure:"slow_threshold"`
	LogLevel                  string `mapstructure:"log_level"`
	IgnoreRecordNotFoundError bool   `mapstructure:"ignore_record_not_found_error"`
	Colorful                  bool   `mapstructure:"colorful"`
}

type databaseConfig struct {
	Alias    string        `mapstructure:"alias"`
	Type     string        `mapstructure:"type"`
	DSN      string        `mapstructure:"dsn"`
	Database string        `mapstructure:"database"`
	Pool     poolConfig    `mapstructure:"pool"`
	Tables   []tableConfig `mapstructure:"tables"`
}

type poolConfig struct {
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"max_life_time"`
	ConnMaxIdleTime time.Duration `mapstructure:"max_idle_time"`
}

type tableConfig struct {
	Name             string                 `mapstructure:"name"`
	Alias            string                 `mapstructure:"alias"`
	PrimaryKey       string                 `mapstructure:"primary_key"`
	UniqueKeys       interface{}            `mapstructure:"unique_keys"` // 支持多级结构
	DefaultValues    map[string]interface{} `mapstructure:"default_values"`
	SoftDeleteKey    string                 `mapstructure:"softdel_key"`
	SoftDeleteType   string                 `mapstructure:"softdel_type"`
	AutoUpdateFields interface{}            `mapstructure:"auto_update"`
}

// 新增：解析 unique_keys 为 [][]string
func (tc *tableConfig) GetAutoUpdateFields() []string {
	if tc.AutoUpdateFields == nil {
		return nil
	}
	switch v := tc.AutoUpdateFields.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, len(v))
		for i, val := range v {
			result[i] = fmt.Sprint(val)
		}
		return result
	case map[string]interface{}:
		result := make([]string, 0, len(v))
		for k := range v {
			result = append(result, k)
		}
		return result
	default:
		return nil
	}
}

func (tc *tableConfig) GetUniqueKeys() [][]string {
	var result [][]string
	if tc.UniqueKeys == nil {
		return nil
	}
	switch v := tc.UniqueKeys.(type) {
	case []interface{}:
		for _, item := range v {
			switch arr := item.(type) {
			case []interface{}:
				keys := make([]string, 0, len(arr))
				for _, val := range arr {
					keys = append(keys, fmt.Sprint(val))
				}
				result = append(result, keys)
			default:
				result = append(result, []string{fmt.Sprint(arr)})
			}
		}
		return result
	case []string:
		for _, s := range v {
			result = append(result, []string{s})
		}
		return result
	case map[string]interface{}:
		for k := range v {
			result = append(result, []string{k})
		}
		return result
	default:
		return nil
	}
}

// 校验字段组合是否在 unique_keys 配置中
func (tc *tableConfig) IsValidKeyCombination(fields []string) bool {
	keys := tc.GetUniqueKeys()
OUTER:
	for _, k := range keys {
		if len(k) != len(fields) {
			continue
		}
		for i := range k {
			if k[i] != fields[i] {
				continue OUTER
			}
		}
		return true
	}
	return false
}

// 解析 key 参数如 "phone,email" 为 []string
func parseKeyFields(keyParam string) []string {
	keyParam = strings.TrimSpace(keyParam)
	if keyParam == "" {
		return nil
	}
	fields := strings.Split(keyParam, ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	return fields
}

type listParams struct {
	Page         int
	PageSize     int
	Fields       string
	Order        string
	QueryFilters url.Values
}

type databaseAdapter interface {
	List(ctx context.Context, tableConfig *tableConfig, params listParams) (data []map[string]interface{}, total int64, err error)
	BatchCreate(ctx context.Context, tableConfig *tableConfig, records []map[string]interface{}) (insertedIDs []interface{}, updatedRecords []map[string]interface{}, err error)
	BatchUpdate(ctx context.Context, tableConfig *tableConfig, records []map[string]interface{}) (matchedCount int64, modifiedCount int64, err error)
	BatchDelete(ctx context.Context, tableConfig *tableConfig, ids []interface{}) (affectedCount int64, err error)
	GetOne(ctx context.Context, tableConfig *tableConfig, filter map[string]interface{}, fields string) (record map[string]interface{}, err error)
	UpdateOne(ctx context.Context, tableConfig *tableConfig, filter map[string]interface{}, data map[string]interface{}) (matchedCount int64, modifiedCount int64, err error)
	DeleteOne(ctx context.Context, tableConfig *tableConfig, filter map[string]interface{}) (affectedCount int64, err error)
	CountAll(ctx context.Context, tableConfig *tableConfig) (int64, error)
	Close() error
}

var (
	globalSnowflakeNode *snowflake.Node
)

// --------- 主管理器 ---------
type databaseManager struct {
	config             *dmConfig
	gormDBs            map[string]*gorm.DB
	mongoClients       map[string]*mongo.Client
	adapters           map[string]databaseAdapter
	mutex              sync.RWMutex
	tableCounts        map[string]int64
	countMutex         sync.RWMutex
	cancelTableCounter context.CancelFunc
}

// --------- RegisterRestAPI 及初始化 ---------

func RegisterRestAPI(router *gin.Engine, prefix string, config ...string) {
	var configPath string
	if len(config) > 0 {
		configPath = config[0]
	} else {
		if fileExists("config.yaml") {
			configPath = "config.yaml"
		} else if dirExists("cfgs") {
			configPath = "cfgs"
		} else {
			configPath = ""
		}
	}
	dbManager, err := newDatabaseManager(configPath)
	if err != nil {
		log.Fatalf("Failed to initialize database manager: %v", err)
	}
	api := router.Group(prefix)
	{
		api.GET("/:database/:table", dbManager.handleList)
		api.POST("/:database/:table", dbManager.handleBatchCreate)
		api.PUT("/:database/:table", dbManager.handleBatchUpdate)
		api.POST("/:database/:table/batch_delete", dbManager.handleBatchDelete)
		api.GET("/:database/:table/:id", dbManager.handleGetOne)
		api.PUT("/:database/:table/:id", dbManager.handleUpdateOne)
		api.DELETE("/:database/:table/:id", dbManager.handleDeleteOne)
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func loadConfigFromDir(configDir string) (*dmConfig, error) {
	// 自动找目录顶级yaml配置
	configPath := filepath.Join(configDir, "_base.yaml")
	baseDir := filepath.Dir(configPath)

	mainV := viper.New()
	mainV.SetConfigFile(configPath)
	mainV.SetDefault("default_page", 1)
	mainV.SetDefault("default_page_size", 10)
	mainV.SetDefault("max_page_size", 1000)
	mainV.SetDefault("snowflake_node_id", 1)
	mainV.SetDefault("total_cnt_interval", 30)
	mainV.SetDefault("gorm_log.filename", "logs/gorm.log")
	mainV.SetDefault("gorm_log.max_size", 100)
	mainV.SetDefault("gorm_log.max_backups", 3)
	mainV.SetDefault("gorm_log.max_age", 28)
	mainV.SetDefault("gorm_log.compress", true)
	mainV.SetDefault("gorm_log.slow_threshold", "1s")
	mainV.SetDefault("gorm_log.log_level", "info")
	mainV.SetDefault("gorm_log.ignore_record_not_found_error", true)
	mainV.SetDefault("gorm_log.colorful", false)
	if err := mainV.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read main config: %w", err)
	}
	config := &dmConfig{}
	if err := mainV.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal main config: %w", err)
	}
	if config.Databases == nil {
		config.Databases = make(map[string]databaseConfig)
	}

	databaseDir := filepath.Join(baseDir, "database")
	tableDir := filepath.Join(baseDir, "table")
	entries, err := os.ReadDir(databaseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read database config dir: %w", err)
	}
	for _, entry := range entries {
		if !strings.Contains(entry.Name(), ".enable.yaml") {
			continue
		}
		// 读库配置文件
		databaseYaml := filepath.Join(databaseDir, entry.Name())
		if _, err := os.Stat(databaseYaml); err != nil {
			continue
		}
		dsV := viper.New()
		dsV.SetConfigFile(databaseYaml)
		dfName := strings.Replace(entry.Name(), ".enable.yaml", "", 1)
		if err := dsV.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config for datasource %s: %w", dfName, err)
		}
		dsConf := databaseConfig{}
		if err := dsV.Unmarshal(&dsConf); err != nil {
			return nil, fmt.Errorf("failed to unmarshal yaml for datasource %s: %w", dfName, err)
		}

		// 遍历表配置文件
		tbPath := filepath.Join(tableDir, dfName)
		files, _ := os.ReadDir(tbPath)
		var tables []tableConfig
		for _, f := range files {
			if !strings.Contains(f.Name(), ".enable.yaml") {
				continue
			}
			tblV := viper.New()
			tblV.SetConfigFile(filepath.Join(tbPath, f.Name()))
			if err := tblV.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("failed to read table config %s: %w", f.Name(), err)
			}
			tblConf := tableConfig{}
			if err := tblV.Unmarshal(&tblConf); err != nil {
				return nil, fmt.Errorf("failed to unmarshal table config %s: %w", f.Name(), err)
			}
			tables = append(tables, tblConf)
		}
		dsConf.Tables = tables
		config.Databases[dsConf.Alias] = dsConf
	}
	return config, nil
}

func newDatabaseManager(configPath string) (*databaseManager, error) {
	if configPath == "" {
		return nil, errors.New("config path is empty")
	}
	_, err := os.Stat(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}
	var cfg *dmConfig
	cfg, err = loadConfigFromDir(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}
	node, err := snowflake.NewNode(cfg.SnowflakeNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake node: %w", err)
	}
	globalSnowflakeNode = node
	slowThreshold, err := time.ParseDuration(cfg.GormLog.SlowThreshold)
	if err != nil {
		return nil, fmt.Errorf("invalid slow_threshold: %w", err)
	}
	var logLevel logger.LogLevel
	switch strings.ToLower(cfg.GormLog.LogLevel) {
	case "silent":
		logLevel = logger.Silent
	case "error":
		logLevel = logger.Error
	case "warn":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	default:
		return nil, fmt.Errorf("invalid gorm log level: %s", cfg.GormLog.LogLevel)
	}
	gormLogger := logger.New(
		log.New(&lumberjack.Logger{
			Filename:   cfg.GormLog.Filename,
			MaxSize:    cfg.GormLog.MaxSize,
			MaxBackups: cfg.GormLog.MaxBackups,
			MaxAge:     cfg.GormLog.MaxAge,
			Compress:   cfg.GormLog.Compress,
		}, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             slowThreshold,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)
	dm := &databaseManager{
		config:       cfg,
		gormDBs:      make(map[string]*gorm.DB),
		mongoClients: make(map[string]*mongo.Client),
		adapters:     make(map[string]databaseAdapter),
		tableCounts:  make(map[string]int64),
	}
	for name, dbConfig := range cfg.Databases {
		switch strings.ToLower(dbConfig.Type) {
		case "mysql":
			db, err := setupGormDB(dbConfig, gormLogger, mysql.Open(dbConfig.DSN))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to MySQL %s: %w", name, err)
			}
			dm.gormDBs[name] = db
			dm.adapters[name] = newGormAdapter(db, &dbConfig)
		case "postgresql":
			db, err := setupGormDB(dbConfig, gormLogger, postgres.Open(dbConfig.DSN))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to PostgreSQL %s: %w", name, err)
			}
			dm.gormDBs[name] = db
			dm.adapters[name] = newGormAdapter(db, &dbConfig)
		case "sqlite":
			db, err := setupGormDB(dbConfig, gormLogger, sqlite.Open(dbConfig.DSN))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to SQLite %s: %w", name, err)
			}
			dm.gormDBs[name] = db
			dm.adapters[name] = newGormAdapter(db, &dbConfig)
		case "sqlserver":
			db, err := setupGormDB(dbConfig, gormLogger, sqlserver.Open(dbConfig.DSN))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to SQL Server %s: %w", name, err)
			}
			dm.gormDBs[name] = db
			dm.adapters[name] = newGormAdapter(db, &dbConfig)
		case "clickhouse":
			db, err := setupGormDB(dbConfig, gormLogger, clickhouse.Open(dbConfig.DSN))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to ClickHouse %s: %w", name, err)
			}
			dm.gormDBs[name] = db
			dm.adapters[name] = newGormAdapter(db, &dbConfig)
		case "mongodb":
			clientOptions := options.Client().ApplyURI(dbConfig.DSN)
			if dbConfig.Pool.MaxOpenConns > 0 {
				clientOptions.SetMaxPoolSize(uint64(dbConfig.Pool.MaxOpenConns))
			}
			if dbConfig.Pool.ConnMaxIdleTime > 0 {
				clientOptions.SetMaxConnIdleTime(dbConfig.Pool.ConnMaxIdleTime)
			}
			client, err := mongo.Connect(context.TODO(), clientOptions)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to MongoDB %s: %w", name, err)
			}
			pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
			err = client.Ping(pingCtx, readpref.Primary())
			cancelPing()
			if err != nil {
				return nil, fmt.Errorf("failed to ping MongoDB %s: %w", name, err)
			}
			dm.mongoClients[name] = client
			dm.adapters[name] = newMongoAdapter(client, dbConfig.Database, &dbConfig)
		default:
			return nil, fmt.Errorf("unsupported database type for %s: %s", name, dbConfig.Type)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	dm.cancelTableCounter = cancel
	go dm.startTableCounter(ctx, time.Duration(cfg.TotalCntInterval)*time.Second)
	return dm, nil
}

func setupGormDB(dbConfig databaseConfig, gormLogger logger.Interface, dialector gorm.Dialector) (*gorm.DB, error) {
	gormConfig := &gorm.Config{
		Logger: gormLogger,
	}
	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return nil, err
	}
	sqlDB, _ := db.DB()
	if dbConfig.Pool.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(dbConfig.Pool.MaxOpenConns)
	}
	if dbConfig.Pool.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(dbConfig.Pool.MaxIdleConns)
	}
	if dbConfig.Pool.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(dbConfig.Pool.ConnMaxLifetime)
	}
	if dbConfig.Pool.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(dbConfig.Pool.ConnMaxIdleTime)
	}
	return db, nil
}

// --------- tableConfig 方法 & table counter ---------

func (dm *databaseManager) startTableCounter(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	dm.updateAllTableCounts(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dm.updateAllTableCounts(ctx)
		}
	}
}

func (dm *databaseManager) updateAllTableCounts(ctx context.Context) {
	dm.mutex.RLock()
	adaptersToUpdate := make(map[string]databaseAdapter)
	configsToUpdate := make(map[string]databaseConfig)
	for name, adapter := range dm.adapters {
		adaptersToUpdate[name] = adapter
		configsToUpdate[name] = dm.config.Databases[name]
	}
	dm.mutex.RUnlock()
	for dbName, adapter := range adaptersToUpdate {
		dbCfg := configsToUpdate[dbName]
		for _, tableCfg := range dbCfg.Tables {
			currentTableCfg := tableCfg
			key := fmt.Sprintf("%s_%s", dbName, currentTableCfg.Alias)
			countCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			count, err := adapter.CountAll(countCtx, &currentTableCfg)
			cancel()
			if err != nil {
				continue
			}
			dm.countMutex.Lock()
			dm.tableCounts[key] = count
			dm.countMutex.Unlock()
		}
	}
}

func (dm *databaseManager) getAdapterAndTableConfig(dbName, tableAlias string) (databaseAdapter, *tableConfig, error) {
	dm.mutex.RLock()
	adapter, ok := dm.adapters[dbName]
	dbCfg, dbOk := dm.config.Databases[dbName]
	dm.mutex.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("database adapter for %s not found or not configured", dbName)
	}
	if !dbOk {
		return nil, nil, fmt.Errorf("database configuration for %s not found", dbName)
	}
	for i := range dbCfg.Tables {
		if dbCfg.Tables[i].Alias == tableAlias {
			return adapter, &dbCfg.Tables[i], nil
		}
	}
	return nil, nil, fmt.Errorf("table configuration for alias %s in database %s not found", tableAlias, dbName)
}

// --------- 通用辅助函数 ---------

func applyGormSoftDeleteFilter(db *gorm.DB, tc *tableConfig) *gorm.DB {
	if tc.SoftDeleteKey != "" {
		switch tc.SoftDeleteType {
		case softDeleteTypeTimestamp:
			return db.Where(fmt.Sprintf("%s IS NULL OR %s = ?", tc.SoftDeleteKey, tc.SoftDeleteKey), time.Time{})
		case softDeleteTypeBoolean:
			return db.Where(fmt.Sprintf("%s = ?", tc.SoftDeleteKey), false)
		case softDeleteTypeInt:
			return db.Where(fmt.Sprintf("%s = ?", tc.SoftDeleteKey), 0)
		default:
			return db.Where(fmt.Sprintf("%s IS NULL", tc.SoftDeleteKey))
		}
	}
	return db
}

func applyMongoSoftDeleteFilter(filter bson.M, tc *tableConfig) bson.M {
	if tc.SoftDeleteKey != "" {
		if filter == nil {
			filter = bson.M{}
		}
		switch tc.SoftDeleteType {
		case softDeleteTypeTimestamp:
			filter["$or"] = []bson.M{
				{tc.SoftDeleteKey: bson.M{"$exists": false}},
				{tc.SoftDeleteKey: nil},
				{tc.SoftDeleteKey: ""},
			}
		case softDeleteTypeBoolean:
			filter[tc.SoftDeleteKey] = false
		case softDeleteTypeInt:
			filter[tc.SoftDeleteKey] = 0
		default:
			filter["$or"] = []bson.M{
				{tc.SoftDeleteKey: bson.M{"$exists": false}},
				{tc.SoftDeleteKey: nil},
			}
		}
	}
	return filter
}

func parseFilterValue(value string) interface{} {
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(value); err == nil {
		return b
	}
	return value
}

func parseStringList(value string) []string {
	if value == "" {
		return []string{}
	}
	return strings.Split(value, ",")
}

func parseFilterValues(value string) []interface{} {
	parts := strings.Split(value, ",")
	values := make([]interface{}, len(parts))
	for i, part := range parts {
		values[i] = parseFilterValue(strings.TrimSpace(part))
	}
	return values
}

func normalizeLikeValue(value string) string {
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func applyDefaultValues(record map[string]interface{}, tc *tableConfig) {
	if tc.DefaultValues == nil {
		return
	}
	for field, defaultValue := range tc.DefaultValues {
		if val, exists := record[field]; !exists || val == nil || val == "" {
			if strVal, ok := defaultValue.(string); ok {
				switch strVal {
				case defaultValueNow:
					record[field] = time.Now()
				case defaultValueSnowflake:
					id, _ := generateSnowflakeID()
					record[field] = id
				case defaultValueULID:
					id, _ := generateULID()
					record[field] = id
				case defaultValueUUIDv4:
					id, _ := generateUUIDv4()
					record[field] = id
				case defaultValueUUIDv7:
					id, _ := generateUUIDv7()
					record[field] = id
				default:
					record[field] = defaultValue
				}
			} else {
				record[field] = defaultValue
			}
		}
	}
}

func applyAutoUpdateFields(record map[string]interface{}, tc *tableConfig) {
	if tc.AutoUpdateFields == nil || len(tc.GetAutoUpdateFields()) == 0 {
		return
	}
	now := time.Now()
	for _, field := range tc.GetAutoUpdateFields() {
		record[field] = now
	}
}

func generateSnowflakeID() (string, error) {
	if globalSnowflakeNode == nil {
		return "", fmt.Errorf("snowflake node not initialized")
	}
	return globalSnowflakeNode.Generate().String(), nil
}

func generateULID() (string, error) {
	t := time.Now().UTC()
	entropy := ulid.Monotonic(rand.Reader, 0)
	id, err := ulid.New(ulid.Timestamp(t), entropy)
	if err != nil {
		return "", fmt.Errorf("failed to generate ULID: %w", err)
	}
	return id.String(), nil
}

func generateUUIDv4() (string, error) {
	uuid := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, uuid)
	if err != nil {
		return "", fmt.Errorf("failed to generate UUID v4: %w", err)
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16]), nil
}

func generateUUIDv7() (string, error) {
	uuid := make([]byte, 16)
	now := time.Now().UnixMilli()
	binary.BigEndian.PutUint64(uuid[0:8], uint64(now))
	_, err := io.ReadFull(rand.Reader, uuid[6:])
	if err != nil {
		return "", fmt.Errorf("failed to generate UUID v7: %w", err)
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x70
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16]), nil
}

func fixPkFieldToString(obj interface{}, keys ...string) interface{} {
	switch v := obj.(type) {
	case map[string]interface{}:
		// 自动把 "@id" 替换成指定主键名
		if len(keys) > 0 {
			pk := keys[0]
			if val, ok := v["@id"]; ok {
				v[pk] = val
				delete(v, "@id")
			}
		}
		for k, val := range v {
			if contains(keys, k) {
				switch t := val.(type) {
				case int64:
					v[k] = fmt.Sprintf("%d", t)
				case float64:
					v[k] = fmt.Sprintf("%.0f", t)
				case int:
					v[k] = fmt.Sprintf("%d", t)
				}
			} else {
				v[k] = fixPkFieldToString(val, keys...)
			}
		}
		return v
	case []map[string]interface{}:
		for i, e := range v {
			v[i] = fixPkFieldToString(e, keys...).(map[string]interface{})
		}
		return v
	case []interface{}:
		for i, e := range v {
			v[i] = fixPkFieldToString(e, keys...)
		}
		return v
	default:
		return obj
	}
}

func contains(arr []string, target string) bool {
	for _, s := range arr {
		if s == target {
			return true
		}
	}
	return false
}

// --------- Gin Handler 实现部分 ---------

func (dm *databaseManager) handleList(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	pageStr := c.DefaultQuery(queryParamPage, strconv.Itoa(dm.config.DefaultPage))
	pageSizeStr := c.DefaultQuery(queryParamPageSize, strconv.Itoa(dm.config.DefaultPageSize))
	page, _ := strconv.Atoi(pageStr)
	pageSize, _ := strconv.Atoi(pageSizeStr)
	if page <= 0 {
		page = dm.config.DefaultPage
	}
	if pageSize <= 0 {
		pageSize = dm.config.DefaultPageSize
	}
	if pageSize > dm.config.MaxPageSize {
		pageSize = dm.config.MaxPageSize
	}
	listParams := listParams{
		Page:         page,
		PageSize:     pageSize,
		Fields:       c.Query(queryParamFields),
		Order:        c.Query(queryParamOrder),
		QueryFilters: c.Request.URL.Query(),
	}
	isFiltered := false
	for key := range listParams.QueryFilters {
		if key != queryParamPage && key != queryParamPageSize && key != queryParamOrder && key != queryParamFields {
			isFiltered = true
			break
		}
	}
	data, totalFromAdapter, err := adapter.List(c.Request.Context(), tableConfig, listParams)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	finalTotal := totalFromAdapter
	if !isFiltered {
		dm.countMutex.RLock()
		cachedCount, ok := dm.tableCounts[fmt.Sprintf("%s_%s", dbName, tableAlias)]
		dm.countMutex.RUnlock()
		if ok {
			finalTotal = cachedCount
		}
	}
	if data == nil {
		data = []map[string]interface{}{}
	}
	data = fixPkFieldToString(data, tableConfig.PrimaryKey).([]map[string]interface{})
	c.JSON(http.StatusOK, gin.H{"total": finalTotal, "data": data})
}

func (dm *databaseManager) handleBatchCreate(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	var records []map[string]interface{}
	if err := c.ShouldBindJSON(&records); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload: " + err.Error()})
		return
	}
	if len(records) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No records to create"})
		return
	}
	for i := range records {
		applyDefaultValues(records[i], tableConfig)
	}
	insertedIDs, updatedRecords, err := adapter.BatchCreate(c.Request.Context(), tableConfig, records)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to batch create: " + err.Error()})
		return
	}
	if insertedIDs != nil && len(insertedIDs) == len(updatedRecords) {
		pk := tableConfig.PrimaryKey
		for i, id := range insertedIDs {
			updatedRecords[i][pk] = id
		}
	}
	updatedRecords = fixPkFieldToString(updatedRecords, tableConfig.PrimaryKey).([]map[string]interface{})
	c.JSON(http.StatusCreated, updatedRecords)
}

func (dm *databaseManager) handleBatchUpdate(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if tableConfig.PrimaryKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Primary key not defined for table, batch update requires primary key."})
		return
	}
	var records []map[string]interface{}
	if err := c.ShouldBindJSON(&records); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload: " + err.Error()})
		return
	}
	if len(records) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No records to update"})
		return
	}
	for i := range records {
		applyAutoUpdateFields(records[i], tableConfig)
	}
	matchedCount, modifiedCount, err := adapter.BatchUpdate(c.Request.Context(), tableConfig, records)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to batch update: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Batch update successful", "matched_count": matchedCount, "modified_count": modifiedCount})
}

func (dm *databaseManager) handleBatchDelete(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if tableConfig.PrimaryKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Primary key not defined for table, batch delete requires primary key."})
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Read body failed"})
		return
	}
	var idsToDelete []interface{}
	var recordsToDelete []map[string]interface{}
	if errObj := json.Unmarshal(body, &recordsToDelete); errObj == nil && len(recordsToDelete) > 0 {
		for _, rec := range recordsToDelete {
			if idVal, ok := rec[tableConfig.PrimaryKey]; ok {
				idsToDelete = append(idsToDelete, idVal)
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Record in array missing primary key '%s'", tableConfig.PrimaryKey)})
				return
			}
		}
	} else {
		var plainIds []interface{}
		if errPlain := json.Unmarshal(body, &plainIds); errPlain == nil && len(plainIds) > 0 {
			idsToDelete = plainIds
		} else {
			errMsg := "Invalid JSON payload. Expected array of IDs or array of objects with primary keys."
			if errObj != nil && errPlain != nil {
				errMsg = fmt.Sprintf("Invalid JSON payload. Object array error: %s. Plain ID array error: %s", errObj, errPlain)
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
	}
	if len(idsToDelete) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No IDs provided for deletion"})
		return
	}
	affectedCount, err := adapter.BatchDelete(c.Request.Context(), tableConfig, idsToDelete)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to batch delete: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Batch delete successful", "deleted_count": affectedCount})
}

func (dm *databaseManager) handleGetOne(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	idValStr := c.Param("id")
	keyFieldParam := c.Query(queryParamKey)
	fields := c.Query(queryParamFields)
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	keyFields := parseKeyFields(keyFieldParam)
	var filter map[string]interface{}
	if len(keyFields) > 0 {
		if !tableConfig.IsValidKeyCombination(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Key combination '%v' is not a configured unique key", keyFields)})
			return
		}
		vals := parseStringList(idValStr)
		if len(vals) != len(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id value count does not match unique key fields"})
			return
		}
		filter = make(map[string]interface{})
		for i, f := range keyFields {
			filter[f] = vals[i]
		}
	} else {
		if tableConfig.PrimaryKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "No identifiable key (primary or unique) configured for table"})
			return
		}
		filter = map[string]interface{}{tableConfig.PrimaryKey: idValStr}
	}
	record, err := adapter.GetOne(c.Request.Context(), tableConfig, filter, fields)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, mongo.ErrNoDocuments) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Record not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get record: " + err.Error()})
		}
		return
	}
	record = fixPkFieldToString(record, tableConfig.PrimaryKey).(map[string]interface{})
	c.JSON(http.StatusOK, record)
}

func (dm *databaseManager) handleUpdateOne(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	idValStr := c.Param("id")
	keyFieldParam := c.Query(queryParamKey)
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	keyFields := parseKeyFields(keyFieldParam)
	var filter map[string]interface{}
	if len(keyFields) > 0 {
		if !tableConfig.IsValidKeyCombination(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Key combination '%v' is not a configured unique key", keyFields)})
			return
		}
		vals := parseStringList(idValStr)
		if len(vals) != len(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id value count does not match unique key fields"})
			return
		}
		filter = make(map[string]interface{})
		for i, f := range keyFields {
			filter[f] = vals[i]
		}
	} else {
		if tableConfig.PrimaryKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "No identifiable key (primary or unique) configured for table"})
			return
		}
		filter = map[string]interface{}{tableConfig.PrimaryKey: idValStr}
	}
	var updateData map[string]interface{}
	if err := c.ShouldBindJSON(&updateData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload: " + err.Error()})
		return
	}
	// 移除所有filter字段
	for k := range filter {
		delete(updateData, k)
	}
	if len(updateData) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No fields to update in payload"})
		return
	}
	applyAutoUpdateFields(updateData, tableConfig)
	matchedCount, modifiedCount, err := adapter.UpdateOne(c.Request.Context(), tableConfig, filter, updateData)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, mongo.ErrNoDocuments) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Record not found to update"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update record: " + err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Update successful", "matched_count": matchedCount, "modified_count": modifiedCount})
}

func (dm *databaseManager) handleDeleteOne(c *gin.Context) {
	dbName := c.Param("database")
	tableAlias := c.Param("table")
	idValStr := c.Param("id")
	keyFieldParam := c.Query(queryParamKey)
	adapter, tableConfig, err := dm.getAdapterAndTableConfig(dbName, tableAlias)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	keyFields := parseKeyFields(keyFieldParam)
	var filter map[string]interface{}
	if len(keyFields) > 0 {
		if !tableConfig.IsValidKeyCombination(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Key combination '%v' is not a configured unique key", keyFields)})
			return
		}
		vals := parseStringList(idValStr)
		if len(vals) != len(keyFields) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id value count does not match unique key fields"})
			return
		}
		filter = make(map[string]interface{})
		for i, f := range keyFields {
			filter[f] = vals[i]
		}
	} else {
		if tableConfig.PrimaryKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "No identifiable key (primary or unique) configured for table"})
			return
		}
		filter = map[string]interface{}{tableConfig.PrimaryKey: idValStr}
	}
	affectedCount, err := adapter.DeleteOne(c.Request.Context(), tableConfig, filter)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, mongo.ErrNoDocuments) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Record not found to delete"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete record: " + err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Delete successful", "deleted_count": affectedCount})
}

// --------- GORM Adapter 实现 ---------

type gormAdapter struct {
	db     *gorm.DB
	config *databaseConfig
}

func newGormAdapter(db *gorm.DB, cfg *databaseConfig) *gormAdapter {
	return &gormAdapter{db: db, config: cfg}
}

func (a *gormAdapter) List(ctx context.Context, tc *tableConfig, params listParams) ([]map[string]interface{}, int64, error) {
	var results []map[string]interface{}
	var total int64
	db := a.db.WithContext(ctx).Table(tc.Name)
	db = applyGormSoftDeleteFilter(db, tc)
	hasFilter := false
	for key, values := range params.QueryFilters {
		if key == queryParamPage || key == queryParamPageSize || key == queryParamFields || key == queryParamOrder {
			continue
		}
		if len(values) == 0 {
			continue
		}
		value := values[0]
		hasFilter = true
		var fieldName, op string
		if strings.Contains(key, "__") {
			parts := strings.SplitN(key, "__", 2)
			fieldName = parts[0]
			op = "__" + parts[1]
		} else {
			fieldName = key
			op = "="
		}
		parsedVal := parseFilterValue(value)
		stringVals := parseStringList(value)
		parsedVals := parseFilterValues(value)
		switch op {
		case "=":
			db = db.Where(fmt.Sprintf("%s = ?", fieldName), parsedVal)
		case "__gte":
			db = db.Where(fmt.Sprintf("%s >= ?", fieldName), parsedVal)
		case "__lte":
			db = db.Where(fmt.Sprintf("%s <= ?", fieldName), parsedVal)
		case "__gt":
			db = db.Where(fmt.Sprintf("%s > ?", fieldName), parsedVal)
		case "__lt":
			db = db.Where(fmt.Sprintf("%s < ?", fieldName), parsedVal)
		case "__ne":
			db = db.Where(fmt.Sprintf("%s <> ?", fieldName), parsedVal)
		case "__like":
			db = db.Where(fmt.Sprintf("%s LIKE ?", fieldName), normalizeLikeValue(value))
		case "__icontains":
			db = db.Where(fmt.Sprintf("LOWER(%s) LIKE LOWER(?)", fieldName), "%"+normalizeLikeValue(value)+"%")
		case "__in":
			db = db.Where(fmt.Sprintf("%s IN (?)", fieldName), stringVals)
		case "__isnull":
			if bVal, ok := parsedVal.(bool); ok {
				if bVal {
					db = db.Where(fmt.Sprintf("%s IS NULL", fieldName))
				} else {
					db = db.Where(fmt.Sprintf("%s IS NOT NULL", fieldName))
				}
			}
		case "__between":
			if len(parsedVals) == 2 {
				db = db.Where(fmt.Sprintf("%s BETWEEN ? AND ?", fieldName), parsedVals[0], parsedVals[1])
			}
		default:
		}
	}
	if hasFilter {
		if err := db.Count(&total).Error; err != nil {
			return nil, 0, fmt.Errorf("failed to count records: %w", err)
		}
	}
	if params.Order != "" {
		if strings.HasPrefix(params.Order, "-") {
			db = db.Order(fmt.Sprintf("%s DESC", params.Order[1:]))
		} else {
			db = db.Order(fmt.Sprintf("%s ASC", params.Order))
		}
	}
	if params.Fields != "" {
		db = db.Select(params.Fields)
	}
	offset := (params.Page - 1) * params.PageSize
	if err := db.Offset(offset).Limit(params.PageSize).Find(&results).Error; err != nil {
		return nil, total, fmt.Errorf("failed to query database: %w", err)
	}
	return results, total, nil
}

func (a *gormAdapter) BatchCreate(ctx context.Context, tc *tableConfig, records []map[string]interface{}) ([]interface{}, []map[string]interface{}, error) {
	err := a.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err_create := tx.Table(tc.Name).Create(&records).Error; err_create != nil {
			return err_create
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return nil, records, nil
}

func (a *gormAdapter) BatchUpdate(ctx context.Context, tc *tableConfig, records []map[string]interface{}) (int64, int64, error) {
	var totalAffected int64 = 0
	pkField := tc.PrimaryKey
	err := a.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, record := range records {
			idVal, ok := record[pkField]
			if !ok {
				return fmt.Errorf("record missing primary key '%s'", pkField)
			}
			updateData := make(map[string]interface{})
			for k, v := range record {
				if k != pkField {
					updateData[k] = v
				}
			}
			if len(updateData) == 0 {
				continue
			}
			res := tx.Table(tc.Name).Where(fmt.Sprintf("%s = ?", pkField), idVal).Updates(updateData)
			if res.Error != nil {
				return res.Error
			}
			totalAffected += res.RowsAffected
		}
		return nil
	})
	return totalAffected, totalAffected, err
}

func (a *gormAdapter) BatchDelete(ctx context.Context, tc *tableConfig, ids []interface{}) (int64, error) {
	var affectedRows int64 = 0
	pkField := tc.PrimaryKey
	err := a.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		targetDB := tx.Table(tc.Name).Where(fmt.Sprintf("%s IN (?)", pkField), ids)
		var res *gorm.DB
		if tc.SoftDeleteKey != "" {
			updateData := map[string]interface{}{}
			switch tc.SoftDeleteType {
			case softDeleteTypeTimestamp:
				updateData[tc.SoftDeleteKey] = time.Now()
			case softDeleteTypeBoolean:
				updateData[tc.SoftDeleteKey] = true
			case softDeleteTypeInt:
				updateData[tc.SoftDeleteKey] = 1
			default:
				updateData[tc.SoftDeleteKey] = time.Now()
			}
			res = targetDB.Updates(updateData)
		} else {
			res = targetDB.Delete(nil)
		}
		if res.Error != nil {
			return res.Error
		}
		affectedRows = res.RowsAffected
		return nil
	})
	return affectedRows, err
}

func (a *gormAdapter) GetOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}, fields string) (map[string]interface{}, error) {
	var result map[string]interface{}
	db := a.db.WithContext(ctx).Table(tc.Name)
	db = applyGormSoftDeleteFilter(db, tc)
	if fields != "" {
		db = db.Select(fields)
	}
	for k, v := range filter {
		db = db.Where(fmt.Sprintf("%s = ?", k), v)
	}
	err := db.Take(&result).Error
	return result, err
}

func (a *gormAdapter) UpdateOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}, data map[string]interface{}) (int64, int64, error) {
	var affectedRows int64 = 0
	err := a.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Table(tc.Name)
		query = applyGormSoftDeleteFilter(query, tc)
		for k, v := range filter {
			query = query.Where(fmt.Sprintf("%s = ?", k), v)
		}
		res := query.Updates(data)
		if res.Error != nil {
			return res.Error
		}
		affectedRows = res.RowsAffected
		if affectedRows == 0 {
			var count int64
			existQ := tx.Table(tc.Name)
			for k, v := range filter {
				existQ = existQ.Where(fmt.Sprintf("%s = ?", k), v)
			}
			errExists := existQ.Count(&count).Error
			if errExists != nil {
				return errExists
			}
			if count == 0 {
				return gorm.ErrRecordNotFound
			}
		}
		return nil
	})
	return affectedRows, affectedRows, err
}

func (a *gormAdapter) DeleteOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}) (int64, error) {
	var affectedRows int64 = 0
	err := a.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Table(tc.Name)
		for k, v := range filter {
			query = query.Where(fmt.Sprintf("%s = ?", k), v)
		}
		var res *gorm.DB
		if tc.SoftDeleteKey != "" {
			updateData := map[string]interface{}{}
			switch tc.SoftDeleteType {
			case softDeleteTypeTimestamp:
				updateData[tc.SoftDeleteKey] = time.Now()
			case softDeleteTypeBoolean:
				updateData[tc.SoftDeleteKey] = true
			case softDeleteTypeInt:
				updateData[tc.SoftDeleteKey] = 1
			default:
				updateData[tc.SoftDeleteKey] = time.Now()
			}
			res = query.Updates(updateData)
		} else {
			res = query.Delete(nil)
		}
		if res.Error != nil {
			return res.Error
		}
		affectedRows = res.RowsAffected
		if affectedRows == 0 {
			var count int64
			existQ := tx.Table(tc.Name)
			for k, v := range filter {
				existQ = existQ.Where(fmt.Sprintf("%s = ?", k), v)
			}
			errExists := existQ.Count(&count).Error
			if errExists != nil {
				return errExists
			}
			if count == 0 {
				return gorm.ErrRecordNotFound
			}
		}
		return nil
	})
	return affectedRows, err
}

func (a *gormAdapter) CountAll(ctx context.Context, tc *tableConfig) (int64, error) {
	var count int64
	db := a.db.WithContext(ctx).Table(tc.Name)
	db = applyGormSoftDeleteFilter(db, tc)
	if err := db.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (a *gormAdapter) Close() error {
	sqlDB, err := a.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB for GORM adapter: %w", err)
	}
	return sqlDB.Close()
}

// --------- Mongo Adapter 实现 ---------

type mongoAdapter struct {
	client   *mongo.Client
	database string
	config   *databaseConfig
}

func newMongoAdapter(client *mongo.Client, dbName string, cfg *databaseConfig) *mongoAdapter {
	return &mongoAdapter{client: client, database: dbName, config: cfg}
}

func (a *mongoAdapter) List(ctx context.Context, tc *tableConfig, params listParams) ([]map[string]interface{}, int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	filter := bson.M{}
	filter = applyMongoSoftDeleteFilter(filter, tc)
	isFiltered := false
	for key, values := range params.QueryFilters {
		if key == queryParamPage || key == queryParamPageSize || key == queryParamFields || key == queryParamOrder {
			continue
		}
		if len(values) == 0 {
			continue
		}
		value := values[0]
		isFiltered = true
		var fieldName, op string
		if strings.Contains(key, "__") {
			parts := strings.SplitN(key, "__", 2)
			fieldName = parts[0]
			op = "__" + parts[1]
		} else {
			fieldName = key
			op = "="
		}
		switch op {
		case "=":
			filter[fieldName] = parseFilterValue(value)
		case "__gte":
			filter[fieldName] = bson.M{"$gte": parseFilterValue(value)}
		case "__lte":
			filter[fieldName] = bson.M{"$lte": parseFilterValue(value)}
		case "__gt":
			filter[fieldName] = bson.M{"$gt": parseFilterValue(value)}
		case "__lt":
			filter[fieldName] = bson.M{"$lt": parseFilterValue(value)}
		case "__ne":
			filter[fieldName] = bson.M{"$ne": parseFilterValue(value)}
		case "__like":
			pattern := regexp.QuoteMeta(value)
			pattern = strings.ReplaceAll(pattern, "%", ".*")
			pattern = strings.ReplaceAll(pattern, "_", ".")
			if !strings.HasPrefix(pattern, ".*") {
				pattern = "^" + pattern
			}
			if !strings.HasSuffix(pattern, ".*") {
				pattern = pattern + "$"
			}
			filter[fieldName] = bson.M{"$regex": pattern, "$options": "i"}
		case "__icontains":
			filter[fieldName] = bson.M{"$regex": value, "$options": "i"}
		case "__in":
			filter[fieldName] = bson.M{"$in": parseFilterValues(value)}
		case "__isnull":
			if bVal, ok := parseFilterValue(value).(bool); ok {
				if bVal {
					filter[fieldName] = bson.M{"$exists": false}
				} else {
					filter[fieldName] = bson.M{"$exists": true}
				}
			}
		case "__between":
			vals := parseFilterValues(value)
			if len(vals) == 2 {
				filter[fieldName] = bson.M{"$gte": vals[0], "$lte": vals[1]}
			}
		default:
		}
	}
	opts := options.Find()
	if params.Order != "" {
		sort := bson.D{}
		fields := strings.Split(params.Order, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if strings.HasPrefix(field, "-") {
				sort = append(sort, bson.E{Key: field[1:], Value: -1})
			} else {
				sort = append(sort, bson.E{Key: field, Value: 1})
			}
		}
		opts.SetSort(sort)
	}
	if params.Fields != "" {
		projection := bson.M{}
		for _, field := range strings.Split(params.Fields, ",") {
			projection[strings.TrimSpace(field)] = 1
		}
		opts.SetProjection(projection)
	}
	skip := int64((params.Page - 1) * params.PageSize)
	opts.SetSkip(skip)
	opts.SetLimit(int64(params.PageSize))
	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close(ctx)
	var results []map[string]interface{}
	for cur.Next(ctx) {
		var doc map[string]interface{}
		if err := cur.Decode(&doc); err != nil {
			return nil, 0, err
		}
		results = append(results, doc)
	}
	var total int64
	if isFiltered {
		total, err = collection.CountDocuments(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
	}
	return results, total, nil
}

func (a *mongoAdapter) BatchCreate(ctx context.Context, tc *tableConfig, records []map[string]interface{}) ([]interface{}, []map[string]interface{}, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	docs := make([]interface{}, len(records))
	for i, rec := range records {
		docs[i] = rec
	}
	res, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, nil, err
	}
	return res.InsertedIDs, records, nil
}

func (a *mongoAdapter) BatchUpdate(ctx context.Context, tc *tableConfig, records []map[string]interface{}) (int64, int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	var matched, modified int64
	for _, record := range records {
		idVal, ok := record[tc.PrimaryKey]
		if !ok {
			return matched, modified, fmt.Errorf("record missing primary key '%s'", tc.PrimaryKey)
		}
		if tc.PrimaryKey == "_id" {
			if str, ok := idVal.(string); ok && len(str) == 24 {
				if oid, err := primitive.ObjectIDFromHex(str); err == nil {
					idVal = oid
				}
			}
		}
		updateData := bson.M{}
		for k, v := range record {
			if k != tc.PrimaryKey {
				updateData[k] = v
			}
		}
		if len(updateData) == 0 {
			continue
		}
		filter := bson.M{tc.PrimaryKey: idVal}
		res, err := collection.UpdateOne(ctx, filter, bson.M{"$set": updateData})
		if err != nil {
			return matched, modified, err
		}
		matched += res.MatchedCount
		modified += res.ModifiedCount
	}
	return matched, modified, nil
}

func (a *mongoAdapter) BatchDelete(ctx context.Context, tc *tableConfig, ids []interface{}) (int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	convertedIds := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		if tc.PrimaryKey == "_id" {
			if str, ok := id.(string); ok && len(str) == 24 {
				if oid, err := primitive.ObjectIDFromHex(str); err == nil {
					id = oid
				}
			}
		}
		convertedIds = append(convertedIds, id)
	}
	filter := bson.M{tc.PrimaryKey: bson.M{"$in": convertedIds}}
	var res *mongo.UpdateResult
	var err error
	if tc.SoftDeleteKey != "" {
		updateData := bson.M{}
		switch tc.SoftDeleteType {
		case softDeleteTypeTimestamp:
			updateData[tc.SoftDeleteKey] = time.Now()
		case softDeleteTypeBoolean:
			updateData[tc.SoftDeleteKey] = true
		case softDeleteTypeInt:
			updateData[tc.SoftDeleteKey] = 1
		default:
			updateData[tc.SoftDeleteKey] = time.Now()
		}
		res, err = collection.UpdateMany(ctx, filter, bson.M{"$set": updateData})
		if err != nil {
			return 0, err
		}
		return res.ModifiedCount, nil
	}
	delRes, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, err
	}
	return delRes.DeletedCount, nil
}

func (a *mongoAdapter) GetOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}, fields string) (map[string]interface{}, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	// mongo主键类型自动转换
	if len(filter) == 1 {
		for k, v := range filter {
			if k == "_id" {
				if str, ok := v.(string); ok && len(str) == 24 {
					if oid, err := primitive.ObjectIDFromHex(str); err == nil {
						filter[k] = oid
					}
				}
			}
		}
	}
	filterBson := bson.M{}
	for k, v := range filter {
		filterBson[k] = v
	}
	filterBson = applyMongoSoftDeleteFilter(filterBson, tc)
	opts := options.FindOne()
	if fields != "" {
		projection := bson.M{}
		for _, field := range strings.Split(fields, ",") {
			projection[strings.TrimSpace(field)] = 1
		}
		opts.SetProjection(projection)
	}
	var result map[string]interface{}
	err := collection.FindOne(ctx, filterBson, opts).Decode(&result)
	return result, err
}

func (a *mongoAdapter) UpdateOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}, data map[string]interface{}) (int64, int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	filterBson := bson.M{}
	for k, v := range filter {
		if k == "_id" {
			if str, ok := v.(string); ok && len(str) == 24 {
				if oid, err := primitive.ObjectIDFromHex(str); err == nil {
					v = oid
				}
			}
		}
		filterBson[k] = v
	}
	filterBson = applyMongoSoftDeleteFilter(filterBson, tc)
	update := bson.M{"$set": data}
	res, err := collection.UpdateOne(ctx, filterBson, update)
	if err != nil {
		return 0, 0, err
	}
	if res.MatchedCount == 0 {
		count, err := collection.CountDocuments(ctx, filterBson)
		if err != nil {
			return 0, 0, err
		}
		if count == 0 {
			return 0, 0, mongo.ErrNoDocuments
		}
	}
	return res.MatchedCount, res.ModifiedCount, nil
}

func (a *mongoAdapter) DeleteOne(ctx context.Context, tc *tableConfig, filter map[string]interface{}) (int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	filterBson := bson.M{}
	for k, v := range filter {
		if k == "_id" {
			if str, ok := v.(string); ok && len(str) == 24 {
				if oid, err := primitive.ObjectIDFromHex(str); err == nil {
					v = oid
				}
			}
		}
		filterBson[k] = v
	}
	var res *mongo.UpdateResult
	var err error
	if tc.SoftDeleteKey != "" {
		updateData := bson.M{}
		switch tc.SoftDeleteType {
		case softDeleteTypeTimestamp:
			updateData[tc.SoftDeleteKey] = time.Now()
		case softDeleteTypeBoolean:
			updateData[tc.SoftDeleteKey] = true
		case softDeleteTypeInt:
			updateData[tc.SoftDeleteKey] = 1
		default:
			updateData[tc.SoftDeleteKey] = time.Now()
		}
		res, err = collection.UpdateOne(ctx, filterBson, bson.M{"$set": updateData})
		if err != nil {
			return 0, err
		}
		if res.MatchedCount == 0 {
			count, err := collection.CountDocuments(ctx, filterBson)
			if err != nil {
				return 0, err
			}
			if count == 0 {
				return 0, mongo.ErrNoDocuments
			}
		}
		return res.ModifiedCount, nil
	}
	delRes, err := collection.DeleteOne(ctx, filterBson)
	if err != nil {
		return 0, err
	}
	if delRes.DeletedCount == 0 {
		count, err := collection.CountDocuments(ctx, filterBson)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			return 0, mongo.ErrNoDocuments
		}
	}
	return delRes.DeletedCount, nil
}

func (a *mongoAdapter) CountAll(ctx context.Context, tc *tableConfig) (int64, error) {
	collection := a.client.Database(a.database).Collection(tc.Name)
	filter := bson.M{}
	filter = applyMongoSoftDeleteFilter(filter, tc)
	return collection.CountDocuments(ctx, filter)
}

func (a *mongoAdapter) Close() error {
	return a.client.Disconnect(context.Background())
}
