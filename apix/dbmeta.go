package apix

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/glebarez/sqlite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)

// ====== 数据结构 ======
type TableMeta struct {
	Name        string
	Alias       string
	Comment     string
	PrimaryKey  string
	UniqueKeys  [][]string // 每个唯一索引是一组字段
	Fields      []FieldMeta
	SoftDelKey  string
	SoftDelType string
	AutoUpdate  map[string]interface{}
	DefaultVals map[string]interface{}
}
type FieldMeta struct {
	Name       string
	Type       string
	Nullable   bool
	IsPrimary  bool
	IsUnique   bool
	AutoInc    bool
	HasDefault bool
	Default    interface{}
	Comment    string
	OnUpdate   bool
}

// Config 基础结构
type DbBaseCfg struct {
	Type     string `yaml:"type"`
	DSN      string `yaml:"dsn"`
	Database string `yaml:"database"`
	Alias    string `yaml:"alias"`
	Dir      string
}

// ====== 主入口：扫描 database 下的启用库，生成 table 配置和 swagger 文件 ======
func ExtractDbMeta(cfgsDir string, apiPrefix string) error {
	dbCfgDir := filepath.Join(cfgsDir, "database")
	tableCfgDir := filepath.Join(cfgsDir, "table")

	dbCfgs, err := listEnableDbCfgs(dbCfgDir)
	if err != nil {
		return err
	}

	for _, dbcfg := range dbCfgs {
		dbAlias := dbcfg.Alias
		if dbAlias == "" {
			dbAlias = dbcfg.Database
		}
		dbcfg.Alias = dbAlias
		dbTableDir := filepath.Join(tableCfgDir, dbcfg.Database)
		disableTables, err := listDisableTables(dbTableDir)
		if err != nil {
			return err
		}
		tables, err := extractTableMetaWithDefaultAlias(dbcfg.Type, dbcfg.DSN, dbcfg.Database)
		if err != nil {
			log.Printf("extractTableMeta failed for %s: %v", dbcfg.Database, err)
			continue
		}

		// 生成表配置文件
		for i, tbl := range tables {
			// 跳过 disable 的表
			if _, found := disableTables[tbl.Name]; found {
				continue
			}
			tblYaml := fmt.Sprintf("%s.enable.yaml", tbl.Name)
			oldAlias := getAliasFromYAML(filepath.Join(dbTableDir, tblYaml))
			if oldAlias != "" {
				tbl.Alias = oldAlias
				tables[i].Alias = oldAlias
			}
			yamlContent, err := toConfigYamlSingleWithAlias(tbl)
			if err != nil {
				log.Printf("generate yaml for table %s failed: %v", tbl.Name, err)
				continue
			}
			if err := writeConfigYamlToDir(yamlContent, dbTableDir, tbl.Name, "enable"); err != nil {
				log.Printf("write config yaml failed for table %s: %v", tbl.Name, err)
			}
		}

		// 生成 swagger.yaml
		enabledTables := make([]TableMeta, 0, len(tables))
		for _, tbl := range tables {
			if _, found := disableTables[tbl.Name]; !found {
				enabledTables = append(enabledTables, tbl)
			}
		}
		swaggerContent, err := toSwaggerYaml(enabledTables, dbcfg.Alias, apiPrefix)
		if err != nil {
			log.Printf("generate swagger yaml failed for db %s: %v", dbcfg.Database, err)
			continue
		}
		if err := writeSwaggerYamlToDir(swaggerContent, dbTableDir); err != nil {
			log.Printf("write swagger yaml failed for db %s: %v", dbcfg.Database, err)
		}
	}
	return nil
}

// ====== 工具函数：扫描启用的 database 配置文件 ======
func listEnableDbCfgs(dbCfgDir string) ([]DbBaseCfg, error) {
	files, err := os.ReadDir(dbCfgDir)
	if err != nil {
		return nil, fmt.Errorf("read directory failed: %w", err)
	}
	enableRe := regexp.MustCompile(`^(.+)\.enable\.ya?ml$`)
	var results []DbBaseCfg
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		m := enableRe.FindStringSubmatch(file.Name())
		if m == nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dbCfgDir, file.Name()))
		if err != nil {
			log.Printf("read db config file %s failed: %v", file.Name(), err)
			continue
		}
		var cfg DbBaseCfg
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Printf("unmarshal db config file %s failed: %v", file.Name(), err)
			continue
		}
		if cfg.Alias == "" {
			cfg.Alias = cfg.Database
		}
		cfg.Dir = dbCfgDir
		results = append(results, cfg)
	}
	return results, nil
}

// ====== 工具函数：检测 db 下 disable 的表 ======
func listDisableTables(tableDir string) (map[string]struct{}, error) {
	files, err := os.ReadDir(tableDir)
	if err != nil {
		// 不存在直接返回空
		return map[string]struct{}{}, nil
	}
	disableRe := regexp.MustCompile(`^(.+)\.disable\.ya?ml$`)
	result := map[string]struct{}{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		m := disableRe.FindStringSubmatch(file.Name())
		if m == nil {
			continue
		}
		result[m[1]] = struct{}{}
	}
	return result, nil
}

// ====== 生成表配置文件带 alias 字段 ======
func toConfigYamlSingleWithAlias(table TableMeta) (string, error) {
	type tableConf struct {
		Name          string                 `yaml:"name"`
		Alias         string                 `yaml:"alias"`
		PrimaryKey    string                 `yaml:"primary_key,omitempty"`
		UniqueKeys    [][]string             `yaml:"unique_keys,omitempty"`
		DefaultValues map[string]interface{} `yaml:"default_values,omitempty"`
		SoftDelKey    string                 `yaml:"softdel_key,omitempty"`
		SoftDelType   string                 `yaml:"softdel_type,omitempty"`
		AutoUpdate    map[string]interface{} `yaml:"auto_update,omitempty"`
	}
	conf := tableConf{
		Name:          table.Name,
		Alias:         table.Alias,
		PrimaryKey:    table.PrimaryKey,
		UniqueKeys:    dedupUniques(table.UniqueKeys),
		DefaultValues: table.DefaultVals,
		SoftDelKey:    table.SoftDelKey,
		SoftDelType:   table.SoftDelType,
		AutoUpdate:    table.AutoUpdate,
	}
	buf := &bytes.Buffer{}
	yamlEncoder := yaml.NewEncoder(buf)
	yamlEncoder.SetIndent(2)
	if err := yamlEncoder.Encode(conf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ====== 写入文件方法（带 enable/disable）=======
func writeConfigYamlToDir(yamlContent, outputDir, tableAlias, suffix string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create directory failed: %w", err)
	}
	filename := filepath.Join(outputDir, fmt.Sprintf("%s.%s.yaml", tableAlias, suffix))
	if err := os.WriteFile(filename, []byte(yamlContent), 0644); err != nil {
		return fmt.Errorf("write file %s failed: %w", filename, err)
	}
	return nil
}

// ====== swagger.yaml 生成（用 alias） ======
func toSwaggerYaml(tables []TableMeta, dbAlias, apiPrefix string) (string, error) {
	sw := map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":   fmt.Sprintf("%s RESTful API", strings.Title(dbAlias)),
			"version": "1.0.0",
			"description": `本接口对于GET列表请求支持以下查询参数格式，允许在字段名后加上双下划线（__）以使用扩展操作符：

			【支持的查询操作符】：

			- 字段=xxx：等于（默认操作）
			- 字段__ne=xxx：不等于
			- 字段__gt=xxx：大于
			- 字段__gte=xxx：大于等于
			- 字段__lt=xxx：小于
			- 字段__lte=xxx：小于等于
			- 字段__like=xxx%25：模糊匹配（需 URL 编码 % 为 %25）
			- 字段__icontains=xxx：不区分大小写包含（转换为 LOWER LIKE）
			- 字段__in=a,b,c：在指定列表中匹配
			- 字段__isnull=true|false：判断字段是否为 NULL
			- 字段__between=a,b：字段值在 a 与 b 之间（包含边界）

			【分页、排序、字段筛选参数】：

			- page=1，page_size=10：分页参数（从 1 开始）
			- order=字段 或 order=-字段：升序/降序
			- fields=字段1,字段2：只返回指定字段

			【示例】：

			- 精确查询用户：username=alice
			- 区间查询成绩：score__between=60,90
			- 模糊匹配邮箱：email__like=%25gmail.com
			- 排序+分页：order=-id&page=2&page_size=10
			`,
		},
		"paths": map[string]interface{}{},
		"tags":  []map[string]string{},
		"components": map[string]interface{}{
			"schemas": map[string]interface{}{},
		},
	}
	paths := sw["paths"].(map[string]interface{})
	tags := sw["tags"].([]map[string]string)
	schemas := sw["components"].(map[string]interface{})["schemas"].(map[string]interface{})

	for _, t := range tables {
		props, required := toSwaggerSchemaFields(t.Fields)
		schemas[t.Alias] = map[string]interface{}{
			"type":       "object",
			"properties": props,
			"required":   required,
		}
		// 生成batch_update模型时主键必填
		batchProps := map[string]interface{}{}
		for k, v := range props {
			if k == t.PrimaryKey {
				prop := make(map[string]interface{})
				for pk, pv := range v.(map[string]interface{}) {
					if pk != "readOnly" {
						prop[pk] = pv
					}
				}
				batchProps[k] = prop
			} else {
				batchProps[k] = v
			}
		}
		batchRequired := append([]string{}, required...)
		hasPk := false
		for _, r := range batchRequired {
			if r == t.PrimaryKey && r != "" {
				hasPk = true
				break
			}
		}
		if t.PrimaryKey != "" && !hasPk {
			batchRequired = append(batchRequired, t.PrimaryKey)
		}
		schemas[t.Alias+"_batch_update"] = map[string]interface{}{
			"type":       "object",
			"properties": batchProps,
			"required":   batchRequired,
		}

		tags = append(tags, map[string]string{"name": t.Alias, "description": sanitizeSwaggerText(t.Comment)})

		basePath := fmt.Sprintf("%s/%s/%s", apiPrefix, dbAlias, t.Alias)
		idPath := fmt.Sprintf("%s/{id}", basePath)
		batchDeletePath := fmt.Sprintf("%s/batch_delete", basePath)

		getParams := makeSwaggerQueryParameters()
		idParam := map[string]interface{}{
			"name":        "id",
			"in":          "path",
			"required":    true,
			"description": "主键ID",
			"schema":      map[string]string{"type": "string"},
		}
		fieldsParam := map[string]interface{}{
			"name":        "fields",
			"in":          "query",
			"schema":      map[string]string{"type": "string"},
			"description": "返回字段，逗号分隔",
		}
		keyParam := map[string]interface{}{
			"name":        "key",
			"in":          "query",
			"schema":      map[string]string{"type": "string"},
			"description": "字段名称，即最末尾路径参数对应字段名称",
		}
		paths[basePath] = map[string]interface{}{
			"get": map[string]interface{}{
				"tags":        []string{t.Alias},
				"summary":     fmt.Sprintf("List %s records", t.Alias),
				"description": "支持等值、模糊、区间、in等各种字段过滤。字段类型和参数请参考开头说明部分。",
				"parameters":  getParams,
				"responses": map[string]interface{}{
					"200": map[string]interface{}{
						"description": "OK",
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"schema": map[string]interface{}{
									"type": "object",
									"properties": map[string]interface{}{
										"total": map[string]interface{}{"type": "integer"},
										"data": map[string]interface{}{
											"type":  "array",
											"items": map[string]interface{}{"$ref": "#/components/schemas/" + t.Alias},
										},
									},
								},
							},
						},
					},
				},
			},
			"post": map[string]interface{}{
				"tags":    []string{t.Alias},
				"summary": fmt.Sprintf("Batch create %s", t.Alias),
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{
							"schema": map[string]interface{}{
								"type":  "array",
								"items": map[string]interface{}{"$ref": "#/components/schemas/" + t.Alias},
							},
						},
					},
				},
				"responses": map[string]interface{}{
					"201": map[string]interface{}{"description": "Created"},
				},
			},
			"put": map[string]interface{}{
				"tags":    []string{t.Alias},
				"summary": fmt.Sprintf("Batch update %s", t.Alias),
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{
							"schema": map[string]interface{}{
								"type":  "array",
								"items": map[string]interface{}{"$ref": "#/components/schemas/" + t.Alias + "_batch_update"},
							},
						},
					},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "Updated"},
				},
			},
		}
		paths[batchDeletePath] = map[string]interface{}{
			"post": map[string]interface{}{
				"tags":    []string{t.Alias},
				"summary": fmt.Sprintf("Batch delete %s", t.Alias),
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{
							"schema": map[string]interface{}{
								"type":  "array",
								"items": map[string]interface{}{"type": "string"},
							},
						},
					},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "Deleted"},
				},
			},
		}
		paths[idPath] = map[string]interface{}{
			"get": map[string]interface{}{
				"tags":       []string{t.Alias},
				"summary":    fmt.Sprintf("Get %s by id", t.Alias),
				"parameters": []interface{}{idParam, fieldsParam, keyParam},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{
						"description": "OK",
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"schema": map[string]interface{}{
									"$ref": "#/components/schemas/" + t.Alias,
								},
							},
						},
					},
				},
			},
			"put": map[string]interface{}{
				"tags":       []string{t.Alias},
				"summary":    fmt.Sprintf("Update %s by id", t.Alias),
				"parameters": []interface{}{idParam},
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{
							"schema": map[string]interface{}{
								"$ref": "#/components/schemas/" + t.Alias,
							},
						},
					},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "Updated"},
				},
			},
			"delete": map[string]interface{}{
				"tags":       []string{t.Alias},
				"summary":    fmt.Sprintf("Delete %s by id", t.Alias),
				"parameters": []interface{}{idParam},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "Deleted"},
				},
			},
		}
	}
	sw["tags"] = tags
	buf := &bytes.Buffer{}
	yamlEncoder := yaml.NewEncoder(buf)
	yamlEncoder.SetIndent(2)
	if err := yamlEncoder.Encode(sw); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ====== 写入 swagger.yaml 到表目录 ======
func writeSwaggerYamlToDir(yamlContent, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create directory failed: %w", err)
	}
	filename := filepath.Join(outputDir, "swagger.yaml")
	if err := os.WriteFile(filename, []byte(yamlContent), 0644); err != nil {
		return fmt.Errorf("write file %s failed: %w", filename, err)
	}
	return nil
}

// ========== 其余数据库元数据适配器和通用工具 ==========

// extractTableMetaWithDefaultAlias 会给 TableMeta.Alias 默认赋值为表名
func extractTableMetaWithDefaultAlias(dbType, dsn, dbName string) ([]TableMeta, error) {
	tables, err := extractTableMeta(dbType, dsn, dbName)
	if err != nil {
		return nil, err
	}
	for i := range tables {
		tables[i].Alias = tables[i].Name
	}
	return tables, nil
}

// 封装一个方法来获取 alias，失败时返回空字符串
func getAliasFromYAML(filePath string) string {
	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return ""
	}

	// 读取 YAML 文件内容
	data, err := os.ReadFile(filePath)
	if err != nil {
		return ""
	}

	// 解析 YAML 内容到结构体
	var tbl TableMeta
	err = yaml.Unmarshal(data, &tbl)
	if err != nil {
		return ""
	}

	return tbl.Alias
}

// ====== 类型推断工具 ======
func isStringType(typ string) bool {
	t := strings.ToLower(typ)
	return strings.Contains(t, "char") || strings.Contains(t, "text") || strings.Contains(t, "json") || strings.Contains(t, "enum") || strings.Contains(t, "varchar")
}

func isIntType(typ string) bool {
	t := strings.ToLower(typ)
	return strings.Contains(t, "int") || strings.Contains(t, "bigint") || strings.Contains(t, "tinyint") || strings.Contains(t, "smallint")
}

func isFloatType(typ string) bool {
	t := strings.ToLower(typ)
	return strings.Contains(t, "float") || strings.Contains(t, "double") || strings.Contains(t, "decimal") || strings.Contains(t, "numeric") || strings.Contains(t, "real")
}

func isTimeType(typ string) bool {
	t := strings.ToLower(typ)
	return strings.Contains(t, "time") || strings.Contains(t, "date") || strings.Contains(t, "timestamp")
}

func isSnowflakeBigIntType(typ string) bool {
	t := strings.ToLower(typ)
	switch t {
	case "bigint", "bigint unsigned", "int8", "int64":
		return true
	default:
		return false
	}
}

func toSwaggerType(dbType string) string {
	l := strings.ToLower(dbType)
	switch {
	case strings.Contains(l, "int"):
		return "integer"
	case strings.Contains(l, "float") || strings.Contains(l, "double") || strings.Contains(l, "decimal") || strings.Contains(l, "numeric") || strings.Contains(l, "real"):
		return "number"
	case strings.Contains(l, "bool"):
		return "boolean"
	case strings.Contains(l, "date") || strings.Contains(l, "time"):
		return "string"
	case strings.Contains(l, "char") || strings.Contains(l, "text") || strings.Contains(l, "json") || strings.Contains(l, "enum"):
		return "string"
	default:
		return "string"
	}
}

func sanitizeSwaggerText(s string) string {
	// 保证没有swagger特殊符号，防注入
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\"", "'")
}

// ====== 字段特殊意义判断 ======
func isSoftDelField(name string) bool {
	n := strings.ToLower(name)
	softDelNames := []string{
		"is_delete", "is_deleted", "is_remove", "is_removed", "is_obsolete", "is_obsoleted",
		"delete_at", "deleted_at", "delete_time", "deleted_time",
		"remove_at", "removed_at", "remove_time", "removed_time",
		"obsolete_at", "obsoleted_at", "obsolete_time", "obsoleted_time",
		"delete_flag", "deleted_flag", "remove_flag", "removed_flag", "obsolete_flag", "obsoleted_flag", "gmt_delete", "gmt_deleted",
	}
	for _, cand := range softDelNames {
		if n == cand {
			return true
		}
	}
	return false
}

func isAutoUpdateField(name string) bool {
	n := strings.ToLower(name)
	autoUpdateNames := []string{
		"update_at", "updated_at", "update_time", "updated_time",
		"modify_at", "modified_at", "modify_time", "modified_time",
		"last_update", "last_updated", "last_modify", "last_modified",
		"login_time", "last_login", "checked_at", "gmt_update", "gmt_updated",
	}
	for _, cand := range autoUpdateNames {
		if n == cand {
			return true
		}
	}
	return false
}

func isCommonReadOnlyField(name string) bool {
	n := strings.ToLower(name)
	roFields := []string{
		"id", "_id",
	}
	for _, cand := range roFields {
		if n == cand {
			return true
		}
	}
	return false
}

func isResponseReadOnlyField(name string) bool {
	n := strings.ToLower(name)
	respRO := []string{
		"joined_time", "joined_at", "join_time", "join_at",
		"created_time", "created_at", "create_time", "create_at",
		"updated_time", "updated_at", "update_time", "update_at",
		"login_time", "login_at", "last_login", "gmt_create", "gmt_created",
	}
	for _, v := range respRO {
		if n == v {
			return true
		}
	}
	return false
}

func guessSoftDelType(typ string) string {
	ltyp := strings.ToLower(typ)
	if strings.Contains(ltyp, "int") {
		return "int"
	}
	if strings.Contains(ltyp, "bool") {
		return "boolean"
	}
	if strings.Contains(ltyp, "time") || strings.Contains(ltyp, "date") {
		return "timestamp"
	}
	return "int"
}

func isNowDefault(val string) bool {
	val = strings.ToLower(strings.TrimSpace(val))
	val = strings.Trim(val, "()'\"")
	return val == "current_timestamp" || val == "now" || val == "now()" || val == "getdate" || val == "getdate()" || val == "sysdate"
}

// ====== 默认值类型推断（nullable字段不处理默认值）=======
func convertDefaultByType(raw, typ string, nullable bool) interface{} {
	if nullable {
		// nullable字段默认值通常数据库用NULL，swagger不强制给默认值
		return nil
	}
	if raw != "" {
		clean := strings.TrimSpace(raw)
		clean = strings.Trim(clean, "'\"()")
		if strings.ToLower(clean) == "null" {
			return nil
		}
		if isNowDefault(clean) && isTimeType(typ) {
			return "{{now}}"
		}
		if isIntType(typ) {
			if v, err := parseInt(clean); err == nil {
				return v
			}
		}
		if isFloatType(typ) {
			if v, err := parseFloat(clean); err == nil {
				return v
			}
		}
		return clean
	}
	if isTimeType(typ) {
		return "{{now}}"
	}
	if isStringType(typ) {
		return ""
	}
	if isIntType(typ) {
		return 0
	}
	if isFloatType(typ) {
		return 0.0
	}
	return nil
}

func parseInt(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}

func parseFloat(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// ====== 高级查询参数生成 ======
func makeSwaggerQueryParameters() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "fields", "in": "query", "schema": map[string]string{"type": "string"}, "description": "返回字段，逗号分隔"},
		{"name": "order", "in": "query", "schema": map[string]string{"type": "string"}, "description": "排序，格式如 id desc"},
		{"name": "page", "in": "query", "schema": map[string]string{"type": "integer"}, "description": "页码"},
		{"name": "page_size", "in": "query", "schema": map[string]string{"type": "integer"}, "description": "每页条数"},
	}
}

// ====== 字段属性生成（必填字段/默认值字段规则）=======
func toSwaggerSchemaFields(fields []FieldMeta) (map[string]interface{}, []string) {
	props := map[string]interface{}{}
	var required []string
	for _, f := range fields {
		fieldType := toSwaggerType(f.Type)
		prop := map[string]interface{}{
			"type": fieldType,
		}
		if f.Comment != "" {
			prop["description"] = sanitizeSwaggerText(f.Comment)
		}
		readOnly := f.AutoInc || f.OnUpdate || isAutoUpdateField(f.Name) || isSoftDelField(f.Name) || isResponseReadOnlyField(f.Name) || isCommonReadOnlyField(f.Name)
		if readOnly {
			prop["readOnly"] = true
		}
		props[f.Name] = prop

		if !f.Nullable && !f.HasDefault && !f.AutoInc && !f.OnUpdate &&
			!isAutoUpdateField(f.Name) && !isSoftDelField(f.Name) && !isResponseReadOnlyField(f.Name) && !isCommonReadOnlyField(f.Name) {
			required = append(required, f.Name)
		}
	}
	return props, required
}

// ====== default_values 生成（更严格规则）=======
func collectDefaultValueFields(fields []FieldMeta, primaryKey string) map[string]interface{} {
	defs := map[string]interface{}{}
	var pkField *FieldMeta
	for i, f := range fields {
		if f.Name == primaryKey {
			pkField = &fields[i]
		}
		if f.HasDefault {
			val := f.Default
			if val != nil {
				defs[f.Name] = val
			}
			continue
		}
		if f.OnUpdate || isAutoUpdateField(f.Name) {
			defs[f.Name] = "{{now}}"
			continue
		}
		if isSoftDelField(f.Name) {
			if isIntType(f.Type) || isFloatType(f.Type) {
				defs[f.Name] = 0
			} else if isStringType(f.Type) {
				defs[f.Name] = ""
			} else {
				defs[f.Name] = 0
			}
			continue
		}
		if isResponseReadOnlyField(f.Name) {
			if isTimeType(f.Type) {
				defs[f.Name] = "{{now}}"
			}
			continue
		}
	}

	// 主键特殊处理
	if pkField != nil {
		if !pkField.HasDefault && !pkField.AutoInc && isSnowflakeBigIntType(pkField.Type) {
			defs[primaryKey] = "{{snowflake}}"
		} else {
			delete(defs, primaryKey)
		}
	}
	return defs
}

// ====== 唯一索引去重 ======
func dedupUniques(keys [][]string) [][]string {
	seen := make(map[string]struct{})
	var res [][]string
	for _, arr := range keys {
		k := strings.Join(arr, ",")
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		res = append(res, arr)
	}
	return res
}

// ====== 数据库元数据提取适配器 ======
func extractTableMeta(dbType, dsn, dbName string) ([]TableMeta, error) {
	switch strings.ToLower(dbType) {
	case "mysql":
		return extractMySQLMeta(dsn, dbName)
	case "postgres", "postgresql":
		return extractPostgreSQLMeta(dsn, dbName)
	case "sqlite":
		return extractSQLiteMeta(dsn, dbName)
	case "sqlserver":
		return extractSQLServerMeta(dsn, dbName)
	case "clickhouse":
		return extractClickHouseMeta(dsn, dbName)
	case "mongodb":
		return extractMongoDBMeta(dsn, dbName)
	default:
		return nil, fmt.Errorf("unsupported dbtype: %s", dbType)
	}
}

// ---- MySQL ----
func extractMySQLMeta(dsn, dbName string) ([]TableMeta, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql database %s failed: %w", dbName, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT TABLE_NAME, IFNULL(TABLE_COMMENT,'') FROM information_schema.tables WHERE TABLE_SCHEMA=? LIMIT 500`, dbName)
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, TableMeta{Name: name, Comment: comment})
	}
	rows.Close()
	for i := range tables {
		fieldsRows, err := db.Query(`
			SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_DEFAULT, IFNULL(COLUMN_COMMENT,''), EXTRA
			FROM information_schema.columns
			WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
			ORDER BY ORDINAL_POSITION
		`, dbName, tables[i].Name)
		if err != nil {
			return nil, err
		}
		var fields []FieldMeta
		for fieldsRows.Next() {
			var f FieldMeta
			var nullable, colKey, extra, defaultVal sql.NullString
			if err := fieldsRows.Scan(&f.Name, &f.Type, &nullable, &colKey, &extra, &defaultVal, &f.Comment, &extra); err != nil {
				fieldsRows.Close()
				return nil, err
			}
			f.Nullable = nullable.String == "YES"
			f.IsPrimary = colKey.String == "PRI"
			f.IsUnique = colKey.String == "UNI"
			f.AutoInc = strings.Contains(extra.String, "auto_increment")
			f.HasDefault = defaultVal.Valid
			if defaultVal.Valid {
				f.Default = convertDefaultByType(defaultVal.String, f.Type, f.Nullable)
			} else {
				f.Default = convertDefaultByType("", f.Type, f.Nullable)
			}
			f.OnUpdate = strings.Contains(extra.String, "on update")
			fields = append(fields, f)
		}
		fieldsRows.Close()
		tables[i].Fields = fields
		for _, f := range fields {
			if f.IsPrimary {
				tables[i].PrimaryKey = f.Name
				break
			}
		}
		// 提取唯一索引（支持联合唯一）
		idxRows, err := db.Query(`
			SELECT INDEX_NAME, COLUMN_NAME
			FROM information_schema.statistics
			WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 AND INDEX_NAME != 'PRIMARY'
			ORDER BY INDEX_NAME, SEQ_IN_INDEX
		`, dbName, tables[i].Name)
		if err == nil {
			idxMap := map[string][]string{}
			for idxRows.Next() {
				var idxName, colName string
				if err := idxRows.Scan(&idxName, &colName); err == nil {
					idxMap[idxName] = append(idxMap[idxName], colName)
				}
			}
			idxRows.Close()
			var uniques [][]string
			for _, cols := range idxMap {
				uniques = append(uniques, cols)
			}
			tables[i].UniqueKeys = dedupUniques(uniques)
		}
		tables[i].DefaultVals = collectDefaultValueFields(fields, tables[i].PrimaryKey)
		for _, f := range fields {
			if isSoftDelField(f.Name) {
				tables[i].SoftDelKey = f.Name
				tables[i].SoftDelType = guessSoftDelType(f.Type)
				break
			}
		}
		autoUpdate := map[string]interface{}{}
		for _, f := range fields {
			if (isAutoUpdateField(f.Name) || f.OnUpdate) && isTimeType(f.Type) {
				autoUpdate[f.Name] = "{{now}}"
			}
		}
		if len(autoUpdate) > 0 {
			tables[i].AutoUpdate = autoUpdate
		}
	}
	return tables, nil
}

// ---- PostgreSQL ----
func extractPostgreSQLMeta(dsn, dbName string) ([]TableMeta, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgresql database %s failed: %w", dbName, err)
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT table_name, COALESCE(obj_description(('"'||table_schema||'"."'||table_name||'"')::regclass),'')
		FROM information_schema.tables
		WHERE table_schema='public' AND table_type='BASE TABLE'
		LIMIT 500
	`)
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, TableMeta{Name: name, Comment: comment})
	}
	rows.Close()
	for i := range tables {
		colsRows, err := db.Query(`
			SELECT column_name, data_type, is_nullable, column_default, col_description(('"'||table_schema||'"."'||table_name||'"')::regclass, ordinal_position)
			FROM information_schema.columns
			WHERE table_schema='public' AND table_name=$1
			ORDER BY ordinal_position
		`, tables[i].Name)
		if err != nil {
			return nil, err
		}
		var fields []FieldMeta
		for colsRows.Next() {
			var f FieldMeta
			var nullable, defaultVal, comment sql.NullString
			if err := colsRows.Scan(&f.Name, &f.Type, &nullable, &defaultVal, &comment); err != nil {
				colsRows.Close()
				return nil, err
			}
			f.Nullable = nullable.String == "YES"
			f.HasDefault = defaultVal.Valid
			if defaultVal.Valid {
				f.Default = convertDefaultByType(defaultVal.String, f.Type, f.Nullable)
			} else {
				f.Default = convertDefaultByType("", f.Type, f.Nullable)
			}
			if comment.Valid {
				f.Comment = comment.String
			}
			fields = append(fields, f)
		}
		colsRows.Close()
		pkRow := db.QueryRow(`
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			WHERE tc.table_schema = 'public' AND tc.table_name = $1 AND tc.constraint_type='PRIMARY KEY'
			LIMIT 1
		`, tables[i].Name)
		var pk string
		_ = pkRow.Scan(&pk)
		for j := range fields {
			if fields[j].Name == pk {
				fields[j].IsPrimary = true
				tables[i].PrimaryKey = pk
			}
		}
		// 唯一索引（支持联合唯一）
		uniqRows, err := db.Query(`
			SELECT tc.constraint_name, kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			WHERE tc.table_schema = 'public' AND tc.table_name = $1 AND tc.constraint_type='UNIQUE'
			ORDER BY tc.constraint_name, kcu.ordinal_position
		`, tables[i].Name)
		if err == nil {
			idxMap := map[string][]string{}
			for uniqRows.Next() {
				var idxName, colName string
				if err := uniqRows.Scan(&idxName, &colName); err == nil {
					idxMap[idxName] = append(idxMap[idxName], colName)
				}
			}
			uniqRows.Close()
			var uniques [][]string
			for _, cols := range idxMap {
				uniques = append(uniques, cols)
			}
			tables[i].UniqueKeys = dedupUniques(uniques)
		}
		tables[i].Fields = fields
		tables[i].DefaultVals = collectDefaultValueFields(fields, tables[i].PrimaryKey)
		for _, f := range fields {
			if isSoftDelField(f.Name) {
				tables[i].SoftDelKey = f.Name
				tables[i].SoftDelType = guessSoftDelType(f.Type)
				break
			}
		}
		autoUpdate := map[string]interface{}{}
		for _, f := range fields {
			if isAutoUpdateField(f.Name) && isTimeType(f.Type) {
				autoUpdate[f.Name] = "{{now}}"
			}
		}
		if len(autoUpdate) > 0 {
			tables[i].AutoUpdate = autoUpdate
		}
	}
	return tables, nil
}

// ---- SQLite ----
func extractSQLiteMeta(dsn, dbName string) ([]TableMeta, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database %s failed: %w", dbName, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' LIMIT 500`)
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		if strings.HasPrefix(name, "sqlite_") {
			continue // 跳过系统表
		}
		tables = append(tables, TableMeta{Name: name})
	}
	rows.Close()
	for i := range tables {
		stmt := fmt.Sprintf(`PRAGMA table_info('%s')`, tables[i].Name)
		colsRows, err := db.Query(stmt)
		if err != nil {
			return nil, fmt.Errorf("exec %s: %w", stmt, err)
		}
		var fields []FieldMeta
		for colsRows.Next() {
			var cid int
			var f FieldMeta
			var notnull, pk int
			var dflt_value sql.NullString
			if err := colsRows.Scan(&cid, &f.Name, &f.Type, &notnull, &dflt_value, &pk); err != nil {
				colsRows.Close()
				return nil, err
			}
			f.Nullable = notnull == 0
			f.IsPrimary = pk > 0
			f.HasDefault = dflt_value.Valid
			if dflt_value.Valid {
				f.Default = convertDefaultByType(dflt_value.String, f.Type, f.Nullable)
			} else {
				f.Default = convertDefaultByType("", f.Type, f.Nullable)
			}
			fields = append(fields, f)
		}
		colsRows.Close()
		// 唯一索引（支持联合唯一）
		var uniques [][]string
		idxRows, err := db.Query(fmt.Sprintf(`PRAGMA index_list('%s')`, tables[i].Name))
		if err == nil {
			for idxRows.Next() {
				var idxSeq int
				var idxName string
				var idxUnique int
				var origin, partial interface{}
				if err := idxRows.Scan(&idxSeq, &idxName, &idxUnique, &origin, &partial); err == nil && idxUnique == 1 {
					colRows, err := db.Query(fmt.Sprintf(`PRAGMA index_info('%s')`, idxName))
					if err == nil {
						var cols []string
						for colRows.Next() {
							var seqno, cid int
							var colName string
							if err := colRows.Scan(&seqno, &cid, &colName); err == nil {
								cols = append(cols, colName)
							}
						}
						colRows.Close()
						if len(cols) > 0 {
							uniques = append(uniques, cols)
						}
					}
				}
			}
			idxRows.Close()
		}
		var pk string
		for _, f := range fields {
			if f.IsPrimary {
				pk = f.Name
				break
			}
		}
		tables[i].PrimaryKey = pk
		tables[i].Fields = fields
		tables[i].UniqueKeys = dedupUniques(uniques)
		tables[i].DefaultVals = collectDefaultValueFields(fields, tables[i].PrimaryKey)
		for _, f := range fields {
			if isSoftDelField(f.Name) {
				tables[i].SoftDelKey = f.Name
				tables[i].SoftDelType = guessSoftDelType(f.Type)
			}
		}
		autoUpdate := map[string]interface{}{}
		for _, f := range fields {
			if isAutoUpdateField(f.Name) && isTimeType(f.Type) {
				autoUpdate[f.Name] = "{{now}}"
			}
		}
		if len(autoUpdate) > 0 {
			tables[i].AutoUpdate = autoUpdate
		}
	}
	return tables, nil
}

// ---- SQLServer ----
func extractSQLServerMeta(dsn, dbName string) ([]TableMeta, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlserver database %s failed: %w", dbName, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sys.tables`)
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, TableMeta{Name: name})
	}
	rows.Close()
	for i := range tables {
		stmt := fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'`, tables[i].Name)
		colsRows, err := db.Query(stmt)
		if err != nil {
			return nil, fmt.Errorf("exec %s: %w", stmt, err)
		}
		var fields []FieldMeta
		for colsRows.Next() {
			var f FieldMeta
			var nullable string
			var defaultVal sql.NullString
			if err := colsRows.Scan(&f.Name, &f.Type, &nullable, &defaultVal); err != nil {
				colsRows.Close()
				return nil, err
			}
			f.Nullable = nullable == "YES"
			f.HasDefault = defaultVal.Valid
			if defaultVal.Valid {
				f.Default = convertDefaultByType(defaultVal.String, f.Type, f.Nullable)
			} else {
				f.Default = convertDefaultByType("", f.Type, f.Nullable)
			}
			fields = append(fields, f)
		}
		colsRows.Close()
		// 唯一索引（支持联合唯一）
		idxRows, err := db.Query(fmt.Sprintf(`
			SELECT i.name, c.name
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			WHERE i.object_id = OBJECT_ID('%s') AND i.is_unique = 1 AND i.is_primary_key = 0
			ORDER BY i.name, ic.key_ordinal
		`, tables[i].Name))
		if err == nil {
			idxMap := map[string][]string{}
			for idxRows.Next() {
				var idxName, colName string
				if err := idxRows.Scan(&idxName, &colName); err == nil {
					idxMap[idxName] = append(idxMap[idxName], colName)
				}
			}
			idxRows.Close()
			var uniques [][]string
			for _, cols := range idxMap {
				uniques = append(uniques, cols)
			}
			tables[i].UniqueKeys = dedupUniques(uniques)
		}
		tables[i].Fields = fields
		tables[i].DefaultVals = collectDefaultValueFields(fields, tables[i].PrimaryKey)
	}
	return tables, nil
}

// ---- ClickHouse ----
func extractClickHouseMeta(dsn, dbName string) ([]TableMeta, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse database %s failed: %w", dbName, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM system.tables WHERE database=? LIMIT 500`, dbName)
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, TableMeta{Name: name})
	}
	rows.Close()
	for i := range tables {
		stmt := fmt.Sprintf("DESCRIBE TABLE `%s`.`%s`", dbName, tables[i].Name)
		colsRows, err := db.Query(stmt)
		if err != nil {
			return nil, fmt.Errorf("exec %s: %w", stmt, err)
		}
		var fields []FieldMeta
		for colsRows.Next() {
			var f FieldMeta
			var def, comment sql.NullString
			var dummy1, dummy2 interface{}
			if err := colsRows.Scan(&f.Name, &f.Type, &def, &dummy1, &comment, &dummy2); err != nil {
				colsRows.Close()
				return nil, err
			}
			if def.Valid {
				f.Default = convertDefaultByType(def.String, f.Type, f.Nullable)
			} else {
				f.Default = convertDefaultByType("", f.Type, f.Nullable)
			}
			fields = append(fields, f)
		}
		colsRows.Close()
		// ClickHouse 无唯一索引，略
		tables[i].Fields = fields
		tables[i].DefaultVals = collectDefaultValueFields(fields, tables[i].PrimaryKey)
	}
	return tables, nil
}

// ---- MongoDB ----
func extractMongoDBMeta(dsn, dbName string) ([]TableMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		return nil, fmt.Errorf("open mongo database %s failed: %w", dbName, err)
	}
	defer client.Disconnect(ctx)

	names, err := client.Database(dbName).ListCollectionNames(ctx, struct{}{})
	if err != nil {
		return nil, err
	}
	var tables []TableMeta
	for _, col := range names {
		coll := client.Database(dbName).Collection(col)
		cur, err := coll.Find(ctx, bson.M{}, options.Find().SetLimit(100)) // 采样100条，提升类型推断准确性
		if err != nil {
			return nil, err
		}

		fieldTypes := map[string]string{}
		for cur.Next(ctx) {
			var doc bson.M
			if err := cur.Decode(&doc); err != nil {
				continue
			}
			for k, v := range doc {
				t := mongoTypeToSwaggerType(v)
				if old, ok := fieldTypes[k]; ok {
					fieldTypes[k] = mergeSwaggerType(old, t)
				} else {
					fieldTypes[k] = t
				}
			}
		}
		cur.Close(ctx)

		var fields []FieldMeta
		primaryKey := ""
		softDelKey := ""
		softDelType := ""
		autoUpdate := map[string]interface{}{}
		defaultVals := map[string]interface{}{}

		uniqueKeysMap := map[string][]string{}
		indexView := coll.Indexes()
		cursor, err := indexView.List(ctx)
		if err == nil {
			for cursor.Next(ctx) {
				var idx bson.M
				if err := cursor.Decode(&idx); err != nil {
					continue
				}
				if unique, ok := idx["unique"].(bool); ok && unique {
					if keys, ok := idx["key"].(bson.M); ok {
						var cols []string
						for key := range keys {
							if key != "_id" {
								cols = append(cols, key)
							}
						}
						if len(cols) > 0 {
							idxName := fmt.Sprintf("%v", idx["name"])
							uniqueKeysMap[idxName] = cols
						}
					}
				}
			}
			cursor.Close(ctx)
		}
		var uniqueKeys [][]string
		for _, cols := range uniqueKeysMap {
			uniqueKeys = append(uniqueKeys, cols)
		}

		for name, typ := range fieldTypes {
			isPrimary := (name == "_id")
			if isPrimary {
				primaryKey = name
			}
			if isSoftDelField(name) {
				softDelKey = name
				if isTimeType(typ) || isTimeType(name) {
					softDelType = "timestamp"
				} else {
					softDelType = "string"
				}
			}

			if isAutoUpdateField(name) && (isTimeType(typ) || isTimeType(name)) {
				autoUpdate[name] = "{{now}}"
			}
			if isPrimary {
				fields = append(fields, FieldMeta{
					Name:      name,
					Type:      typ,
					IsPrimary: isPrimary,
				})
				continue
			}

			if isResponseReadOnlyField(name) && (isTimeType(typ) || isTimeType(name)) {
				defaultVals[name] = "{{now}}"
			} else if isSoftDelField(name) {
				defaultVals[name] = ""
			}
			fields = append(fields, FieldMeta{
				Name:      name,
				Type:      typ,
				IsPrimary: isPrimary,
				IsUnique:  false,
			})
		}

		tables = append(tables, TableMeta{
			Name:        col,
			Fields:      fields,
			PrimaryKey:  primaryKey,
			UniqueKeys:  dedupUniques(uniqueKeys),
			SoftDelKey:  softDelKey,
			SoftDelType: softDelType,
			AutoUpdate:  autoUpdate,
			DefaultVals: defaultVals,
		})
	}
	return tables, nil
}

func mongoTypeToSwaggerType(v interface{}) string {
	switch v.(type) {
	case int32, int64, int:
		return "integer"
	case float32, float64:
		return "number"
	case bool:
		return "boolean"
	case string:
		return "string"
	case time.Time:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}, bson.M:
		return "object"
	default:
		return "string"
	}
}

func mergeSwaggerType(a, b string) string {
	if a == b {
		return a
	}
	if (a == "integer" && b == "number") || (a == "number" && b == "integer") {
		return "number"
	}
	if a == "string" || b == "string" {
		return "string"
	}
	if a == "object" || b == "object" {
		return "object"
	}
	if a == "array" || b == "array" {
		return "array"
	}
	return "string"
}
