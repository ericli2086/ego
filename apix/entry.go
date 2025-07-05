package apix

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

func RegisterRestfulAndGraphql(router *gin.Engine, cfgs string, port int) {
	dbCfgDir := filepath.Join(cfgs, "database")
	tableCfgDir := filepath.Join(cfgs, "table")

	// 解析数据元信息（多库）
	ExtractDbMeta(cfgs, "/api/rest")

	// 注册 REST API（多库）
	RegisterRestAPI(router, "/api/rest", cfgs)

	// 注册 Swagger UI（多库）
	RegisterSwaggerUI(router, "/swagger", cfgs)

	// 注册 Graphql API（多库）
	entries, err := os.ReadDir(tableCfgDir)
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dfName := entry.Name()
			swaggerDir := filepath.Join(tableCfgDir, dfName)

			dbAlias := findAliasByDatabase(dbCfgDir, dfName)
			graphqlPath := fmt.Sprintf("/api/graphql/%s", dbAlias)

			// 注册 Graphql API
			RegisterGraphqlAPI(router, graphqlPath, swaggerDir, fmt.Sprintf("http://localhost:%d", port))

			// 注册 GraphiQL
			RegisterGraphiQL(router, fmt.Sprintf("/graphiql/%s", dbAlias), graphqlPath)
		}
	}
}

// 遍历目录并查找匹配的文件
func findAliasByDatabase(dirPath string, targetDatabase string) string {
	var result string

	// 定义一个结构体，用于映射 YAML 文件中的数据
	type Config struct {
		Database string `yaml:"database"`
		Alias    string `yaml:"alias"`
	}

	// 遍历目录下所有的文件
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 如果遇到错误，返回错误
			return err
		}

		// 如果是 .enable.yaml 文件
		if strings.HasSuffix(info.Name(), ".enable.yaml") {
			// 读取并解析 YAML 文件
			data, err := os.ReadFile(path)
			if err != nil {
				return err // 如果读取文件失败，返回错误
			}

			// 解析 YAML 内容到结构体
			var config Config
			err = yaml.Unmarshal(data, &config)
			if err != nil {
				return err // 如果解析 YAML 失败，返回错误
			}

			// 如果 database 匹配，则返回 alias 字段
			if config.Database == targetDatabase {
				result = config.Alias
				return filepath.SkipDir // 找到匹配后跳过剩下的文件
			}
		}
		return nil
	})

	// 如果遍历中没有遇到错误，返回找到的结果
	if err != nil {
		fmt.Println("遍历目录时出错:", err)
		return ""
	}

	// 返回找到的 alias 或空字符串
	return result
}
