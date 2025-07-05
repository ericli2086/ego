package apix

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

const swaggerHTMLTpl = `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>SwaggerUI</title>
    <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj4KDTwhLS0gVXBsb2FkZWQgdG86IFNWRyBSZXBvLCB3d3cuc3ZncmVwby5jb20sIFRyYW5zZm9ybWVkIGJ5OiBTVkcgUmVwbyBNaXhlciBUb29scyAtLT4KPHN2ZyBmaWxsPSIjMDAwMDAwIiB3aWR0aD0iODAwcHgiIGhlaWdodD0iODAwcHgiIHZpZXdCb3g9IjAgMCAzMi4wMCAzMi4wMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KDTxnIGlkPSJTVkdSZXBvX2JnQ2FycmllciIgc3Ryb2tlLXdpZHRoPSIwIj4KDTxyZWN0IHg9IjAiIHk9IjAiIHdpZHRoPSIzMi4wMCIgaGVpZ2h0PSIzMi4wMCIgcng9IjE2IiBmaWxsPSIjN2VlYzhiIiBzdHJva2V3aWR0aD0iMCIvPgoNPC9nPgoNPGcgaWQ9IlNWR1JlcG9fdHJhY2VyQ2FycmllciIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIi8+Cg08ZyBpZD0iU1ZHUmVwb19pY29uQ2FycmllciI+IDxwYXRoIGQ9Ik0xNiAwYy04LjgyMyAwLTE2IDcuMTc3LTE2IDE2czcuMTc3IDE2IDE2IDE2YzguODIzIDAgMTYtNy4xNzcgMTYtMTZzLTcuMTc3LTE2LTE2LTE2ek0xNiAxLjUyN2M3Ljk5NSAwIDE0LjQ3MyA2LjQ3OSAxNC40NzMgMTQuNDczcy02LjQ3OSAxNC40NzMtMTQuNDczIDE0LjQ3M2MtNy45OTUgMC0xNC40NzMtNi40NzktMTQuNDczLTE0LjQ3M3M2LjQ3OS0xNC40NzMgMTQuNDczLTE0LjQ3M3pNMTEuMTYxIDcuODIzYy0wLjE4OC0wLjAwNS0wLjM3NSAwLTAuNTY4IDAuMDA1LTEuMzA3IDAuMDc5LTIuMDkzIDAuNjkzLTIuMzEyIDEuOTY0LTAuMTUxIDAuODkxLTAuMTI1IDEuNzk2LTAuMTg4IDIuNjkyLTAuMDIwIDAuNDY0LTAuMDY3IDAuOTI4LTAuMTU2IDEuMzgtMC4xNzcgMC44MTMtMC41MjUgMS4wNjgtMS4zNTMgMS4xMDktMC4xMTEgMC4wMTEtMC4yMiAwLjAzMi0wLjMyNCAwLjA1N3YxLjk0OGMxLjUgMC4wNzMgMS43MDQgMC42MDUgMS44MjMgMi4xNzIgMC4wNDggMC41NzMtMC4wMTUgMS4xNDcgMC4wMjEgMS43MTkgMC4wMjcgMC41NDMgMC4wOTkgMS4wNzkgMC4yMDggMS42IDAuMzQ0IDEuNDMyIDEuNzQ1IDEuOTExIDMuNDMzIDEuNjI0di0xLjcxM2MtMC4yNzIgMC0wLjUxMSAwLjAwNS0wLjc0IDAtMC41NzktMC4wMTYtMC43OTItMC4xNjEtMC44NDQtMC43MTMtMC4wNzktMC43MTMtMC4wNTctMS40MzctMC4wOTktMi4xNTYtMC4wODktMS4zMzktMC4yMzUtMi42NTEtMS41NDEtMy41IDAuNjcyLTAuNDk1IDEuMTYxLTEuMDg0IDEuMzEyLTEuODY1IDAuMTA5LTAuNTQ3IDAuMTc3LTEuMDk5IDAuMjE5LTEuNjUxcy0wLjAyNS0xLjEyIDAuMDIxLTEuNjY3YzAuMDc3LTAuODg1IDAuMTM1LTEuMjQ5IDEuMTk3LTEuMjEzIDAuMTYxIDAgMC4zMTctMC4wMjEgMC40OTUtMC4wMzZ2LTEuNzQ1Yy0wLjIxMyAwLTAuNDExLTAuMDA1LTAuNjA0LTAuMDExek0yMS4yODcgNy44MzljLTAuMzY1LTAuMDExLTAuNzI5IDAuMDE2LTEuMDg5IDAuMDc5djEuNjk3YzAuMzI5IDAgMC41ODQgMCAwLjgzMyAwLjAwNSAwLjQzOSAwLjAwNSAwLjc3MiAwLjE3NyAwLjgxMyAwLjY2MSAwLjA0MSAwLjQ0MyAwLjA0MSAwLjg5MSAwLjA4MyAxLjMzOSAwLjA4OSAwLjg5NiAwLjEzNiAxLjc5NiAwLjI5MiAyLjY3NyAwLjEzNiAwLjcyNCAwLjYzNiAxLjI2NSAxLjI1NSAxLjcxMy0xLjA4OCAwLjcyOS0xLjQxMSAxLjc3Ni0xLjQ2MyAyLjk1My0wLjAzMiAwLjgwMS0wLjA1MiAxLjYxNS0wLjA5MyAyLjQyNy0wLjAzNyAwLjc0LTAuMjk3IDAuOTc5LTEuMDQzIDAuOTk1LTAuMjA4IDAuMDExLTAuNDExIDAuMDI3LTAuNjQgMC4wNDF2MS43NGMwLjQzMiAwIDAuODMzIDAuMDI3IDEuMjM1IDAgMS4yMzktMC4wNzMgMS45OTUtMC42NzcgMi4yMzktMS44ODUgMC4xMDQtMC42NjEgMC4xNjctMS4zMzMgMC4xODMtMi4wMDUgMC4wNDEtMC42MTUgMC4wMzYtMS4yMzUgMC4wOTktMS44NDQgMC4wOTMtMC45NTMgMC41MzItMS4zNDkgMS40ODQtMS40MTEgMC4wODktMC4wMTEgMC4xNzctMC4wMzIgMC4yNjctMC4wNTd2LTEuOTUzYy0wLjE2MS0wLjAyMS0wLjI3MS0wLjAzNy0wLjM5MS0wLjA0MS0wLjcxMy0wLjAzMi0xLjA2OC0wLjI3Mi0xLjI1MS0wLjk0OC0wLjEwOS0wLjQzMy0wLjE3Ny0wLjg3Ni0wLjE5Ny0xLjMyNC0wLjA1Mi0wLjgyMy0wLjA0Ny0xLjY1Ni0wLjA5OS0yLjQ3OS0wLjEwOS0xLjU4OC0xLjA2My0yLjMzOS0yLjUxNi0yLjM4ek0xMi4wOTkgMTQuODc1Yy0xLjQzMiAwLTEuNTM2IDIuMTA5LTAuMTE1IDIuMjQ1aDAuMDc5YzAuNjA5IDAuMDM2IDEuMTMxLTAuNDI3IDEuMTY3LTEuMDM3di0wLjA2MWMwLjAxMS0wLjYyLTAuNDg0LTEuMTM2LTEuMTA0LTEuMTQ3ek0xNS45NzkgMTQuODc1Yy0wLjU5My0wLjAyMC0xLjA5MyAwLjQ0OC0xLjExNSAxLjA0MyAwIDAuMDM2IDAgMC4wNjcgMC4wMDUgMC4xMDQgMCAwLjY3MiAwLjQ1OSAxLjA5OSAxLjE0NyAxLjA5OSAwLjY3NyAwIDEuMTA0LTAuNDQzIDEuMTA0LTEuMTM2LTAuMDA1LTAuNjcyLTAuNDU5LTEuMTE1LTEuMTQxLTEuMTA5ek0xOS45MjcgMTQuODc1Yy0wLjYyNC0wLjAxMS0xLjE0NSAwLjQ4NS0xLjE2NyAxLjExNSAwIDAuNjI1IDAuNTA1IDEuMTMxIDEuMTM2IDEuMTMxaDAuMDExYzAuNTY3IDAuMDk5IDEuMTM1LTAuNDQ4IDEuMTcyLTEuMTA0IDAuMDMxLTAuNjA5LTAuNTIxLTEuMTQxLTEuMTUyLTEuMTQxeiIvPiA8L2c+Cg08L3N2Zz4=">
	<link href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.1.3/swagger-ui.css" rel="stylesheet">
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.1.3/swagger-ui-bundle.js"></script>
    <script>
      SwaggerUIBundle({
        url: "%s",
        dom_id: "#swagger-ui"
      });
    </script>
  </body>
</html>
`

func RegisterSwaggerUI(router *gin.Engine, prefix, cfgsDir string) {
	router.GET(prefix+"/:dbalias/swagger.yaml", func(c *gin.Context) {
		dbalias := c.Param("dbalias")
		databaseDir := filepath.Join(cfgsDir, "database")
		tableDir := filepath.Join(cfgsDir, "table")

		// 根据数据库别名找到配置目录名
		dbname := findFileByAlias(databaseDir, dbalias)

		swaggerPath := filepath.Join(tableDir, dbname, "swagger.yaml")
		data, err := os.ReadFile(swaggerPath)
		if err != nil {
			c.String(http.StatusNotFound, "swagger.yaml not found for db: %s", dbname)
			return
		}
		c.Data(http.StatusOK, "application/yaml", data)
	})

	// swagger ui 页面, 路径: /swagger/:dbalias
	router.GET(prefix+"/:dbalias", func(c *gin.Context) {
		dbalias := c.Param("dbalias")
		yamlURL := prefix + "/" + dbalias + "/swagger.yaml"
		html := fmt.Sprintf(swaggerHTMLTpl, yamlURL)
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, html)
	})
}

// 遍历目录并查找匹配的文件
func findFileByAlias(dirPath string, targetAlias string) string {
	var result string

	// 定义一个结构体，用于映射 YAML 文件中的数据
	type Config struct {
		Alias string `yaml:"alias"`
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

			// 如果 alias 匹配，则记录文件名，去掉 .enable.yaml 后缀
			if config.Alias == targetAlias {
				result = strings.Replace(info.Name(), ".enable.yaml", "", 1)
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

	// 返回找到的文件名或空字符串
	return result
}
