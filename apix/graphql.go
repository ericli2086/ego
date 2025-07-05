package apix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/handler"
	"gopkg.in/yaml.v3"
)

// 用于记录批量更新schema的主键字段（支持复合主键）
var batchUpdatePkMap = make(map[string][]string)

// RegisterGraphqlAPI registers /api/graphql as a proxy to all parsed RESTful endpoints from swagger yamls.
func RegisterGraphqlAPI(router *gin.Engine, path string, cfgDir string, restBaseURL string) error {
	types, inputTypes, queries, mutations := map[string]*graphql.Object{}, map[string]*graphql.InputObject{}, graphql.Fields{}, graphql.Fields{}

	// 1. Parse all _swagger.yaml
	err := filepath.WalkDir(cfgDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Printf("walk dir error at %s: %v", p, err)
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(p, "swagger.yaml") {
			data, readErr := os.ReadFile(p)
			if readErr != nil {
				log.Printf("failed to read %s: %v", p, readErr)
				return nil
			}
			mergeSwaggerToGraphql(data, types, inputTypes, queries, mutations, restBaseURL)
		}
		return nil
	})
	if err != nil {
		log.Printf("WalkDir failed: %v", err)
		return err
	}

	// 2. Ensure queries is not empty
	if len(queries) == 0 {
		queries["hello"] = &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "world", nil
			},
		}
	}

	rootQuery := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: queries,
	})
	rootMutation := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Mutation",
		Fields: mutations,
	})

	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    rootQuery,
		Mutation: rootMutation,
	})
	if err != nil {
		log.Printf("GraphQL schema build failed: %v", err)
		return err
	}

	h := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false, // set true for dev env
	})

	router.POST(path, gin.WrapH(h))
	router.GET(path, gin.WrapH(h))
	log.Printf("[GraphQL] Registered at %s", path)
	return nil
}

// 转为匈牙利风格：user_batch_update => InputUserBatchUpdate
func toHungarianInputTypeName(name string) string {
	parts := strings.Split(name, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	tmp := strings.Join(parts, "") + "Input"
	if len(tmp) == 0 {
		return tmp // 空字符串直接返回
	}
	return strings.ToLower(string(tmp[0])) + tmp[1:]
}

func mergeSwaggerToGraphql(
	data []byte,
	types map[string]*graphql.Object,
	inputTypes map[string]*graphql.InputObject,
	queries graphql.Fields,
	mutations graphql.Fields,
	restBaseURL string,
) {
	type swaggerSchema struct {
		Type       string                            `yaml:"type"`
		Properties map[string]map[string]interface{} `yaml:"properties"`
		Required   []string                          `yaml:"required"`
	}
	type swagger struct {
		Components struct {
			Schemas map[string]swaggerSchema `yaml:"schemas"`
		} `yaml:"components"`
		Paths map[string]map[string]interface{} `yaml:"paths"`
	}
	var sw swagger
	if err := yaml.Unmarshal(data, &sw); err != nil {
		log.Printf("YAML unmarshal error: %v", err)
		return
	}

	// 1. Generate all graphql.Object and graphql.InputObject
	for name, sch := range sw.Components.Schemas {
		fields := graphql.Fields{}
		inFields := graphql.InputObjectConfigFieldMap{}
		for fname, prop := range sch.Properties {
			ftype := graphqlTypeBySwagger(prop, fname, types)
			fields[fname] = &graphql.Field{Type: ftype}
			if ro, _ := prop["readOnly"].(bool); !ro {
				inFields[fname] = &graphql.InputObjectFieldConfig{Type: graphqlInputTypeBySwagger(prop, fname, types, inputTypes)}
			}
		}
		types[name] = graphql.NewObject(graphql.ObjectConfig{
			Name:   name,
			Fields: fields,
		})
		inputTypeName := toHungarianInputTypeName(name)
		inputTypes[inputTypeName] = graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   inputTypeName,
			Fields: inFields,
		})
		// 记录所有 batch_update schema 的主键 required 字段
		if (strings.Contains(name, "batch_update") || strings.Contains(name, "BatchUpdate")) && len(sch.Required) > 0 {
			batchUpdatePkMap[name] = sch.Required
		}
	}

	// 2. Parse paths to generate query/mutation
	for path, methods := range sw.Paths {
		for m := range methods {
			method := strings.ToLower(m)
			base := getBaseNameFromPath(path)
			if base == "" {
				continue
			}
			typ := types[base]
			inTyp := inputTypes[toHungarianInputTypeName(base)]
			switch method {
			case "get":
				if strings.Contains(path, "{id}") {
					if typ != nil {
						queries[base] = &graphql.Field{
							Type: typ,
							Args: graphql.FieldConfigArgument{
								"id": &graphql.ArgumentConfig{Type: graphql.String},
							},
							Resolve: restGetByIDResolver(restBaseURL+path, typ),
						}
					}
				} else {
					if typ != nil {
						queries[base+"List"] = &graphql.Field{
							Type: graphql.NewObject(graphql.ObjectConfig{
								Name: base + "ListResult",
								Fields: graphql.Fields{
									"data":  &graphql.Field{Type: graphql.NewList(typ)},
									"total": &graphql.Field{Type: graphql.Int},
								},
							}),
							Args:    buildGraphqlFieldConfigArgument(),
							Resolve: restListResolver(restBaseURL+path, typ),
						}
					}
				}
			case "post":
				if strings.HasSuffix(path, "batch_delete") {
					mutations["batchDelete"+upperFirst(base)] = &graphql.Field{
						Type: graphql.Boolean,
						Args: graphql.FieldConfigArgument{
							"ids": &graphql.ArgumentConfig{Type: graphql.NewList(graphql.String)},
						},
						Resolve: restBatchDeleteResolver(restBaseURL + path),
					}
				} else {
					if typ != nil && inTyp != nil {
						mutations["batchCreate"+upperFirst(base)] = &graphql.Field{
							Type: graphql.NewList(typ),
							Args: graphql.FieldConfigArgument{
								"input": &graphql.ArgumentConfig{Type: graphql.NewList(inTyp)},
							},
							Resolve: restBatchCreateResolver(restBaseURL+path, typ),
						}
					}
				}
			case "put":
				if typ != nil && inTyp != nil {
					if strings.Contains(path, "{id}") {
						mutations["update"+upperFirst(base)] = &graphql.Field{
							Type: typ,
							Args: graphql.FieldConfigArgument{
								"id":    &graphql.ArgumentConfig{Type: graphql.String},
								"input": &graphql.ArgumentConfig{Type: inTyp},
							},
							Resolve: restUpdateByIDResolver(restBaseURL+path, typ),
						}
					} else {
						// 用 batch_update 的 inputType，如果不存在则 fallback
						batchUpdateTypeName := base + "_batch_update"
						hungarianTypeName := toHungarianInputTypeName(batchUpdateTypeName)
						batchInTyp := inputTypes[hungarianTypeName]
						if batchInTyp != nil {
							mutations["batchUpdate"+upperFirst(base)] = &graphql.Field{
								Type: graphql.NewList(typ),
								Args: graphql.FieldConfigArgument{
									"input": &graphql.ArgumentConfig{Type: graphql.NewList(batchInTyp)},
								},
								Resolve: restBatchUpdateResolver(restBaseURL+path, typ),
							}
						} else {
							mutations["batchUpdate"+upperFirst(base)] = &graphql.Field{
								Type: graphql.NewList(typ),
								Args: graphql.FieldConfigArgument{
									"input": &graphql.ArgumentConfig{Type: graphql.NewList(inTyp)},
								},
								Resolve: restBatchUpdateResolver(restBaseURL+path, typ),
							}
						}
					}
				}
			case "delete":
				if strings.Contains(path, "{id}") {
					mutations["delete"+upperFirst(base)] = &graphql.Field{
						Type: graphql.Boolean,
						Args: graphql.FieldConfigArgument{
							"id": &graphql.ArgumentConfig{Type: graphql.String},
						},
						Resolve: restDeleteByIDResolver(restBaseURL + path),
					}
				}
			}
		}
	}
}

// 支持通用参数和filter字符串
func buildGraphqlFieldConfigArgument() graphql.FieldConfigArgument {
	return graphql.FieldConfigArgument{
		"page":      &graphql.ArgumentConfig{Type: graphql.Int},
		"page_size": &graphql.ArgumentConfig{Type: graphql.Int},
		"order":     &graphql.ArgumentConfig{Type: graphql.String},
		"fields":    &graphql.ArgumentConfig{Type: graphql.String},
		"filter":    &graphql.ArgumentConfig{Type: graphql.String}, // 直接字符串
	}
}

// ===== RESTful 代理 resolver（核心入口） =====

func restGetByIDResolver(urlTemplate string, typ *graphql.Object) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"]
		if !ok {
			return nil, fmt.Errorf("missing id argument")
		}
		urlStr := strings.Replace(urlTemplate, "{id}", fmt.Sprintf("%v", id), 1)
		fieldsStr := getLeafFieldsFromResolveParams(p)
		query := url.Values{}
		if fieldsStr != "" {
			query.Set("fields", fieldsStr)
		}
		if len(query) > 0 {
			if strings.Contains(urlStr, "?") {
				urlStr += "&" + query.Encode()
			} else {
				urlStr += "?" + query.Encode()
			}
		}
		resp, err := http.Get(urlStr)
		if err != nil {
			log.Printf("restGetByIDResolver failed: %v", err)
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("rest error: %s", resp.Status)
		}
		var out map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("json decode error: %w", err)
		}
		return out, nil
	}
}

// 关键：filter参数直接拆分并与主参数并列拼接
func restListResolver(urlStr string, typ *graphql.Object) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		// 自动合成 fields 字段
		if _, ok := p.Args["fields"]; !ok {
			fieldsStr := getDataLeafFieldsFromResolveParams(p)
			if fieldsStr != "" {
				p.Args["fields"] = fieldsStr
			}
		}

		// 主参数
		mainKeys := []string{"page", "page_size", "order", "fields"}
		params := []string{}
		for _, k := range mainKeys {
			if v, ok := p.Args[k]; ok && v != nil {
				params = append(params, fmt.Sprintf("%s=%v", url.QueryEscape(k), url.QueryEscape(fmt.Sprintf("%v", v))))
			}
		}

		// filter参数直接拆成多个参数
		if v, ok := p.Args["filter"]; ok && v != nil && v.(string) != "" {
			filterStr := v.(string)
			filterStr = strings.Trim(filterStr, "&")
			if filterStr != "" {
				for _, part := range strings.Split(filterStr, "&") {
					part = strings.TrimSpace(part)
					if part != "" {
						params = append(params, part)
					}
				}
			}
		}

		finalURL := urlStr
		if len(params) > 0 {
			finalURL += "?" + strings.Join(params, "&")
		}

		resp, err := http.Get(finalURL)
		if err != nil {
			log.Printf("restListResolver failed: %v", err)
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("rest error: %s", resp.Status)
		}
		var out map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("json decode error: %w", err)
		}
		return out, nil
	}
}

// 工具函数：收集 GraphQL 查询需要的字段（叶子字段）
func getLeafFieldsFromResolveParams(p graphql.ResolveParams) string {
	fields := []string{}
	for _, astField := range p.Info.FieldASTs {
		if astField.SelectionSet != nil {
			for _, sel := range astField.SelectionSet.Selections {
				if f, ok := sel.(*ast.Field); ok {
					fields = append(fields, f.Name.Value)
				}
			}
		}
	}
	return strings.Join(fields, ",")
}

// 针对 userList 这种结构，收集 data 字段下的叶子字段
func getDataLeafFieldsFromResolveParams(p graphql.ResolveParams) string {
	for _, astField := range p.Info.FieldASTs {
		if astField.SelectionSet != nil {
			for _, sel := range astField.SelectionSet.Selections {
				if f, ok := sel.(*ast.Field); ok && f.Name.Value == "data" && f.SelectionSet != nil {
					leafs := []string{}
					for _, leaf := range f.SelectionSet.Selections {
						if lf, ok := leaf.(*ast.Field); ok {
							leafs = append(leafs, lf.Name.Value)
						}
					}
					return strings.Join(leafs, ",")
				}
			}
		}
	}
	return ""
}

func restBatchCreateResolver(url string, typ *graphql.Object) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		input, ok := p.Args["input"]
		if !ok {
			return nil, fmt.Errorf("missing input argument")
		}
		body, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("marshal input error: %w", err)
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("restBatchCreateResolver failed: %v", err)
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("rest error: %s", resp.Status)
		}
		var out []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("json decode error: %w", err)
		}
		return out, nil
	}
}

// 批量更新后自动主键in回查最新对象
func restBatchUpdateResolver(burl string, typ *graphql.Object) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		input, ok := p.Args["input"]
		if !ok {
			return nil, fmt.Errorf("missing input argument")
		}
		body, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("marshal input error: %w", err)
		}
		req, err := http.NewRequest("PUT", burl, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request error: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("restBatchUpdateResolver failed: %v", err)
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			errMsg := resp.Status
			if len(b) > 0 {
				errMsg = string(b)
			}
			return nil, fmt.Errorf("rest error: %s", errMsg)
		}

		// 推断 batch_update schema 名
		paths := strings.Split(burl, "/")
		baseName := ""
		for i := len(paths) - 1; i >= 0; i-- {
			s := paths[i]
			if s == "" || s == "batch_update" || strings.HasPrefix(s, "batch") {
				continue
			}
			baseName = s
			break
		}
		batchSchemaName := baseName + "_batch_update"
		pkList, hasPk := batchUpdatePkMap[batchSchemaName]

		if hasPk && len(pkList) > 0 {
			// 构造 url param: id_in=1,2,3 或 id_in=...&code_in=...
			var paramParts []string
			var inputArr []interface{}
			switch arr := input.(type) {
			case []interface{}:
				inputArr = arr
			default:
				return nil, fmt.Errorf("batch update input must be array")
			}
			for _, pk := range pkList {
				var vals []string
				for _, item := range inputArr {
					if mp, ok := item.(map[string]interface{}); ok {
						if val, ok := mp[pk]; ok {
							vals = append(vals, fmt.Sprintf("%v", val))
						}
					}
				}
				if len(vals) == 0 {
					return nil, fmt.Errorf("no values for primary key %s found in input", pk)
				}
				paramParts = append(paramParts, pk+"__in="+url.QueryEscape(strings.Join(vals, ",")))
			}
			paramStr := strings.Join(paramParts, "&")

			// 推断list url（去掉结尾 /batch_update，用 TrimSuffix，避免 staticcheck S1017）
			listURL := strings.TrimSuffix(burl, "/batch_update")
			listURL = strings.TrimSuffix(listURL, "batch_update")
			listURL = strings.TrimSuffix(listURL, "/")

			queryURL := listURL + "?" + paramStr

			getResp, err := http.Get(queryURL)
			if err != nil {
				return nil, fmt.Errorf("get after batch update failed: %w", err)
			}
			defer getResp.Body.Close()
			if getResp.StatusCode < 200 || getResp.StatusCode >= 300 {
				return nil, fmt.Errorf("rest error: %s", getResp.Status)
			}
			var out map[string]interface{}
			if err := json.NewDecoder(getResp.Body).Decode(&out); err != nil {
				return nil, fmt.Errorf("json decode error: %w", err)
			}
			if data, ok := out["data"]; ok {
				return data, nil
			}
			return out, nil
		}

		// fallback 老逻辑
		var out []map[string]interface{}
		outDecodeErr := json.NewDecoder(resp.Body).Decode(&out)
		if outDecodeErr == nil {
			return out, nil
		}
		return []map[string]interface{}{}, nil
	}
}

func restBatchDeleteResolver(url string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		ids, ok := p.Args["ids"]
		if !ok {
			return false, fmt.Errorf("missing ids argument")
		}
		body, err := json.Marshal(ids)
		if err != nil {
			return false, fmt.Errorf("marshal ids error: %w", err)
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("restBatchDeleteResolver failed: %v", err)
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return false, fmt.Errorf("rest error: %s", resp.Status)
		}
		return true, nil
	}
}

func restUpdateByIDResolver(urlTemplate string, typ *graphql.Object) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"]
		if !ok {
			return nil, fmt.Errorf("missing id argument")
		}
		input, ok := p.Args["input"]
		if !ok {
			return nil, fmt.Errorf("missing input argument")
		}
		urlStr := strings.Replace(urlTemplate, "{id}", fmt.Sprintf("%v", id), 1)
		fieldsStr := getLeafFieldsFromResolveParams(p)
		query := url.Values{}
		if fieldsStr != "" {
			query.Set("fields", fieldsStr)
		}

		// 1. 执行 PUT 更新
		body, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("marshal input error: %w", err)
		}
		req, err := http.NewRequest("PUT", urlStr, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request error: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("restUpdateByIDResolver failed: %v", err)
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			errMsg := resp.Status
			if len(b) > 0 {
				errMsg = string(b)
			}
			return nil, fmt.Errorf("rest error: %s", errMsg)
		}

		// 2. 再 GET 一次最新对象
		getUrl := urlStr
		// 如果 urlStr 已经有 query 参数，去掉，重组 GET url
		if idx := strings.Index(getUrl, "?"); idx > -1 {
			getUrl = getUrl[:idx]
		}
		if fieldsStr != "" {
			getUrl += "?fields=" + url.QueryEscape(fieldsStr)
		}
		getResp, err := http.Get(getUrl)
		if err != nil {
			return nil, fmt.Errorf("get after update failed: %w", err)
		}
		defer getResp.Body.Close()
		if getResp.StatusCode < 200 || getResp.StatusCode >= 300 {
			return nil, fmt.Errorf("rest error: %s", getResp.Status)
		}
		var out map[string]interface{}
		if err := json.NewDecoder(getResp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("json decode error: %w", err)
		}
		return out, nil
	}
}

func restDeleteByIDResolver(urlTemplate string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"]
		if !ok {
			return false, fmt.Errorf("missing id argument")
		}
		urlStr := strings.Replace(urlTemplate, "{id}", fmt.Sprintf("%v", id), 1)
		req, err := http.NewRequest("DELETE", urlStr, nil)
		if err != nil {
			return false, fmt.Errorf("create request error: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("restDeleteByIDResolver failed: %v", err)
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return false, fmt.Errorf("rest error: %s", resp.Status)
		}
		return true, nil
	}
}

// ===== 工具函数 =====

// graphqlTypeBySwagger 支持基础类型、$ref
func graphqlTypeBySwagger(prop map[string]interface{}, fname string, types map[string]*graphql.Object) graphql.Output {
	if fname == "id" || fname == "_id" {
		return graphql.String
	}
	if ref, ok := prop["$ref"].(string); ok {
		refType := extractTypeFromRef(ref)
		if t, exists := types[refType]; exists {
			return t
		}
		return graphql.String
	}
	switch prop["type"] {
	case "integer":
		return graphql.Int
	case "number":
		return graphql.Float
	case "boolean":
		return graphql.Boolean
	case "array":
		if items, ok := prop["items"].(map[string]interface{}); ok {
			if ref, ok := items["$ref"].(string); ok {
				refType := extractTypeFromRef(ref)
				if t, exists := types[refType]; exists {
					return graphql.NewList(t)
				}
			}
			if typ, ok := items["type"].(string); ok {
				switch typ {
				case "integer":
					return graphql.NewList(graphql.Int)
				case "number":
					return graphql.NewList(graphql.Float)
				case "boolean":
					return graphql.NewList(graphql.Boolean)
				default:
					return graphql.NewList(graphql.String)
				}
			}
		}
		return graphql.NewList(graphql.String)
	default:
		return graphql.String
	}
}

func graphqlInputTypeBySwagger(prop map[string]interface{}, fname string, types map[string]*graphql.Object, inputTypes map[string]*graphql.InputObject) graphql.Input {
	if fname == "id" || fname == "_id" {
		return graphql.String
	}
	if ref, ok := prop["$ref"].(string); ok {
		refType := extractTypeFromRef(ref)
		hungarianTypeName := toHungarianInputTypeName(refType)
		if t, exists := inputTypes[hungarianTypeName]; exists {
			return t
		}
		return graphql.String
	}
	switch prop["type"] {
	case "integer":
		return graphql.Int
	case "number":
		return graphql.Float
	case "boolean":
		return graphql.Boolean
	case "array":
		if items, ok := prop["items"].(map[string]interface{}); ok {
			if ref, ok := items["$ref"].(string); ok {
				refType := extractTypeFromRef(ref)
				hungarianTypeName := toHungarianInputTypeName(refType)
				if t, exists := inputTypes[hungarianTypeName]; exists {
					return graphql.NewList(t)
				}
			}
			if typ, ok := items["type"].(string); ok {
				switch typ {
				case "integer":
					return graphql.NewList(graphql.Int)
				case "number":
					return graphql.NewList(graphql.Float)
				case "boolean":
					return graphql.NewList(graphql.Boolean)
				default:
					return graphql.NewList(graphql.String)
				}
			}
		}
		return graphql.NewList(graphql.String)
	default:
		return graphql.String
	}
}

func extractTypeFromRef(ref string) string {
	parts := strings.Split(ref, "/")
	return parts[len(parts)-1]
}

func getBaseNameFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		s := parts[i]
		if s == "" || s == "{id}" || s == "batch_delete" || strings.HasPrefix(s, "batch") {
			continue
		}
		return s
	}
	return ""
}

func upperFirst(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
