package apix

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// 注意 __GRAPHQL_ENDPOINT__ 会被动态替换为你的 GraphQL API 路径
const graphiqlHTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>GraphiQL</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg viewBox='0 0 64 64' xmlns='http://www.w3.org/2000/svg'%3E%3Cpolygon points='32,8 56,20 56,44 32,56 8,44 8,20' fill='none' stroke='%23e535ab' stroke-width='3'/%3E%3Cpolygon points='32,8 56,44 8,44' fill='none' stroke='%23e535ab' stroke-width='3'/%3E%3Ccircle cx='32' cy='8' r='4.5' fill='%23e535ab'/%3E%3Ccircle cx='56' cy='20' r='4.5' fill='%23e535ab'/%3E%3Ccircle cx='56' cy='44' r='4.5' fill='%23e535ab'/%3E%3Ccircle cx='32' cy='56' r='4.5' fill='%23e535ab'/%3E%3Ccircle cx='8' cy='44' r='4.5' fill='%23e535ab'/%3E%3Ccircle cx='8' cy='20' r='4.5' fill='%23e535ab'/%3E%3C/svg%3E" />
  <link href="https://cdnjs.cloudflare.com/ajax/libs/graphiql/2.4.7/graphiql.min.css" rel="stylesheet" />
</head>
<body style="margin: 0; height: 100vh;">
  <div id="graphiql" style="height: 100vh;"></div>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/react/17.0.2/umd/react.production.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/react-dom/17.0.2/umd/react-dom.production.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/graphiql/2.4.7/graphiql.min.js"></script>
  <script>
    const endpoint = '__GRAPHQL_ENDPOINT__';
    function graphQLFetcher(graphQLParams) {
      return fetch(endpoint, {
        method: 'post',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(graphQLParams),
      }).then(response => response.json());
    }
    ReactDOM.render(
      React.createElement(GraphiQL, {
        fetcher: graphQLFetcher,
        defaultEditorToolsVisibility: true,
      }),
      document.getElementById('graphiql'),
    );
  </script>
</body>
</html>
`

func RegisterGraphiQL(router *gin.Engine, graphiqlPath string, graphqlEndpoint string) {
	html := strings.ReplaceAll(graphiqlHTML, "__GRAPHQL_ENDPOINT__", graphqlEndpoint)
	router.GET(graphiqlPath, func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, html)
	})
}
