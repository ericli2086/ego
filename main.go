package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ego/apix"

	"github.com/gin-gonic/gin"
)

func main() {
	port := 8080
	cfgs := "./cfgs"

	// 创建 Gin 引擎
	router := gin.Default()

	// 注册Restful Graphql API
	apix.RegisterRestfulAndGraphql(router, cfgs, port)

	// 创建服务器实例
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port), // 服务端口
		Handler: router,
	}

	// 创建一个 channel 来接收退出信号
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务器的 goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("Server failed:", err)
		}
	}()

	// 等待终止信号
	<-stopChan

	// 关闭服务器，优雅停止
	fmt.Println("Shutting down server...")

	// 设置超时时间，等待现有请求完成后退出
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Println("Server forced to shutdown:", err)
	}

	fmt.Println("Server gracefully stopped")
}
