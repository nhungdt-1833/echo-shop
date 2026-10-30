package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"echo-shop/inventory-service/internal/db"
	"echo-shop/inventory-service/internal/handler"
	"echo-shop/inventory-service/internal/model"
	invrpc "echo-shop/inventory-service/internal/rpc"
	"echo-shop/shared/auth"
	"echo-shop/shared/cache"
	"echo-shop/shared/health"
)

func main() {
	_ = godotenv.Load()

	dsn := os.Getenv("INVENTORY_DB_URL")
	if dsn == "" {
		log.Fatal("INVENTORY_DB_URL is required")
	}
	gdb, err := db.Connect(dsn)
	if err != nil {
		log.Fatal("connect db: ", err)
	}
	if err := gdb.AutoMigrate(&model.Product{}, &model.Reservation{}); err != nil {
		log.Fatal("auto migrate: ", err)
	}
	if err := db.SeedProducts(gdb); err != nil {
		log.Fatal("seed products: ", err)
	}

	rdb := cache.NewRedis(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASS"))

	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must be at least 32 bytes (openssl rand -base64 32)")
	}

	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Inventory", invrpc.New(gdb, rdb)); err != nil {
		log.Fatal("register rpc: ", err)
	}
	ln, err := net.Listen("tcp", ":"+envOr("INVENTORY_RPC_PORT", "9000"))
	if err != nil {
		log.Fatal("listen rpc: ", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go rpcServer.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}()
	log.Println("json-rpc listening on", ln.Addr())

	e := echo.New()
	e.Use(middleware.Recover(), middleware.RequestID())
	health.Register(e, "inventory-service")

	jwtAuth := auth.JWTAuth(secret)
	checkBlacklist := auth.CheckBlackList(rdb)
	adminOnly := []echo.MiddlewareFunc{jwtAuth, checkBlacklist, auth.RequireRole(auth.RoleAdmin)}
	productHandler := handler.NewProductHandler(gdb, rdb)

	products := e.Group("/products")
	products.GET("/top", productHandler.Top)
	products.GET("", productHandler.List, cache.RedisCache(rdb, 60*time.Second))
	products.GET("/:id", productHandler.Get, cache.RedisCache(rdb, 60*time.Second))
	products.POST("", productHandler.Create, adminOnly...)
	products.PUT("/:id", productHandler.Update, adminOnly...)
	products.DELETE("/:id", productHandler.Delete, adminOnly...)

	port := envOr("INVENTORY_HTTP_PORT", "8084")
	go func() {
		if err := e.Start(":" + port); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ln.Close()
	if err := e.Shutdown(ctx); err != nil {
		log.Println("shutdown http: ", err)
	}
	if err := rdb.Close(); err != nil {
		log.Println("close redis: ", err)
	}
	if sqlDB, err := gdb.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
