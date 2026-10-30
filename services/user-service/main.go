package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"echo-shop/shared/auth"
	"echo-shop/shared/cache"
	"echo-shop/shared/health"
	"echo-shop/user-service/internal/db"
	"echo-shop/user-service/internal/handler"
	"echo-shop/user-service/internal/model"
	"echo-shop/user-service/internal/ratelimit"
)

func main() {
	_ = godotenv.Load()

	gdb, err := db.Connect(os.Getenv("USER_DB_URL"))
	if err != nil {
		log.Fatal("connect db: ", err)
	}
	if err := gdb.AutoMigrate(&model.User{}); err != nil {
		log.Fatal("auto migrate: ", err)
	}
	if err := db.SeedUsers(gdb, os.Getenv("SEED_PASSWORD")); err != nil {
		log.Fatal("seed users: ", err)
	}

	rdb := cache.NewRedis(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASS"))

	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must be at least 32 bytes (openssl rand -base64 32)")
	}

	e := echo.New()
	e.Use(middleware.Recover(), middleware.RequestID())

	if os.Getenv("BEHIND_PROXY") == "true" {
		e.IPExtractor = echo.ExtractIPFromXFFHeader()
	} else {
		e.IPExtractor = echo.ExtractIPDirect()
	}

	health.Register(e, "user-service")

	jwtAuth := auth.JWTAuth(secret)
	checkBlacklist := auth.CheckBlackList(rdb)
	authHandler := handler.NewAuthHandler(gdb, secret, rdb)
	userHandler := handler.NewUserHandler(gdb, secret, rdb)

	e.POST("/login", authHandler.Login, ratelimit.LoginLimiter())
	e.POST("/refresh", authHandler.Refresh)
	e.POST("/logout", authHandler.Logout, jwtAuth, checkBlacklist)
	e.GET("/me", authHandler.Me, jwtAuth, checkBlacklist)
	e.GET("/users/:id", userHandler.User, jwtAuth, checkBlacklist)

	port := envOr("USER_PORT", "8081")
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
