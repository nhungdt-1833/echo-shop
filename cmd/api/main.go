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

	"echo-shop/internal/auth"
	"echo-shop/internal/db"
	"echo-shop/internal/handler"
	"echo-shop/internal/model"
)

func main() {
	_ = godotenv.Load()
	e := echo.New()
	e.Use(middleware.Recover())

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	gdb, err := db.Connect(dsn)
	if err != nil {
		log.Fatal("connect db: ", err)
	}

	if err := gdb.AutoMigrate(&model.User{}, &model.Product{}, &model.Order{}); err != nil {
		log.Fatal("auto migrate: ", err)
	}

	if os.Getenv("APP_ENV") == "development" {
		if err := db.SeedUsers(gdb, os.Getenv("SEED_PASSWORD")); err != nil {
			log.Fatal("seed users: ", err)
		}
	}

	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must be at least 32 bytes (openssl rand -base64 32)")
	}

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	authHandler := handler.NewAuthHandler(gdb, secret)
	e.POST("/api/v1/login", authHandler.Login)
	e.GET("/api/v1/me", authHandler.Me, auth.JWTAuth(secret))

	admin := e.Group("/admin/api/v1/", auth.JWTAuth(secret), auth.RequireRole(model.RoleAdmin))
	admin.GET("/ping", func(c echo.Context) error {
		return c.String(http.StatusOK, "pong")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
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

	sqlDB, err := gdb.DB()
	if err != nil {
		log.Println("get DB instance: ", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Println("close DB: ", err)
	}
}
