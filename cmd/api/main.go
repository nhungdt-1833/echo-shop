package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"echo-shop/internal/auth"
	"echo-shop/internal/db"
	"echo-shop/internal/handler"
	"echo-shop/internal/model"
	"echo-shop/internal/ws"
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
		if err := db.SeedProducts(gdb); err != nil {
			log.Fatal("seed products: ", err)
		}
	}

	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must be at least 32 bytes (openssl rand -base64 32)")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	hub := ws.NewHub(handler.OrderRoomAuthorizer(gdb))
	hubCtx, stopHub := context.WithCancel(context.Background())
	go hub.Run(hubCtx)

	jwtAuth := auth.JWTAuth(secret)
	authHandler := handler.NewAuthHandler(gdb, secret)
	orderHandler := handler.NewOrderHandler(gdb, hub)
	wsHandler := handler.NewWSHandler(hub, secret, allowedOrigins(port))

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.Static("/web", "web")

	e.POST("/login", authHandler.Login)
	e.GET("/me", authHandler.Me, jwtAuth)
	e.GET("/ws", wsHandler.Connect)
	e.POST("/orders", orderHandler.Create, jwtAuth, auth.RequireRole(model.RoleCustomer))
	e.PATCH("/orders/:id/status", orderHandler.UpdateStatus, jwtAuth, auth.RequireRole(model.RoleAdmin))

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
	stopHub()
	select {
	case <-hub.Done():
	case <-ctx.Done():
		log.Println("shutdown hub: timeout")
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

func allowedOrigins(port string) []string {
	raw := os.Getenv("WS_ALLOWED_ORIGINS")
	if raw == "" {
		return []string{"http://localhost:" + port, "http://127.0.0.1:" + port}
	}
	origins := strings.Split(raw, ",")
	for i := range origins {
		origins[i] = strings.TrimSpace(origins[i])
	}
	return origins
}
