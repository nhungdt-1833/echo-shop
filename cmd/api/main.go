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
	"echo-shop/internal/cache"
	"echo-shop/internal/db"
	"echo-shop/internal/handler"
	"echo-shop/internal/model"
	"echo-shop/internal/queue"
	"echo-shop/internal/ratelimit"
	"echo-shop/internal/ws"
	"echo-shop/internal/wsbus"
)

func main() {
	_ = godotenv.Load()
	e := echo.New()
	e.Use(middleware.Recover())

	if os.Getenv("BEHIND_PROXY") == "true" {
		e.IPExtractor = echo.ExtractIPFromXFFHeader()
	} else {
		e.IPExtractor = echo.ExtractIPDirect()
	}

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

	rdb := cache.NewRedis(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASSWORD"))

	// Task 3: every WebSocket notification goes through Redis channel "ws:events",
	// so clients connected to ANY instance (8080, 8081...) receive it.
	bus := wsbus.NewRedisBus(rdb, wsbus.Channel)
	hub := ws.NewHub(handler.OrderRoomAuthorizer(gdb), bus)
	bgCtx, stopBackground := context.WithCancel(context.Background())
	go hub.Run(bgCtx)
	go bus.Run(bgCtx, hub)

	mq, err := queue.DialRabbit(envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal("rabbitmq: ", err)
	}

	limiter, err := ratelimit.New(os.Getenv("RATE_LIMIT_ALGO"), rdb)
	if err != nil {
		log.Fatal(err)
	}
	apiLimit := ratelimit.Middleware(limiter, ratelimit.APIRule())
	ordersLimit := ratelimit.Middleware(limiter, ratelimit.OrdersRule())

	jwtAuth := auth.JWTAuth(secret)
	authHandler := handler.NewAuthHandler(gdb, secret, rdb)
	orderHandler := handler.NewOrderHandler(gdb, hub, mq)
	wsHandler := handler.NewWSHandler(hub, secret, rdb, allowedOrigins(port))
	checkBlacklist := auth.CheckBlackList(rdb)
	rateLimitHandler := handler.NewRateLimitHandler(gdb, limiter)

	productHandler := handler.NewProductHandler(gdb, rdb)
	products := e.Group("/products")
	products.GET("/top", productHandler.Top)
	products.GET("", productHandler.List, cache.RedisCache(rdb, 60*time.Second))
	products.POST("", productHandler.Create, jwtAuth, checkBlacklist, apiLimit, auth.RequireRole(model.RoleAdmin))
	products.GET("/:id", productHandler.Get, cache.RedisCache(rdb, 60*time.Second))
	products.PUT("/:id", productHandler.Update, jwtAuth, checkBlacklist, apiLimit, auth.RequireRole(model.RoleAdmin))
	products.DELETE("/:id", productHandler.Delete, jwtAuth, checkBlacklist, apiLimit, auth.RequireRole(model.RoleAdmin))

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.Static("/web", "web")

	e.POST("/login", authHandler.Login, ratelimit.LoginLimiter())
	e.POST("/refresh", authHandler.Refresh)
	e.POST("/logout", authHandler.Logout, jwtAuth, checkBlacklist, apiLimit)
	e.GET("/me", authHandler.Me, jwtAuth, checkBlacklist, apiLimit)
	e.GET("/ws", wsHandler.Connect)
	e.POST("/orders", orderHandler.Create, jwtAuth, checkBlacklist, apiLimit, ordersLimit, auth.RequireRole(model.RoleCustomer))
	e.PATCH("/orders/:id/status", orderHandler.UpdateStatus, jwtAuth, checkBlacklist, apiLimit, auth.RequireRole(model.RoleAdmin))
	e.GET("/admin/ratelimit/:user", rateLimitHandler.Show, jwtAuth, checkBlacklist, apiLimit, auth.RequireRole(model.RoleAdmin))

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
	stopBackground()
	for _, done := range []<-chan struct{}{hub.Done(), bus.Done()} {
		select {
		case <-done:
		case <-ctx.Done():
			log.Println("shutdown background: timeout")
		}
	}
	mq.Close()
	if err := rdb.Close(); err != nil {
		log.Println("close redis: ", err)
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
