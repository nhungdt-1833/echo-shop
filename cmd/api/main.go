package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"echo-shop/internal/db"
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

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	// Start server trong goroutine để có thể graceful shutdown
	go func() {
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal(err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = e.Shutdown(ctx)

	sqlDB, err := gdb.DB()
	if err != nil {
		log.Fatal("get DB instance: ", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Fatal("close DB: ", err)
	}
}
