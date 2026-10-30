package cache

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
)

type bodyRecorder struct {
	http.ResponseWriter
	buf *bytes.Buffer
}

func (w *bodyRecorder) Write(b []byte) (int, error) {
	w.buf.Write(b)
	return w.ResponseWriter.Write(b)
}

func RedisCache(rdb *redis.Client, ttl time.Duration) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if c.Request().Method != http.MethodGet {
				return next(c)
			}
			ctx, cancel := context.WithTimeout(c.Request().Context(),
				200*time.Millisecond)
			defer cancel()
			key := "cache:" + c.Request().URL.RequestURI()
			cached, err := rdb.Get(ctx, key).Bytes()
			if err == nil {
				c.Response().Header().Set("X-Cache", "HIT")
				return c.JSONBlob(http.StatusOK, cached)
			} else if err != nil && err != redis.Nil {
				log.Println(err)
			}
			rec := &bodyRecorder{ResponseWriter: c.Response().Writer, buf: new(bytes.Buffer)}
			c.Response().Writer = rec
			c.Response().Header().Set("X-Cache", "MISS")
			if err := next(c); err != nil {
				return err
			}
			if c.Response().Status == http.StatusOK {
				rdb.Set(context.Background(), key, rec.buf.Bytes(), ttl)
			}
			return nil
		}
	}
}
