package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
)

func newServer(t *testing.T, limit int) (*echo.Echo, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })

	rule := Rule{Name: "test", Window: time.Minute, Subject: func(echo.Context) (string, int, bool) { return "7", limit, true }}
	e := echo.New()
	e.GET("/", func(c echo.Context) error { return c.String(http.StatusOK, "ok") },
		Middleware(&SlidingWindow{rdb: rdb, now: time.Now}, rule))
	return e, mr
}

func call(e *echo.Echo) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec
}

func TestMiddlewareHeadersAnd429(t *testing.T) {
	e, _ := newServer(t, 2)

	first := call(e)
	if first.Code != 200 || first.Header().Get("X-RateLimit-Limit") != "2" || first.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first: %d %v", first.Code, first.Header())
	}
	if first.Header().Get("X-RateLimit-Reset") == "" {
		t.Fatal("missing X-RateLimit-Reset")
	}
	call(e)
	third := call(e)
	if third.Code != http.StatusTooManyRequests || third.Header().Get("Retry-After") == "" || third.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("third: %d %v", third.Code, third.Header())
	}
}

func TestMiddlewareFailOpenWhenRedisDown(t *testing.T) {
	e, mr := newServer(t, 1)
	mr.Close()
	for range 3 {
		if rec := call(e); rec.Code != http.StatusOK {
			t.Fatalf("Redis down must not block: %d", rec.Code)
		}
	}
}
