package ratelimit

import (
	"context"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
)

const redisTimeout = 200 * time.Millisecond

type Rule struct {
	Name   string
	Window time.Duration
	Subject func(c echo.Context) (id string, limit int, ok bool)
}

func Middleware(l Limiter, rule Rule) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			id, limit, ok := rule.Subject(c)
			if !ok {
				return next(c)
			}
			ctx, cancel := context.WithTimeout(c.Request().Context(), redisTimeout)
			res, err := l.Allow(ctx, rule.Name+":"+id, limit, rule.Window)
			cancel()
			if err != nil {
				log.Printf("[ratelimit] %s check failed, fail-open: %v", rule.Name, err)
				return next(c)
			}

			h := c.Response().Header()
			h.Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
			h.Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			h.Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetAt.Unix(), 10))
			if !res.Allowed {
				retry := int(math.Ceil(time.Until(res.ResetAt).Seconds()))
				h.Set("Retry-After", strconv.Itoa(max(1, retry)))
				LogBlocked(c, rule.Name, id)
				return c.JSON(http.StatusTooManyRequests, echo.Map{"error": "too many requests"})
			}
			return next(c)
		}
	}
}

func LogBlocked(c echo.Context, rule, id string) {
	log.Printf("[ratelimit] BLOCKED rule=%s id=%s ip=%s route=%s %s",
		rule, id, c.RealIP(), c.Request().Method, c.Path())
}
