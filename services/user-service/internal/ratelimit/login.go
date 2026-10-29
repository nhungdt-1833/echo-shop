package ratelimit

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/time/rate"
)

const (
	window     = time.Minute
	loginLimit = 5
)

func LoginLimiter() echo.MiddlewareFunc {
	every := window / loginLimit
	return middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(middleware.RateLimiterMemoryStoreConfig{
			Rate:      rate.Every(every),
			Burst:     loginLimit,
			ExpiresIn: 3 * time.Minute,
		}),
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		ErrorHandler: func(c echo.Context, err error) error {
			return c.JSON(http.StatusForbidden, echo.Map{"error": "cannot identify client"})
		},
		DenyHandler: func(c echo.Context, id string, err error) error {
			log.Printf("[ratelimit] BLOCKED rule=login ip=%s", id)
			c.Response().Header().Set("Retry-After", strconv.Itoa(int(every.Seconds())))
			return c.JSON(http.StatusTooManyRequests, echo.Map{"error": "too many login attempts"})
		},
	})
}
