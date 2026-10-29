package ratelimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/time/rate"

	"echo-shop/internal/auth"
	"echo-shop/internal/model"
)

const (
	Window      = time.Minute
	OrdersLimit = 10
	LoginLimit  = 5
)

var planLimits = map[string]int{
	model.PlanFree: 60,
	model.PlanPro:  600,
}

func PlanLimit(plan string) int {
	if n, ok := planLimits[plan]; ok {
		return n
	}
	return planLimits[model.PlanFree]
}

func APIRule() Rule {
	return Rule{Name: "api", Window: Window, Subject: func(c echo.Context) (string, int, bool) {
		claims, ok := auth.ClaimsFrom(c)
		if !ok {
			return "", 0, false
		}
		return userID(claims), PlanLimit(claims.Plan), true
	}}
}

func OrdersRule() Rule {
	return Rule{Name: "orders", Window: Window, Subject: func(c echo.Context) (string, int, bool) {
		claims, ok := auth.ClaimsFrom(c)
		if !ok {
			return "", 0, false
		}
		return userID(claims), OrdersLimit, true
	}}
}

func userID(claims *auth.Claims) string { return strconv.FormatUint(uint64(claims.UserID), 10) }

func LoginLimiter() echo.MiddlewareFunc {
	every := Window / LoginLimit
	return middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(middleware.RateLimiterMemoryStoreConfig{
			Rate:      rate.Every(every),
			Burst:     LoginLimit,
			ExpiresIn: 3 * time.Minute,
		}),
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		ErrorHandler: func(c echo.Context, err error) error {
			return c.JSON(http.StatusForbidden, echo.Map{"error": "cannot identify client"})
		},
		DenyHandler: func(c echo.Context, id string, err error) error {
			LogBlocked(c, "login", id)
			c.Response().Header().Set("Retry-After", strconv.Itoa(int(every.Seconds())))
			return c.JSON(http.StatusTooManyRequests, echo.Map{"error": "too many login attempts"})
		},
	})
}
