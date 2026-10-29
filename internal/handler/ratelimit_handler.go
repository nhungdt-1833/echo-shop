package handler

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"echo-shop/internal/model"
	"echo-shop/internal/ratelimit"
)

type quota struct {
	Limit     int   `json:"limit"`
	Remaining int   `json:"remaining"`
	ResetAt   int64 `json:"reset_at"`
}

type RateLimitHandler struct {
	db      *gorm.DB
	limiter ratelimit.Limiter
}

func NewRateLimitHandler(db *gorm.DB, limiter ratelimit.Limiter) *RateLimitHandler {
	return &RateLimitHandler{db: db, limiter: limiter}
}

func (h *RateLimitHandler) Show(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("user"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid user id")
	}
	ctx := c.Request().Context()
	var user model.User
	if err := h.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return queryError(c, err, "user not found")
	}

	uid := c.Param("user")
	api, err := h.limiter.Peek(ctx, "api:"+uid, ratelimit.PlanLimit(user.Plan), ratelimit.Window)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "rate limit store unavailable")
	}
	orders, err := h.limiter.Peek(ctx, "orders:"+uid, ratelimit.OrdersLimit, ratelimit.Window)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "rate limit store unavailable")
	}
	return c.JSON(http.StatusOK, echo.Map{
		"user_id": user.ID,
		"plan":    user.Plan,
		"api":     quota{api.Limit, api.Remaining, api.ResetAt.Unix()},
		"orders":  quota{orders.Limit, orders.Remaining, orders.ResetAt.Unix()},
	})
}
