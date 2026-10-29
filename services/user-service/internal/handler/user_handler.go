package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/shared/auth"
	"echo-shop/user-service/internal/model"
)

type UserHandler struct {
	db     *gorm.DB
	secret []byte
	rdb    *redis.Client
}

func NewUserHandler(db *gorm.DB, secret []byte, rdb *redis.Client) *UserHandler {
	return &UserHandler{db: db, secret: secret, rdb: rdb}
}

func (h *UserHandler) User(c echo.Context) error {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid user id")
	}
	claims, ok := auth.ClaimsFrom(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
	}
	if claims.Role != model.RoleAdmin && uint(id) != claims.UserID {
		return echo.NewHTTPError(http.StatusForbidden, "cannot read another user")
	}

	var user model.User
	err = h.db.WithContext(c.Request().Context()).First(&user, id).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return echo.NewHTTPError(http.StatusNotFound, "user not found")
	case err != nil:
		c.Logger().Errorf("get user: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
	return c.JSON(http.StatusOK, user)
}
