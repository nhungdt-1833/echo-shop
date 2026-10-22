package handler

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"echo-shop/internal/auth"
	"echo-shop/internal/model"
)

const tokenTTL = time.Hour

var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)

type loginReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthHandler struct {
	db     *gorm.DB
	secret []byte
}

func NewAuthHandler(db *gorm.DB, secret []byte) *AuthHandler {
	return &AuthHandler{db: db, secret: secret}
}

func (h *AuthHandler) Login(c echo.Context) error {
	var req loginReq
	if err := c.Bind(&req); err != nil || req.Email == "" || req.Password == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "email and password are required")
	}

	var user model.User
	err := h.db.WithContext(c.Request().Context()).
		Where("email = ?", req.Email).First(&user).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(req.Password))
		return invalidCredentials()
	case err != nil:
		c.Logger().Errorf("login query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}

	if bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)) != nil {
		return invalidCredentials()
	}

	token, err := auth.GenerateToken(h.secret, user.ID, user.Role, tokenTTL)
	if err != nil {
		c.Logger().Errorf("sign token: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
	return c.JSON(http.StatusOK, echo.Map{
		"token":      token,
		"expires_in": int(tokenTTL.Seconds()),
	})
}

func (h *AuthHandler) Me(c echo.Context) error {
	claims, ok := auth.ClaimsFrom(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
	}

	var user model.User
	err := h.db.WithContext(c.Request().Context()).First(&user, claims.UserID).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return echo.NewHTTPError(http.StatusUnauthorized, "user no longer exists")
	case err != nil:
		c.Logger().Errorf("me query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
	return c.JSON(http.StatusOK, user)
}

func invalidCredentials() error {
	return echo.NewHTTPError(http.StatusUnauthorized, "invalid email or password")
}
