package handler

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"echo-shop/shared/auth"
	"echo-shop/user-service/internal/model"
)

const (
	tokenTTL   = time.Hour
	refreshTTL = 7 * 24 * time.Hour
)

var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)

type loginReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

type AuthHandler struct {
	db     *gorm.DB
	secret []byte
	rdb    *redis.Client
}

func NewAuthHandler(db *gorm.DB, secret []byte, rdb *redis.Client) *AuthHandler {
	return &AuthHandler{db: db, secret: secret, rdb: rdb}
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

	return h.issueTokens(c, user)
}

func (h *AuthHandler) Refresh(c echo.Context) error {
	var req refreshReq
	if err := c.Bind(&req); err != nil || req.RefreshToken == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "refresh_token is required")
	}

	ctx := c.Request().Context()
	val, err := h.rdb.GetDel(ctx, "refresh:"+req.RefreshToken).Result()
	switch {
	case errors.Is(err, redis.Nil): // wrong, expired or already used
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid refresh token")
	case err != nil:
		log.Println("read refresh token failed:", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "try again later")
	}
	userID, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid refresh token")
	}

	var user model.User
	if err := h.db.WithContext(ctx).First(&user, userID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusUnauthorized, "user no longer exists")
		}
		c.Logger().Errorf("refresh query: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
	return h.issueTokens(c, user)
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

func (h *AuthHandler) Logout(c echo.Context) error {
	claims, ok := auth.ClaimsFrom(c)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
	}
	ttl := time.Until(claims.ExpiresAt.Time)
	ctx := c.Request().Context()
	if err := h.rdb.Set(ctx, "blacklist:"+claims.ID, 1, ttl).Err(); err != nil {
		log.Println("logout failed:", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "logout failed, try again")
	}

	var req refreshReq
	if c.Bind(&req) == nil && req.RefreshToken != "" {
		if err := h.rdb.Del(ctx, "refresh:"+req.RefreshToken).Err(); err != nil {
			log.Println("delete refresh token failed:", err)
		}
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *AuthHandler) issueTokens(c echo.Context, user model.User) error {
	token, err := auth.GenerateToken(h.secret, user.ID, user.Role, user.Plan, tokenTTL)
	if err != nil {
		c.Logger().Errorf("sign token: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
	resp := echo.Map{"token": token, "expires_in": int(tokenTTL.Seconds())}

	refresh := newRefreshToken()
	ctx := c.Request().Context()
	if err := h.rdb.Set(ctx, "refresh:"+refresh, user.ID, refreshTTL).Err(); err != nil {
		log.Println("save refresh token failed:", err)
	} else {
		resp["refresh_token"] = refresh
	}
	return c.JSON(http.StatusOK, resp)
}

func invalidCredentials() error {
	return echo.NewHTTPError(http.StatusUnauthorized, "invalid email or password")
}

func newRefreshToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
