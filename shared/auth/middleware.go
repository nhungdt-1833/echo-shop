package auth

import (
	"log"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
)

const claimsKey = "claims"

func JWTAuth(secret []byte) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			header := c.Request().Header.Get(echo.HeaderAuthorization)
			tokenString, ok := strings.CutPrefix(header, "Bearer ")
			if !ok || tokenString == "" {
				return echo.NewHTTPError(http.StatusUnauthorized, "missing bearer token")
			}

			claims, err := ParseToken(secret, tokenString)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid or expired token")
			}

			c.Set(claimsKey, claims)
			return next(c)
		}
	}
}

func RequireRole(role string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			claims, ok := ClaimsFrom(c)
			if !ok {
				return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
			}
			if claims.Role != role {
				return echo.NewHTTPError(http.StatusForbidden, "forbidden")
			}
			return next(c)
		}
	}
}

func ClaimsFrom(c echo.Context) (*Claims, bool) {
	claims, ok := c.Get(claimsKey).(*Claims)
	return claims, ok
}

func CheckBlackList(rdb *redis.Client) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			claims, ok := ClaimsFrom(c)
			if !ok {
				return echo.NewHTTPError(http.StatusUnauthorized, "missing auth")
			}
			ctx := c.Request().Context()
			key := "blacklist:" + claims.ID
			found, err := rdb.Exists(ctx, key).Result()
			if err != nil {
				log.Println("check blacklist failed:", err)
				return next(c)
			}
			if found > 0 {
				return echo.NewHTTPError(http.StatusUnauthorized, "token revoked")
			}
			return next(c)
		}
	}
}
