package auth

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
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
