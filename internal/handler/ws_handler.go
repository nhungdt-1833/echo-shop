package handler

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	"echo-shop/internal/auth"
	"echo-shop/internal/ws"
)

type WSHandler struct {
	hub      *ws.Hub
	secret   []byte
	rdb      *redis.Client // blacklist check; nil = skip (tests)
	upgrader websocket.Upgrader
}

func NewWSHandler(hub *ws.Hub, secret []byte, rdb *redis.Client, allowedOrigins []string) *WSHandler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	return &WSHandler{
		hub:    hub,
		secret: secret,
		rdb:    rdb,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return allowed[r.Header.Get("Origin")]
			},
		},
	}
}

func (h *WSHandler) Connect(c echo.Context) error {
	claims, err := auth.ParseToken(h.secret, c.QueryParam("token"))
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid or expired token")
	}
	// Same rule as CheckBlackList: a logged-out token cannot open a socket.
	if h.rdb != nil {
		found, err := h.rdb.Exists(c.Request().Context(), "blacklist:"+claims.ID).Result()
		if err != nil {
			log.Println("check blacklist failed:", err) // fail-open
		} else if found > 0 {
			return echo.NewHTTPError(http.StatusUnauthorized, "token revoked")
		}
	}

	conn, err := h.upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return nil
	}

	client := ws.NewClient(h.hub, conn, claims.UserID, claims.Role, claims.ExpiresAt.Time)
	client.Serve(c.Request().Context())
	return nil
}
