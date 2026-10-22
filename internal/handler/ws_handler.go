package handler

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"

	"echo-shop/internal/auth"
	"echo-shop/internal/ws"
)

type WSHandler struct {
	hub      *ws.Hub
	secret   []byte
	upgrader websocket.Upgrader
}

func NewWSHandler(hub *ws.Hub, secret []byte, allowedOrigins []string) *WSHandler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	return &WSHandler{
		hub:    hub,
		secret: secret,
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

	conn, err := h.upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return nil
	}

	client := ws.NewClient(h.hub, conn, claims.UserID, claims.Role, claims.ExpiresAt.Time)
	client.Serve(c.Request().Context())
	return nil
}
