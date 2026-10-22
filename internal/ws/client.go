package ws

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 512 * 1024
	sendBuffer     = 256
	authTimeout    = 2 * time.Second
)

type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	userID uint
	role   string
	rooms  map[string]struct{}

	expiresAt time.Time
}

func NewClient(hub *Hub, conn *websocket.Conn, userID uint, role string, expiresAt time.Time) *Client {
	return &Client{
		hub:       hub,
		conn:      conn,
		send:      make(chan []byte, sendBuffer),
		userID:    userID,
		role:      role,
		rooms:     map[string]struct{}{},
		expiresAt: expiresAt,
	}
}

func (c *Client) Serve(ctx context.Context) {
	if !c.hub.registerClient(c) {
		_ = c.conn.Close()
		return
	}
	go c.writePump()
	c.readPump(ctx)
}

func (c *Client) readPump(ctx context.Context) {
	defer func() {
		c.hub.unregisterClient(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("ws read user=%d: %v", c.userID, err)
			}
			return
		}
		var msg Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.hub.sendToClient(c, Message{Type: TypeError, Error: "invalid json"})
			continue
		}
		c.handle(ctx, msg)
	}
}

func (c *Client) handle(ctx context.Context, msg Message) {
	switch msg.Type {
	case TypeJoin:
		if !c.mayJoin(ctx, msg.Room) {
			c.hub.sendToClient(c, Message{Type: TypeError, Room: msg.Room, Error: "forbidden room"})
			return
		}
		c.hub.joinRoom(c, msg.Room)
	case TypeLeave:
		c.hub.leaveRoom(c, msg.Room)
	case TypeTyping:
		c.hub.relay(c, Message{Type: TypeTyping, Room: msg.Room, UserID: c.userID}, true)
	case TypeChat:
		text := strings.TrimSpace(msg.Text)
		if text == "" {
			c.hub.sendToClient(c, Message{Type: TypeError, Error: "empty message"})
			return
		}
		c.hub.relay(c, Message{Type: TypeChat, Room: msg.Room, UserID: c.userID, Text: text}, false)
	default:
		c.hub.sendToClient(c, Message{Type: TypeError, Error: "unknown type"})
	}
}

func (c *Client) mayJoin(ctx context.Context, room string) bool {
	if c.hub.authorize == nil || room == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, authTimeout)
	defer cancel()
	return c.hub.authorize(ctx, c.userID, c.role, room)
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	var expired <-chan time.Time
	if !c.expiresAt.IsZero() {
		timer := time.NewTimer(time.Until(c.expiresAt))
		defer timer.Stop()
		expired = timer.C
	}

	for {
		select {
		case data, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-expired:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "token expired"))
			return
		}
	}
}
