package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"

	"echo-shop/internal/auth"
	"echo-shop/internal/model"
	"echo-shop/internal/ws"
)

func newWSServer(t *testing.T) (hub *ws.Hub, wsURL string, secret []byte) {
	t.Helper()
	secret = []byte(strings.Repeat("s", 32))
	hub = ws.NewHub(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)

	e := echo.New()
	e.GET("/ws", NewWSHandler(hub, secret, nil, []string{"http://allowed.test"}).Connect)
	srv := httptest.NewServer(e)
	t.Cleanup(func() { cancel(); <-hub.Done(); srv.Close() })
	return hub, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=", secret
}

func TestWSRejectsBadTokenBeforeUpgrade(t *testing.T) {
	_, url, _ := newWSServer(t)
	_, resp, err := websocket.DefaultDialer.Dial(url+"not-a-jwt", nil)
	if err == nil {
		t.Fatal("expected handshake to fail")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %v", resp)
	}
}

var allowedOrigin = http.Header{"Origin": {"http://allowed.test"}}

func TestWSRejectsForeignOrMissingOrigin(t *testing.T) {
	_, url, secret := newWSServer(t)
	token, _ := auth.GenerateToken(secret, 2, model.RoleCustomer, time.Minute)
	for name, header := range map[string]http.Header{
		"foreign": {"Origin": {"http://evil.test"}},
		"missing": nil,
	} {
		_, resp, err := websocket.DefaultDialer.Dial(url+token, header)
		if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s origin: want 403, got err=%v resp=%v", name, err, resp)
		}
	}
}

func TestWSClosesWith1008WhenTokenExpires(t *testing.T) {
	_, url, secret := newWSServer(t)
	token, _ := auth.GenerateToken(secret, 2, model.RoleCustomer, time.Second)
	conn, _, err := websocket.DefaultDialer.Dial(url+token, allowedOrigin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.ClosePolicyViolation || closeErr.Text != "token expired" {
		t.Fatalf("want close 1008 \"token expired\", got %v", err)
	}
}

func TestWSNoGoroutineLeakAfterAbruptDisconnect(t *testing.T) {
	_, url, secret := newWSServer(t)
	token, _ := auth.GenerateToken(secret, 2, model.RoleCustomer, time.Minute)
	before := runtime.NumGoroutine()

	const n = 20
	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conn, _, err := websocket.DefaultDialer.Dial(url+token, allowedOrigin)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn
	}
	waitGoroutines(t, func(g int) bool { return g >= before+2*n }, "clients to start")

	for _, conn := range conns {
		_ = conn.UnderlyingConn().Close()
	}
	waitGoroutines(t, func(g int) bool { return g <= before }, "goroutines to return to baseline")
}

func waitGoroutines(t *testing.T, ok func(int) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok(runtime.NumGoroutine()) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s (now %d goroutines)", what, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWSDeliversTargetedMessage(t *testing.T) {
	hub, url, secret := newWSServer(t)
	token, _ := auth.GenerateToken(secret, 2, model.RoleCustomer, time.Minute)
	conn, _, err := websocket.DefaultDialer.Dial(url+token, allowedOrigin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	got := make(chan ws.Message, 1)
	go func() {
		var m ws.Message
		if conn.ReadJSON(&m) == nil {
			got <- m
		}
	}()

	deadline := time.After(2 * time.Second)
	for {
		hub.SendToUser(2, ws.Message{Type: ws.TypeOrderStatus, OrderID: 5, Status: model.OrderPaid})
		select {
		case m := <-got:
			if m.OrderID != 5 || m.Status != model.OrderPaid {
				t.Fatalf("got %+v", m)
			}
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("message never arrived")
		}
	}
}
