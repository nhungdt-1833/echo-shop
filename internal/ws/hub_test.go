package ws

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func newTestHub(t *testing.T, authorize RoomAuthorizer) *Hub {
	t.Helper()
	h := NewHub(authorize, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)
	t.Cleanup(func() { cancel(); <-h.Done() })
	return h
}

func newTestClient(t *testing.T, h *Hub, userID uint, role string, buffer int) *Client {
	t.Helper()
	c := &Client{id: newClientID(), hub: h, send: make(chan []byte, buffer), userID: userID, role: role, rooms: map[string]struct{}{}}
	if !h.registerClient(c) {
		t.Fatal("hub stopped")
	}
	return c
}

func recv(t *testing.T, c *Client) Message {
	t.Helper()
	select {
	case data, ok := <-c.send:
		if !ok {
			t.Fatalf("user %d: send channel closed", c.userID)
		}
		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		return m
	case <-time.After(time.Second):
		t.Fatalf("user %d: no message", c.userID)
	}
	return Message{}
}

func expectNext(t *testing.T, c *Client, wantType string) Message {
	t.Helper()
	m := recv(t, c)
	if m.Type != wantType {
		t.Fatalf("user %d: got %q, want %q (%+v)", c.userID, m.Type, wantType, m)
	}
	return m
}

func TestSendToUserReachesEveryTabOfThatUserOnly(t *testing.T) {
	h := newTestHub(t, nil)
	claraTab1 := newTestClient(t, h, 2, "customer", sendBuffer)
	claraTab2 := newTestClient(t, h, 2, "customer", sendBuffer)
	leo := newTestClient(t, h, 3, "customer", sendBuffer)

	h.SendToUser(2, Message{Type: TypeOrderStatus, OrderID: 5, Status: "PAID"})
	if m := expectNext(t, claraTab1, TypeOrderStatus); m.Status != "PAID" {
		t.Fatalf("got %+v", m)
	}
	expectNext(t, claraTab2, TypeOrderStatus)

	h.SendToUser(3, Message{Type: TypeNewOrder})
	expectNext(t, leo, TypeNewOrder)
}

func TestSendToRole(t *testing.T) {
	h := newTestHub(t, nil)
	admin := newTestClient(t, h, 1, "admin", sendBuffer)
	clara := newTestClient(t, h, 2, "customer", sendBuffer)

	h.SendToRole("admin", Message{Type: TypeNewOrder, OrderID: 9})
	expectNext(t, admin, TypeNewOrder)

	h.SendToUser(2, Message{Type: TypeOrderStatus})
	expectNext(t, clara, TypeOrderStatus)
}

func TestSlowClientIsDroppedNotBlocking(t *testing.T) {
	h := newTestHub(t, nil)
	slow := newTestClient(t, h, 2, "customer", 1)
	fast := newTestClient(t, h, 3, "customer", sendBuffer)

	h.SendToUser(2, Message{Type: TypeChat})
	h.SendToUser(2, Message{Type: TypeChat})
	h.SendToUser(3, Message{Type: TypeChat})
	expectNext(t, fast, TypeChat)

	<-slow.send
	if _, ok := <-slow.send; ok {
		t.Fatal("slow client should have been closed")
	}
	h.SendToUser(2, Message{Type: TypeChat})
}

func TestRoomChatOnlyForMembers(t *testing.T) {
	h := newTestHub(t, nil)
	clara := newTestClient(t, h, 2, "customer", sendBuffer)
	admin := newTestClient(t, h, 1, "admin", sendBuffer)
	leo := newTestClient(t, h, 3, "customer", sendBuffer)

	h.joinRoom(clara, "order:5")
	expectNext(t, clara, TypeJoin)
	h.joinRoom(admin, "order:5")
	expectNext(t, clara, TypeJoin)
	expectNext(t, admin, TypeJoin)

	h.relay(leo, Message{Type: TypeChat, Room: "order:5", UserID: 3, Text: "hi"}, false)
	if m := expectNext(t, leo, TypeError); m.Error != "join the room first" {
		t.Fatalf("got %+v", m)
	}

	h.relay(clara, Message{Type: TypeChat, Room: "order:5", UserID: 2, Text: "khi nào giao?"}, false)
	if m := expectNext(t, admin, TypeChat); m.UserID != 2 || m.Text != "khi nào giao?" {
		t.Fatalf("got %+v", m)
	}
	expectNext(t, clara, TypeChat)

	h.relay(clara, Message{Type: TypeTyping, Room: "order:5", UserID: 2}, true)
	expectNext(t, admin, TypeTyping)
	h.SendToUser(2, Message{Type: TypeNewOrder})
	expectNext(t, clara, TypeNewOrder)
}

func TestDisconnectLeavesRoomsAndNotifiesOthers(t *testing.T) {
	h := newTestHub(t, nil)
	clara := newTestClient(t, h, 2, "customer", sendBuffer)
	admin := newTestClient(t, h, 1, "admin", sendBuffer)
	h.joinRoom(clara, "order:5")
	h.joinRoom(admin, "order:5")
	expectNext(t, clara, TypeJoin)
	expectNext(t, clara, TypeJoin)
	expectNext(t, admin, TypeJoin)

	h.unregisterClient(clara)
	if m := expectNext(t, admin, TypeLeave); m.UserID != 2 {
		t.Fatalf("got %+v", m)
	}
	if _, ok := <-clara.send; ok {
		t.Fatal("clara's send channel should be closed")
	}
	h.unregisterClient(clara)
}

func TestJoinRequiresAuthorization(t *testing.T) {
	allowOnlyOwnOrder := func(_ context.Context, userID uint, _ string, room string) bool {
		return userID == 2 && room == "order:5"
	}
	h := newTestHub(t, allowOnlyOwnOrder)
	leo := newTestClient(t, h, 3, "customer", sendBuffer)
	clara := newTestClient(t, h, 2, "customer", sendBuffer)

	leo.handle(context.Background(), Message{Type: TypeJoin, Room: "order:5"})
	if m := expectNext(t, leo, TypeError); m.Error != "forbidden room" {
		t.Fatalf("got %+v", m)
	}
	clara.handle(context.Background(), Message{Type: TypeJoin, Room: "order:5"})
	expectNext(t, clara, TypeJoin)
}

func TestClientCannotSpoofUserID(t *testing.T) {
	h := newTestHub(t, func(context.Context, uint, string, string) bool { return true })
	clara := newTestClient(t, h, 2, "customer", sendBuffer)
	clara.handle(context.Background(), Message{Type: TypeJoin, Room: "order:5"})
	expectNext(t, clara, TypeJoin)

	clara.handle(context.Background(), Message{Type: TypeChat, Room: "order:5", UserID: 1, Text: "I am admin"})
	if m := expectNext(t, clara, TypeChat); m.UserID != 2 {
		t.Fatalf("user_id must come from the token, got %d", m.UserID)
	}
}
