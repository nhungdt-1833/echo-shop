package ws

import (
	"context"
	"testing"
)

// memoryBus imitates Redis Pub/Sub: every published event is handed, in
// order, to every attached hub (= every server instance), sender included.
type memoryBus struct {
	events chan Event
	hubs   []*Hub
}

func newMemoryBus(t *testing.T) *memoryBus {
	b := &memoryBus{events: make(chan Event, 256)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-b.events:
				for _, h := range b.hubs {
					h.Deliver(e)
				}
			}
		}
	}()
	return b
}

func (b *memoryBus) Publish(e Event) { b.events <- e }

// twoInstances returns two hubs that only know about each other through the bus.
func twoInstances(t *testing.T) (*Hub, *Hub) {
	bus := newMemoryBus(t)
	allowAll := func(context.Context, uint, string, string) bool { return true }
	a, b := NewHub(allowAll, bus), NewHub(allowAll, bus)
	bus.hubs = []*Hub{a, b} // set before Run starts: no race
	for _, h := range bus.hubs {
		ctx, cancel := context.WithCancel(context.Background())
		go h.Run(ctx)
		t.Cleanup(func() { cancel(); <-h.Done() })
	}
	return a, b
}

func TestNotificationReachesClientOnOtherInstance(t *testing.T) {
	i8080, i8081 := twoInstances(t)
	admin := newTestClient(t, i8080, 1, "admin", sendBuffer)
	clara := newTestClient(t, i8081, 2, "customer", sendBuffer) // connected to the OTHER instance
	leo := newTestClient(t, i8080, 3, "customer", sendBuffer)

	// Admin changes the status through instance 8080...
	i8080.SendToUser(2, Message{Type: TypeOrderStatus, OrderID: 5, Status: "PAID"})
	if m := expectNext(t, clara, TypeOrderStatus); m.Status != "PAID" { // ...clara on 8081 gets it
		t.Fatalf("got %+v", m)
	}

	i8081.SendToRole("admin", Message{Type: TypeNewOrder, OrderID: 6})
	expectNext(t, admin, TypeNewOrder)

	i8080.SendToUser(3, Message{Type: TypeChat}) // marker
	expectNext(t, leo, TypeChat)                 // leo saw neither message
}

func TestRoomChatAcrossInstances(t *testing.T) {
	i8080, i8081 := twoInstances(t)
	admin := newTestClient(t, i8080, 1, "admin", sendBuffer)
	clara := newTestClient(t, i8081, 2, "customer", sendBuffer)

	clara.handle(context.Background(), Message{Type: TypeJoin, Room: "order:5"})
	expectNext(t, clara, TypeJoin)
	admin.handle(context.Background(), Message{Type: TypeJoin, Room: "order:5"})
	expectNext(t, admin, TypeJoin)
	if m := expectNext(t, clara, TypeJoin); m.UserID != 1 { // clara sees admin join from 8080
		t.Fatalf("got %+v", m)
	}

	clara.handle(context.Background(), Message{Type: TypeChat, Room: "order:5", Text: "khi nào giao?"})
	if m := expectNext(t, admin, TypeChat); m.UserID != 2 || m.Text != "khi nào giao?" {
		t.Fatalf("got %+v", m)
	}
	expectNext(t, clara, TypeChat)

	clara.handle(context.Background(), Message{Type: TypeTyping, Room: "order:5"})
	expectNext(t, admin, TypeTyping)
	i8081.SendToUser(2, Message{Type: TypeNewOrder}) // marker
	expectNext(t, clara, TypeNewOrder)               // typing not echoed to its sender

	i8081.unregisterClient(clara)
	if m := expectNext(t, admin, TypeLeave); m.UserID != 2 {
		t.Fatalf("got %+v", m)
	}
}
