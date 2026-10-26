package wsbus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"echo-shop/internal/ws"
)

type recorder chan ws.Event

func (r recorder) Deliver(e ws.Event) { r <- e }

func startBus(t *testing.T, addr string) (*RedisBus, recorder) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	bus := NewRedisBus(rdb, Channel)
	got := make(recorder, 16)
	ctx, cancel := context.WithCancel(context.Background())
	go bus.Run(ctx, got)
	t.Cleanup(func() { cancel(); <-bus.Done(); _ = rdb.Close() })
	return bus, got
}

func waitEvent(t *testing.T, r recorder) ws.Event {
	t.Helper()
	select {
	case e := <-r:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("no event")
	}
	return ws.Event{}
}

func waitSubscribers(t *testing.T, mr *miniredis.Miniredis, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for mr.PubSubNumSub(Channel)[Channel] < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d subscribers", mr.PubSubNumSub(Channel)[Channel])
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEventReachesEveryInstance(t *testing.T) {
	mr := miniredis.RunT(t)
	bus8080, got8080 := startBus(t, mr.Addr())
	_, got8081 := startBus(t, mr.Addr())
	waitSubscribers(t, mr, 2)

	data, _ := json.Marshal(ws.Message{Type: ws.TypeOrderStatus, OrderID: 5, Status: "PAID"})
	bus8080.Publish(ws.Event{Target: ws.TargetUser, UserID: 2, Data: data})

	for name, r := range map[string]recorder{"8080 (sender)": got8080, "8081": got8081} {
		if e := waitEvent(t, r); e.UserID != 2 || string(e.Data) != string(data) {
			t.Fatalf("%s got %+v", name, e)
		}
	}
}

func TestRedisDownStillDeliversLocally(t *testing.T) {
	mr := miniredis.RunT(t)
	bus, got := startBus(t, mr.Addr())
	waitSubscribers(t, mr, 1)
	mr.Close()

	bus.Publish(ws.Event{Target: ws.TargetRole, Key: "admin", Data: json.RawMessage(`{}`)})
	if e := waitEvent(t, got); e.Key != "admin" {
		t.Fatalf("got %+v", e)
	}
}
