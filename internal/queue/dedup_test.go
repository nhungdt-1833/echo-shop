package queue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"echo-shop/internal/model"
)

func TestDedupSeenOnlyAfterMark(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	d, ctx := NewDedup(rdb), context.Background()

	if d.Seen(ctx, "email", "order.paid:1") {
		t.Fatal("new event must not be seen")
	}
	d.Mark(ctx, "email", "order.paid:1")
	if !d.Seen(ctx, "email", "order.paid:1") {
		t.Fatal("marked event must be seen")
	}
	if d.Seen(ctx, "notify", "order.paid:1") {
		t.Fatal("each consumer has its own memory")
	}
	if ttl := mr.TTL("processed:email:order.paid:1"); ttl != dedupTTL {
		t.Fatalf("ttl = %v", ttl)
	}

	mr.Close()
	if d.Seen(ctx, "email", "order.paid:2") {
		t.Fatal("Redis error must not skip the message")
	}
}

func TestEventIDIsDeterministic(t *testing.T) {
	o := model.Order{UserID: 2, ProductID: 3, Quantity: 1, Total: 100, Status: model.OrderPaid}
	o.ID = 12
	a, b := NewOrderEvent(KeyPaid, o), NewOrderEvent(KeyPaid, o)
	if a.EventID != "order.paid:12" || a.EventID != b.EventID {
		t.Fatalf("got %q and %q", a.EventID, b.EventID)
	}
}
