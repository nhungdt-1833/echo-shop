package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"echo-shop/internal/model"
	"echo-shop/internal/queue"
	"echo-shop/internal/ws"
)

func event(typ string, orderID, productID uint, qty int) queue.OrderEvent {
	o := model.Order{UserID: 2, ProductID: productID, Quantity: qty, Total: 1000}
	o.ID = orderID
	o.Status = map[string]string{queue.KeyCreated: model.OrderPending, queue.KeyPaid: model.OrderPaid}[typ]
	return queue.NewOrderEvent(typ, o)
}

func TestAnalyticsCountsOnceEvenWhenReplayed(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	a, ctx := NewAnalytics(rdb, "test"), context.Background()

	stream := []queue.OrderEvent{
		event(queue.KeyCreated, 1, 3, 2),
		event(queue.KeyCreated, 2, 5, 1),
		event(queue.KeyPaid, 1, 3, 2),
	}
	for round := 0; round < 2; round++ {
		for _, ev := range stream {
			counted, err := a.Apply(ctx, ev)
			if err != nil {
				t.Fatal(err)
			}
			if counted != (round == 0) {
				t.Fatalf("round %d %s counted=%v", round, ev.EventID, counted)
			}
		}
	}

	if got := mr.HGet(AnalyticsStatusKey, model.OrderPending); got != "1" {
		t.Fatalf("PENDING = %s, want 1 (order 2)", got)
	}
	if got := mr.HGet(AnalyticsStatusKey, model.OrderPaid); got != "1" {
		t.Fatalf("PAID = %s, want 1 (order 1)", got)
	}
	if score, _ := mr.ZScore(TopProductsKey, "3"); score != 2 {
		t.Fatalf("product 3 sold %v, want 2", score)
	}
}

func TestNotifyEvents(t *testing.T) {
	created := NotifyEvents(event(queue.KeyCreated, 7, 3, 1))
	if len(created) != 2 || created[0].Target != ws.TargetRole || created[0].Key != model.RoleAdmin {
		t.Fatalf("order.created must notify admins first: %+v", created)
	}
	paid := NotifyEvents(event(queue.KeyPaid, 7, 3, 1))
	var m ws.Message
	_ = json.Unmarshal(paid[0].Data, &m)
	if paid[0].Target != ws.TargetUser || paid[0].UserID != 2 || m.Status != model.OrderPaid || m.OrderID != 7 {
		t.Fatalf("order.paid must tell the owner PAID: %+v %+v", paid[0], m)
	}
	if NotifyEvents(queue.OrderEvent{Type: "order.unknown"}) != nil {
		t.Fatal("unknown events cause no notification")
	}
}
