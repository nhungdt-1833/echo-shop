package worker

import (
	"context"

	"github.com/redis/go-redis/v9"

	"echo-shop/internal/model"
	"echo-shop/internal/queue"
	"echo-shop/internal/ws"
	"echo-shop/internal/wsbus"
)

type Notify struct {
	rdb *redis.Client
}

func NewNotify(rdb *redis.Client) *Notify { return &Notify{rdb: rdb} }

func (n *Notify) Handle(ctx context.Context, ev queue.OrderEvent) error {
	for _, e := range NotifyEvents(ev) {
		if err := wsbus.Send(ctx, n.rdb, e); err != nil {
			return err
		}
	}
	return nil
}

func NotifyEvents(ev queue.OrderEvent) []ws.Event {
	status := ws.Message{Type: ws.TypeOrderStatus, OrderID: ev.OrderID, Status: ev.Status}
	switch ev.Type {
	case queue.KeyCreated:
		return []ws.Event{
			ws.RoleEvent(model.RoleAdmin, ws.Message{
				Type: ws.TypeNewOrder, OrderID: ev.OrderID, UserID: ev.UserID, Total: ev.Total, Status: ev.Status,
			}),
			ws.UserEvent(ev.UserID, status),
		}
	case queue.KeyPaid:
		return []ws.Event{
			ws.UserEvent(ev.UserID, status),
			ws.RoleEvent(model.RoleAdmin, status),
		}
	}
	return nil
}
