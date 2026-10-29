package worker

import (
	"context"
	"log"

	"echo-shop/internal/queue"
)

type Email struct{}

func (Email) Handle(_ context.Context, ev queue.OrderEvent) error {
	log.Printf("[email] 📧 order_confirm → user #%d: order #%d paid, total %dđ (event %s)",
		ev.UserID, ev.OrderID, ev.Total, ev.EventID)
	return nil
}
