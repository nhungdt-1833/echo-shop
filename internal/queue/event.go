package queue

import (
	"time"

	"echo-shop/internal/model"
)

const (
	KeyCreated = "order.created"
	KeyPaid    = "order.paid"
)

type OrderEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	OrderID   uint      `json:"order_id"`
	UserID    uint      `json:"user_id"`
	ProductID uint      `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Total     int64     `json:"total"`
	Status    string    `json:"status"`
	At        time.Time `json:"at"`
}

func NewOrderEvent(typ string, o model.Order) OrderEvent {
	return OrderEvent{
		EventID:   typ + ":" + uintToString(o.ID),
		Type:      typ,
		OrderID:   o.ID,
		UserID:    o.UserID,
		ProductID: o.ProductID,
		Quantity:  o.Quantity,
		Total:     o.Total,
		Status:    o.Status,
		At:        time.Now(),
	}
}
