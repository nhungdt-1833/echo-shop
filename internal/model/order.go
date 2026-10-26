package model

import (
	"gorm.io/gorm"
)

const (
	OrderPending   = "PENDING"
	OrderPaid      = "PAID"
	OrderShipped   = "SHIPPED"
	OrderDelivered = "DELIVERED"
	OrderCancelled = "CANCELLED"
)

func IsValidOrderStatus(s string) bool {
	switch s {
	case OrderPending, OrderPaid, OrderShipped, OrderDelivered, OrderCancelled:
		return true
	}
	return false
}

type Order struct {
	gorm.Model

	UserID    uint   `json:"user_id"`
	ProductID uint   `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Total     int64  `json:"total"`
	Status    string `json:"status" gorm:"not null;default:PENDING"`
}
