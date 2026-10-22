package model

import (
	"gorm.io/gorm"
)

type Order struct {
	gorm.Model

	UserID    uint    `json:"user_id"`
	ProductID uint    `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Total     int64  `json:"total"`
	Status    string `json:"status" gorm:"not null;default:PENDING"`
}
