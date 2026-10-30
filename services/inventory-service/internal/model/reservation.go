package model

import (
	"time"
)

type Reservation struct {
	ID        string `gorm:"primaryKey"`
	ProductID uint   `gorm:"not null"`
	Quantity  int    `gorm:"not null"`
	Price     int64  `gorm:"not null"`
	Status    string `gorm:"not null"`
	CreatedAt time.Time
}
