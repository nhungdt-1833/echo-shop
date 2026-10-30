package model

import (
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model

	Name        string `json:"name"`
	Description string `json:"description"`
	Price       int64  `json:"price"`
	Quantity    int    `json:"quantity"`
}
