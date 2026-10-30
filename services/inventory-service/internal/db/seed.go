package db

import (
	"fmt"

	"gorm.io/gorm"

	"echo-shop/inventory-service/internal/model"
)

func SeedProducts(gdb *gorm.DB) error {
	products := []model.Product{
		{Name: "Áo thun", Description: "Cotton 100%", Price: 250000, Quantity: 100},
		{Name: "Cốc", Description: "Sứ 350ml", Price: 75000, Quantity: 50},
		{Name: "Sticker", Description: "Bộ 10 sticker", Price: 25000, Quantity: 500},
	}
	for _, p := range products {
		if err := gdb.Where(model.Product{Name: p.Name}).FirstOrCreate(&p).Error; err != nil {
			return fmt.Errorf("seed product %s: %w", p.Name, err)
		}
	}
	return nil
}
