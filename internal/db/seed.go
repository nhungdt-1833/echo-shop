package db

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"echo-shop/internal/model"
)

func SeedUsers(gdb *gorm.DB, password string) error {
	if password == "" {
		return fmt.Errorf("seed password is empty")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash seed password: %w", err)
	}

	users := []model.User{
		{Name: "Admin", Email: "admin@echo-shop.vn", Role: model.RoleAdmin},
		{Name: "Clara", Email: "clara@echo-shop.vn", Role: model.RoleCustomer},
		{Name: "Leo", Email: "leo@echo-shop.vn", Role: model.RoleCustomer, Plan: model.PlanPro},
	}
	for _, u := range users {
		u.Password = string(hash)
		if err := gdb.Where(model.User{Email: u.Email}).FirstOrCreate(&u).Error; err != nil {
			return fmt.Errorf("seed %s: %w", u.Email, err)
		}
	}
	return nil
}

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
