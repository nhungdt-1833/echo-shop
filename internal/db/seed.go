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
		{Name: "Leo", Email: "leo@echo-shop.vn", Role: model.RoleCustomer},
	}
	for _, u := range users {
		u.Password = string(hash)
		if err := gdb.Where(model.User{Email: u.Email}).FirstOrCreate(&u).Error; err != nil {
			return fmt.Errorf("seed %s: %w", u.Email, err)
		}
	}
	return nil
}
