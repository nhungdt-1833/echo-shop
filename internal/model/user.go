package model

import (
	"gorm.io/gorm"
)

const (
	RoleAdmin    = "admin"
	RoleCustomer = "customer"

	PlanFree = "free"
	PlanPro  = "pro"
)

type User struct {
	gorm.Model

	Name     string `json:"name"`
	Password string `json:"-"`
	Email    string `json:"email" gorm:"uniqueIndex;not null"`
	Role     string `json:"role" gorm:"not null;default:customer"`
	Plan     string `json:"plan" gorm:"not null;default:free"` // Task 5: free 60 req/min, pro 600 req/min
}
