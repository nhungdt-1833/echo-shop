package model

import (
	"gorm.io/gorm"
)

type User struct {
	gorm.Model

	Name     string `json:"name"`
	Password string `json:"-"`
	Email    string `json:"email" gorm:"uniqueIndex;not null"`
	Role     string `json:"role" gorm:"not null;default:customer"`
}
