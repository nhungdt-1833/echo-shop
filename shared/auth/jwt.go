package auth

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	RoleAdmin    = "admin"
	RoleCustomer = "customer"
)

type Claims struct {
	UserID uint   `json:"user_id"`
	Role   string `json:"role"`
	Plan   string `json:"plan"`
	jwt.RegisteredClaims
}

func GenerateToken(secret []byte, userID uint, role, plan string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID: userID,
		Role:   role,
		Plan:   plan,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        newJTI(),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

func ParseToken(secret []byte, tokenString string) (*Claims, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(tokenString, claims,
		func(*jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(), // a token without exp would never expire
	)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func newJTI() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
