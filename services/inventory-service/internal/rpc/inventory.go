package rpc

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"echo-shop/inventory-service/internal/model"
)

var (
	ErrOutOfStock = errors.New("out of stock")
	ErrNotFound   = errors.New("product not found")
	ErrBadArgs    = errors.New("invalid arguments")
)

const (
	StatusReserved = "RESERVED"
	callTimeout    = 2 * time.Second
)

type ReserveArgs struct {
	ReservationID string `json:"reservation_id"`
	ProductID     uint   `json:"product_id"`
	Quantity      int    `json:"quantity"`
}

type ReserveReply struct {
	Price     int64 `json:"price"`
	Remaining int   `json:"remaining"`
}

type Inventory struct {
	db  *gorm.DB
	rdb *redis.Client
}

func New(db *gorm.DB, rdb *redis.Client) *Inventory {
	return &Inventory{db: db, rdb: rdb}
}

func (s *Inventory) Reserve(args ReserveArgs, reply *ReserveReply) error {
	if args.ReservationID == "" || args.ProductID == 0 || args.Quantity <= 0 {
		return ErrBadArgs
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	replayed := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.Reservation
		err := tx.First(&existing, "id = ?", args.ReservationID).Error
		if err == nil {
			replayed = true
			var p model.Product
			if err := tx.First(&p, existing.ProductID).Error; err != nil {
				return err
			}
			*reply = ReserveReply{Price: existing.Price, Remaining: p.Quantity}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		res := tx.Model(&model.Product{}).
			Where("id = ? AND quantity >= ?", args.ProductID, args.Quantity).
			Update("quantity", gorm.Expr("quantity - ?", args.Quantity))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			var count int64
			if err := tx.Model(&model.Product{}).Where("id = ?", args.ProductID).Count(&count).Error; err != nil {
				return err
			}
			if count == 0 {
				return ErrNotFound
			}
			return ErrOutOfStock
		}

		var p model.Product
		if err := tx.First(&p, args.ProductID).Error; err != nil {
			return err
		}
		if err := tx.Create(&model.Reservation{
			ID:        args.ReservationID,
			ProductID: args.ProductID,
			Quantity:  args.Quantity,
			Price:     p.Price,
			Status:    StatusReserved,
		}).Error; err != nil {
			return err
		}
		*reply = ReserveReply{Price: p.Price, Remaining: p.Quantity}
		return nil
	})
	if err != nil {
		if !errors.Is(err, ErrOutOfStock) && !errors.Is(err, ErrNotFound) {
			log.Printf("[rpc] reserve %s failed: %v", args.ReservationID, err)
		}
		return err
	}
	if replayed {
		log.Printf("[rpc] reserve %s replayed, stock untouched", args.ReservationID)
		return nil
	}

	id := strconv.FormatUint(uint64(args.ProductID), 10)
	if err := s.rdb.Del(ctx, "cache:/products", "cache:/products/"+id).Err(); err != nil {
		log.Println("delete cache failed:", err)
	}
	log.Printf("[rpc] reserve %s product=%d qty=%d remaining=%d",
		args.ReservationID, args.ProductID, args.Quantity, reply.Remaining)
	return nil
}
