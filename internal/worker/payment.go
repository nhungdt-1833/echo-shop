package worker

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"time"

	"gorm.io/gorm"

	"echo-shop/internal/model"
	"echo-shop/internal/queue"
)

type eventPublisher interface {
	Publish(ctx context.Context, ev queue.OrderEvent) error
}

type Payment struct {
	db       *gorm.DB
	mq       eventPublisher
	failRate float64
}

func NewPayment(db *gorm.DB, mq eventPublisher, failRate float64) *Payment {
	return &Payment{db: db, mq: mq, failRate: failRate}
}

func (p *Payment) Handle(ctx context.Context, ev queue.OrderEvent) error {
	var order model.Order
	if err := p.db.WithContext(ctx).First(&order, ev.OrderID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("[payment] order #%d not found, skip", ev.OrderID)
			return nil
		}
		return err
	}
	if order.Status != model.OrderPending {
		log.Printf("[payment] order #%d already %s, skip", order.ID, order.Status)
		return nil
	}

	if err := p.charge(order); err != nil {
		return err
	}

	return p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&order).Where("status = ?", model.OrderPending).Update("status", model.OrderPaid)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}
		order.Status = model.OrderPaid
		if err := p.mq.Publish(ctx, queue.NewOrderEvent(queue.KeyPaid, order)); err != nil {
			return err
		}
		log.Printf("[payment] order #%d PAID (%dđ)", order.ID, order.Total)
		return nil
	})
}

func (p *Payment) charge(order model.Order) error {
	time.Sleep(200 * time.Millisecond)
	if rand.Float64() < p.failRate {
		return errors.New("payment gateway error (simulated)")
	}
	return nil
}
