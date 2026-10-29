package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	prefetch   = 10
	retryDelay = 500 * time.Millisecond
)

type Handler func(ctx context.Context, ev OrderEvent) error

func (r *Rabbit) Consume(ctx context.Context, queue string, dedup *Dedup, h Handler) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf("[%s] waiting for messages", queue)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("rabbitmq channel closed")
			}
			handle(ctx, queue, msg, dedup, h)
		}
	}
}

func handle(ctx context.Context, queue string, msg amqp.Delivery, dedup *Dedup, h Handler) {
	var ev OrderEvent
	if err := json.Unmarshal(msg.Body, &ev); err != nil || ev.EventID == "" {
		log.Printf("[%s] poison message → %s: %v", queue, DLQ, err)
		_ = msg.Nack(false, false)
		return
	}
	if dedup.Seen(ctx, queue, ev.EventID) {
		log.Printf("[%s] %s already processed, skip", queue, ev.EventID)
		_ = msg.Ack(false)
		return
	}

	if err := h(ctx, ev); err != nil {
		attempt := deliveryCount(msg) + 1
		if attempt > MaxRetries {
			log.Printf("[%s] %s failed (attempt %d), giving up → %s: %v", queue, ev.EventID, attempt, DLQ, err)
		} else {
			log.Printf("[%s] %s failed (attempt %d), retry: %v", queue, ev.EventID, attempt, err)
		}
		time.Sleep(retryDelay)
		_ = msg.Nack(false, true)
		return
	}

	dedup.Mark(ctx, queue, ev.EventID)
	_ = msg.Ack(false)
}

func deliveryCount(msg amqp.Delivery) int64 {
	switch v := msg.Headers["x-delivery-count"].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	}
	return 0
}
