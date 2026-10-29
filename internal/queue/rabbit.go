package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange = "orders"
	DLX      = "orders.dlx"
	DLQ      = "orders.dlq"

	QueuePayment = "payment"
	QueueEmail   = "email-queue"
	QueueNotify  = "notify"
	QueueKafka   = "kafka-bridge"

	MaxRetries = 3
)

var bindings = map[string][]string{
	QueuePayment: {KeyCreated},
	QueueEmail:   {KeyPaid},
	QueueNotify:  {"order.*"},
	QueueKafka:   {"order.*"},
}

type Rabbit struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	pub  *amqp.Channel
}

func DialRabbit(url string) (*Rabbit, error) {
	r := &Rabbit{url: url}
	if err := r.connect(); err != nil {
		return nil, err
	}
	if err := r.declare(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func (r *Rabbit) connect() error {
	if r.conn != nil {
		_ = r.conn.Close()
	}
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return err
	}
	r.conn, r.pub = conn, ch
	return nil
}

func (r *Rabbit) declare() error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(DLX, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(DLQ, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(DLQ, "", DLX, false, nil); err != nil {
		return err
	}

	args := amqp.Table{
		"x-queue-type":           "quorum",
		"x-delivery-limit":       int32(MaxRetries),
		"x-dead-letter-exchange": DLX,
	}
	for queue, keys := range bindings {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare %s: %w", queue, err)
		}
		for _, key := range keys {
			if err := ch.QueueBind(queue, key, Exchange, false, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Rabbit) Publish(ctx context.Context, ev OrderEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pub == nil || r.pub.IsClosed() {
		if err := r.connect(); err != nil {
			return err
		}
	}
	confirm, err := r.pub.PublishWithDeferredConfirmWithContext(ctx, Exchange, ev.Type, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    ev.EventID,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish %s: %w", ev.EventID, err)
	}
	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirm %s: %w", ev.EventID, err)
	}
	if !ok {
		return errors.New("rabbitmq refused " + ev.EventID)
	}
	return nil
}

func (r *Rabbit) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

func uintToString(v uint) string { return strconv.FormatUint(uint64(v), 10) }
