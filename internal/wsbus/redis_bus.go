// Package wsbus syncs WebSocket hubs of several server instances through
// Redis Pub/Sub (doc 4.4): every instance publishes to one channel and every
// instance (sender included) delivers what it receives to its local clients.
package wsbus

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"echo-shop/internal/ws"
)

const (
	Channel        = "ws:events"
	publishTimeout = time.Second
	queueSize      = 1024
)

// Deliverer is the local side, implemented by *ws.Hub.
type Deliverer interface {
	Deliver(e ws.Event)
}

type RedisBus struct {
	rdb     *redis.Client
	channel string
	out     chan ws.Event
	done    chan struct{}
}

func NewRedisBus(rdb *redis.Client, channel string) *RedisBus {
	return &RedisBus{
		rdb:     rdb,
		channel: channel,
		out:     make(chan ws.Event, queueSize),
		done:    make(chan struct{}),
	}
}

// Publish never blocks (Hub.Run calls it). A full queue drops the event.
func (b *RedisBus) Publish(e ws.Event) {
	select {
	case b.out <- e:
	default:
		log.Printf("wsbus: queue full, dropping %s event", e.Target)
	}
}

// Run publishes queued events and delivers received ones until ctx is done.
// One goroutine does both, so events keep the order they were published in.
func (b *RedisBus) Run(ctx context.Context, local Deliverer) {
	defer close(b.done)
	sub := b.rdb.Subscribe(ctx, b.channel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil { // wait for SUBSCRIBE confirmation
		log.Printf("wsbus: subscribe %s: %v (go-redis keeps retrying)", b.channel, err)
	}
	incoming := sub.Channel() // reconnects automatically

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-incoming:
			if !ok {
				return
			}
			var e ws.Event
			if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
				log.Printf("wsbus: bad payload: %v", err)
				continue
			}
			local.Deliver(e)
		case e := <-b.out:
			b.publish(ctx, e, local)
		}
	}
}

func (b *RedisBus) publish(ctx context.Context, e ws.Event, local Deliverer) {
	data, _ := json.Marshal(e) // Event has only plain fields
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := b.rdb.Publish(ctx, b.channel, data).Err(); err != nil {
		// Redis down: other instances miss it, but our own clients still get it.
		log.Printf("wsbus: publish failed, delivering locally only: %v", err)
		local.Deliver(e)
	}
}

func (b *RedisBus) Done() <-chan struct{} { return b.done }

func Send(ctx context.Context, rdb *redis.Client, e ws.Event) error {
	data, _ := json.Marshal(e) // Event has only plain fields
	return rdb.Publish(ctx, Channel, data).Err()
}
