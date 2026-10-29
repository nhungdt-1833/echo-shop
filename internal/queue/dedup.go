package queue

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const dedupTTL = 7 * 24 * time.Hour

type Dedup struct {
	rdb *redis.Client
}

func NewDedup(rdb *redis.Client) *Dedup { return &Dedup{rdb: rdb} }

func key(consumer, eventID string) string { return "processed:" + consumer + ":" + eventID }

func (d *Dedup) Seen(ctx context.Context, consumer, eventID string) bool {
	if d == nil {
		return false
	}
	n, err := d.rdb.Exists(ctx, key(consumer, eventID)).Result()
	if err != nil {
		log.Printf("dedup check failed (processing anyway): %v", err)
		return false
	}
	return n > 0
}

func (d *Dedup) Mark(ctx context.Context, consumer, eventID string) {
	if d == nil {
		return
	}
	if err := d.rdb.Set(ctx, key(consumer, eventID), 1, dedupTTL).Err(); err != nil {
		log.Printf("dedup mark failed: %v", err)
	}
}
