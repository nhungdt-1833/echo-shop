package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
}

type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error)
	Peek(ctx context.Context, key string, limit int, window time.Duration) (Result, error)
}

func New(algo string, rdb *redis.Client) (Limiter, error) {
	switch algo {
	case "fixed":
		return &FixedWindow{rdb: rdb, now: time.Now}, nil
	case "sliding", "":
		return &SlidingWindow{rdb: rdb, now: time.Now}, nil
	}
	return nil, fmt.Errorf("unknown RATE_LIMIT_ALGO %q (fixed|sliding)", algo)
}

func remaining(limit, used int) int {
	return max(0, limit-used)
}
