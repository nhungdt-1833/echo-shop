package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var slidingScript = redis.NewScript(`
local now, window, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local n = redis.call('ZCARD', KEYS[1])
local allowed = 0
if limit > 0 and n < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  n = n + 1
  allowed = 1
end
redis.call('PEXPIRE', KEYS[1], window)
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')[2] or now
return {allowed, n, tonumber(oldest)}`)

type SlidingWindow struct {
	rdb *redis.Client
	now func() time.Time
}

func (s *SlidingWindow) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	return s.run(ctx, key, limit, window, limit)
}

func (s *SlidingWindow) Peek(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	return s.run(ctx, key, limit, window, 0)
}

func (s *SlidingWindow) run(ctx context.Context, key string, limit int, window time.Duration, scriptLimit int) (Result, error) {
	now := s.now()
	member := fmt.Sprintf("%d-%s", now.UnixNano(), randomSuffix())
	vals, err := slidingScript.Run(ctx, s.rdb, []string{"rl:sliding:" + key},
		now.UnixMilli(), window.Milliseconds(), scriptLimit, member).Int64Slice()
	if err != nil {
		return Result{}, err
	}
	allowed, n, oldest := vals[0] == 1, int(vals[1]), vals[2]
	if scriptLimit == 0 {
		allowed = n < limit
	}
	return Result{
		Allowed:   allowed,
		Limit:     limit,
		Remaining: remaining(limit, n),
		ResetAt:   time.UnixMilli(oldest).Add(window),
	}, nil
}

func randomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
