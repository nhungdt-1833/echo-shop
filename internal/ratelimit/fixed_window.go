package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var incrScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n`)

type FixedWindow struct {
	rdb *redis.Client
	now func() time.Time
}

func (f *FixedWindow) redisKey(key string, window time.Duration) (string, time.Time) {
	start := f.now().Truncate(window)
	return "rl:fixed:" + key + ":" + strconv.FormatInt(start.Unix(), 10), start
}

func (f *FixedWindow) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	rk, start := f.redisKey(key, window)
	n, err := incrScript.Run(ctx, f.rdb, []string{rk}, window.Milliseconds()).Int()
	if err != nil {
		return Result{}, err
	}
	return Result{Allowed: n <= limit, Limit: limit, Remaining: remaining(limit, n), ResetAt: start.Add(window)}, nil
}

func (f *FixedWindow) Peek(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	rk, start := f.redisKey(key, window)
	n, err := f.rdb.Get(ctx, rk).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Result{}, err
	}
	return Result{Allowed: n < limit, Limit: limit, Remaining: remaining(limit, n), ResetAt: start.Add(window)}, nil
}
