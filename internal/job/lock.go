package job

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

type Locker struct {
	rdb *redis.Client
}

func NewLocker(rdb *redis.Client) *Locker { return &Locker{rdb: rdb} }

func (l *Locker) TryLock(ctx context.Context, name string, ttl time.Duration) (bool, error) {
	return l.rdb.SetNX(ctx, "lock:job:"+name, token(), ttl).Result()
}

func token() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
