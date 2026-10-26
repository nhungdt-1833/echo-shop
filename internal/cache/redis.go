package cache

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedis(addr, password string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		PoolSize:     20,
		DialTimeout:  300 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Println(err)
	}
	return rdb
}

func demo(ctx context.Context, rdb *redis.Client) {
	rdb.Set(ctx, "user:1:name", "Alice", 10*time.Minute)
	val, err := rdb.Get(ctx, "user:1:name").Result()
	switch {
	case err == redis.Nil:
		fmt.Println("key không tồn tại") // cache miss
	case err != nil:
		log.Println("redis error:", err)
	default:
		fmt.Println(val)
	}
}
