package worker

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"echo-shop/internal/model"
	"echo-shop/internal/queue"
)

const (
	AnalyticsSeenKey   = "analytics:seen"
	AnalyticsStatusKey = "analytics:orders_by_status"
	TopProductsKey     = "products:top"
)

var countScript = redis.NewScript(`
if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then return 0 end
if ARGV[2] ~= '' then redis.call('HINCRBY', KEYS[2], ARGV[2], -1) end
redis.call('HINCRBY', KEYS[2], ARGV[3], 1)
if ARGV[4] ~= '' then redis.call('ZINCRBY', KEYS[3], ARGV[5], ARGV[4]) end
return 1`)

type Analytics struct {
	rdb  *redis.Client
	name string
}

func NewAnalytics(rdb *redis.Client, name string) *Analytics {
	return &Analytics{rdb: rdb, name: name}
}

func (a *Analytics) Run(ctx context.Context, r *kafka.Reader) error {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var ev queue.OrderEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("[analytics %s] bad message at offset %d, skip: %v", a.name, msg.Offset, err)
		} else {
			for {
				counted, err := a.Apply(ctx, ev)
				if err == nil {
					log.Printf("[analytics %s] partition=%d offset=%d %s counted=%v",
						a.name, msg.Partition, msg.Offset, ev.EventID, counted)
					break
				}
				log.Printf("[analytics %s] apply %s failed, retry: %v", a.name, ev.EventID, err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Second):
				}
			}
		}
		if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			return err
		}
	}
}

func (a *Analytics) Apply(ctx context.Context, ev queue.OrderEvent) (bool, error) {
	var from, to, product, qty string
	switch ev.Type {
	case queue.KeyCreated:
		to = model.OrderPending
	case queue.KeyPaid:
		from, to = model.OrderPending, model.OrderPaid
		product, qty = strconv.FormatUint(uint64(ev.ProductID), 10), strconv.Itoa(ev.Quantity)
	default:
		return false, nil
	}
	n, err := countScript.Run(ctx, a.rdb,
		[]string{AnalyticsSeenKey, AnalyticsStatusKey, TopProductsKey},
		ev.EventID, from, to, product, qty).Int()
	return n == 1, err
}
