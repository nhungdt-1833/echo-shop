package worker

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/segmentio/kafka-go"

	"echo-shop/internal/queue"
)

type KafkaBridge struct {
	w *kafka.Writer
}

func NewKafkaBridge(w *kafka.Writer) *KafkaBridge { return &KafkaBridge{w: w} }

func (k *KafkaBridge) Handle(ctx context.Context, ev queue.OrderEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return k.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatUint(uint64(ev.OrderID), 10)),
		Value: body,
	})
}
