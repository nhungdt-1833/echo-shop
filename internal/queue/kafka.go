package queue

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

const (
	Topic           = "order-events"
	AnalyticsGroup  = "analytics"
	topicPartitions = 3
)

func EnsureTopic(ctx context.Context, broker string) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cconn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cconn.Close()

	err = cconn.CreateTopics(kafka.TopicConfig{Topic: Topic, NumPartitions: topicPartitions, ReplicationFactor: 1})
	if errors.Is(err, kafka.TopicAlreadyExists) {
		return nil
	}
	return err
}

func NewWriter(broker string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
}

func NewAnalyticsReader(broker string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       Topic,
		GroupID:     AnalyticsGroup,
		StartOffset: kafka.FirstOffset,
	})
}
