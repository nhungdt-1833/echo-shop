package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/joho/godotenv"

	"echo-shop/internal/cache"
	"echo-shop/internal/db"
	"echo-shop/internal/queue"
	"echo-shop/internal/worker"
)

func main() {
	_ = godotenv.Load()
	if len(os.Args) < 2 {
		log.Fatal("usage: worker <payment|email|notify|kafka-bridge|analytics>")
	}
	name := os.Args[1]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, name); err != nil {
		log.Fatalf("[%s] %v", name, err)
	}
	log.Printf("[%s] stopped", name)
}

func run(ctx context.Context, name string) error {
	rdb := cache.NewRedis(env("REDIS_ADDR", "localhost:6379"), os.Getenv("REDIS_PASSWORD"))
	defer rdb.Close()
	broker := env("KAFKA_BROKER", "localhost:9092")

	if name == "analytics" {
		if err := queue.EnsureTopic(ctx, broker); err != nil {
			return fmt.Errorf("kafka topic: %w", err)
		}
		r := queue.NewAnalyticsReader(broker)
		defer r.Close()
		host, _ := os.Hostname()
		return worker.NewAnalytics(rdb, fmt.Sprintf("%s-%d", host, os.Getpid())).Run(ctx, r)
	}

	mq, err := queue.DialRabbit(env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		return err
	}
	defer mq.Close()
	dedup := queue.NewDedup(rdb)

	switch name {
	case "payment":
		gdb, err := db.Connect(os.Getenv("DATABASE_URL"))
		if err != nil {
			return err
		}
		failRate, _ := strconv.ParseFloat(env("PAYMENT_FAIL_RATE", "0.3"), 64)
		return mq.Consume(ctx, queue.QueuePayment, dedup, worker.NewPayment(gdb, mq, failRate).Handle)
	case "email":
		return mq.Consume(ctx, queue.QueueEmail, dedup, worker.Email{}.Handle)
	case "notify":
		return mq.Consume(ctx, queue.QueueNotify, dedup, worker.NewNotify(rdb).Handle)
	case "kafka-bridge":
		if err := queue.EnsureTopic(ctx, broker); err != nil {
			return fmt.Errorf("kafka topic: %w", err)
		}
		w := queue.NewWriter(broker)
		defer w.Close()
		return mq.Consume(ctx, queue.QueueKafka, dedup, worker.NewKafkaBridge(w).Handle)
	}
	return fmt.Errorf("unknown worker %q", name)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
