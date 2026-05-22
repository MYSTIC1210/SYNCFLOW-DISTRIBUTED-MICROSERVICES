// cmd/worker/main.go — standalone worker binary entrypoint.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MYSTIC1210/syncflow/db"
	"github.com/MYSTIC1210/syncflow/telemetry"
	"github.com/MYSTIC1210/syncflow/worker"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// OpenTelemetry
	shutdown, err := telemetry.Init(ctx, "syncflow-worker")
	if err != nil {
		log.Fatalf("[worker] otel init: %v", err)
	}
	defer shutdown(ctx)

	// Database
	pool, err := db.Connect(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("[worker] db connect: %v", err)
	}
	defer pool.Close()

	// RabbitMQ
	conn, err := amqp.Dial(mustEnv("RABBITMQ_URL"))
	if err != nil {
		log.Fatalf("[worker] amqp dial: %v", err)
	}
	defer conn.Close()

	// Worker pool
	n := envInt("WORKER_COUNT", 8)
	p, err := worker.New(conn, n)
	if err != nil {
		log.Fatalf("[worker] pool init: %v", err)
	}

	// Register task handlers
	p.Register("echo", func(ctx context.Context, t worker.Task) (string, error) {
		return string(t.Payload), nil
	})
	p.Register("noop", func(ctx context.Context, t worker.Task) (string, error) {
		return "ok", nil
	})

	// Drain results → PostgreSQL
	go func() {
		for res := range p.Results() {
			if err := db.SaveTaskResult(ctx, pool, db.TaskRow{
				ID:         res.TaskID,
				Status:     res.Status,
				Output:     res.Output,
				DurationMs: res.Duration,
			}); err != nil {
				log.Printf("[worker] save result: %v", err)
			}
		}
	}()

	log.Printf("[worker] starting %d workers", n)
	if err := p.Run(ctx); err != nil {
		log.Printf("[worker] pool stopped: %v", err)
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("required env var %s not set", k)
	}
	return v
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
