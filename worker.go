// worker/worker.go — SyncFlow RabbitMQ Worker Pool
// Consumes tasks from the "syncflow.tasks" queue using N goroutine workers.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	Queue      = "syncflow.tasks"
	Exchange   = "syncflow.exchange"
	RoutingKey = "task.*"
	MaxRetries = 3
)

// Task is the JSON payload published by the API gateway.
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	Attempt   int             `json:"attempt"`
}

// Result is stored back to PostgreSQL after processing.
type Result struct {
	TaskID    string    `json:"task_id"`
	Status    string    `json:"status"` // success | failed | retrying
	Output    string    `json:"output"`
	Duration  int64     `json:"duration_ms"`
	Timestamp time.Time `json:"timestamp"`
}

// Handler processes a single task type.
type Handler func(ctx context.Context, task Task) (string, error)

// Pool manages N concurrent worker goroutines.
type Pool struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	workers  int
	handlers map[string]Handler
	results  chan Result
	wg       sync.WaitGroup
}

func New(conn *amqp.Connection, workers int) (*Pool, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	// Declare exchange + queue + binding
	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare(Queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": "syncflow.dlx",
	})
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, RoutingKey, Exchange, false, nil); err != nil {
		return nil, err
	}
	if err := ch.Qos(workers, 0, false); err != nil {
		return nil, err
	}

	return &Pool{
		conn:     conn,
		ch:       ch,
		workers:  workers,
		handlers: make(map[string]Handler),
		results:  make(chan Result, 256),
	}, nil
}

// Register adds a handler for a specific task type.
func (p *Pool) Register(taskType string, h Handler) {
	p.handlers[taskType] = h
}

// Run starts N workers and blocks until ctx is cancelled.
func (p *Pool) Run(ctx context.Context) error {
	msgs, err := p.ch.Consume(Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.work(ctx, msgs, i)
	}

	log.Printf("[pool] %d workers running on queue %q", p.workers, Queue)
	<-ctx.Done()
	p.wg.Wait()
	return nil
}

// Results returns the result channel (drain to persist to DB).
func (p *Pool) Results() <-chan Result { return p.results }

func (p *Pool) work(ctx context.Context, msgs <-chan amqp.Delivery, id int) {
	defer p.wg.Done()
	tr := otel.Tracer("syncflow-worker")

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			ctx, span := tr.Start(ctx, "process-task")
			span.SetAttributes(attribute.Int("worker.id", id))

			var task Task
			if err := json.Unmarshal(d.Body, &task); err != nil {
				log.Printf("[worker-%d] bad payload: %v", id, err)
				d.Nack(false, false)
				span.End()
				continue
			}

			start := time.Now()
			res := p.dispatch(ctx, task)
			res.Duration = time.Since(start).Milliseconds()

			select {
			case p.results <- res:
			default:
				log.Printf("[worker-%d] result buffer full, dropping %s", id, task.ID)
			}

			if res.Status == "success" {
				d.Ack(false)
			} else if task.Attempt >= MaxRetries {
				d.Nack(false, false) // → DLX
			} else {
				d.Nack(false, true) // requeue
			}
			span.End()
		}
	}
}

func (p *Pool) dispatch(ctx context.Context, task Task) Result {
	h, ok := p.handlers[task.Type]
	if !ok {
		return Result{TaskID: task.ID, Status: "failed", Output: "no handler for type " + task.Type, Timestamp: time.Now()}
	}
	out, err := h(ctx, task)
	if err != nil {
		return Result{TaskID: task.ID, Status: "failed", Output: err.Error(), Timestamp: time.Now()}
	}
	return Result{TaskID: task.ID, Status: "success", Output: out, Timestamp: time.Now()}
}
