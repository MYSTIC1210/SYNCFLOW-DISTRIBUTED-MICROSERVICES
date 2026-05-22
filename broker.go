// internal/broker/broker.go — RabbitMQ connection wrapper + task publisher.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"

	"github.com/MYSTIC1210/syncflow/worker"
)

const (
	Exchange   = "syncflow.exchange"
	RoutingKey = "task.default"
)

// Connection wraps an AMQP connection + publish channel.
type Connection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// Dial opens a connection and a dedicated publish channel.
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	// Ensure exchange exists
	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange: %w", err)
	}
	return &Connection{conn: conn, ch: ch}, nil
}

// PublishTask serialises a Task and publishes it to the exchange.
// Returns the generated task ID.
func (c *Connection) PublishTask(ctx context.Context, taskType string, payload json.RawMessage) (string, error) {
	id := uuid.NewString()
	task := worker.Task{
		ID:        id,
		Type:      taskType,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
		Attempt:   0,
	}
	body, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("marshal task: %w", err)
	}
	err = c.ch.PublishWithContext(ctx, Exchange, RoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    id,
		Timestamp:    time.Now(),
		Body:         body,
	})
	if err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}
	return id, nil
}

// Close shuts down the channel and connection cleanly.
func (c *Connection) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
