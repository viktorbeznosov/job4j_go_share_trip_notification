package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type EventType string

const (
	EventTypeTripPublished EventType = "trip_published"
	EventTypeTripStarted   EventType = "trip_started"
)

type TripPublished struct {
	EventID    string    `json:"event_id"`
	EventType  EventType `json:"event_type"`
	TripID     string    `json:"trip_id"`
	DriverID   string    `json:"driver_id"`
	CompanyID  string    `json:"company_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader messageReader
}

func NewConsumer(brokers []string, topic string, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			StartOffset:    kafka.FirstOffset,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
		}),
	}
}

func (c *Consumer) Run(
	ctx context.Context,
	handler func(context.Context, TripPublished) error,
) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			slog.Error("failed to fetch kafka message", slog.Any("error", err))
			if !sleep(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}

		if err := c.process(ctx, msg, handler); err != nil {
			return err
		}
	}
}

func (c *Consumer) process(
	ctx context.Context,
	msg kafka.Message,
	handler func(context.Context, TripPublished) error,
) error {
	var event TripPublished
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		slog.Error("skip invalid kafka message", slog.Any("error", err))
		return c.commit(ctx, msg)
	}

	if event.EventType != EventTypeTripPublished {
		slog.Info("skip unsupported event type",
			slog.String("event_type", string(event.EventType)),
		)
		return c.commit(ctx, msg)
	}

	for {
		if err := handler(ctx, event); err != nil {
			slog.Error("failed to process trip event",
				slog.String("event_id", event.EventID),
				slog.Any("error", err),
			)
			if !sleep(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		break
	}

	return c.commit(ctx, msg)
}

func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		slog.Error("failed to commit kafka offset", slog.Any("error", err))
		return err
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
