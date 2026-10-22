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
	EventTypeTripPublished          EventType = "trip_published"
	EventTypeTripStarted            EventType = "trip_started"
	EventTypeTripPreapproved        EventType = "trip_preapproved"
	EventTypeTripCanceled           EventType = "trip_canceled"
	EventTypeTripPublishNotificated EventType = "trip_publish_notificated"
	EventTypeRequestPreconfirmed    EventType = "request_preconfirmed"
	EventTypeRequestConfirmed       EventType = "request_confirmed"
	EventTypeRequestRejected        EventType = "request_rejected"
	EventTypeRequestCompleted       EventType = "request_completed"
	EventTypeRequestExpired         EventType = "request_expired"
)

type TripPublished struct {
	EventID       string    `json:"event_id"`
	EventType     EventType `json:"event_type"`
	TripID        string    `json:"trip_id"`
	DriverID      string    `json:"driver_id"`
	CompanyID     string    `json:"company_id"`
	RequestID     string    `json:"request_id,omitempty"`
	PassengerID   string    `json:"passenger_id,omitempty"`
	PassengerIDs  []string  `json:"passenger_ids,omitempty"`
	RequestIDs    []string  `json:"request_ids,omitempty"`
	FromPoint     string    `json:"from_point,omitempty"`
	ToPoint       string    `json:"to_point,omitempty"`
	DepartureTime time.Time `json:"departure_time,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	Traceparent   string    `json:"traceparent,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
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

	applyMetadata(&event, metadataFromHeaders(msg))

	if !supportedEventType(event.EventType) {
		slog.Info("skip unsupported event type",
			slog.String("service", "notification"),
			slog.String("operation", "ConsumeTripEvent"),
			slog.String("event_type", string(event.EventType)),
			slog.String("event_id", event.EventID),
			slog.String("correlation_id", event.CorrelationID),
		)
		return c.commit(ctx, msg)
	}

	operation := consumeOperation(event.EventType)
	slog.Info("consume trip event started",
		slog.String("service", "notification"),
		slog.String("operation", operation),
		slog.String("event_id", event.EventID),
		slog.String("correlation_id", event.CorrelationID),
		slog.String("causation_id", event.CausationID),
		slog.String("trace_id", TraceIDFromParent(event.Traceparent)),
		slog.String("result", "started"),
	)

	for {
		if err := handler(ctx, event); err != nil {
			slog.Error("failed to process trip event",
				slog.String("service", "notification"),
				slog.String("operation", operation),
				slog.String("event_id", event.EventID),
				slog.String("correlation_id", event.CorrelationID),
				slog.String("causation_id", event.CausationID),
				slog.String("trace_id", TraceIDFromParent(event.Traceparent)),
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

func supportedEventType(eventType EventType) bool {
	switch eventType {
	case EventTypeTripPublished,
		EventTypeTripStarted,
		EventTypeTripPreapproved,
		EventTypeTripCanceled,
		EventTypeTripPublishNotificated,
		EventTypeRequestPreconfirmed,
		EventTypeRequestConfirmed,
		EventTypeRequestRejected,
		EventTypeRequestCompleted,
		EventTypeRequestExpired:
		return true
	default:
		return false
	}
}

func consumeOperation(eventType EventType) string {
	switch eventType {
	case EventTypeTripStarted:
		return "ConsumeTripStarted"
	case EventTypeTripPreapproved:
		return "ConsumeTripPreapproved"
	case EventTypeTripCanceled:
		return "ConsumeTripCanceled"
	case EventTypeTripPublishNotificated:
		return "ConsumeTripPublishNotificated"
	case EventTypeRequestPreconfirmed:
		return "ConsumeRequestPreconfirmed"
	case EventTypeRequestConfirmed:
		return "ConsumeRequestConfirmed"
	case EventTypeRequestRejected:
		return "ConsumeRequestRejected"
	case EventTypeRequestCompleted:
		return "ConsumeRequestCompleted"
	case EventTypeRequestExpired:
		return "ConsumeRequestExpired"
	default:
		return "ConsumeTripPublished"
	}
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
