package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"job4j_go_share_trip_notification/internal/domain/notification/entity"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/observability/metrics"
	"job4j_go_share_trip_notification/internal/storage"
)

type Processor struct {
	pool            *pgxpool.Pool
	notifications   *repository.NotificationRepository
	processedEvents *repository.ProcessedEventRepository
	metrics         *metrics.Metrics
}

func NewProcessor(
	pool *pgxpool.Pool,
	notifications *repository.NotificationRepository,
	processedEvents *repository.ProcessedEventRepository,
	m *metrics.Metrics,
) *Processor {
	return &Processor{
		pool:            pool,
		notifications:   notifications,
		processedEvents: processedEvents,
		metrics:         m,
	}
}

func (p *Processor) Handle(ctx context.Context, event TripPublished) error {
	started := time.Now()
	result := "success"
	defer func() {
		p.metrics.ConsumeTotal.WithLabelValues(result).Inc()
		p.metrics.ConsumeDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
	}()

	if event.EventID == "" {
		result = "error"
		return fmt.Errorf("missing event_id")
	}
	if err := validateEvent(event); err != nil {
		result = "error"
		return err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		result = "error"
		return fmt.Errorf("marshal payload: %w", err)
	}

	recipients, notificationType := recipientsFor(event)

	err = storage.TxWithoutResult(ctx, p.pool, func(tx pgx.Tx) error {
		inserted, err := p.processedEvents.TryInsert(ctx, tx, event.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			p.metrics.InboxDuplicate.Inc()
			slog.Info("inbox duplicate skipped",
				slog.String("service", "notification"),
				slog.String("operation", "InboxInsert"),
				slog.String("event_id", event.EventID),
				slog.String("correlation_id", event.CorrelationID),
				slog.String("causation_id", event.CausationID),
				slog.String("trace_id", TraceIDFromParent(event.Traceparent)),
				slog.String("result", "duplicate"),
			)
			return nil
		}

		slog.Info("inbox event created",
			slog.String("service", "notification"),
			slog.String("operation", "InboxInsert"),
			slog.String("event_id", event.EventID),
			slog.String("correlation_id", event.CorrelationID),
			slog.String("causation_id", event.CausationID),
			slog.String("trace_id", TraceIDFromParent(event.Traceparent)),
			slog.String("result", "created"),
		)

		for _, recipientID := range recipients {
			notification := entity.NewNotification(
				recipientID,
				notificationType,
				payload,
			)
			if err := p.notifications.CreateTx(ctx, tx, notification); err != nil {
				p.metrics.SendTotal.WithLabelValues("error").Inc()
				return err
			}
			p.metrics.SendTotal.WithLabelValues("success").Inc()
			slog.Info("notification sent",
				slog.String("service", "notification"),
				slog.String("operation", "SendNotification"),
				slog.String("event_id", event.EventID),
				slog.String("correlation_id", event.CorrelationID),
				slog.String("causation_id", event.CausationID),
				slog.String("trace_id", TraceIDFromParent(event.Traceparent)),
				slog.String("trip_id", event.TripID),
				slog.String("notification_id", notification.ID.String()),
				slog.String("recipient_id", recipientID),
				slog.String("result", "sent"),
			)
		}

		return nil
	})
	if err != nil {
		result = "error"
	}
	return err
}

func validateEvent(event TripPublished) error {
	switch event.EventType {
	case EventTypeRequestCompleted, EventTypeRequestExpired, EventTypeRequestConfirmed, EventTypeRequestRejected:
		if event.PassengerID == "" {
			return fmt.Errorf("missing passenger_id")
		}
	case EventTypeRequestPreconfirmed:
		if event.DriverID == "" {
			return fmt.Errorf("missing driver_id")
		}
	case EventTypeTripCanceled, EventTypeTripPublishNotificated:
		return nil
	default:
		if event.DriverID == "" {
			return fmt.Errorf("missing driver_id")
		}
	}
	return nil
}

func recipientsFor(event TripPublished) ([]string, entity.Type) {
	switch event.EventType {
	case EventTypeTripStarted:
		return []string{event.DriverID}, entity.TypeTripStarted
	case EventTypeTripPreapproved:
		return []string{event.DriverID}, entity.TypeTripPreapproved
	case EventTypeTripCanceled:
		return uniqueNonEmpty(event.PassengerIDs), entity.TypeTripCanceled
	case EventTypeTripPublishNotificated:
		return uniqueNonEmpty(event.PassengerIDs), entity.TypeTripPublishNotificated
	case EventTypeRequestPreconfirmed:
		return []string{event.DriverID}, entity.TypeRequestPreconfirmed
	case EventTypeRequestConfirmed:
		return []string{event.PassengerID}, entity.TypeRequestConfirmed
	case EventTypeRequestRejected:
		return []string{event.PassengerID}, entity.TypeRequestRejected
	case EventTypeRequestCompleted:
		return []string{event.PassengerID}, entity.TypeRequestCompleted
	case EventTypeRequestExpired:
		return []string{event.PassengerID}, entity.TypeRequestExpired
	default:
		return []string{event.DriverID}, entity.TypeTripPublished
	}
}

func uniqueNonEmpty(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
