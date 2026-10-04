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
	if event.DriverID == "" {
		result = "error"
		return fmt.Errorf("missing driver_id")
	}

	payload, err := json.Marshal(map[string]any{
		"event_id":       event.EventID,
		"trip_id":        event.TripID,
		"driver_id":      event.DriverID,
		"company_id":     event.CompanyID,
		"correlation_id": event.CorrelationID,
		"causation_id":   event.CausationID,
		"traceparent":    event.Traceparent,
		"occurred_at":    event.OccurredAt,
	})
	if err != nil {
		result = "error"
		return fmt.Errorf("marshal payload: %w", err)
	}

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

		notificationType := entity.TypeTripPublished
		if event.EventType == EventTypeTripStarted {
			notificationType = entity.TypeTripStarted
		}

		notification := entity.NewNotification(
			event.DriverID,
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
			slog.String("result", "sent"),
		)

		return nil
	})
	if err != nil {
		result = "error"
	}
	return err
}
