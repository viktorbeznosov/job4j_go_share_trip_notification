package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"job4j_go_share_trip_notification/internal/domain/notification/entity"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/storage"
)

type Processor struct {
	pool            *pgxpool.Pool
	notifications   *repository.NotificationRepository
	processedEvents *repository.ProcessedEventRepository
}

func NewProcessor(
	pool *pgxpool.Pool,
	notifications *repository.NotificationRepository,
	processedEvents *repository.ProcessedEventRepository,
) *Processor {
	return &Processor{
		pool:            pool,
		notifications:   notifications,
		processedEvents: processedEvents,
	}
}

func (p *Processor) Handle(ctx context.Context, event TripPublished) error {
	if event.EventID == "" {
		return fmt.Errorf("missing event_id")
	}
	if event.DriverID == "" {
		return fmt.Errorf("missing driver_id")
	}

	payload, err := json.Marshal(map[string]any{
		"event_id":    event.EventID,
		"trip_id":     event.TripID,
		"driver_id":   event.DriverID,
		"company_id":  event.CompanyID,
		"occurred_at": event.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	return storage.TxWithoutResult(ctx, p.pool, func(tx pgx.Tx) error {
		inserted, err := p.processedEvents.TryInsert(ctx, tx, event.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			slog.Info("skip already processed event",
				slog.String("event_id", event.EventID),
			)
			return nil
		}

		notification := entity.NewNotification(
			event.DriverID,
			entity.TypeTripPublished,
			payload,
		)
		if err := p.notifications.CreateTx(ctx, tx, notification); err != nil {
			return err
		}

		slog.Info("created notification from trip event",
			slog.String("event_id", event.EventID),
			slog.String("trip_id", event.TripID),
			slog.String("notification_id", notification.ID.String()),
		)

		return nil
	})
}
