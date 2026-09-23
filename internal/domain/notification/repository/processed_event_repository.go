package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type ProcessedEventRepository struct{}

func NewProcessedEventRepository() *ProcessedEventRepository {
	return &ProcessedEventRepository{}
}

func (r *ProcessedEventRepository) TryInsert(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
) (bool, error) {
	query := `
		INSERT INTO processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`

	tag, err := tx.Exec(ctx, query, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to insert processed event: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
