package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"job4j_go_share_trip_notification/internal/domain/notification/entity"
)

type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

var (
	ErrNotificationNotFound = errors.New("notification not found")
)

type NotificationRepository struct {
	db *pgxpool.Pool
}

func NewNotificationRepository(db *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{db: db}
}

func (r *NotificationRepository) Create(ctx context.Context, n *entity.Notification) error {
	return r.insert(ctx, r.db, n)
}

func (r *NotificationRepository) CreateTx(
	ctx context.Context,
	tx pgx.Tx,
	n *entity.Notification,
) error {
	return r.insert(ctx, tx, n)
}

func (r *NotificationRepository) insert(
	ctx context.Context,
	exec executor,
	n *entity.Notification,
) error {
	query := `
		INSERT INTO notifications (id, recipient_id, type, status, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := exec.Exec(ctx, query,
		n.ID,
		n.RecipientID,
		string(n.Type),
		string(n.Status),
		n.Payload,
		n.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create notification: %w", err)
	}

	return nil
}

func (r *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (*entity.Notification, error) {
	query := `
		SELECT id, recipient_id, type, status, payload, created_at
		FROM notifications
		WHERE id = $1
	`

	var n entity.Notification
	var payload json.RawMessage

	err := r.db.QueryRow(ctx, query, id).Scan(
		&n.ID,
		&n.RecipientID,
		&n.Type,
		&n.Status,
		&payload,
		&n.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotificationNotFound
		}
		return nil, fmt.Errorf("failed to get notification: %w", err)
	}

	n.Payload = payload

	return &n, nil
}
