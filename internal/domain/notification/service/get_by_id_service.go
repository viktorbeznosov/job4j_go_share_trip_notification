package service

import (
	"context"
	"job4j_go_share_trip_notification/internal/domain/notification/entity"

	"github.com/google/uuid"
)

func (s *NotificationService) GetByID(ctx context.Context, id uuid.UUID) (*entity.Notification, error) {
	return s.notificationRepository.GetByID(ctx, id)
}


