package service

import (
	"context"
	"encoding/json"
	"fmt"
	"job4j_go_share_trip_notification/internal/domain/notification/entity"
)

func (s *NotificationService) Create(ctx context.Context, recipientID string, notificationType entity.Type, payload json.RawMessage) (*entity.Notification, error) {
	notification := entity.NewNotification(recipientID, notificationType, payload)

	if err := s.notificationRepository.Create(ctx, notification); err != nil {
		return nil, fmt.Errorf("failed to create notification: %w", err)
	}

	return notification, nil
}

