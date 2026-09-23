package api

import "job4j_go_share_trip_notification/internal/domain/notification/service"

type NotificationHandler struct {
	NotificationService *service.NotificationService
}

func NewNotificationHandler(notificationService *service.NotificationService) *NotificationHandler {
	return &NotificationHandler{
		NotificationService: notificationService,
	}
}
