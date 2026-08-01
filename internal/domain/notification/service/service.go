package service

import "job4j_go_share_trip_notification/internal/domain/notification/repository"

type NotificationService struct {
    notificationRepository repository.NotificationRepository
}

func NewNotificationService(notificationRepository repository.NotificationRepository) *NotificationService {
    return &NotificationService{
        notificationRepository: notificationRepository,
    }
}



