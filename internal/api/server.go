package api

import (
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/domain/notification/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	NotificationHandler *NotificationHandler
}

func NewServer(pgxpool *pgxpool.Pool) *Server {
	notificationService := service.NewNotificationService(
		*repository.NewNotificationRepository(pgxpool),
	)

	return &Server{
		NotificationHandler: NewNotificationHandler(notificationService),
	}
}
