package api

import (
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/domain/notification/service"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type Server struct {
	NotificationHandler *NotificationHandler
	Registry            *prometheus.Registry
}

func NewServer(pgxpool *pgxpool.Pool) *Server {
	return NewServerWithRegistry(pgxpool, nil)
}

func NewServerWithRegistry(pgxpool *pgxpool.Pool, registry *prometheus.Registry) *Server {
	notificationService := service.NewNotificationService(
		*repository.NewNotificationRepository(pgxpool),
	)

	return &Server{
		NotificationHandler: NewNotificationHandler(notificationService),
		Registry:            registry,
	}
}
