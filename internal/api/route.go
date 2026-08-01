package api

import (
	"github.com/gofiber/fiber/v2"
)

func (s *Server) Route(route fiber.Router) {
	route.Get("/ready", s.NotificationHandler.Ready)

	notification := route.Group("/notification")
	notification.Post("/", s.NotificationHandler.CreateNotification)
	notification.Get("/:uuid", s.NotificationHandler.GetNotification)
}
