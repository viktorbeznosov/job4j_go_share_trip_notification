package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) Route(route fiber.Router) {
	route.Get("/ready", s.NotificationHandler.Ready)
	if s.Registry != nil {
		route.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{})))
	}

	notification := route.Group("/notification")
	notification.Post("/", s.NotificationHandler.CreateNotification)
	notification.Get("/:uuid", s.NotificationHandler.GetNotification)
}
