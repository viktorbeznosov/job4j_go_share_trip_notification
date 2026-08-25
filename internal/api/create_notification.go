package api

import (
	"encoding/json"
	"job4j_go_share_trip_notification/internal/domain/notification/handler/request"
	"job4j_go_share_trip_notification/internal/domain/notification/handler/response"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
)

func (h *NotificationHandler) CreateNotification(c *fiber.Ctx) error {
	var req request.CreateNotificationRequest

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "invalid request body",
			"details": err.Error(),
		})
	}

	if req.RecipientID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "recipientId is required",
		})
	}

	if req.Type == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "type is required",
		})
	}

    if !req.Type.IsValid() {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "type is invalid",
		})
    }

    if len(req.Payload) == 0 || string(req.Payload) == "null" {
        req.Payload = json.RawMessage(`{}`)
    }

	notification, err := h.NotificationService.Create(c.Context(), req.RecipientID, req.Type, req.Payload)
	if err != nil {
	    log.Println("failed to create notification", err.Error())
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to create notification",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(response.CreateNotificationResponse{
		ID:          notification.ID.String(),
		RecipientID: notification.RecipientID,
		Type:        notification.Type,
		Status:      notification.Status,
		Payload:     notification.Payload,
		CreatedAt:   time.Now().String(),
	})
}

