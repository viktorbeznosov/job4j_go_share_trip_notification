package handler

import (
	"errors"
	"job4j_go_share_trip_notification/internal/domain/notification/handler/response"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func (h *NotificationHandler) GetNotification(c *fiber.Ctx) error {
	idParam := c.Params("uuid")

	id, err := uuid.Parse(idParam)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid notification id",
		})
	}

	notification, err := h.NotificationService.GetByID(c.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotificationNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "notification not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to get notification",
		})
	}

	return c.Status(fiber.StatusOK).JSON(response.CreateNotificationResponse{
		ID:          notification.ID.String(),
		RecipientID: notification.RecipientID,
		Type:        notification.Type,
		Status:      notification.Status,
		Payload:     notification.Payload,
		CreatedAt:   time.Now().String(),
	})
}


