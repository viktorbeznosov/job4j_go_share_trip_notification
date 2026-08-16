package handler

import (
	"github.com/gofiber/fiber/v2"
)

func(h *NotificationHandler) Ready(c *fiber.Ctx) error {
    return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "Success",
		"ready":  true,
    })
}


