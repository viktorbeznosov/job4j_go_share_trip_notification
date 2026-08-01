package response

import (
	"encoding/json"
	"job4j_go_share_trip_notification/internal/domain/notification/entity"
)

type GetNotificationResponse struct {
	ID          string          `json:"id"`
	RecipientID string          `json:"recipientId"`
	Type        entity.Type     `json:"type"`
	Status      entity.Status   `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   string          `json:"createdAt"`
}



