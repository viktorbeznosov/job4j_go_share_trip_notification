package request

import (
	"encoding/json"
	"job4j_go_share_trip_notification/internal/domain/notification/entity"
)

type CreateNotificationRequest struct {
	RecipientID string          `json:"recipientId"`
	Type        entity.Type     `json:"type"`
	Payload     json.RawMessage `json:"payload"`
}



