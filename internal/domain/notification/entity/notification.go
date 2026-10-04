package entity

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusCreated Status = "created"
	StatusSent    Status = "sent"
	StatusFailed  Status = "failed"
)

type Type string

const (
	TypeTripPublished Type = "trip_published"
	TypeTripCreated   Type = "trip_created"
	TypeTripStarted   Type = "trip_started"
)

type Notification struct {
	ID          uuid.UUID       `json:"id"`
	RecipientID string          `json:"recipient_id"`
	Type        Type            `json:"type"`
	Status      Status          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

func NewNotification(recipientID string, notificationType Type, payload json.RawMessage) *Notification {
	return &Notification{
		ID:          uuid.New(),
		RecipientID: recipientID,
		Type:        notificationType,
		Status:      StatusCreated,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
}

var ValidTypes = map[Type]bool{
	TypeTripPublished: true,
	TypeTripCreated:   true,
	TypeTripStarted:   true,
}

func (t Type) IsValid() bool {
	return ValidTypes[t]
}
