package events

import (
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	HeaderEventID       = "event_id"
	HeaderEventType     = "event_type"
	HeaderCorrelationID = "correlation_id"
	HeaderCausationID   = "causation_id"
	HeaderTraceparent   = "traceparent"
)

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     EventType `json:"event_type"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id"`
	Traceparent   string    `json:"traceparent"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func metadataFromHeaders(msg kafka.Message) EventMetadata {
	return EventMetadata{
		EventID:       headerValue(msg, HeaderEventID),
		EventType:     EventType(headerValue(msg, HeaderEventType)),
		CorrelationID: headerValue(msg, HeaderCorrelationID),
		CausationID:   headerValue(msg, HeaderCausationID),
		Traceparent:   headerValue(msg, HeaderTraceparent),
	}
}

func headerValue(msg kafka.Message, key string) string {
	for _, header := range msg.Headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func TraceIDFromParent(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func applyMetadata(event *TripPublished, meta EventMetadata) {
	if meta.EventID != "" {
		event.EventID = meta.EventID
	}
	if meta.EventType != "" {
		event.EventType = meta.EventType
	}
	if meta.CorrelationID != "" {
		event.CorrelationID = meta.CorrelationID
	}
	if meta.CausationID != "" {
		event.CausationID = meta.CausationID
	}
	if meta.Traceparent != "" {
		event.Traceparent = meta.Traceparent
	}
}
