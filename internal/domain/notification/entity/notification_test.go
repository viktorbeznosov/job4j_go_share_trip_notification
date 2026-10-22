package entity

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNotification(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"trip_id":"1"}`)
	n := NewNotification("driver-1", TypeTripPublished, payload)

	require.NotNil(t, n)
	assert.NotEmpty(t, n.ID)
	assert.Equal(t, "driver-1", n.RecipientID)
	assert.Equal(t, TypeTripPublished, n.Type)
	assert.Equal(t, StatusCreated, n.Status)
	assert.Equal(t, payload, n.Payload)
	assert.False(t, n.CreatedAt.IsZero())
}

func TestTypeIsValid(t *testing.T) {
	t.Parallel()

	assert.True(t, TypeTripPublished.IsValid())
	assert.True(t, TypeTripStarted.IsValid())
	assert.True(t, TypeTripPreapproved.IsValid())
	assert.True(t, TypeTripCanceled.IsValid())
	assert.True(t, TypeTripPublishNotificated.IsValid())
	assert.True(t, TypeRequestCompleted.IsValid())
	assert.True(t, TypeRequestExpired.IsValid())
	assert.False(t, Type("unknown").IsValid())
}
