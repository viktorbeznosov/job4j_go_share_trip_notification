package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"job4j_go_share_trip_notification/internal/domain/notification/handler/request"
	"job4j_go_share_trip_notification/internal/domain/notification/handler/response"
)

func Test_Create_Notification(t *testing.T) {
	t.Run("success - проверка сохранения в БД", func(t *testing.T) {
		recipientID := uuid.New().String()

		payload := request.CreateNotificationRequest{
			RecipientID: recipientID,
			Type:        "trip_created",
			Payload:     json.RawMessage(`{"trip_id": "123", "message": "test"}`),
		}

		body, err := json.Marshal(payload)
		require.NoError(t, err)

		req, err := http.NewRequest(
			http.MethodPost,
			"/notification",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := testApp.Test(req, -1)
		require.NoError(t, err)

		defer func() {
            err := resp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
		}()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		var createResp response.CreateNotificationResponse
		err = json.NewDecoder(resp.Body).Decode(&createResp)
		require.NoError(t, err)

		var count int
		err = testDB.QueryRow(
			"SELECT COUNT(*) FROM notifications WHERE id = $1",
			createResp.ID,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}