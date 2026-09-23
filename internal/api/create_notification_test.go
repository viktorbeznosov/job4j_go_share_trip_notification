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
	t.Parallel()

	t.Run("success - проверка сохранения в БД", func(t *testing.T) {
		t.Parallel()
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

func TestCreateNotificationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		statusCode int
	}{
		{
			name:       "invalid json",
			body:       "{",
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "empty recipient",
			body:       `{"recipientId":"","type":"trip_created","payload":{"trip_id":"1"}}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "empty type",
			body:       `{"recipientId":"user-1","type":"","payload":{"trip_id":"1"}}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "invalid type",
			body:       `{"recipientId":"user-1","type":"unknown","payload":{"trip_id":"1"}}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "empty payload",
			body:       `{"recipientId":"user-1","type":"trip_created"}`,
			statusCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(
				http.MethodPost,
				"/notification",
				bytes.NewReader([]byte(tc.body)),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			resp, err := testApp.Test(req, -1)
			require.NoError(t, err)
			defer func() {
				_ = resp.Body.Close()
			}()

			assert.Equal(t, tc.statusCode, resp.StatusCode)
		})
	}
}

func TestReady(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(http.MethodGet, "/ready", nil)
	require.NoError(t, err)

	resp, err := testApp.Test(req, -1)
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
