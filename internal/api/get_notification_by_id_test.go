// internal/api/get_notification_by_id_test.go
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

func TestGetNotificationByID(t *testing.T) {
	t.Run("success - получение уведомления по ID", func(t *testing.T) {
		recipientID := uuid.New().String()
		createPayload := request.CreateNotificationRequest{
			RecipientID: recipientID,
			Type:        "trip_created",
			Payload:     json.RawMessage(`{"trip_id": "123", "message": "test"}`),
		}

		createBody, err := json.Marshal(createPayload)
		require.NoError(t, err)

		createReq, err := http.NewRequest(
			http.MethodPost,
			"/notification",
			bytes.NewReader(createBody),
		)
		require.NoError(t, err)
		createReq.Header.Set("Content-Type", "application/json")

		createResp, err := testApp.Test(createReq, -1)
		require.NoError(t, err)

        defer func() {
            err := createResp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusCreated, createResp.StatusCode)

		var created response.CreateNotificationResponse
		err = json.NewDecoder(createResp.Body).Decode(&created)
		require.NoError(t, err)

		getReq, err := http.NewRequest(
			http.MethodGet,
			"/notification/"+created.ID,
			nil,
		)
		require.NoError(t, err)

		getResp, err := testApp.Test(getReq, -1)
		require.NoError(t, err)

        defer func() {
            err := getResp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusOK, getResp.StatusCode)

		var got response.CreateNotificationResponse
		err = json.NewDecoder(getResp.Body).Decode(&got)
		require.NoError(t, err)

		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, created.RecipientID, got.RecipientID)
		assert.Equal(t, created.Type, got.Type)
		assert.Equal(t, created.Status, got.Status)
	})

	t.Run("error - невалидный UUID", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodGet,
			"/notification/invalid-uuid",
			nil,
		)
		require.NoError(t, err)

		resp, err := testApp.Test(req, -1)
		require.NoError(t, err)

        defer func() {
            err := resp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var errResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&errResp)
		require.NoError(t, err)
		assert.Equal(t, "invalid notification id", errResp["error"])
	})

	t.Run("error - уведомление не найдено", func(t *testing.T) {
		nonExistentID := uuid.New().String()

		req, err := http.NewRequest(
			http.MethodGet,
			"/notification/"+nonExistentID,
			nil,
		)
		require.NoError(t, err)

		resp, err := testApp.Test(req, -1)
		require.NoError(t, err)

        defer func() {
            err := resp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		var errResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&errResp)
		require.NoError(t, err)
		assert.Equal(t, "notification not found", errResp["error"])
	})

	t.Run("success - проверка данных в БД", func(t *testing.T) {
		recipientID := uuid.New().String()
		createPayload := request.CreateNotificationRequest{
			RecipientID: recipientID,
			Type:        "trip_published",
			Payload:     json.RawMessage(`{"trip_id": "456", "message": "integration test"}`),
		}

		createBody, err := json.Marshal(createPayload)
		require.NoError(t, err)

		createReq, err := http.NewRequest(
			http.MethodPost,
			"/notification",
			bytes.NewReader(createBody),
		)
		require.NoError(t, err)
		createReq.Header.Set("Content-Type", "application/json")

		createResp, err := testApp.Test(createReq, -1)
		require.NoError(t, err)

        defer func() {
            err := createResp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusCreated, createResp.StatusCode)

		var created response.CreateNotificationResponse
		err = json.NewDecoder(createResp.Body).Decode(&created)
		require.NoError(t, err)

		getReq, err := http.NewRequest(
			http.MethodGet,
			"/notification/"+created.ID,
			nil,
		)
		require.NoError(t, err)

		getResp, err := testApp.Test(getReq, -1)
		require.NoError(t, err)

        defer func() {
            err := getResp.Body.Close()
            if err != nil {
                t.Errorf("failed to close response body: %v", err)
            }
        }()

		assert.Equal(t, http.StatusOK, getResp.StatusCode)

		var got response.CreateNotificationResponse
		err = json.NewDecoder(getResp.Body).Decode(&got)
		require.NoError(t, err)

		var dbRecipientID string
		var dbType string
		var dbStatus string
		var dbPayload []byte

		err = testDB.QueryRow(
			"SELECT recipient_id, type, status, payload FROM notifications WHERE id = $1",
			created.ID,
		).Scan(&dbRecipientID, &dbType, &dbStatus, &dbPayload)
		require.NoError(t, err)

		assert.Equal(t, dbRecipientID, got.RecipientID)
        assert.Equal(t, dbType, string(got.Type))
        assert.Equal(t, dbStatus, string(got.Status))
	})
}
