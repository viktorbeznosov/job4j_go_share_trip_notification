package events

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/prometheus/client_golang/prometheus"

	"job4j_go_share_trip_notification/internal/domain/notification/entity"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/observability/metrics"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(
		ctx,
		"postgres:16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open sql db: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("database is not ready: %v", err)
	}

	if err = goose.SetDialect("postgres"); err != nil {
		log.Fatalf("set goose dialect: %v", err)
	}
	if err = goose.Up(db, "../../migrations"); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create pgx pool: %v", err)
	}

	code := m.Run()

	testPool.Close()
	_ = db.Close()
	_ = container.Terminate(ctx)

	os.Exit(code)
}

func newTestProcessor() *Processor {
	return NewProcessor(
		testPool,
		repository.NewNotificationRepository(testPool),
		repository.NewProcessedEventRepository(),
		metrics.New(prometheus.NewRegistry()),
	)
}

func TestProcessor_CreatesNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:    "event-create-1",
		EventType:  EventTypeTripPublished,
		TripID:     "trip-1",
		DriverID:   "driver-1",
		CompanyID:  "company-1",
		OccurredAt: time.Now().UTC().Truncate(time.Second),
	}

	err := processor.Handle(ctx, event)
	require.NoError(t, err)

	var notificationCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeTripPublished),
	).Scan(&notificationCount)
	require.NoError(t, err)
	assert.Equal(t, 1, notificationCount)

	var processedCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`,
		event.EventID,
	).Scan(&processedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, processedCount)
}

func TestProcessor_CreatesTripPreapprovedNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:       "event-preapproved-1",
		EventType:     EventTypeTripPreapproved,
		TripID:        "trip-preapproved-1",
		DriverID:      "driver-preapproved-1",
		CompanyID:     "company-preapproved-1",
		CorrelationID: "corr-preapproved",
		CausationID:   "req-preapproved",
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		OccurredAt:    time.Now().UTC().Truncate(time.Second),
	}

	err := processor.Handle(ctx, event)
	require.NoError(t, err)

	var notificationCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeTripPreapproved),
	).Scan(&notificationCount)
	require.NoError(t, err)
	assert.Equal(t, 1, notificationCount)
}

func TestProcessor_CreatesTripStartedNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:       "event-started-1",
		EventType:     EventTypeTripStarted,
		TripID:        "trip-started-1",
		DriverID:      "driver-started-1",
		CorrelationID: "corr-started",
		CausationID:   "req-started",
	}

	err := processor.Handle(ctx, event)
	require.NoError(t, err)

	var notificationCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeTripStarted),
	).Scan(&notificationCount)
	require.NoError(t, err)
	assert.Equal(t, 1, notificationCount)
}

func TestProcessor_IsIdempotentByEventID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:   "event-idempotent-1",
		EventType: EventTypeTripPublished,
		TripID:    "trip-2",
		DriverID:  "driver-2",
		CompanyID: "company-2",
	}

	require.NoError(t, processor.Handle(ctx, event))
	require.NoError(t, processor.Handle(ctx, event))

	var notificationCount int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1`,
		event.DriverID,
	).Scan(&notificationCount)
	require.NoError(t, err)
	assert.Equal(t, 1, notificationCount)

	var processedCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`,
		event.EventID,
	).Scan(&processedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, processedCount)
}

func TestProcessor_CreatesTripCanceledNotificationsForPassengers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:      "event-canceled-1",
		EventType:    EventTypeTripCanceled,
		TripID:       "trip-canceled-1",
		DriverID:     "driver-canceled-1",
		PassengerIDs: []string{"passenger-a", "passenger-b", "passenger-a"},
	}

	require.NoError(t, processor.Handle(ctx, event))

	var driverCount int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeTripCanceled),
	).Scan(&driverCount)
	require.NoError(t, err)
	assert.Equal(t, 0, driverCount)

	for _, passenger := range []string{"passenger-a", "passenger-b"} {
		var count int
		err = testPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
			passenger,
			string(entity.TypeTripCanceled),
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	}
}

func TestProcessor_CreatesRequestPreconfirmedNotificationForDriver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:     "event-request-preconfirmed-1",
		EventType:   EventTypeRequestPreconfirmed,
		RequestID:   "request-preconfirmed-1",
		PassengerID: "passenger-preconfirmed-1",
		DriverID:    "driver-preconfirmed-1",
		TripID:      "trip-preconfirmed-1",
	}

	require.NoError(t, processor.Handle(ctx, event))

	var driverCount int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeRequestPreconfirmed),
	).Scan(&driverCount)
	require.NoError(t, err)
	assert.Equal(t, 1, driverCount)

	var passengerCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.PassengerID,
		string(entity.TypeRequestPreconfirmed),
	).Scan(&passengerCount)
	require.NoError(t, err)
	assert.Equal(t, 0, passengerCount)
}

func TestProcessor_CreatesRequestConfirmedNotificationForPassenger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:     "event-request-confirmed-1",
		EventType:   EventTypeRequestConfirmed,
		RequestID:   "request-confirmed-1",
		PassengerID: "passenger-confirmed-1",
		DriverID:    "driver-confirmed-1",
	}

	require.NoError(t, processor.Handle(ctx, event))

	var passengerCount int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.PassengerID,
		string(entity.TypeRequestConfirmed),
	).Scan(&passengerCount)
	require.NoError(t, err)
	assert.Equal(t, 1, passengerCount)

	var driverCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeRequestConfirmed),
	).Scan(&driverCount)
	require.NoError(t, err)
	assert.Equal(t, 0, driverCount)
}

func TestProcessor_CreatesRequestRejectedNotificationForPassenger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:     "event-request-rejected-1",
		EventType:   EventTypeRequestRejected,
		RequestID:   "request-rejected-1",
		PassengerID: "passenger-rejected-1",
	}

	require.NoError(t, processor.Handle(ctx, event))

	var count int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.PassengerID,
		string(entity.TypeRequestRejected),
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestProcessor_CreatesRequestCompletedNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:     "event-request-completed-1",
		EventType:   EventTypeRequestCompleted,
		RequestID:   "request-1",
		PassengerID: "passenger-completed-1",
		TripID:      "trip-completed-req",
	}

	require.NoError(t, processor.Handle(ctx, event))

	var count int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.PassengerID,
		string(entity.TypeRequestCompleted),
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestProcessor_CreatesRequestExpiredNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:     "event-request-expired-1",
		EventType:   EventTypeRequestExpired,
		RequestID:   "request-expired-1",
		PassengerID: "passenger-expired-1",
	}

	require.NoError(t, processor.Handle(ctx, event))

	var count int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.PassengerID,
		string(entity.TypeRequestExpired),
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestProcessor_CreatesTripPublishNotificatedForPassengers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	processor := newTestProcessor()

	event := TripPublished{
		EventID:      "event-publish-notified-1",
		EventType:    EventTypeTripPublishNotificated,
		TripID:       "trip-notified-1",
		DriverID:     "driver-notified-1",
		FromPoint:    "Moscow",
		ToPoint:      "Kazan",
		PassengerIDs: []string{"passenger-match-1"},
	}

	require.NoError(t, processor.Handle(ctx, event))

	var passengerCount int
	err := testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		"passenger-match-1",
		string(entity.TypeTripPublishNotificated),
	).Scan(&passengerCount)
	require.NoError(t, err)
	assert.Equal(t, 1, passengerCount)

	var driverCount int
	err = testPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND type = $2`,
		event.DriverID,
		string(entity.TypeTripPublishNotificated),
	).Scan(&driverCount)
	require.NoError(t, err)
	assert.Equal(t, 0, driverCount)
}

func TestProcessor_RequiresEventID(t *testing.T) {
	t.Parallel()

	err := newTestProcessor().Handle(context.Background(), TripPublished{
		DriverID: "driver-3",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing event_id")
}

func TestProcessor_RequiresDriverID(t *testing.T) {
	t.Parallel()

	err := newTestProcessor().Handle(context.Background(), TripPublished{
		EventID: "event-missing-driver",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing driver_id")
}
