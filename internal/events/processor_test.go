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

	"job4j_go_share_trip_notification/internal/domain/notification/entity"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
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
