package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubReader struct {
	messages  []kafka.Message
	commits   []kafka.Message
	fetchErr  error
	commitErr error
	idx       int
}

func (s *stubReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if s.fetchErr != nil {
		return kafka.Message{}, s.fetchErr
	}
	if s.idx >= len(s.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := s.messages[s.idx]
	s.idx++
	return msg, nil
}

func (s *stubReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	s.commits = append(s.commits, msgs...)
	return nil
}

func (s *stubReader) Close() error {
	return nil
}

func TestConsumer_CommitsAfterSuccessfulTripPublished(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-1",
		EventType: EventTypeTripPublished,
		TripID:    "trip-1",
		DriverID:  "driver-1",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{Value: body}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err = consumer.Run(ctx, func(_ context.Context, got TripPublished) error {
		handled++
		assert.Equal(t, event.EventID, got.EventID)
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_AppliesKafkaHeadersToEvent(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "payload-event",
		EventType: EventTypeTripPublished,
		TripID:    "trip-headers",
		DriverID:  "driver-headers",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{
			Value: body,
			Headers: []kafka.Header{
				{Key: HeaderEventID, Value: []byte("header-event")},
				{Key: HeaderCorrelationID, Value: []byte("corr-1")},
				{Key: HeaderCausationID, Value: []byte("cause-1")},
				{Key: HeaderTraceparent, Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
			},
		}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = consumer.Run(ctx, func(_ context.Context, got TripPublished) error {
		assert.Equal(t, "header-event", got.EventID)
		assert.Equal(t, "corr-1", got.CorrelationID)
		assert.Equal(t, "cause-1", got.CausationID)
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", TraceIDFromParent(got.Traceparent))
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestConsumer_HandlesTripCanceled(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:      "evt-canceled",
		EventType:    EventTypeTripCanceled,
		TripID:       "trip-canceled",
		PassengerIDs: []string{"p-1"},
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{messages: []kafka.Message{{Value: body}}}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err = consumer.Run(ctx, func(_ context.Context, got TripPublished) error {
		handled++
		assert.Equal(t, EventTypeTripCanceled, got.EventType)
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_SkipsNonTripPublishedAndCommits(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-2",
		EventType: EventType("trip_created"),
		TripID:    "trip-2",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{Value: body}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err = consumer.Run(ctx, func(_ context.Context, _ TripPublished) error {
		handled++
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 0, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_HandlesTripPreapproved(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-preapproved",
		EventType: EventTypeTripPreapproved,
		TripID:    "trip-preapproved",
		DriverID:  "driver-preapproved",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{Value: body}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err = consumer.Run(ctx, func(_ context.Context, got TripPublished) error {
		handled++
		assert.Equal(t, EventTypeTripPreapproved, got.EventType)
		assert.Equal(t, "evt-preapproved", got.EventID)
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_HandlesTripStarted(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-started",
		EventType: EventTypeTripStarted,
		TripID:    "trip-started",
		DriverID:  "driver-started",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{
			Value: body,
			Headers: []kafka.Header{
				{Key: HeaderCorrelationID, Value: []byte("corr-start")},
				{Key: HeaderCausationID, Value: []byte("req-start")},
			},
		}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err = consumer.Run(ctx, func(_ context.Context, got TripPublished) error {
		handled++
		assert.Equal(t, EventTypeTripStarted, got.EventType)
		assert.Equal(t, "evt-started", got.EventID)
		assert.Equal(t, "corr-start", got.CorrelationID)
		assert.Equal(t, "req-start", got.CausationID)
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_DoesNotCommitWhenHandlerFails(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-3",
		EventType: EventTypeTripPublished,
		TripID:    "trip-3",
		DriverID:  "driver-3",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages: []kafka.Message{{Value: body}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	err = consumer.Run(ctx, func(_ context.Context, _ TripPublished) error {
		return errors.New("db unavailable")
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, reader.commits)
}

func TestConsumer_SkipsInvalidJSONAndCommits(t *testing.T) {
	t.Parallel()

	reader := &stubReader{
		messages: []kafka.Message{{Value: []byte("not-json")}},
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handled := 0
	err := consumer.Run(ctx, func(_ context.Context, _ TripPublished) error {
		handled++
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 0, handled)
	require.Len(t, reader.commits, 1)
}

func TestConsumer_ReturnsCommitError(t *testing.T) {
	t.Parallel()

	event := TripPublished{
		EventID:   "evt-4",
		EventType: EventTypeTripPublished,
		TripID:    "trip-4",
		DriverID:  "driver-4",
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)

	reader := &stubReader{
		messages:  []kafka.Message{{Value: body}},
		commitErr: errors.New("commit failed"),
	}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = consumer.Run(ctx, func(_ context.Context, _ TripPublished) error {
		return nil
	})

	require.EqualError(t, err, "commit failed")
}

func TestNewConsumerAndClose(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer([]string{"localhost:9092"}, "trip.events", "test-group")
	require.NotNil(t, consumer)
	require.NoError(t, consumer.Close())
}

func TestSleepReturnsFalseWhenContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok := sleep(ctx, time.Second)
	assert.False(t, ok)
}

func TestConsumer_RetriesFetchErrorUntilCanceled(t *testing.T) {
	t.Parallel()

	reader := &stubReader{fetchErr: errors.New("broker down")}
	consumer := &Consumer{reader: reader}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	err := consumer.Run(ctx, func(_ context.Context, _ TripPublished) error {
		return nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}
