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

func TestConsumer_SkipsNonTripPublishedAndCommits(t *testing.T) {
	t.Parallel()
	event := TripPublished{
		EventID:   "evt-2",
		EventType: EventTypeTripStarted,
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
