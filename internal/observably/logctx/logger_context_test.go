package logctx

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggerFallbacks(t *testing.T) {
	t.Parallel()

	assert.Equal(t, slog.Default(), Logger(context.Background()))
}

func TestWithLogger(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := WithLogger(context.Background(), logger)

	assert.Equal(t, logger, Logger(ctx))
}

func TestRequestID(t *testing.T) {
	t.Parallel()

	assert.Empty(t, RequestID(context.Background()))

	ctx := WithRequestID(context.Background(), "req-1")
	require.NotNil(t, ctx)
	assert.Equal(t, "req-1", RequestID(ctx))
}
