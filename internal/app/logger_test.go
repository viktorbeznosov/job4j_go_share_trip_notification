package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	t.Parallel()

	logger, file, err := NewLogger()
	require.NoError(t, err)
	require.NotNil(t, logger)
	require.NotNil(t, file)
	require.NoError(t, file.Close())
}
