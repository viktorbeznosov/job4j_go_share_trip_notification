package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDSN(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		DBName:   "db",
		SSLMode:  "disable",
	}

	assert.Equal(
		t,
		"postgres://user:pass@localhost:5432/db?sslmode=disable",
		cfg.DSN(),
	)
}

func TestConfigDSNDefaultSSLMode(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		DBName:   "db",
	}

	assert.Contains(t, cfg.DSN(), "sslmode=disable")
}

func TestNewPoolInvalidDSN(t *testing.T) {
	t.Parallel()

	_, err := NewPool(context.Background(), "://bad")
	require.Error(t, err)
}
