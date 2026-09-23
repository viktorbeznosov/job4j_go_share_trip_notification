package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvReturnsDefaultWhenEmpty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "fallback", Env("JOB4J_UNSET_ENV_STRING", "fallback"))
}

func TestEnvIntReturnsDefaultWhenEmptyOrInvalid(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 42, EnvInt("JOB4J_UNSET_ENV_INT", 42))
}

func TestEnvBoolReturnsDefaultWhenEmpty(t *testing.T) {
	t.Parallel()

	assert.True(t, EnvBool("JOB4J_UNSET_ENV_BOOL", true))
	assert.False(t, EnvBool("JOB4J_UNSET_ENV_BOOL", false))
}

func TestSplitCSV(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"localhost:9092", "localhost:9093"}, splitCSV("localhost:9092, localhost:9093"))
	assert.Empty(t, splitCSV(" , "))
}

func TestGetAppConfigHasKafkaDefaults(t *testing.T) {
	t.Parallel()

	cfg := GetAppConfig()
	assert.NotEmpty(t, cfg.Kafka.Brokers)
	assert.NotEmpty(t, cfg.Kafka.Topic)
	assert.NotEmpty(t, cfg.Kafka.GroupID)
}
