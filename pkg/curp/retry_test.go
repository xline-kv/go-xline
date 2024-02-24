package curp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFixedBackoffWorks(t *testing.T) {
	config := newFixedRetryConfig(time.Second, 3)
	backoff := config.initBackoff()
	assert.Equal(t, *backoff.nextDelay(), time.Second)
	assert.Equal(t, *backoff.nextDelay(), time.Second)
	assert.Equal(t, *backoff.nextDelay(), time.Second)
	assert.Equal(t, backoff.nextDelay(), (*time.Duration)(nil))
}

func TestExponentialBackoffWorks(t *testing.T) {
	config := newExponentialRetryConfig(time.Second, 5*time.Second, 4)
	backoff := config.initBackoff()
	assert.Equal(t, *backoff.nextDelay(), 1 * time.Second)
	assert.Equal(t, *backoff.nextDelay(), 2 * time.Second)
	assert.Equal(t, *backoff.nextDelay(), 4 * time.Second)
	assert.Equal(t, *backoff.nextDelay(), 5 * time.Second) // 8 > 5
	assert.Equal(t, backoff.nextDelay(), (*time.Duration)(nil))
}
