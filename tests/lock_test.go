package test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestLock(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	c, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	lockClient := c.Lock

	t.Run("lock_contention_should_occur_when_acquire_by_two", func(t *testing.T) {
		res, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(res.Key), "lock-test/"))

		_, err = lockClient.Unlock(&xlineapi.UnlockRequest{Key: res.Key})
		assert.NoError(t, err)
	})

	t.Run("lock_should_timeout_when_ttl_is_set", func(t *testing.T) {
		_, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}, TTL: 1})
		assert.NoError(t, err)

		time.Sleep(2 * time.Second)

		res, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(res.Key), "lock-test/"))

		_, err = lockClient.Unlock(&xlineapi.UnlockRequest{Key: res.Key})
		assert.NoError(t, err)
	})

	t.Run("lock_should_unlock_after_cancelled", func(t *testing.T) {
		// first acquire the lock
		res, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)

		// acquire the lock again and then
		_, err = lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.Error(t, err)

		// unlock the first one
		_, err = lockClient.Unlock(&xlineapi.UnlockRequest{Key: res.Key})
		assert.NoError(t, err)

		// try lock again, it should success
		res, err = lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(res.Key), "lock-test/"))

		_, err = lockClient.Unlock(&xlineapi.UnlockRequest{Key: res.Key})
		assert.NoError(t, err)
	})
}
