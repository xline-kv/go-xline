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

	t.Run("lock_unlock_should_success_in_normal_path", func(t *testing.T) {
		res, _ := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.True(t, strings.HasPrefix(string(res.Key), "lock-test/"))

		lockClient.UnLock(&xlineapi.UnlockRequest{Key: res.Key})
	})

	t.Run("lock_should_not_occur_when_acquire_by_two", func(t *testing.T) {
		lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})

		_, err = lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.Error(t, err)

		time.Sleep(2 * time.Second)

		res, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)

		lockClient.UnLock(&xlineapi.UnlockRequest{Key: res.Key})
	})

	t.Run("lock_should_timeout_when_ttl_is_set", func(t *testing.T) {
		lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}, TTL: 1})

		lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.Error(t, err)

		time.Sleep(2 * time.Second)

		res, err := lockClient.Lock(client.LockRequest{Inner: &xlineapi.LockRequest{Name: []byte("lock-test")}})
		assert.NoError(t, err)

		lockClient.UnLock(&xlineapi.UnlockRequest{Key: res.Key})
	})
}
