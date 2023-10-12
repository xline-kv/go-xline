package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"

	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
)

func TestWatch(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv
	watchClient := client.Watch

	t.Run("watch_should_receive_consistent_events", func(t *testing.T) {
		watcher, err := watchClient.Watch()
		assert.NoError(t, err)

		err = watcher.Send(&xlineapi.WatchRequest{
			RequestUnion: &xlineapi.WatchRequest_CreateRequest{
				CreateRequest: &xlineapi.WatchCreateRequest{
					Key: []byte("watch01"),
				},
			},
		})
		assert.NoError(t, err)
		res, err := watcher.Recv()
		assert.NoError(t, err)
		id := res.WatchId

		_, err = kvClient.Put(&xlineapi.PutRequest{Key: []byte("watch01"), Value: []byte("01")})
		assert.NoError(t, err)

		res, err = watcher.Recv()
		assert.NoError(t, err)
		assert.Equal(t, id, res.WatchId)
		assert.Len(t, res.Events, 1)
		kv := res.Events[0].Kv
		assert.Equal(t, "watch01", string(kv.Key))
		assert.Equal(t, "01", string(kv.Value))

		err = watcher.Send(&xlineapi.WatchRequest{
			RequestUnion: &xlineapi.WatchRequest_CancelRequest{
				CancelRequest: &xlineapi.WatchCancelRequest{
					WatchId: id,
				},
			},
		})
		assert.NoError(t, err)

		res, err = watcher.Recv()
		assert.NoError(t, err)
		assert.Equal(t, id, res.WatchId)
		assert.True(t, res.Canceled)
	})
}
