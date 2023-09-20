package client

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestCurp(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}
	curpClient, err := client.BuildCurpClientFromAddrs(curpMembers)
	assert.NoError(t, err)

	cmd := &xlineapi.Command{
		Request: &xlineapi.RequestWithToken{
			Token: nil,
			RequestWrapper: &xlineapi.RequestWithToken_PutRequest{
				PutRequest: &xlineapi.PutRequest{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		},
		ProposeId: fmt.Sprintf("%s-%s", "client", uuid.New().String()),
	}

	t.Run("whether_the_fast_path_will_be_successful_or_not", func(t *testing.T) {
		res, err := curpClient.Propose(cmd, true)
		assert.NoError(t, err)
		assert.True(t, res.IsFastRoundSuccess)
		assert.Nil(t, res.SyncResp)
		assert.NotNil(t, res.CommandResp)
	})

	t.Run("whether_the_slow_path_will_be_successful_or_not", func(t *testing.T) {
		res, err := curpClient.Propose(cmd, true)
		assert.NoError(t, err)
		assert.False(t, res.IsFastRoundSuccess)
		assert.NotNil(t, res.SyncResp)
		assert.NotNil(t, res.CommandResp)
	})
}
