package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestMaintenance(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	maintenanceClient := client.Maintenance

	t.Run("snapshot_should_get_valid_data", func(t *testing.T) {
		msg, err := maintenanceClient.Snapshot()
		assert.NoError(t, err)

		for {
			res, err := msg.Recv()
			assert.NoError(t, err)
			assert.NotEmpty(t, res.Blob)
			if res.RemainingBytes == 0 {
				break
			}
		}
	})
}
