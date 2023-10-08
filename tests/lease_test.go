package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestLeaseUser(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	leaseClient := client.Lease

	t.Run("Grant revoke should success in normal path", func(t *testing.T) {
		res, _ := leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 123})
		assert.Equal(t, int64(123), res.TTL)

		id := res.ID
		_, err := leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: id})
		assert.NoError(t, err)
	})

	t.Run("Keep alive should success in normal path", func(t *testing.T) {
		res1, err := leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 60})
		assert.NoError(t, err)
		assert.Equal(t, int64(60), res1.TTL)
		id := res1.ID

		keeper, err := leaseClient.KeepAlive(&xlineapi.LeaseKeepAliveRequest{ID: id})
		assert.NoError(t, err)

		err = keeper.Send(&xlineapi.LeaseKeepAliveRequest{ID: id})
		assert.NoError(t, err)
		res2, err := keeper.Recv()
		assert.NoError(t, err)
		assert.Equal(t, id, res2.ID)
		assert.Equal(t, int64(60), res2.TTL)

		_, err = leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: id})
		assert.NoError(t, err)
	})

	t.Run("Time to live ttl is consistent in normal path", func(t *testing.T) {
		var leaseId int64 = 200

		res1, _ := leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 60, ID: leaseId})
		assert.Equal(t, leaseId, res1.ID)
		assert.Equal(t, int64(60), res1.TTL)

		res2, _ := leaseClient.TimeToLive(&xlineapi.LeaseTimeToLiveRequest{ID: leaseId})
		assert.Equal(t, leaseId, res2.ID)
		assert.Equal(t, int64(60), res2.GrantedTTL)

		_, err = leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: leaseId})
		assert.NoError(t, err)
	})

	t.Run("Leases should include granted in normal path", func(t *testing.T) {
		var lease1 int64 = 100
		var lease2 int64 = 101
		var lease3 int64 = 102

		res1, _ := leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 60, ID: lease1})
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease1, res1.ID)

		res1, _ = leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 60, ID: lease2})
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease2, res1.ID)

		res1, _ = leaseClient.Grant(&xlineapi.LeaseGrantRequest{TTL: 60, ID: lease3})
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease3, res1.ID)

		res2, _ := leaseClient.Leases(&xlineapi.LeaseLeasesRequest{})
		assert.True(t, leaseExists(res2.Leases, &lease1))
		assert.True(t, leaseExists(res2.Leases, &lease2))
		assert.True(t, leaseExists(res2.Leases, &lease3))

		_, err = leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: lease1})
		assert.NoError(t, err)
		_, err = leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: lease2})
		assert.NoError(t, err)
		_, err = leaseClient.Revoke(&xlineapi.LeaseRevokeRequest{ID: lease3})
		assert.NoError(t, err)
	})
}

func leaseExists(leases []*xlineapi.LeaseStatus, lease *int64) bool {
	for _, l := range leases {
		if l.ID == *lease {
			return true
		}
	}
	return false
}
