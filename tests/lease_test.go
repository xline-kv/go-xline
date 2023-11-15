// Copyright 2023 The xline Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestLeaseUser(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	leaseClient := xlineClient.Lease

	t.Run("Grant revoke should success in normal path", func(t *testing.T) {
		res, _ := leaseClient.Grant(123)
		assert.Equal(t, int64(123), res.TTL)

		id := res.ID
		_, err := leaseClient.Revoke(id)
		assert.NoError(t, err)
	})

	t.Run("Keep alive should success in normal path", func(t *testing.T) {
		cnt := 0
		res1, _ := leaseClient.Grant(60)
		assert.Equal(t, int64(60), res1.TTL)
		id := res1.ID

		ctx, cancel := context.WithCancel(context.Background())
		stream, _ := leaseClient.KeepAlive(ctx, id)
		for res2 := range stream {
			assert.Equal(t, id, res2.ID)
			assert.Equal(t, int64(60), res2.TTL)
			cnt++
			if cnt > 5 {
				cancel()
				break
			}
		}

		leaseClient.Revoke(id)
	})

	t.Run("Keep alive once should success in normal path", func(t *testing.T) {
		res1, _ := leaseClient.Grant(60)
		assert.Equal(t, int64(60), res1.TTL)
		id := res1.ID

		res2, _ := leaseClient.KeepAliveOnce(id)
		assert.Equal(t, id, res2.ID)
		assert.Equal(t, int64(60), res2.TTL)

		leaseClient.Revoke(id)
	})

	t.Run("Time to live ttl is consistent in normal path", func(t *testing.T) {
		var leaseId int64 = 200

		res1, _ := leaseClient.Grant(60, client.WithID(leaseId))
		assert.Equal(t, leaseId, res1.ID)
		assert.Equal(t, int64(60), res1.TTL)

		res2, _ := leaseClient.TimeToLive(leaseId)
		assert.Equal(t, leaseId, res2.ID)
		assert.Equal(t, int64(60), res2.GrantedTTL)

		leaseClient.Revoke(leaseId)
	})

	t.Run("Leases should include granted in normal path", func(t *testing.T) {
		var lease1 int64 = 100
		var lease2 int64 = 101
		var lease3 int64 = 102

		res1, _ := leaseClient.Grant(60, client.WithID(lease1))
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease1, res1.ID)

		res1, _ = leaseClient.Grant(60, client.WithID(lease2))
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease2, res1.ID)

		res1, _ = leaseClient.Grant(60, client.WithID(lease3))
		assert.Equal(t, int64(60), res1.TTL)
		assert.Equal(t, lease3, res1.ID)

		res2, _ := leaseClient.Leases()
		assert.True(t, leaseExists(res2.Leases, &lease1))
		assert.True(t, leaseExists(res2.Leases, &lease2))
		assert.True(t, leaseExists(res2.Leases, &lease3))

		leaseClient.Revoke(lease1)
		leaseClient.Revoke(lease2)
		leaseClient.Revoke(lease3)
	})
}

func leaseExists(leases []*pb.LeaseStatus, lease *int64) bool {
	for _, l := range leases {
		if l.ID == *lease {
			return true
		}
	}
	return false
}
