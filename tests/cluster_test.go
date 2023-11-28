package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
)

func TestCluster(t *testing.T) {
	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	clusterClient := xlineClient.Cluster

	var id1 uint64
	var id2 uint64
	addr1 := "172.20.0.6:2379"
	addr2 := "172.20.0.7:2379"
	addr3 := "172.20.0.8:2379"

	t.Run("cluster_add_member", func(t *testing.T) {
		res, _ := clusterClient.MemberAdd(context.TODO(), []string{addr1})
		id1 = res.Member.ID
		assert.Equal(t, addr1, res.Member.PeerURLs[0])
		assert.Len(t, res.Members, 4)

		res, _ = clusterClient.MemberAddAsLearner(context.TODO(), []string{addr2})
		id2 = res.Member.ID
		assert.Equal(t, addr2, res.Member.PeerURLs[0])
		assert.True(t, res.Member.IsLearner)
		assert.Len(t, res.Members, 5)
	})

	t.Run("cluster_update_member", func(t *testing.T) {
		res, _ := clusterClient.MemberUpdate(context.TODO(), id1, []string{addr3})
		for _, m := range res.Members {
			if m.ID == id1 {
				assert.Equal(t, addr3, m.PeerURLs[0])
			}
		}
	})

	t.Run("cluster_promote_member", func(t *testing.T) {
		res, _ := clusterClient.MemberPromote(context.TODO(), id2)
		for _, m := range res.Members {
			if m.ID == id2 {
				assert.False(t, m.IsLearner)
			}
		}
	})

	t.Run("cluster_member_list", func(t *testing.T) {
		res, _ := clusterClient.MemberList(context.TODO())
		assert.Len(t, res.Members, 5)
	})

	t.Run("cluster_remove_member", func(t *testing.T) {
		res, _ := clusterClient.MemberRemove(context.TODO(), id1)
		assert.Len(t, res.Members, 4)

		res, _ = clusterClient.MemberRemove(context.TODO(), id2)
		assert.Len(t, res.Members, 3)
	})
}
