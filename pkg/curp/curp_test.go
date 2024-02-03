package curp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	xlineapi "github.com/xline-kv/go-xline/api/gen/xline"
	"github.com/xline-kv/go-xline/pkg/rpc"
)

func TestUnaryFetchClusters(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fetchCluster()
	assert.Nil(t, err)
	fcRes := rpc.NewFetchClusterResponse(res)
	mems := fcRes.IntoMembersAddrs()
	assert.Equal(t, 5, len(mems))
	assert.Equal(t, []string{"A0"}, mems[0])
	assert.Equal(t, []string{"A1"}, mems[1])
	assert.Equal(t, []string{"A2"}, mems[2])
	assert.Equal(t, []string{"A3"}, mems[3])
	assert.Equal(t, []string{"A4"}, mems[4])
}

func TestUnaryFetchClustersFailed(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fetchCluster()
	assert.Nil(t, res)
	assert.NotNil(t, err.GetRpcTransport())
}

func TestFastRoundWorks(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fastRound(&curppb.ProposeId{SeqNum: 1}, nil)
	assert.NotNil(t, res.ExeResult.GetRangeResponse())
	assert.Nil(t, err)
}

func TestFastRoundReturnEarlyErr(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fastRound(&curppb.ProposeId{SeqNum: 2}, nil)
	assert.Nil(t, res)
	assert.NotNil(t, err.GetDuplicated())
}

func TestFastRoundLessQuorum(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fastRound(&curppb.ProposeId{SeqNum: 3}, nil)
	assert.Nil(t, res)
	assert.NotNil(t, err.GetKeyConflict())
}

func TestFastRoundWithTwoLeader(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	defer func() {
		if r := recover(); r != nil {
			unary.fastRound(&curppb.ProposeId{SeqNum: 4}, nil)
		}
	}()
}

func TestFastRoundWithoutLeader(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.fastRound(&curppb.ProposeId{SeqNum: 5}, nil)
	assert.Nil(t, res)
	assert.NotNil(t, err.GetWrongClusterVersion())
}

func TestUnarySlowRoundFetchLeaderFirst(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).build()
	assert.Nil(t, err)
	res, err := unary.slowRound(&curppb.ProposeId{SeqNum: 6})
	assert.Nil(t, err)
	assert.NotNil(t, res.Ok.ExeResult.GetRangeResponse())
}

func TestUnaryProposeFastPathWorks(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).setLeaderState(0, 1).build()
	assert.Nil(t, err)
	res, err := unary.repeatablePropose(&curppb.ProposeId{SeqNum: 7}, &xlineapi.Command{}, true)
	assert.Nil(t, err)
	assert.Equal(t, &xlineapi.RangeResponse{Count: 1}, res.ExeResult.GetRangeResponse())
}

func TestUnaryProposeSlowPathWorks(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).setLeaderState(0, 1).build()
	assert.Nil(t, err)
	res, err := unary.repeatablePropose(&curppb.ProposeId{SeqNum: 7}, &xlineapi.Command{}, false)
	assert.Nil(t, err)
	assert.Equal(t, &xlineapi.RangeResponse{Count: 1}, res.ExeResult.GetRangeResponse())
	assert.Equal(t, int64(1), res.AfterSyncedResult.Revision)
}

func TestUnaryProposeFastPathFallbackSlowPath(t *testing.T) {
	allMembers := map[serverId][]string{
		0: {"127.0.0.1:48081"},
		1: {"127.0.0.1:48082"},
		2: {"127.0.0.1:48083"},
		3: {"127.0.0.1:48084"},
		4: {"127.0.0.1:48085"},
	}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).setLeaderState(0, 1).build()
	assert.Nil(t, err)
	res, err := unary.repeatablePropose(&curppb.ProposeId{SeqNum: 8}, &xlineapi.Command{}, true)
	assert.Nil(t, err)
	assert.Equal(t, &xlineapi.RangeResponse{Count: 1}, res.ExeResult.GetRangeResponse())
	assert.Equal(t, int64(1), res.AfterSyncedResult.Revision)
}

func TestUnaryProposeReturnEarlyErr(t *testing.T) {
	allMembers := map[serverId][]string{0: {"127.0.0.1:48081"}}
	unaryConfig := newUnaryConfig(1*time.Second, 2*time.Second)
	unary, err := newUnaryBuilder(allMembers, unaryConfig).setLeaderState(0, 1).build()
	assert.Nil(t, err)
	res, err := unary.repeatablePropose(&curppb.ProposeId{SeqNum: 9}, &xlineapi.Command{}, true)
	assert.Nil(t, res)
	assert.NotNil(t, err.GetShuttingDown())
}
