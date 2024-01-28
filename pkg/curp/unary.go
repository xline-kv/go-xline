package curp

import (
	"context"
	"math/rand"
	"sync"
	"time"

	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	xlinepb "github.com/xline-kv/go-xline/api/gen/xline"
	"github.com/xline-kv/go-xline/pkg/rpc"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server Id
type serverId = uint64

// Client state
type state struct {
	// Leader id. At the beginning, we may not know who the leader is.
	leader *serverId
	// Term, initialize to 0, calibrated by the server.
	term uint64
	// Cluster version, initialize to 0, calibrated by the server.
	clusterVersion uint64
	// Members' connect
	connects map[serverId]*rpc.Connect
}

// Leader state
type leaderState struct {
	// Server ID
	serverId serverId
	// Term
	term uint64
}

// Unary builder
type unaryBuilder struct {
	// All members (required)
	all_members map[serverId][]string
	// Unary config (required)
	config *unaryConfig
	// Leader state (optional)
	leader_state *leaderState
	// Cluster version (optional)
	cluster_version *uint64
}

// Create unary builder
func newUnaryBuilder(all_members map[serverId][]string, config *unaryConfig) *unaryBuilder {
	return &unaryBuilder{
		all_members: all_members,
		config:      config,
	}
}

// Set the leader state (optional)
func (u *unaryBuilder) setLeaderState(id serverId, term uint64) *unaryBuilder {
	u.leader_state = &leaderState{
		serverId: id,
		term:     term,
	}
	return u
}

// nolint: unused
// Set the cluster version (optional)
func (u *unaryBuilder) setClusterVersion(clusterVersion uint64) *unaryBuilder {
	u.cluster_version = &clusterVersion
	return u
}

// Inner build
func (u *unaryBuilder) buildWithConnects(connects map[serverId]*rpc.Connect) *unary {
	var leader *uint64 = nil
	var term uint64 = 0
	if u.leader_state != nil {
		leader = &u.leader_state.serverId
		term = u.leader_state.term
	}
	var clusterVersion uint64 = 0
	if u.cluster_version != nil {
		clusterVersion = *u.cluster_version
	}
	state := &state{
		leader:         leader,
		term:           term,
		clusterVersion: clusterVersion,
		connects:       connects,
	}
	logger, err := zap.NewProduction()
	sugarLogger := logger.Sugar()
	if err != nil {
		panic(err)
	}
	return &unary{
		state:  state,
		config: u.config,
		logger: sugarLogger,
	}
}

// Build the unary client
func (u *unaryBuilder) build() (*unary, *rpc.CurpError) {
	connects, err := rpc.NewConnects(u.all_members)
	if err != nil {
		return nil, rpc.NewCurpError(err)
	}
	return u.buildWithConnects(connects), nil
}

// The unary client config
type unaryConfig struct {
	// The rpc timeout of a propose request
	proposeTimeout time.Duration
	// The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
	// The recommended the values is within (propose_timeout, 2 * propose_timeout].
	waitSyncedTimeout time.Duration
}

// Create a unary config
func newUnaryConfig(proposeTimeout time.Duration, waitSyncedTimeout time.Duration) *unaryConfig {
	return &unaryConfig{
		proposeTimeout:    proposeTimeout,
		waitSyncedTimeout: waitSyncedTimeout,
	}
}

// The unary client
type unary struct {
	// Client state
	state *state
	// Unary config
	config *unaryConfig
	// Lock
	mu sync.Mutex
	// Logger
	logger *zap.SugaredLogger
}

// Fetch leader id
func (u *unary) fetchLeaderId() (*serverId, *rpc.CurpError) {
	res, err := u.fetchCluster()
	if err != nil {
		return nil, err
	}
	return res.LeaderId, nil
}

// Send fetch cluster requests to all servers
func (u *unary) fetchCluster() (*curppb.FetchClusterResponse, *rpc.CurpError) {
	timeout := u.config.waitSyncedTimeout

	resCh := make(chan *curppb.FetchClusterResponse)
	errCh := make(chan error)

	for _, conn := range u.state.connects {
		go func(conn *rpc.Connect) {
			res, err := conn.FetchCluster(context.Background(), rpc.NewFetchClusterRequest().FetchClusterRequest, timeout)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(conn)
	}

	quorum := quorum(len(u.state.connects))
	var maxTerm uint64 = 0
	var fcResp *curppb.FetchClusterResponse = nil
	var curpErr *rpc.CurpError = nil
	var okCnt int = 0

	for i := 1; i <= len(u.state.connects); i++ {
		select {
		case res := <-resCh:
			// Ignore the response of a node that doesn't know who the leader is.
			if res.LeaderId != nil {
				if maxTerm < res.Term {
					maxTerm = res.Term
					if len(res.Members) != 0 {
						fcResp = res
					}
					// reset ok count to 1
					okCnt = 1
				} else if maxTerm == res.Term {
					if len(res.Members) != 0 {
						fcResp = res
					}
					okCnt++
				}
			}

			// first check quorum
			if okCnt >= quorum {
				// then check if we got the response
				if fcResp != nil {
					u.logger.Debugf("fetch cluster succeeded, result: %v", fcResp)
					if err := u.checkAndUpdate(fcResp); err != nil {
						u.logger.Warnf("update to a new cluster state failed, error %v", err)
					}
					return fcResp, nil
				}
				u.logger.Debugf("fetch cluster quorum ok, but members are empty")
			}
		case err := <-errCh:
			e := rpc.NewCurpError(err)
			if e.ShouldAbortFastRound() {
				return nil, e
			}
			if curpErr != nil {
				oldErr := curpErr
				if oldErr.Priority() <= e.Priority() {
					curpErr = e
				}
			} else {
				curpErr = e
			}
		}
	}

	if curpErr != nil {
		return nil, curpErr
	}

	return nil, &rpc.CurpError{
		CurpError: &curppb.CurpError{
			Err: &curppb.CurpError_RpcTransport{
				RpcTransport: &emptypb.Empty{},
			},
		},
	}
}

// Update client state based on `FetchClusterResponse`
func (u *unary) checkAndUpdate(res *curppb.FetchClusterResponse) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.checkAndUpdateLeader(u.state, res.LeaderId, res.Term) {
		return nil
	}
	if u.state.clusterVersion == res.ClusterVersion {
		u.logger.Debugf("ignore cluster version%v from server", res.ClusterVersion)
		return nil
	}

	u.logger.Debugf("client cluster version updated to %v", res.ClusterVersion)
	u.state.clusterVersion = res.ClusterVersion

	newMembers := rpc.NewFetchClusterResponse(res).IntoMembersAddrs()

	oldIds := map[serverId]struct{}{}
	newIds := map[serverId]struct{}{}
	for id := range u.state.connects {
		oldIds[id] = struct{}{}
	}
	for id := range newMembers {
		newIds[id] = struct{}{}
	}

	diffs := map[serverId]struct{}{}
	// typos:ignore
	sames := map[serverId]struct{}{}
	for id := range oldIds {
		if _, ok := newIds[id]; !ok {
			diffs[id] = struct{}{}
		} else {
			// typos:ignore
			sames[id] = struct{}{}
		}
	}
	for id := range newIds {
		if _, ok := oldIds[id]; !ok {
			diffs[id] = struct{}{}
		}
	}

	for diff := range diffs {
		if _, ok := newMembers[diff]; ok {
			if _, ok := u.state.connects[diff]; !ok {
				panic("diff must in old member addrs")
			}
			u.logger.Debugf("client connects to a new server%v, address%v", diff, newMembers[diff])
			conn, err := rpc.NewConnect(diff, newMembers[diff])
			if err != nil {
				return err
			}
			u.state.connects[diff] = conn
		} else {
			u.logger.Debugf("client removes old server%v", diff)
			delete(u.state.connects, diff)
		}
	}
	// typos:ignore
	for same := range sames {
		if conn, ok := u.state.connects[same]; ok {
			if addrs, ok := newMembers[same]; ok {
				conn.UpdateAddrs(addrs)
			} else {
				panic("some must in new member addrs")
			}
		} else {
			panic("some must in old member addrs")
		}
	}

	return nil
}

// Update leader
func (u *unary) checkAndUpdateLeader(state *state, leaderId *serverId, term uint64) bool {
	if state.term < term {
		// reset term only when the resp has leader id to prevent:
		// If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
		// But if the client learns about the new term and updates its term to it, it will never get the true leader.
		if leaderId != nil {
			newLeaderId := leaderId
			u.logger.Debugf("client term updates to %v", term)
			u.logger.Debugf("client leader id updates to %v", newLeaderId)
			state.term = term
			state.leader = newLeaderId
		}
	} else if state.term == term {
		if leaderId != nil {
			newLeaderId := leaderId
			if state.leader == nil {
				u.logger.Debugf("client leader id updates to %v", newLeaderId)
				state.leader = newLeaderId
				if state.leader != newLeaderId {
					panic("there should never be two leader in one term")
				}
			}
		}
	} else {
		u.logger.Debugf("ignore old term%v from server", term)
		return false
	}
	return true
}

// nolint: unused
// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
// requests (event the requests are commutative).
func (u *unary) propose(cmd *xlinepb.Command, useFastPath bool) (*rpc.ProposeResult_, *rpc.CurpError) {
	proposeId, err := u.genProposeID()
	if err != nil {
		return nil, rpc.NewCurpError(err)
	}
	return u.repeatablePropose(proposeId, cmd, useFastPath)
}

// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
// requests (event the requests are commutative).
func (u *unary) repeatablePropose(
	proposeId *curppb.ProposeId,
	cmd *xlinepb.Command,
	useFastPath bool,
) (*rpc.ProposeResult_, *rpc.CurpError) {
	fastResCh := make(chan *rpc.ProposeResult)
	fastErrCh := make(chan *rpc.CurpError)
	slowResCh := make(chan *rpc.WaitSyncedResult)
	slowErrCh := make(chan *rpc.CurpError)

	go func() {
		res, err := u.fastRound(proposeId, cmd)
		if err != nil {
			fastErrCh <- err
			return
		}
		fastResCh <- res
	}()
	go func() {
		res, err := u.slowRound(proposeId)
		if err != nil {
			slowErrCh <- err
			return
		}
		slowResCh <- res
	}()

	if useFastPath {
		select {
		case fastResp := <-fastResCh:
			return &rpc.ProposeResult_{
				ExeResult: fastResp.ExeResult,
			}, nil
		case fastErr := <-fastErrCh:
			if fastErr.ShouldAboutSlowRound() {
				return nil, fastErr
			}
			select {
			case slowResp := <-slowResCh:
				return &rpc.ProposeResult_{
					ExeResult:         slowResp.Ok.ExeResult,
					AfterSyncedResult: slowResp.Ok.AfterSyncedResult,
				}, nil
			case slowErr := <-slowErrCh:
				if fastErr.Priority() > slowErr.Priority() {
					return nil, fastErr
				}
				return nil, slowErr
			}
		case slowResp := <-slowResCh:
			return &rpc.ProposeResult_{
				ExeResult:         slowResp.Ok.ExeResult,
				AfterSyncedResult: slowResp.Ok.AfterSyncedResult,
			}, nil
		case slowErr := <-slowErrCh:
			if slowErr.ShouldAbortFastRound() {
				return nil, slowErr
			}
			// try to poll fast round
			select {
			case fastResp := <-fastResCh:
				return &rpc.ProposeResult_{
					ExeResult: fastResp.ExeResult,
				}, nil
			case fastErr := <-fastErrCh:
				if fastErr.Priority() > slowErr.Priority() {
					return nil, fastErr
				}
				return nil, slowErr
			}
		}
	} else {
		select {
		case slowRes := <-slowResCh:
			return &rpc.ProposeResult_{
				ExeResult:         slowRes.Ok.ExeResult,
				AfterSyncedResult: slowRes.Ok.AfterSyncedResult,
			}, nil
		case slowErr := <-slowErrCh:
			return nil, slowErr
		case fastErr := <-fastErrCh:
			slowErr := <-slowErrCh
			if fastErr.Priority() > slowErr.Priority() {
				return nil, fastErr
			}
			return nil, slowErr
		}
	}
}

// Send proposal to all servers
func (u *unary) fastRound(proposeId *curppb.ProposeId, cmd *xlinepb.Command) (*rpc.ProposeResult, *rpc.CurpError) {
	req := rpc.NewProposeRequest(proposeId, cmd, u.state.clusterVersion).ProposeRequest
	timeout := u.config.proposeTimeout

	resCh := make(chan *curppb.ProposeResponse)
	errCh := make(chan error)

	for _, conn := range u.state.connects {
		go func(conn *rpc.Connect) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := conn.Propose(ctx, req, timeout)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(conn)
	}

	superQuorum := superQuorum(len(u.state.connects))
	var curpErr *rpc.CurpError = nil
	var exeResult *xlinepb.CommandResponse = nil
	okCnt := 0

	for i := 0; i < len(u.state.connects); i++ {
		select {
		case res := <-resCh:
			okCnt++
			proposeResult, err := rpc.NewProposeResponse(res).DeserializeResult()
			if err != nil {
				return nil, err
			}
			if proposeResult != nil {
				if exeResult != nil {
					panic("should not set exe result twice")
				}
				if proposeResult.ExeErr != nil {
					return proposeResult, nil
				}
				if proposeResult.ExeResult != nil {
					exeResult = proposeResult.ExeResult
				}
			}
		case err := <-errCh:
			e := rpc.NewCurpError(err)
			if e.ShouldAbortFastRound() {
				return nil, e
			}
			if curpErr != nil {
				oldErr := curpErr
				if oldErr.Priority() <= e.Priority() {
					curpErr = e
				}
			} else {
				curpErr = e
			}
		}

		if okCnt >= superQuorum {
			if exeResult != nil {
				return &rpc.ProposeResult{
					ExeResult: exeResult,
				}, nil
			}
		}
	}

	if curpErr != nil {
		return nil, curpErr
	}

	return nil, &rpc.CurpError{CurpError: &curppb.CurpError{Err: &curppb.CurpError_WrongClusterVersion{WrongClusterVersion: &emptypb.Empty{}}}}
}

// Wait synced result from server
func (u *unary) slowRound(proposeId *curppb.ProposeId) (*rpc.WaitSyncedResult, *rpc.CurpError) {
	timeout := u.config.waitSyncedTimeout
	req := rpc.NewWaitSyncedRequest(proposeId, u.state.clusterVersion).WaitSyncedRequest

	cached_leader := u.state.leader
	// nolint: ineffassign
	var leaderId uint64 = 0
	if cached_leader != nil {
		id := cached_leader
		leaderId = *id
	} else {
		id, err := u.fetchLeaderId()
		if err != nil {
			return nil, err
		}
		leaderId = *id
	}
	conn := u.state.connects[leaderId]
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := conn.WaitSynced(ctx, req, timeout)
	if err != nil {
		return nil, rpc.NewCurpError(err)
	}

	waitSyncedResult, waitSyncedErr := rpc.NewWaitSyncedResponse(res).DeserializeResult()
	if waitSyncedErr != nil {
		return nil, waitSyncedErr
	}
	return waitSyncedResult, nil
}

// nolint: unused
// Generate a propose id
func (u *unary) genProposeID() (*curppb.ProposeId, error) {
	clientID, err := u.getClientId()
	if err != nil {
		return nil, err
	}
	seqNum := u.newSeqNum()
	return &curppb.ProposeId{
		ClientId: clientID,
		SeqNum:   seqNum,
	}, nil
}

// nolint: unused
// Get the client id
// TODO: grant a client id from server
func (u *unary) getClientId() (uint64, error) {
	return rand.Uint64(), nil
}

// nolint: unused
// New a seq num and record it
// TODO: implement request tracker
func (u *unary) newSeqNum() uint64 {
	return 0
}

// / Calculate the super quorum
// Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
// (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
// clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
func superQuorum(nodes int) int {
	faultTolerance := nodes / 2
	quorum := faultTolerance + 1
	superquorum := faultTolerance + (quorum / 2) + 1
	return superquorum
}

// / Calculate the quorum
func quorum(size int) int {
	return size/2 + 1
}
