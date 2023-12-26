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

package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	curpapi "github.com/xline-kv/go-xline/api/curp"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Curp interface {
	// Propose the request to servers, if use_fast_path is false, it will wait for the synced index
	Propose(cmd *xlineapi.Command, useFastPath bool) (*proposeRes, error)

	// Generate a propose id
	GenProposeID() (*curpapi.ProposeId, error)
}

// Protocol client
type protocolClient struct {
	// local server id. Only use in an inner client.
	// localServerID ServerId
	// Current leader and term
	state *state
	// All servers's `Connect`
	connects map[ServerId]*grpc.ClientConn
	/// Cluster version
	clusterVersion uint64
	// Curp client config settings
	// To keep Command type
	config *ClientConfig
	// Logger
	logger *zap.Logger
}

// Build client from addresses, this method will fetch all members from servers
func BuildCurpClientFromAddrs(addrs []string, config *ClientConfig) (*protocolClient, error) {
	conns := make(map[ServerId]*grpc.ClientConn)

	if config == nil {
		return nil, fmt.Errorf("timeout is required")
	}

	res, err := fastFetchCluster(addrs, config.ProposeTimeout)
	if err != nil {
		return nil, err
	}

	for _, node := range res.Members {
		conn, err := grpc.Dial(node.Addrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns[node.Id] = conn
	}

	client := &protocolClient{
		state:          &state{leader: *res.LeaderId, term: res.Term},
		config:         config,
		clusterVersion: res.ClusterVersion,
		connects:       conns,
		logger:         xlog.GetLogger(),
	}

	return client, nil
}

// Fetch cluster from server, return the first `FetchClusterResponse`
func fastFetchCluster(addrs []string, proposeTimeout time.Duration) (*curpapi.FetchClusterResponse, error) {
	logger := xlog.GetLogger()

	resCh := make(chan *curpapi.FetchClusterResponse)
	errCh := make(chan error)

	for _, addr := range addrs {
		addr := addr
		go func() {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			protocolClient := curpapi.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
			defer cancel()
			res, err := protocolClient.FetchCluster(ctx, &curpapi.FetchClusterRequest{})
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}()
	}

	for i := 0; i < len(addrs); i++ {
		select {
		case res := <-resCh:
			return res, nil
		case err := <-errCh:
			logger.Warn("fetch cluster fail", zap.Error(err))
		}
	}

	return nil, fmt.Errorf("fetch cluster fail")
}

// Propose the request to servers, if use_fast_path is false, it will wait for the synced index
func (c *protocolClient) Propose(cmd *xlineapi.Command, useFastPath bool) (*proposeRes, error) {
	var res *proposeRes
	var err error
	pid, err := c.GenProposeID()
	if err != nil {
		return nil, err
	}
	for {
		if useFastPath {
			res, err = c.fastPath(pid, cmd)
		} else {
			res, err = c.slowPath(pid, cmd)
		}

		if errors.Is(err, ErrWrongClusterVersion) {
			cluster, err := c.fetchCluster(false)
			if err != nil {
				return nil, err
			}
			err = c.setCluster(cluster)
			if err != nil {
				return nil, err
			}
			continue
		}
		return res, err
	}
}

// Fast path of propose
func (c *protocolClient) fastPath(pid *curpapi.ProposeId, cmd *xlineapi.Command) (*proposeRes, error) {
	fastCh := make(chan *fastRoundRes)
	slowCh := make(chan *slowRoundRes)
	errCh := make(chan error)

	go func() {
		res, err := c.fastRound(pid, cmd)
		if err != nil {
			errCh <- err
			return
		}
		fastCh <- res
	}()
	go func() {
		res, err := c.slowRound(pid, cmd)
		if err != nil {
			errCh <- err
			return
		}
		slowCh <- res
	}()

	for {
		select {
		case res := <-fastCh:
			if res.isSucc {
				return &proposeRes{Er: res.er}, nil
			}
		case res := <-slowCh:
			return &proposeRes{Er: res.er, Asr: res.asr}, nil
		case err := <-errCh:
			return nil, err
		}
	}
}

// Slow path of propose
func (c *protocolClient) slowPath(pid *curpapi.ProposeId, cmd *xlineapi.Command) (*proposeRes, error) {
	slowCh := make(chan *slowRoundRes)
	errCh := make(chan error)

	go func() {
		// nolint: errcheck
		c.fastRound(pid, cmd)
	}()
	go func() {
		res, err := c.slowRound(pid, cmd)
		if err != nil {
			errCh <- err
		}
		slowCh <- res
	}()

	select {
	case res := <-slowCh:
		return &proposeRes{Er: res.er, Asr: res.asr}, nil
	case err := <-errCh:
		return nil, err
	}
}

// The fast round of Curp protocol
// It broadcast the requests to all the curp servers.
func (c *protocolClient) fastRound(pid *curpapi.ProposeId, cmd *xlineapi.Command) (*fastRoundRes, error) {
	c.logger.Info("fast round started", zap.Any("propose ID", pid))

	resCh := make(chan *curpapi.ProposeResponse)
	errCh := make(chan error)
	var cmdEr xlineapi.CommandResponse
	var cmdErr xlineapi.ExecuteError
	var superQuorum = superQuorum(len(c.connects))
	okCnt := 0
	isLeaderOK := false

	bcmd, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	req := &curpapi.ProposeRequest{
		Command:        bcmd,
		ClusterVersion: c.clusterVersion,
	}

	for _, conn := range c.connects {
		conn := conn
		go func() {
			protocolClient := curpapi.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), c.config.ProposeTimeout)
			defer cancel()
			res, err := protocolClient.Propose(ctx, req)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}()
	}

	
	for i := 0; i < len(c.connects); i++ {
		select {
		case res := <-resCh:
			okCnt++
			switch cmdRes := res.GetResult().Result.(type) {
			case *curpapi.CmdResult_Ok:
				if isLeaderOK {
					panic("should not set exe result twice")
				}
				isLeaderOK = true
				err := proto.Unmarshal(cmdRes.Ok, &cmdEr)
				if err != nil {
					panic(err)
				}
			case *curpapi.CmdResult_Error:
				err := proto.Unmarshal(cmdRes.Error, &cmdErr)
				if err != nil {
					panic(err)
				}
				return nil, &CommandError{err: &cmdErr}
			}
		case err := <-errCh:
			c.logger.Warn("propose fail", zap.Error(err))
			if fromErr, ok := status.FromError(err); ok {
				msg := fromErr.Message()
				if msg == "shutting down" {
					return nil, ErrShuttingDown
				}
				if msg == "wrong cluster version" {
					return nil, ErrWrongClusterVersion
				}
			}
			return nil, err
		}
	}

	if okCnt >= superQuorum && isLeaderOK {
		c.logger.Info("fast round succeeded", zap.Any("propose ID", pid))
		return &fastRoundRes{er: &cmdEr, isSucc: true}, nil
	}

	c.logger.Info("fast round failed", zap.Any("propose ID", pid))
	return &fastRoundRes{isSucc: false}, nil
}

// The slow round of Curp protocol
func (c *protocolClient) slowRound(pid *curpapi.ProposeId, cmd *xlineapi.Command) (*slowRoundRes, error) {
	c.logger.Info("slow round started", zap.Any("propose ID", pid))

	var asr xlineapi.SyncResponse
	var er xlineapi.CommandResponse
	var exeErr xlineapi.ExecuteError

	retryTimeout := c.getBackoff()
	retryCnt := c.config.RetryCount
	for i := 0; i < retryCnt; i++ {
		leaderID, err := c.getLeaderID()
		if err != nil {
			c.logger.Warn("get leader id error", zap.Error(err))
			continue
		}

		protocolClient := curpapi.NewProtocolClient(c.connects[*leaderID])
		ctx, cancel := context.WithTimeout(context.Background(), c.config.ProposeTimeout)
		defer cancel()
		req := &curpapi.WaitSyncedRequest{
			ProposeId:      pid,
			ClusterVersion: c.clusterVersion,
		}
		res, err := protocolClient.WaitSynced(ctx, req)
		if err != nil {
			if fromErr, ok := status.FromError(err); ok {
				msg := fromErr.Message()
				if msg == "shutting down" {
					return nil, ErrShuttingDown
				}
				if msg == "wrong cluster version" {
					return nil, ErrWrongClusterVersion
				}
				if msg == "rpc transport" {
					// it's quite likely that the leader has crashed, then we should wait for some time and fetch the leader again
					time.Sleep(retryTimeout.nextRetry())
					c.resendPropose(pid, cmd, nil)
					continue
				}
				// TODO: redirect error
			}
			return nil, err
		}

		if res.AfterSyncResult != nil {
			switch r := res.AfterSyncResult.Result.(type) {
			case *curpapi.CmdResult_Ok:
				err := proto.Unmarshal(r.Ok, &asr)
				if err != nil {
					panic(err)
				}
			case *curpapi.CmdResult_Error:
				err := proto.Unmarshal(r.Error, &exeErr)
				if err != nil {
					panic(err)
				}
				c.logger.Info("slow round failed", zap.Any("propose ID", pid))
				return nil, &CommandError{err: &exeErr}
			}
		}
		if res.ExeResult != nil {
			switch r := res.ExeResult.Result.(type) {
			case *curpapi.CmdResult_Ok:
				err := proto.Unmarshal(r.Ok, &er)
				if err != nil {
					panic(err)
				}
			case *curpapi.CmdResult_Error:
				err := proto.Unmarshal(r.Error, &exeErr)
				if err != nil {
					panic(err)
				}
				c.logger.Info("slow round failed", zap.Any("propose ID", pid))
				return nil, &CommandError{err: &exeErr}
			}
		}

		c.logger.Info("slow round succeeded", zap.Any("propose ID", pid))
		return &slowRoundRes{
			asr: &asr,
			er:  &er,
		}, nil
	}

	return nil, ErrTimeout
}

// Resend the propose only to the leader. This is used when leader changes and we need to ensure that the propose is received by the new leader.
func (c *protocolClient) resendPropose(pid *curpapi.ProposeId, cmd *xlineapi.Command, newLeader *ServerId) error {
	retryTimeout := c.getBackoff()
	retryCnt := c.config.RetryCount

	for i := 0; i < retryCnt; i++ {
		time.Sleep(retryTimeout.nextRetry())

		var leaderID ServerId
		if newLeader != nil {
			leaderID = *newLeader
		} else {
			res, err := c.fetchLeader()
			if err != nil {
				return err
			}
			leaderID = *res
		}

		bcmd, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}

		protocolClient := curpapi.NewProtocolClient(c.connects[leaderID])
		ctx, cancel := context.WithTimeout(context.Background(), c.config.ProposeTimeout)
		defer cancel()
		_, err = protocolClient.Propose(ctx, &curpapi.ProposeRequest{ProposeId: pid, Command: bcmd, ClusterVersion: c.clusterVersion})
		if err != nil {
			if fromErr, ok := status.FromError(err); ok {
				msg := fromErr.Message()
				if msg == "shutting down" {
					return ErrShuttingDown
				}
				if msg == "wrong cluster version" {
					return ErrWrongClusterVersion
				}
			}
		}
	}

	return ErrTimeout
}

// Generate a propose id
func (c *protocolClient) GenProposeID() (*curpapi.ProposeId, error) {
	clientID, err := c.getClientID()
	if err != nil {
		return nil, err
	}
	seqNum := c.newSeqNum()
	return &curpapi.ProposeId{
		ClientId: clientID,
		SeqNum:   seqNum,
	}, nil
}

// Get the client id
// TODO: grant a client id from server
func (c *protocolClient) getClientID() (uint64, error) {
	return rand.Uint64(), nil
}

// New a seq num and record it
// TODO: implement request tracker
func (c *protocolClient) newSeqNum() uint64 {
	return 0
}

// Send fetch cluster requests to all servers
// Note: The fetched cluster may still be outdated if `linearizable` is false
func (c *protocolClient) fetchCluster(linearizable bool) (*curpapi.FetchClusterResponse, error) {
	var resCh chan *curpapi.FetchClusterResponse
	var errCh chan error

	timeout := c.getBackoff()
	retryCnt := c.config.RetryCount

	var maxTerm uint64 = 0
	var res *curpapi.FetchClusterResponse
	okCnt := 0
	majorityCnt := len(c.connects)/2 + 1

	for i := 0; i < retryCnt; i++ {
		retryTimeout := timeout.nextRetry()
		for _, conn := range c.connects {
			conn := conn
			go func() {
				protocolClient := curpapi.NewProtocolClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), retryTimeout)
				defer cancel()
				r, err := protocolClient.FetchCluster(ctx, &curpapi.FetchClusterRequest{Linearizable: linearizable})
				if err != nil {
					errCh <- err
				}
				resCh <- r
			}()
		}

	Out:
		for i := 0; i < len(c.connects); i++ {
			select {
			case r := <-resCh:
				if maxTerm < r.Term {
					maxTerm = r.Term
					if len(r.Members) != 0 {
						res = r
					}
					okCnt = 1
				}
				if maxTerm == r.Term {
					if len(r.Members) != 0 {
						res = r
					}
					okCnt++
				}
				if okCnt >= majorityCnt {
					break Out
				}
			case err := <-errCh:
				c.logger.Warn("fetch cluster error", zap.Error(err))
			}
		}

		if res != nil {
			c.logger.Info("fetch cluster succeeded")
			c.state.leader = *res.LeaderId
			c.state.term = res.Term
			c.state.checkAndUpdate(*res.LeaderId, res.Term)
			return res, nil
		}
	}
	return nil, errors.New("fetch cluster timeout")
}

func (c *protocolClient) setCluster(cluster *curpapi.FetchClusterResponse) error {
	c.logger.Info("update client by remote cluster", zap.Any("cluster", cluster))

	var conns = make(map[ServerId]*grpc.ClientConn)

	c.state.checkAndUpdate(*cluster.LeaderId, cluster.Term)

	for _, conn := range c.connects {
		conn.Close()
	}

	memberAddrs := cluster.Members
	for _, node := range memberAddrs {
		conn, err := grpc.Dial(node.Addrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		conns[node.Id] = conn
	}
	c.connects = conns

	c.clusterVersion = cluster.ClusterVersion

	return nil
}

// Send fetch leader requests to all servers until there is a leader
// Note: The fetched leader may still be outdated
func (c *protocolClient) fetchLeader() (*ServerId, error) {
	retryCnt := c.config.RetryCount
	for i := 0; i < retryCnt; i++ {
		res, err := c.fetchCluster(false)
		if err != nil {
			c.logger.Warn("fetch cluster error", zap.Error(err))
		} else {
			return res.LeaderId, nil
		}
	}
	return nil, errors.New("fetch leader timeout")
}

// Get leader id from the state or fetch it from servers
func (c *protocolClient) getLeaderID() (*ServerId, error) {
	retryCnt := c.config.RetryCount
	for i := 0; i < retryCnt; i++ {
		if c.state != nil && c.state.leader != 0 {
			return &c.state.leader, nil
		}
		res, err := c.fetchLeader()
		if err != nil {
			c.logger.Warn("fetch leader error", zap.Error(err))
		} else {
			return res, nil
		}
	}
	return nil, errors.New("fetch leader ID timeout")
}

// Get the initial backoff config
func (c *protocolClient) getBackoff() backoff {
	return backoff{
		timeout:    c.config.InitialRetryTimeout,
		maxTimeout: c.config.MaxRetryTimeout,
		useBackoff: *c.config.UseBackoff,
	}
}

// Get the superquorum for curp protocol
// Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
// (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
// clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
func superQuorum(nodes int) int {
	faultTolerance := nodes / 2
	quorum := faultTolerance + 1
	superquorum := faultTolerance + (quorum / 2) + 1
	return superquorum
}

// Server ID
type ServerId = uint64

type state struct {
	// Current leader
	leader ServerId
	// Current term
	term uint64
}

func (s *state) checkAndUpdate(leaderID uint64, term uint64) {
	if s.term < term {
		// reset term only when the resp has leader id to prevent:
		// If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
		// But if the client learns about the new term and updates its term to it, it will never get the true leader.
		if leaderID != 0 {
			newLeaderID := leaderID
			s.updateToTerm(term)
			s.setLeader(newLeaderID)
		}
	}
	if s.term == term {
		if leaderID == 0 {
			newLeaderID := leaderID
			if s.leader == 0 {
				s.setLeader(newLeaderID)
			}
			if s.leader == newLeaderID {
				panic("there should never be two leader in one term")
			}
		}
	}
}

// Set the leader and notify all the waiters
func (s *state) setLeader(id ServerId) {
	s.leader = id
}

// Update to the newest term and reset local cache
func (s *state) updateToTerm(term uint64) {
	if s.term < term {
		panic(fmt.Sprintf("the client's term %d should not be greater than the given term %d when update the term", s.term, term))
	}
	s.term = term
	s.leader = 0
}

type backoff struct {
	// Current timeout
	timeout time.Duration
	// Max timeout
	maxTimeout time.Duration
	// Whether to use backoff
	useBackoff bool
}

func (b *backoff) nextRetry() time.Duration {
	current := b.timeout
	if b.useBackoff {
		if 2*b.timeout <= b.maxTimeout {
			current = 2 * b.timeout
		} else {
			current = b.maxTimeout
		}
	}
	return current
}

type fastRoundRes struct {
	er     *xlineapi.CommandResponse
	isSucc bool
}

type slowRoundRes struct {
	asr *xlineapi.SyncResponse
	er  *xlineapi.CommandResponse
}

type proposeRes struct {
	Er  *xlineapi.CommandResponse
	Asr *xlineapi.SyncResponse
}
