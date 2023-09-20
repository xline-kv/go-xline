package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	curpapi "github.com/xline-kv/go-xline/api/curp"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Protocol client
type client struct {
	// Current leader
	leader uint64
	// Inner protocol clients
	inner []curpapi.ProtocolClient
	// All servers' `Connect`
	connects map[uint64]string
	// Curp client timeout settings
	timeout clientTimeout
}

// Build client from addresses, this method will fetch all members from servers
func BuildCurpClientFromAddrs(addrs []string) (*client, error) {
	logger := xlog.GetLogger()
	clientTimeout := NewDefaultClientTimeout()

	var clients []curpapi.ProtocolClient

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithIdleTimeout(clientTimeout.idleTimeout))
		if err != nil {
			logger.Warn("failed to dial grpc", zap.Error(err), zap.String("addr", addr))
		} else {
			protocolClient := curpapi.NewProtocolClient(conn)
			clients = append(clients[:], protocolClient)
		}
	}

	cluster, err := fetchCluster(clients, clientTimeout.proposeTimeout)
	if err != nil {
		return nil, err
	}

	return &client{
		inner:    clients,
		connects: cluster.AllMembers,
		timeout:  clientTimeout,
	}, nil
}

type fetchClusterRes struct {
	res *curpapi.FetchClusterResponse
	err error
}

// Fetch cluster from server
func fetchCluster(protocolClients []curpapi.ProtocolClient, proposeTimeout time.Duration) (*curpapi.FetchClusterResponse, error) {
	fetchClusterRespCh := make(chan fetchClusterRes)

	for _, protocolClient := range protocolClients {
		client := protocolClient
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
			defer cancel()
			res, err := client.FetchCluster(ctx, &curpapi.FetchClusterRequest{})
			fetchClusterRespCh <- fetchClusterRes{res: res, err: err}
		}()
	}

	for i := 0; i < len(protocolClients); i++ {
		fetchClusterRes := <-fetchClusterRespCh

		if fetchClusterRes.err != nil {
			continue
		}
		return fetchClusterRes.res, nil
	}
	return nil, errors.New("fetch cluster fail")
}

type roundRes struct {
	res *ProposeResponse
	err error
}

// Propose the request to servers, if use_fast_path is false, it will wait for the synced index
func (c *client) Propose(cmd *xlineapi.Command, useFastPath bool) (*ProposeResponse, error) {
	fastResCh := make(chan *roundRes)
	slowResCh := make(chan *roundRes)

	go func() {
		roundRes := c.slowRound(cmd)
		slowResCh <- roundRes
	}()

	go func() {
		roundRes := c.fastRound(cmd)
		fastResCh <- roundRes
	}()

	if useFastPath {
		select {
		case roundRes := <-fastResCh:
			if roundRes.res.IsFastRoundSuccess {
				return roundRes.res, roundRes.err
			}
			roundRes = <-slowResCh
			return roundRes.res, roundRes.err
		case roundRes := <-slowResCh:
			return roundRes.res, roundRes.err
		}
	} else {
		<-fastResCh
		roundRes := <-slowResCh
		return roundRes.res, roundRes.err
	}
}

// The fast round of Curp protocol
// It broadcast the requests to all the curp servers.
func (c *client) fastRound(cmd *xlineapi.Command) *roundRes {
	logger := xlog.GetLogger()

	logger.Info("Fast round start.", zap.String("Propose ID:", cmd.ProposeId))

	isReceivedLeaderReq := false
	isReceivedSuccessRes := 0
	okCnt := 0
	var cmdRes xlineapi.CommandResponse
	var exeErr xlineapi.ExecuteError
	proposeCh := make(chan *curpapi.ProposeResponse)

	serializeCmd, err := proto.Marshal(cmd)
	if err != nil {
		return &roundRes{res: nil, err: errors.New("command marshal fail")}
	}

	for _, protocolClient := range c.inner {
		client := protocolClient
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout.proposeTimeout)
			defer cancel()
			res, err := client.Propose(ctx, &curpapi.ProposeRequest{Command: serializeCmd})
			if err != nil {
				logger.Debug("fast round step fail", zap.Error(err), zap.String("Propose ID:", cmd.ProposeId))
				proposeCh <- nil
			} else {
				proposeCh <- res
			}
		}()
	}

	for i := 0; i < len(c.inner); i++ {
		proposeRes := <-proposeCh

		if proposeRes == nil {
			continue
		}

		switch proposeResp := proposeRes.ExeResult.(type) {
		case *curpapi.ProposeResponse_Result:
			switch cmdResult := proposeResp.Result.Result.(type) {
			case *curpapi.CmdResult_Er:
				err := proto.Unmarshal(cmdResult.Er, &cmdRes)
				if err != nil {
					logger.Debug("fast round step fail", zap.Error(err), zap.String("Propose ID:", cmd.ProposeId))
				} else {
					isReceivedLeaderReq = true
					isReceivedSuccessRes = 1
					okCnt++
				}
			case *curpapi.CmdResult_Error:
				err := proto.Unmarshal(cmdResult.Error, &exeErr)
				if err != nil {
					logger.Debug("fast round step fail", zap.Error(err), zap.String("Propose ID:", cmd.ProposeId))
				} else {
					isReceivedLeaderReq = true
					isReceivedSuccessRes = 2
					okCnt++
				}
			default:
				logger.Warn("unknown command result type")
			}
		case *curpapi.ProposeResponse_Error:
			isReceivedLeaderReq = true
			logger.Debug(fmt.Sprintf("%v", proposeResp.Error))
		default:
			okCnt++
		}

		if isReceivedLeaderReq && okCnt >= superQuorum(len(c.connects)) {
			logger.Info("Fast round success.", zap.String(" ProposeID:", cmd.ProposeId))
			if isReceivedSuccessRes == 1 {
				return &roundRes{
					res: &ProposeResponse{
						CommandResp:        &cmdRes,
						IsFastRoundSuccess: true,
					},
					err: err,
				}
			}
			if isReceivedSuccessRes == 2 {
				return &roundRes{err: ProposeError{ExecuteError: &exeErr}}
			}
		}
	}

	logger.Info("Fast round fail.", zap.String(" ProposeID:", cmd.ProposeId))
	return &roundRes{
		res: &ProposeResponse{
			IsFastRoundSuccess: false,
		},
		err: ProposeError{
			ExecuteError: &exeErr,
		},
	}
}

// The slow round of Curp protocol
func (c *client) slowRound(cmd *xlineapi.Command) *roundRes {
	logger := xlog.GetLogger()

	logger.Info("Slow round start.", zap.String("Propose ID:", cmd.ProposeId))

	var leaderId uint64
	var syncResp xlineapi.SyncResponse
	var commandResp xlineapi.CommandResponse

	if c.leader == 0 {
		leaderId = *c.fetchLeader()
	} else {
		leaderId = c.leader
	}

	conn, err := grpc.Dial(c.connects[leaderId], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithIdleTimeout(c.timeout.idleTimeout))
	if err != nil {
		return &roundRes{res: nil, err: err}
	}
	protocolClient := curpapi.NewProtocolClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout.waitSyncedTimeout)
	defer cancel()
	res, err := protocolClient.WaitSynced(ctx, &curpapi.WaitSyncedRequest{ProposeId: cmd.ProposeId})
	if err != nil {
		return &roundRes{res: nil, err: err}
	}
	waitSyncedResponse := res

	switch syncResult := waitSyncedResponse.SyncResult.(type) {
	case *curpapi.WaitSyncedResponse_Success_:
		err := proto.Unmarshal(syncResult.Success.AfterSyncResult, &syncResp)
		if err != nil {
			return &roundRes{res: nil, err: errors.New("unmarshal syncResp fail")}
		}
		err = proto.Unmarshal(syncResult.Success.ExeResult, &commandResp)
		if err != nil {
			return &roundRes{res: nil, err: errors.New("unmarshal commandResp fail")}
		}
		logger.Info("Slow round success.", zap.String("Propose ID:", cmd.ProposeId))
		return &roundRes{
			res: &ProposeResponse{
				SyncResp:    &syncResp,
				CommandResp: &commandResp,
			},
			err: nil,
		}
	case *curpapi.WaitSyncedResponse_Error:
		logger.Info("Slow round success.", zap.String("Propose ID:", cmd.ProposeId))
		return &roundRes{
			res: nil,
			err: CommandSyncError{
				CommandSyncError: syncResult.Error,
			},
		}
	default:
		logger.Info("Slow round error.", zap.String("Propose ID:", cmd.ProposeId))
		return &roundRes{
			res: nil,
			err: errors.New("unknown synced response"),
		}
	}
}

// Send fetch leader requests to all servers until there is a leader
// Note: The fetched leader may still be outdated
func (c *client) fetchLeader() *uint64 {
	fetchClusterRespCh := make(chan *curpapi.FetchLeaderResponse)
	var leader *uint64
	var maxTerm uint64 = 0
	okCnt := 0
	majorityCnt := len(c.connects)/2 + 1

	for {
		for _, protocolClient := range c.inner {
			client := protocolClient
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), c.timeout.proposeTimeout)
				defer cancel()
				res, err := client.FetchLeader(ctx, &curpapi.FetchLeaderRequest{})
				if err != nil {
					fetchClusterRespCh <- nil
				} else {
					fetchClusterRespCh <- res
				}
			}()
		}

		for {
			fetchClusterRes := <-fetchClusterRespCh

			if fetchClusterRes == nil {
				continue
			}

			if maxTerm < fetchClusterRes.Term {
				maxTerm = fetchClusterRes.Term
				leader = fetchClusterRes.LeaderId
				okCnt = 1
			} else if maxTerm == fetchClusterRes.Term {
				leader = fetchClusterRes.LeaderId
				okCnt += 1
			}
			if okCnt >= majorityCnt {
				break
			}
		}
		c.leader = *leader

		return leader
	}
}

type ProposeResponse struct {
	SyncResp           *xlineapi.SyncResponse
	CommandResp        *xlineapi.CommandResponse
	IsFastRoundSuccess bool
}

// Curp client settings
type clientTimeout struct {
	// Curp client wait idle
	idleTimeout time.Duration
	// Curp client wait sync timeout
	waitSyncedTimeout time.Duration
	// Curp client propose request timeout
	proposeTimeout time.Duration
	// Curp client retry interval
	retry_timeout time.Duration
}

func NewDefaultClientTimeout() clientTimeout {
	return clientTimeout{
		idleTimeout:       1 * time.Second,
		waitSyncedTimeout: 2 * time.Second,
		proposeTimeout:    1 * time.Second,
		retry_timeout:     50 * time.Millisecond,
	}
}

type CommandSyncError struct {
	*curpapi.CommandSyncError
}

type ProposeError struct {
	*xlineapi.ExecuteError
}

func (e CommandSyncError) Error() string {
	logger, _ := zap.NewDevelopment()

	switch cmdSyncErr := e.CommandSyncError.CommandSyncError.(type) {
	case *curpapi.CommandSyncError_WaitSync:
		switch syncErr := cmdSyncErr.WaitSync.WaitSyncError.(type) {
		case *curpapi.WaitSyncError_Redirect:
			return fmt.Sprintf("Sync redirect error. SeverId: %v, Term: %v\n", syncErr.Redirect.ServerId, syncErr.Redirect.Term)
		case *curpapi.WaitSyncError_Other:
			return fmt.Sprintln("Sync other error.", syncErr.Other)
		default:
			return "Sync error"
		}
	case *curpapi.CommandSyncError_Execute:
		var execute xlineapi.ExecuteError
		err := proto.Unmarshal(cmdSyncErr.Execute, &execute)
		if err != nil {
			logger.Warn("", zap.Error(err))
		}
		return fmt.Sprintf("Command sync execute error. %+v", &execute)
	case *curpapi.CommandSyncError_AfterSync:
		var afterSync xlineapi.ExecuteError
		err := proto.Unmarshal(cmdSyncErr.AfterSync, &afterSync)
		if err != nil {
			logger.Warn("", zap.Error(err))
		}
		return fmt.Sprintf("Command after sync execute error. %+v", &afterSync)
	default:
		return "Command sync error"
	}
}

func (e ProposeError) Error() string {
	return fmt.Sprintf("Execute error %+v\n", e.ExecuteError.Error)
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
