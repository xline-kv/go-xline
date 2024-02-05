package rpc

import (
	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	xlinepb "github.com/xline-kv/go-xline/api/gen/xline"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FetchCluster request
type fetchClusterRequest struct {
	*curppb.FetchClusterRequest
}

// Create a new `FetchCluster` request
func NewFetchClusterRequest() *fetchClusterRequest {
	return &fetchClusterRequest{
		FetchClusterRequest: &curppb.FetchClusterRequest{
			Linearizable: true,
		},
	}
}

// FetchCluster response
type fetchClusterResponse struct {
	*curppb.FetchClusterResponse
}

// Create a new `FetchCluster` response
func NewFetchClusterResponse(res *curppb.FetchClusterResponse) *fetchClusterResponse {
	return &fetchClusterResponse{
		FetchClusterResponse: res,
	}
}

// Get all members addresses
func (r *fetchClusterResponse) IntoMembersAddrs() map[serverId][]string {
	members := map[serverId][]string{}
	for _, member := range r.Members {
		members[member.Id] = member.Addrs
	}
	return members
}

// Propose request
type proposeRequest struct {
	*curppb.ProposeRequest
}

// Create a new `Propose` request
func NewProposeRequest(
	proposeId *curppb.ProposeId,
	cmd *xlinepb.Command,
	clusterVersion uint64,
) *proposeRequest {
	bcmd, err := proto.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return &proposeRequest{
		ProposeRequest: &curppb.ProposeRequest{
			ProposeId:      proposeId,
			Command:        bcmd,
			ClusterVersion: clusterVersion,
		},
	}
}

// Propose response
type proposeResponse struct {
	*curppb.ProposeResponse
}

// Create a new `Propose` response
func NewProposeResponse(res *curppb.ProposeResponse) *proposeResponse {
	return &proposeResponse{
		ProposeResponse: res,
	}
}

// Deserialize result in response
func (r *proposeResponse) DeserializeResult() (*ProposeResult, *CurpError) {
	exeResult := xlinepb.CommandResponse{}
	exeErr := xlinepb.ExecuteError{}

	if r.Result == nil {
		return nil, nil
	}
	switch cmdResult := r.Result.Result.(type) {
	case *curppb.CmdResult_Ok:
		err := proto.Unmarshal(cmdResult.Ok, &exeResult)
		if err != nil {
			return nil, NewCurpError(err)
		}
		return &ProposeResult{ExeResult: &exeResult}, nil
	case *curppb.CmdResult_Error:
		err := proto.Unmarshal(cmdResult.Error, &exeErr)
		if err != nil {
			return nil, NewCurpError(err)
		}
		return &ProposeResult{ExeErr: &exeErr}, nil
	default:
		panic("unknown result type")
	}
}

// Wait synced request
type waitSyncedRequest struct {
	*curppb.WaitSyncedRequest
}

// Create a new `WaitSynced` request
func NewWaitSyncedRequest(id *curppb.ProposeId, clusterVersion uint64) *waitSyncedRequest {
	return &waitSyncedRequest{
		WaitSyncedRequest: &curppb.WaitSyncedRequest{
			ProposeId:      id,
			ClusterVersion: clusterVersion,
		},
	}
}

// Create a new `WaitSynced` response
type WaitSyncedResponse struct {
	*curppb.WaitSyncedResponse
}

// Create a new `WaitSynced` response
func NewWaitSyncedResponse(res *curppb.WaitSyncedResponse) *WaitSyncedResponse {
	return &WaitSyncedResponse{
		WaitSyncedResponse: res,
	}
}

// Deserialize result in response
func (r *WaitSyncedResponse) DeserializeResult() (*WaitSyncedResult, *CurpError) {
	// according to the above methods, we can only get the following `WaitSyncedResult`
	// ER.Ok != nil, Asr.Ok != nil  <-  WaitSyncedResponse.Ok
	// ER.Err != nil, ASR == nil     <-  WaitSyncedResponse.Err
	// ER.Ok != nil, ASR.Err != nil <- WaitSyncedResponse.Err
	if r.ExeResult.Result.(*curppb.CmdResult_Ok) != nil && r.AfterSyncResult.Result.(*curppb.CmdResult_Ok) != nil {
		er := xlinepb.CommandResponse{}
		asr := xlinepb.SyncResponse{}
		err := proto.Unmarshal(r.ExeResult.Result.(*curppb.CmdResult_Ok).Ok, &er)
		if err != nil {
			return nil, NewCurpError(err)
		}
		err = proto.Unmarshal(r.AfterSyncResult.Result.(*curppb.CmdResult_Ok).Ok, &asr)
		if err != nil {
			return nil, NewCurpError(err)
		}
		return &WaitSyncedResult{Ok: &WaitSyncedResultOk{ExeResult: &er, AfterSyncedResult: &asr}}, nil
	}
	if r.ExeResult.Result.(*curppb.CmdResult_Error) != nil && r.AfterSyncResult == nil {
		err := xlinepb.ExecuteError{}
		e := proto.Unmarshal(r.ExeResult.Result.(*curppb.CmdResult_Error).Error, &err)
		if e != nil {
			return nil, NewCurpError(e)
		}
		return &WaitSyncedResult{Err: &err}, nil
	}
	if r.ExeResult.Result.(*curppb.CmdResult_Ok) != nil && r.AfterSyncResult.Result.(*curppb.CmdResult_Error) != nil {
		err := xlinepb.ExecuteError{}
		e := proto.Unmarshal(r.AfterSyncResult.Result.(*curppb.CmdResult_Error).Error, &err)
		if e != nil {
			return nil, NewCurpError(e)
		}
		return &WaitSyncedResult{Err: &err}, nil
	}
	panic("unknown result type")
}

// The fast result
type ProposeResult struct {
	ExeResult *xlinepb.CommandResponse
	ExeErr    *xlinepb.ExecuteError
}

// Wait synced ok response
type WaitSyncedResultOk struct {
	ExeResult         *xlinepb.CommandResponse
	AfterSyncedResult *xlinepb.SyncResponse
}

// Wait synced response
type WaitSyncedResult struct {
	Ok  *WaitSyncedResultOk
	Err *xlinepb.ExecuteError
}

// Propose result
type ProposeResult_ struct {
	ExeResult         *xlinepb.CommandResponse
	AfterSyncedResult *xlinepb.SyncResponse
}

// The priority of curp error
type curpErrorPriority uint

var (
	// Low priority, a low-priority error returned may
	// be overridden by a higher-priority error.
	low curpErrorPriority = 0
	// High priority, high-priority errors will override
	// low-priority errors.
	high curpErrorPriority = 1
)

// Curp error
type CurpError struct {
	*curppb.CurpError
}

// Create a new `CurpError`
func NewCurpError(err error) *CurpError {
	if status, ok := status.FromError(err); ok {
		dtl := status.Details()
		if len(dtl) > 0 {
			return &CurpError{
				CurpError: dtl[0].(*curppb.CurpError),
			}
		}
	}
	return &CurpError{
		CurpError: &curppb.CurpError{
			Err: &curppb.CurpError_Internal{
				Internal: err.Error(),
			},
		},
	}
}

// Whether to abort fast round early
func (e *CurpError) ShouldAbortFastRound() bool {
	switch e.Err.(type) {
	case *curppb.CurpError_Duplicated,
		*curppb.CurpError_ShuttingDown,
		*curppb.CurpError_InvalidConfig,
		*curppb.CurpError_NodeAlreadyExists,
		*curppb.CurpError_NodeNotExists,
		*curppb.CurpError_LearnerNotCatchUp,
		*curppb.CurpError_ExpiredClientId,
		*curppb.CurpError_Redirect_:
		return true
	default:
		return false
	}
}

// Whether to abort slow round early
func (e *CurpError) ShouldAboutSlowRound() bool {
	switch e.Err.(type) {
	case *curppb.CurpError_ShuttingDown,
		*curppb.CurpError_InvalidConfig,
		*curppb.CurpError_NodeAlreadyExists,
		*curppb.CurpError_NodeNotExists,
		*curppb.CurpError_LearnerNotCatchUp,
		*curppb.CurpError_ExpiredClientId,
		*curppb.CurpError_Redirect_,
		*curppb.CurpError_WrongClusterVersion:
		return true
	default:
		return false
	}
}

// Get the priority of the error
func (e *CurpError) Priority() curpErrorPriority {
	switch e.Err.(type) {
	case *curppb.CurpError_Duplicated,
		*curppb.CurpError_ShuttingDown,
		*curppb.CurpError_InvalidConfig,
		*curppb.CurpError_NodeAlreadyExists,
		*curppb.CurpError_NodeNotExists,
		*curppb.CurpError_LearnerNotCatchUp,
		*curppb.CurpError_ExpiredClientId,
		*curppb.CurpError_Redirect_,
		*curppb.CurpError_WrongClusterVersion:
		return high
	case *curppb.CurpError_RpcTransport,
		*curppb.CurpError_Internal,
		*curppb.CurpError_KeyConflict:
		return low
	default:
		panic(e.Err)
	}
}
