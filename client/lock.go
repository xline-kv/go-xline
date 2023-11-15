package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap"
)

// Client for Lock operations.
type lockClient struct {
	// Name of the LockClient
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The lease client
	leaseClient leaseClient
	// The watch client
	watchClient watchClient
	// Auth token
	token string
}

// Creates a new `LockClient`
func newLockClient(
	name string,
	curpClient curpClient,
	innerLeaseClient leaseClient,
	innerWatchClient watchClient,
	token string,
) lockClient {
	return lockClient{
		name:        name,
		curpClient:  curpClient,
		leaseClient: innerLeaseClient,
		watchClient: innerWatchClient,
		token:       token,
	}
}

// The inner lock logic
func (c *lockClient) lockInner(
	prefix string,
	key string,
	leaseId int64,
	lockSuccess bool,
) (*xlineapi.LockResponse, error) {
	logger := xlog.GetLogger()

	txn := c.createAcquireTxn(prefix, leaseId)
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_TxnRequest{
			TxnRequest: &txn,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{
		Request:   &requestWithToken,
		ProposeId: proposeId,
	}
	res, err := c.curpClient.propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	txnRes := res.CommandResp.GetTxnResponse()

	if res.SyncResp == nil {
		return nil, errors.New("SyncRes always has value when use slow path")
	}
	myRev := res.SyncResp.Revision

	ownerRes := txnRes.Responses[1].GetResponseRange()
	if ownerRes == nil {
		return nil, errors.New("get owner res fail")
	}
	ownerKey := ownerRes.Kvs

	var header *xlineapi.ResponseHeader
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == myRev {
		header = ownerRes.Header
	} else {
		c.waitDelete(prefix, myRev)
		rangeReq := xlineapi.RangeRequest{
			Key: []byte(key),
		}
		requestWithToken := xlineapi.RequestWithToken{
			Token: &c.token,
			RequestWrapper: &xlineapi.RequestWithToken_RangeRequest{
				RangeRequest: &rangeReq,
			},
		}
		proposeId := c.generateProposeId()
		cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

		res, err := c.curpClient.propose(&cmd, true)
		if err == nil {
			res := res.CommandResp.GetRangeResponse()
			if len(res.Kvs) == 0 {
				return nil, errors.New("rpc error session expired")
			}
			header = res.Header
		} else {
			_, err = c.deleteKey([]byte(key))
			if err != nil {
				logger.Warn("Lease ID not found.", zap.Error(err))
			}
			return nil, err
		}
	}

	return &xlineapi.LockResponse{
		Header: header,
		Key:    []byte(key),
	}, nil
}

// Acquires a distributed shared lock on a given named lock.
// On success, it will return a unique key that exists so long as the
// lock is held by the caller. This key can be used in conjunction with
// transactions to safely ensure updates to Xline only occur while holding
// lock ownership. The lock is held until Unlock is called on the key or the
// lease associate with the owner expires.
func (c *lockClient) Lock(request LockRequest) (*xlineapi.LockResponse, error) {
	leaseId := request.Inner.Lease
	if leaseId == 0 {
		res, err := c.leaseClient.Grant(request.TTL)
		if err != nil {
			return nil, err
		}
		leaseId = res.ID
	}
	prefix := fmt.Sprintf("%s/", string(request.Inner.Name))
	key := fmt.Sprintf("%s%x", prefix, leaseId)
	lockSuccess := false
	lockInner, err := c.lockInner(prefix, key, leaseId, lockSuccess)

	return lockInner, err
}

// Takes a key returned by Lock and releases the hold on lock. The
// next Lock caller waiting for the lock will then be woken up and given
// ownership of the lock.
func (c *lockClient) Unlock(request *xlineapi.UnlockRequest) (*xlineapi.UnlockResponse, error) {
	header, err := c.deleteKey(request.Key)
	if err != nil {
		return nil, err
	}
	return &xlineapi.UnlockResponse{Header: header}, nil
}

// Create txn for try acquire lock
func (c *lockClient) createAcquireTxn(prefix string, leaseId int64) xlineapi.TxnRequest {
	key := fmt.Sprintf("%s%x", prefix, leaseId)
	cmp := xlineapi.Compare{
		Result:   xlineapi.Compare_EQUAL,
		Target:   xlineapi.Compare_CREATE,
		Key:      []byte(key),
		RangeEnd: []byte{},
		TargetUnion: &xlineapi.Compare_CreateRevision{
			CreateRevision: 0,
		},
	}
	put := xlineapi.RequestOp{
		Request: &xlineapi.RequestOp_RequestPut{
			RequestPut: &xlineapi.PutRequest{
				Key:   []byte(key),
				Value: []byte{},
				Lease: leaseId,
			},
		},
	}

	get := xlineapi.RequestOp{
		Request: &xlineapi.RequestOp_RequestRange{
			RequestRange: &xlineapi.RangeRequest{
				Key: []byte(key),
			},
		},
	}
	rangeEnd := c.getPrefix([]byte(key))
	getOwner := xlineapi.RequestOp{
		Request: &xlineapi.RequestOp_RequestRange{
			RequestRange: &xlineapi.RangeRequest{
				Key:        []byte(prefix),
				RangeEnd:   rangeEnd,
				Limit:      1,
				SortOrder:  xlineapi.RangeRequest_ASCEND,
				SortTarget: xlineapi.RangeRequest_CREATE,
			},
		},
	}

	return xlineapi.TxnRequest{
		Compare: []*xlineapi.Compare{&cmp},
		Success: []*xlineapi.RequestOp{&put, &getOwner},
		Failure: []*xlineapi.RequestOp{&get, &getOwner},
	}
}

// Wait until last key deleted
func (c *lockClient) waitDelete(pfx string, myRev int64) {
	logger := xlog.GetLogger()

	rev := c.overflowSub(myRev, 1)
	for {
		rangEnd := c.getPrefix([]byte(pfx))
		getReq := xlineapi.RangeRequest{
			Key:               []byte(pfx),
			RangeEnd:          rangEnd,
			Limit:             1,
			SortOrder:         xlineapi.RangeRequest_DESCEND,
			SortTarget:        xlineapi.RangeRequest_CREATE,
			MaxCreateRevision: rev,
		}
		requestWithToken := xlineapi.RequestWithToken{
			Token: &c.token,
			RequestWrapper: &xlineapi.RequestWithToken_RangeRequest{
				RangeRequest: &getReq,
			},
		}
		proposeId := c.generateProposeId()
		cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

		res, err := c.curpClient.propose(&cmd, false)
		if err != nil {
			logger.Error("Range lease id fail.", zap.Error(err))
		}
		response := res.CommandResp.GetRangeResponse()
		var lastKey []byte
		if len(response.Kvs) > 0 && response.Kvs[0] != nil {
			lastKey = response.Kvs[0].Key
		} else {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watcher, _ := c.watchClient.Watch(ctx, lastKey)
		for r := range watcher {
			f := false
			for _, e := range r.Events {
				if e.Type == xlineapi.Event_DELETE {
					f = true
					break
				}
			}
			if f {
				cancel()
				break
			}
		}
	}
}

// Get end of range with prefix
func (c *lockClient) getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:]
		}
	}
	return []byte{}
}

// Delete key
func (c *lockClient) deleteKey(key []byte) (*xlineapi.ResponseHeader, error) {
	delReq := xlineapi.DeleteRangeRequest{
		Key: key,
	}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_DeleteRangeRequest{
			DeleteRangeRequest: &delReq,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	delRes := res.CommandResp.GetDeleteRangeResponse()
	if delRes == nil {
		return nil, errors.New("get delRes fail")
	}
	return delRes.Header, nil
}

// Generate a new `ProposeId`
func (c *lockClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}

// overflowSub performs subtraction that handles overflow.
func (c *lockClient) overflowSub(a, b int64) int64 {
	res := a - b
	if res > a {
		return math.MinInt64
	} else {
		return res
	}
}

const DEFAULT_SESSION_TTL int64 = 60

// Request for `Lock`
type LockRequest struct {
	// The Inner request
	Inner *xlineapi.LockRequest
	// The TTL of the lease that attached to the lock
	TTL int64
}
