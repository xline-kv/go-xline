package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
)

type KV interface {
	// Put puts a key-value pair into xline.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	Put(ctx context.Context, key, val string, opts ...putOption) (*putResponse, error)

	// Get retrieves keys.
	// By default, Get will return the value for "key", if any.
	// When passed WithRange(end), Get will return the keys in the range [key, end).
	// When passed WithFromKey(), Get returns keys greater than or equal to key.
	// When passed WithRev(rev) with rev > 0, Get retrieves keys at the given revision;
	// if the required revision is compacted, the request will fail with ErrCompacted .
	// When passed WithLimit(limit), the number of returned keys is bounded by limit.
	// When passed WithSort(), the keys will be sorted.
	Get(ctx context.Context, key string, opts ...rangeOption) (*getResponse, error)
}

// / Client for KV operations.
type kvClient struct {
	// Name of the kv client, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The auth token
	token string
}

// Client for KV operations.
func newKvClient(name string, curpClient curpClient, token string) kvClient {
	return kvClient{name: name, curpClient: curpClient, token: token}
}

// Get a range of keys from the store
func (c *kvClient) Get(ctx context.Context, key string, opts ...rangeOption) (*getResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_RangeRequest{
			RangeRequest: opRange(key, opts...).toReq(),
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}
	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*getResponse)(res.CommandResp.GetRangeResponse()), err
}

// Put a key-value into the store
func (c *kvClient) Put(ctx context.Context, key, val string, opts ...putOption) (*putResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_PutRequest{
			PutRequest: opPut(key, val, opts...).toReq(),
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}
	res, err := c.curpClient.propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	return (*putResponse)(res.CommandResp.GetPutResponse()), err
}

// Delete a range of keys from the store
func (c *kvClient) Delete(request *xlineapi.DeleteRangeRequest) (*xlineapi.DeleteRangeResponse, error) {
	keyRange := []*xlineapi.KeyRange{
		{
			Key:      request.Key,
			RangeEnd: request.RangeEnd,
		},
	}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_DeleteRangeRequest{
			DeleteRangeRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Keys: keyRange, Request: &requestWithToken, ProposeId: proposeId}
	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return res.CommandResp.GetDeleteRangeResponse(), err
}

// Creates a transaction, which can provide serializable writes
func (c *kvClient) Txn(request *xlineapi.TxnRequest) (*xlineapi.TxnResponse, error) {
	var keyRange []*xlineapi.KeyRange

	for _, cmp := range request.Compare {
		r := xlineapi.KeyRange{
			Key:      cmp.Key,
			RangeEnd: cmp.RangeEnd,
		}
		keyRange = append(keyRange, &r)
	}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_TxnRequest{
			TxnRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Keys: keyRange, Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	if res.SyncResp == nil {
		return nil, errors.New("syncRes is always Some when useFastPath is false")
	}
	if res.CommandResp.GetTxnResponse() == nil {
		return nil, errors.New("get txn response fail")
	}
	res.CommandResp.GetTxnResponse().Header.Revision = res.SyncResp.Revision

	return res.CommandResp.GetTxnResponse(), err
}

// Compacts the key-value store up to a given revision.
// All keys with revisions less than the given revision will be compacted.
// The compaction process will remove all historical versions of these keys, except for the most recent one.
// For example, here is a revision list: [(A, 1), (A, 2), (A, 3), (A, 4), (A, 5)].
// We compact at revision 3. After the compaction, the revision list will become [(A, 3), (A, 4), (A, 5)].
// All revisions less than 3 are deleted. The latest revision, 3, will be kept.
func (c *kvClient) Compact(request *xlineapi.CompactionRequest) (*xlineapi.CompactionResponse, error) {
	useFastPath := request.Physical
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_CompactionRequest{
			CompactionRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Keys: []*xlineapi.KeyRange{}, Request: &requestWithToken, ProposeId: proposeId}

	if useFastPath {
		res, err := c.curpClient.propose(&cmd, true)
		return res.CommandResp.GetCompactionResponse(), err
	} else {
		res, err := c.curpClient.propose(&cmd, false)
		if err != nil {
			return nil, err
		}
		if res.SyncResp == nil {
			return nil, errors.New("syncRes is always Some when useFastPath is false")
		}
		if res.CommandResp.GetCompactionResponse() == nil {
			return nil, errors.New("get compaction response fail")
		}
		res.CommandResp.GetCompactionResponse().Header.Revision = res.SyncResp.Revision
		return res.CommandResp.GetCompactionResponse(), err
	}
}

// Generate a new `ProposeId`
func (c kvClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}
