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
	"fmt"

	"github.com/google/uuid"
	pb "github.com/xline-kv/go-xline/api/xline"
)

type KV interface {
	// Range retrieves keys.
	// By default, Range will return the value for "key", if any.
	// When passed WithRange(), Range will returns the keys in the range [key, end).
	// When passed WithFromKey(), Range will returns keys greater than or equal to the key.
	// When passed WithPrefix(), Range will returns keys that begin with the key.
	// When passed WithLimit(), Range will returns keys is bounded by limit.
	// When passed WithRev() with rev > 0, Range will returns key at the given revision.
	// When passed WithSort(), Range will returns keys be sorted.
	// When passed WithSerializable(), Range will makes 'Range' request serializable. By default, it's linearizable.
	// When passed WithKeysOnly(), Range will returns only the keys and the corresponding values will be omitted.
	// When passed WithCountOnly(), Range will returns only the count of keys.
	// When passed WithMinModRev(), Range will returns keys with modification revisions less than the given revision.
	// When passed WithMaxModRev(), Range will returns keys with modification revisions greater than the given revision.
	// When passed WithMinCreateRev(), Range will returns keys with creation revisions less than the given revision.
	// When passed WithMaxCreateRev(), Range will returns keys with creation revisions greater than the given revision.
	Range(key string, opts ...OpOption) (*RangeResponse, error)

	// Put puts a key-value pair into xline.
	// When passed WithLease(), Put will attaches a lease ID to a key in 'Put' request.
	// When passed WithPrevKV(), Put will returns the previous key-value pair before the event happens.
	// When passed WithIgnoreValue(), Put will updates the key using its current value.
	// When passed WithIgnoreLease(), Put will updates the key using its current lease.
	Put(key, val []byte, opts ...OpOption) (*PutResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	// When passed WithPrevKV(), Delete will returns the previous key-value pair before the event happens.
	Delete(key string, opts ...OpOption) (*DeleteResponse, error)

	// Txn creates a transaction.
	Txn() Txn

	// Compact compacts xline KV history before the given rev.
	// When passed WithPhysical(), Compact will wait until all compacted entries are removed from the etcd server's storage.
	Compact(rev int64, opts ...OpOption) (*CompactResponse, error)
}

type (
	CompactResponse pb.CompactionResponse
	PutResponse     pb.PutResponse
	RangeResponse   pb.RangeResponse
	DeleteResponse  pb.DeleteRangeResponse
	TxnResponse     pb.TxnResponse
)

// / Client for KV operations.
type kvClient struct {
	// Name of the kv client, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The auth token
	token string
}

// New `KvClient`
func newKvClient(name string, curpClient curpClient, token string) kvClient {
	return kvClient{name: name, curpClient: curpClient, token: token}
}

// Range a range of keys from the store
func (c *kvClient) Range(key []byte, opt ...OpOption) (*RangeResponse, error) {
	op := OpRange(key, opt...)
	krs := []*pb.KeyRange{op.toKeyRange()}
	req := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_RangeRequest{
			RangeRequest: op.toRangeReq(),
		},
	}
	pid := c.generateProposeId()
	cmd := pb.Command{Keys: krs, Request: &req, ProposeId: pid}
	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*RangeResponse)(res.CommandResp.GetRangeResponse()), err
}

// Put a key-value into the store
func (c *kvClient) Put(key, val []byte, opts ...OpOption) (*PutResponse, error) {
	op := OpPut(key, val, opts...)
	krs := []*pb.KeyRange{op.toKeyRange()}
	req := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_PutRequest{
			PutRequest: op.toPutReq(),
		},
	}
	pid := c.generateProposeId()
	cmd := pb.Command{Keys: krs, Request: &req, ProposeId: pid}
	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*PutResponse)(res.CommandResp.GetPutResponse()), err
}

// Delete a range of keys from the store
func (c *kvClient) Delete(key string, opts ...OpOption) (*DeleteResponse, error) {
	op := OpDelete(key, opts...)
	krs := []*pb.KeyRange{op.toKeyRange()}
	req := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_DeleteRangeRequest{
			DeleteRangeRequest: op.toDeleteReq(),
		},
	}
	pid := c.generateProposeId()
	cmd := pb.Command{Keys: krs, Request: &req, ProposeId: pid}
	res, err := c.curpClient.propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	return (*DeleteResponse)(res.CommandResp.GetDeleteRangeResponse()), err
}

// Creates a transaction, which can provide serializable writes
func (c *kvClient) Txn() Txn {
	return &txn{
		name:       c.name,
		curpClient: c.curpClient,
		token:      c.token,
	}
}

// Compacts the key-value store up to a given revision.
func (c *kvClient) Compact(rev int64, opts ...OpOption) (*CompactResponse, error) {
	r := OpCompact(rev, opts...).toCompactReq()
	useFastPath := r.Physical
	req := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_CompactionRequest{
			CompactionRequest: r,
		},
	}
	id := c.generateProposeId()
	cmd := pb.Command{Request: &req, ProposeId: id}
	res, err := c.curpClient.propose(&cmd, useFastPath)
	if err != nil {
		return nil, err
	}
	return (*CompactResponse)(res.CommandResp.GetCompactionResponse()), err
}

// TODO: update the propose id
// FYI: https://github.com/xline-kv/Xline/blob/84c685ac4b311ec035076b295e192c65644f85b9/curp-external-api/src/cmd.rs#L84
// Generate a new `ProposeId`
func (c kvClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}
