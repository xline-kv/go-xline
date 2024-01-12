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

import "github.com/xline-kv/go-xline/api/gen/xline"

// Op represents an Operation that kv can execute.
type Op struct {
	t opType

	rangeOp   *rangeOp
	putOp     *putOp
	delOp     *delOp
	txnOp     *txnOp
	compactOp *compactOp
}

// rangeOp represents an Operation that range can execute.
type rangeOp struct {
	key          []byte
	end          []byte
	limit        int64
	rev          int64
	sort         *SortOption
	serializable bool
	keysOnly     bool
	cntOnly      bool
	minModRev    int64
	maxModRev    int64
	minCreateRev int64
	maxCreateRev int64

	isOptsWithFromKey bool
	isOptsWithPrefix  bool
}

// putOp represents an Operation that put can execute.
type putOp struct {
	key      []byte
	val      []byte
	leaseID  int64
	prevKV   bool
	ignVal   bool
	ignLease bool
}

// delOp represents an Operation that delete can execute.
type delOp struct {
	key    []byte
	end    []byte
	prevKV bool

	isOptsWithFromKey bool
	isOptsWithPrefix  bool
}

// txnOp represents an Operation that transaction can execute.
type txnOp struct {
	cmps    []Cmp
	thenOps []Op
	elseOps []Op
}

// compactOp represents an Operation that compact can execute.
type compactOp struct {
	rev      int64
	physical bool
}

type opType int

const (
	// A default Op has opType 0, which is invalid.
	opRange opType = iota + 1
	opPut
	opDeleteRange
	opTxn
	opCompact
)

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

func (op *Op) applyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpRange returns "range" operation based on given key and operation options.
func OpRange(key []byte, opts ...OpOption) Op {
	op := Op{t: opRange, rangeOp: &rangeOp{key: key}}
	op.applyOpts(opts)

	// WithPrefix and WithFromKey are not supported together
	if op.rangeOp.isOptsWithFromKey && op.rangeOp.isOptsWithPrefix {
		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	}
	switch {
	case op.putOp != nil:
		panic("unexpected put operation in get")
	case op.delOp != nil:
		panic("unexpected delete operation in get")
	case op.txnOp != nil:
		panic("unexpected txn operation in get")
	case op.compactOp != nil:
		panic("unexpected compact operation in get")
	}

	return op
}

// OpPut returns "put" operation based on given key-value and operation options.
func OpPut(key, val []byte, opts ...OpOption) Op {
	op := Op{t: opPut, putOp: &putOp{key: []byte(key), val: []byte(val)}}
	op.applyOpts(opts)

	switch {
	case op.rangeOp != nil:
		panic("unexpected get operation in put")
	case op.delOp != nil:
		panic("unexpected delete operation in put")
	case op.txnOp != nil:
		panic("unexpected txn operation in put")
	case op.compactOp != nil:
		panic("unexpected compact operation in put")
	}

	return op
}

// OpDelete returns "delete" operation based on given key and operation options.
func OpDelete(key string, opts ...OpOption) Op {
	op := Op{t: opDeleteRange, delOp: &delOp{key: []byte(key)}}
	op.applyOpts(opts)

	// WithPrefix and WithFromKey are not supported together
	if op.delOp.isOptsWithFromKey && op.delOp.isOptsWithPrefix {
		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	}
	switch {
	case op.rangeOp != nil:
		panic("unexpected get operation in delete")
	case op.putOp != nil:
		panic("unexpected put operation in delete")
	case op.txnOp != nil:
		panic("unexpected txn operation in delete")
	case op.compactOp != nil:
		panic("unexpected compact operation in delete")
	}

	return op
}

// OpTxn returns "txn" operation based on given transaction conditions.
func OpTxn(cmps []Cmp, thenOps []Op, elseOps []Op) Op {
	return Op{t: opTxn, txnOp: &txnOp{cmps: cmps, thenOps: thenOps, elseOps: elseOps}}
}

// OpCompact returns "compact" operation based on given rev and operation options.
func OpCompact(rev int64, opts ...OpOption) Op {
	op := Op{t: opCompact, compactOp: &compactOp{rev: rev}}
	op.applyOpts(opts)

	switch {
	case op.rangeOp != nil:
		panic("unexpected get operation in compact")
	case op.putOp != nil:
		panic("unexpected put operation in compact")
	case op.delOp != nil:
		panic("unexpected delete operation in compact")
	case op.txnOp != nil:
		panic("unexpected txn operation in compact")
	}

	return op
}

func (op Op) toKeyRange() *xlineapi.KeyRange {
	var kr *xlineapi.KeyRange
	switch op.t {
	case opRange:
		kr = &xlineapi.KeyRange{
			Key:      op.rangeOp.key,
			RangeEnd: op.rangeOp.end,
		}
	case opPut:
		kr = &xlineapi.KeyRange{
			Key:      op.putOp.key,
			RangeEnd: op.putOp.key,
		}
	case opDeleteRange:
		kr = &xlineapi.KeyRange{
			Key:      op.delOp.key,
			RangeEnd: op.delOp.end,
		}
	default:
		panic("undefine operator type")
	}
	return kr
}

func (op Op) toRangeReq() *xlineapi.RangeRequest {
	if op.t != opRange {
		panic("op.t != tRange")
	}
	req := &xlineapi.RangeRequest{
		Key:               op.rangeOp.key,
		RangeEnd:          op.rangeOp.end,
		Limit:             op.rangeOp.limit,
		Revision:          op.rangeOp.rev,
		Serializable:      op.rangeOp.serializable,
		KeysOnly:          op.rangeOp.keysOnly,
		CountOnly:         op.rangeOp.cntOnly,
		MinModRevision:    op.rangeOp.minModRev,
		MaxModRevision:    op.rangeOp.maxModRev,
		MinCreateRevision: op.rangeOp.minCreateRev,
		MaxCreateRevision: op.rangeOp.maxCreateRev,
	}
	if op.rangeOp.sort != nil {
		req.SortOrder = xlineapi.RangeRequest_SortOrder(op.rangeOp.sort.Order)
		req.SortTarget = xlineapi.RangeRequest_SortTarget(op.rangeOp.sort.Target)
	}
	return req
}

func (op Op) toPutReq() *xlineapi.PutRequest {
	if op.t != opPut {
		panic("op.t != opPut")
	}
	req := &xlineapi.PutRequest{
		Key:         op.putOp.key,
		Value:       op.putOp.val,
		Lease:       op.putOp.leaseID,
		PrevKv:      op.putOp.prevKV,
		IgnoreValue: op.putOp.ignVal,
		IgnoreLease: op.putOp.ignLease,
	}
	return req
}

func (op Op) toDeleteReq() *xlineapi.DeleteRangeRequest {
	if op.t != opDeleteRange {
		panic("op.t != opDeleteRange")
	}
	req := &xlineapi.DeleteRangeRequest{
		Key:      op.delOp.key,
		RangeEnd: op.delOp.end,
		PrevKv:   op.delOp.prevKV,
	}
	return req
}

func (op Op) toTxnRequest() *xlineapi.TxnRequest {
	thenOps := make([]*xlineapi.RequestOp, len(op.txnOp.thenOps))
	for i, tOp := range op.txnOp.thenOps {
		thenOps[i] = tOp.toRequestOp()
	}
	elseOps := make([]*xlineapi.RequestOp, len(op.txnOp.elseOps))
	for i, eOp := range op.txnOp.elseOps {
		elseOps[i] = eOp.toRequestOp()
	}
	cmps := make([]*xlineapi.Compare, len(op.txnOp.cmps))
	for i := range op.txnOp.cmps {
		cmps[i] = (*xlineapi.Compare)(&op.txnOp.cmps[i])
	}
	return &xlineapi.TxnRequest{Compare: cmps, Success: thenOps, Failure: elseOps}
}

func (op Op) toRequestOp() *xlineapi.RequestOp {
	switch op.t {
	case opRange:
		return &xlineapi.RequestOp{Request: &xlineapi.RequestOp_RequestRange{RequestRange: op.toRangeReq()}}
	case opPut:
		return &xlineapi.RequestOp{Request: &xlineapi.RequestOp_RequestPut{RequestPut: op.toPutReq()}}
	case opDeleteRange:
		return &xlineapi.RequestOp{Request: &xlineapi.RequestOp_RequestDeleteRange{RequestDeleteRange: op.toDeleteReq()}}
	case opTxn:
		return &xlineapi.RequestOp{Request: &xlineapi.RequestOp_RequestTxn{RequestTxn: op.toTxnRequest()}}
	default:
		panic("Unknown Op")
	}
}

func (op Op) toCompactReq() *xlineapi.CompactionRequest {
	if op.t != opCompact {
		panic("op.t != opCompact")
	}
	req := &xlineapi.CompactionRequest{
		Revision: op.compactOp.rev,
		Physical: op.compactOp.physical,
	}
	return req
}

// WithRange specifies the range of 'Get', 'Delete', 'Watch' requests.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
// endKey must be lexicographically greater than start key.
func WithRange(endKey []byte) OpOption {
	return func(op *Op) {
		switch op.t {
		case opRange:
			op.rangeOp.end = []byte(endKey)
		case opDeleteRange:
			op.delOp.end = []byte(endKey)
		}
	}
}

// WithFromKey specifies the range of 'Get', 'Delete', 'Watch' requests
// to be equal or greater than the key in the argument.
func WithFromKey() OpOption {
	return func(op *Op) {
		switch op.t {
		case opRange:
			if len(op.rangeOp.key) == 0 {
				op.rangeOp.key = []byte{0}
			}
			op.rangeOp.end = []byte("\x00")
			op.rangeOp.isOptsWithFromKey = true
		case opDeleteRange:
			if len(op.delOp.key) == 0 {
				op.delOp.key = []byte{0}
			}
			op.delOp.end = []byte("\x00")
			op.delOp.isOptsWithFromKey = true
		}
	}
}

// WithPrefix enables 'Get', 'Delete', or 'Watch' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() OpOption {
	return func(op *Op) {
		switch op.t {
		case opRange:
			op.rangeOp.isOptsWithPrefix = true
			if len(op.rangeOp.key) == 0 {
				op.rangeOp.key, op.rangeOp.end = []byte{0}, []byte{0}
				return
			}
			op.rangeOp.end = getPrefix(op.rangeOp.key)
		case opDeleteRange:
			op.delOp.isOptsWithPrefix = true
			if len(op.delOp.key) == 0 {
				op.delOp.key, op.delOp.end = []byte{0}, []byte{0}
				return
			}
			op.delOp.end = getPrefix(op.delOp.key)
		}
	}
}

// WithLimit limits the number of results to return from 'Range' request.
// If WithLimit is given a 0 limit, it is treated as no limit.
func WithLimit(n int64) OpOption { return func(op *Op) { op.rangeOp.limit = n } }

// WithRev specifies the store revision for 'Range' request.
// Or the start revision of 'Watch' request.
func WithRev(rev int64) OpOption {
	return func(op *Op) {
		switch op.t {
		case opRange:
			op.rangeOp.rev = rev
		case opCompact:
			op.compactOp.rev = rev
		}
	}
}

// WithSort specifies the ordering in 'Range' request. It requires
// 'WithRange' and/or 'WithPrefix' to be specified too.
// 'target' specifies the target to sort by: key, version, revisions, value.
// 'order' can be either 'SortNone', 'SortAscend', 'SortDescend'.
func WithSort(target SortTarget, order SortOrder) OpOption {
	return func(op *Op) {
		if target == SortByKey && order == SortAscend {
			// If order != SortNone, server fetches the entire key-space,
			// and then applies the sort and limit, if provided.
			// Since by default the server returns results sorted by keys
			// in lexicographically ascending order, the client should ignore
			// SortOrder if the target is SortByKey.
			order = SortNone
		}
		op.rangeOp.sort = &SortOption{target, order}
	}
}

// WithSerializable makes 'Range' request serializable. By default,
// it's linearizable. Serializable requests are better for lower latency
// requirement.
func WithSerializable() OpOption {
	return func(op *Op) { op.rangeOp.serializable = true }
}

// WithKeysOnly makes the 'Range' request return only the keys and the corresponding
// values will be omitted.
func WithKeysOnly() OpOption {
	return func(op *Op) { op.rangeOp.keysOnly = true }
}

// WithCountOnly makes the 'Range' request return only the count of keys.
func WithCountOnly() OpOption {
	return func(op *Op) { op.rangeOp.cntOnly = true }
}

// WithMinModRev filters out keys for Range with modification revisions less than the given revision.
func WithMinModRev(rev int64) OpOption { return func(op *Op) { op.rangeOp.minModRev = rev } }

// WithMaxModRev filters out keys for Range with modification revisions greater than the given revision.
func WithMaxModRev(rev int64) OpOption { return func(op *Op) { op.rangeOp.maxModRev = rev } }

// WithMinCreateRev filters out keys for Range with creation revisions less than the given revision.
func WithMinCreateRev(rev int64) OpOption { return func(op *Op) { op.rangeOp.minCreateRev = rev } }

// WithMaxCreateRev filters out keys for Range with creation revisions greater than the given revision.
func WithMaxCreateRev(rev int64) OpOption { return func(op *Op) { op.rangeOp.maxCreateRev = rev } }

// WithLease attaches a lease ID to a key in 'Put' request.
func WithLease(leaseID int64) OpOption {
	return func(op *Op) { op.putOp.leaseID = leaseID }
}

// WithPrevKV gets the previous key-value pair before the event happens. If the previous KV is already compacted,
// nothing will be returned.
func WithPrevKV() OpOption {
	return func(op *Op) {
		switch op.t {
		case opPut:
			op.putOp.prevKV = true
		case opDeleteRange:
			op.delOp.prevKV = true
		default:
			panic("unexpected previous kv in operation")
		}
	}
}

// WithIgnoreValue updates the key using its current value.
// This option can not be combined with non-empty values.
// Returns an error if the key does not exist.
func WithIgnoreValue() OpOption {
	return func(op *Op) {
		op.putOp.ignVal = true
	}
}

// WithIgnoreLease updates the key using its current lease.
// This option can not be combined with WithLease.
// Returns an error if the key does not exist.
func WithIgnoreLease() OpOption {
	return func(op *Op) {
		op.putOp.ignLease = true
	}
}

// WithPhysical makes Compact wait until all compacted entries are
// removed from the etcd server's storage.
func WithPhysical() OpOption {
	return func(op *Op) { op.compactOp.physical = true }
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return noPrefixEnd
}

var noPrefixEnd = []byte{0}

// LeaseOp represents an Operation that lease can execute.
type LeaseOp struct {
	id int64

	// for Grant
	ttl int64

	// for TimeToLive
	attachedKeys bool
}

// LeaseOption configures lease operations.
type LeaseOption func(*LeaseOp)

func (op *LeaseOp) applyOpts(opts []LeaseOption) {
	for _, opt := range opts {
		opt(op)
	}
}

func toTTLReq(id int64, opts ...LeaseOption) *xlineapi.LeaseTimeToLiveRequest {
	req := &LeaseOp{id: id}
	req.applyOpts(opts)
	return &xlineapi.LeaseTimeToLiveRequest{ID: int64(id), Keys: req.attachedKeys}
}

func toGrantReq(ttl int64, opts ...LeaseOption) *xlineapi.LeaseGrantRequest {
	req := &LeaseOp{ttl: ttl}
	req.applyOpts(opts)
	return &xlineapi.LeaseGrantRequest{TTL: ttl, ID: req.id}
}

// WithID is used to specify the lease ID, otherwise it is automatically generated.
func WithID(id int64) LeaseOption {
	return func(op *LeaseOp) { op.id = id }
}

// WithAttachedKeys makes TimeToLive list the keys attached to the given lease ID.
func WithAttachedKeys() LeaseOption {
	return func(op *LeaseOp) { op.attachedKeys = true }
}
