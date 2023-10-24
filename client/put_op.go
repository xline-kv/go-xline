package client

import xlineapi "github.com/xline-kv/go-xline/api/xline"

// putOp represents a put operation.
type putOp struct {
	key      []byte
	val      []byte
	leaseID  int64
	prevKV   bool
	ignVal   bool
	ignLease bool
}

// putOption configures put operation.
type putOption func(*putOp)

func (op *putOp) applyPutOpts(opts []putOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// opCompact wraps slice putOption to create a putOp.
func opPut(key, val string, opts ...putOption) putOp {
	ret := putOp{key: []byte(key), val: []byte(val)}
	ret.applyPutOpts(opts)
	return ret
}

func (op putOp) toReq() *xlineapi.PutRequest {
	return &xlineapi.PutRequest{
		Key:         op.key,
		Value:       op.val,
		Lease:       op.leaseID,
		PrevKv:      op.prevKV,
		IgnoreValue: op.ignVal,
		IgnoreLease: op.ignLease,
	}
}

// WithLease attaches a lease ID to a key in 'Put' request.
func WithPutLease(leaseID int64) putOption {
	return func(op *putOp) { op.leaseID = leaseID }
}

// WithPrevKV gets the previous key-value pair before the event happens. If the previous KV is already compacted,
// nothing will be returned.
func WithPutPrevKV() putOption {
	return func(op *putOp) {
		op.prevKV = true
	}
}

// WithIgnoreValue updates the key using its current value.
// This option can not be combined with non-empty values.
// Returns an error if the key does not exist.
func WithPutIgnoreValue() putOption {
	return func(op *putOp) {
		op.ignVal = true
	}
}

// WithIgnoreLease updates the key using its current lease.
// This option can not be combined with WithLease.
// Returns an error if the key does not exist.
func WithPutIgnoreLease() putOption {
	return func(op *putOp) {
		op.ignLease = true
	}
}

type putResponse xlineapi.PutResponse
