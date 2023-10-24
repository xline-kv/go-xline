package client

import xlineapi "github.com/xline-kv/go-xline/api/xline"

// delOp represents a delete operation.
type delOp struct {
	key    []byte
	end    []byte
	prevKV bool
}

// delOption configures put operation.
type delOption func(*delOp)

func (op *delOp) applyDelOpts(opts []delOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// opDel wraps slice delOption to create a delOp.
func opDel(key string, opts ...delOption) delOp {
	ret := delOp{key: []byte(key)}
	ret.applyDelOpts(opts)
	return ret
}

func (op delOp) toReq() *xlineapi.DeleteRangeRequest {
	ret := &xlineapi.DeleteRangeRequest{
		Key:      op.key,
		RangeEnd: op.end,
		PrevKv:   op.prevKV,
	}
	return ret
}

// endKey must be lexicographically greater than start key.
func WithDeleteRange(endKey string) delOption {
	return func(op *delOp) { op.end = []byte(endKey) }
}

// WithDeletePrevKV gets the previous key-value pair before the event happens. If the previous KV is already compacted,
// nothing will be returned.
func WithDeletePrevKV() delOption {
	return func(op *delOp) {
		op.prevKV = true
	}
}

type deleteResponse xlineapi.DeleteRangeResponse
