package client

import xlineapi "github.com/xline-kv/go-xline/api/xline"

// compactOp represents a compact operation.
type compactOp struct {
	revision int64
	physical bool
}

// compactOption configures compact operation.
type compactOption func(*compactOp)

func (op *compactOp) applyCompactOpts(opts []compactOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// opCompact wraps slice CompactOption to create a CompactOp.
func opCompact(rev int64, opts ...compactOption) compactOp {
	ret := compactOp{revision: rev}
	ret.applyCompactOpts(opts)
	return ret
}

func (op compactOp) toRequest() *xlineapi.CompactionRequest {
	return &xlineapi.CompactionRequest{Revision: op.revision, Physical: op.physical}
}

// WithCompactPhysical makes Compact wait until all compacted entries are
// removed from the etcd server's storage.
func WithCompactPhysical() compactOption {
	return func(op *compactOp) { op.physical = true }
}

type compactResponse xlineapi.CompactionResponse
