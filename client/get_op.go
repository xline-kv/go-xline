package client

import xlineapi "github.com/xline-kv/go-xline/api/xline"

// rangeOp represents a range operation.
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

// rangeOption configures range operation.
type rangeOption func(*rangeOp)

func (op *rangeOp) applyRangeOpts(opts []rangeOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// opRange wraps slice rangeOption to create a rangeOp.
func opRange(key string, opts ...rangeOption) rangeOp {
	// WithGetPrefix and WithGetFromKey are not supported together
	if isRangeOpWithPrefix(opts) && isGetOpWithFromKey(opts) {
		panic("`WithGetPrefix` and `WithGetFromKey` cannot be set at the same time, choose one")
	}
	ret := rangeOp{key: []byte(key)}
	ret.applyRangeOpts(opts)
	return ret
}

func (op rangeOp) toReq() *xlineapi.RangeRequest {
	ret := &xlineapi.RangeRequest{
		Key:               op.key,
		RangeEnd:          op.end,
		Limit:             op.limit,
		Revision:          op.rev,
		Serializable:      op.serializable,
		KeysOnly:          op.keysOnly,
		CountOnly:         op.cntOnly,
		MinModRevision:    op.minModRev,
		MaxModRevision:    op.maxModRev,
		MinCreateRevision: op.minCreateRev,
		MaxCreateRevision: op.maxCreateRev,
	}
	if op.sort != nil {
		ret.SortOrder = xlineapi.RangeRequest_SortOrder(op.sort.Order)
		ret.SortTarget = xlineapi.RangeRequest_SortTarget(op.sort.Target)
	}
	return ret
}

// WithLimit limits the number of results to return from 'Get' request.
// If WithLimit is given a 0 limit, it is treated as no limit.
func WithGetLimit(n int64) rangeOption { return func(op *rangeOp) { op.limit = n } }

// WithRev specifies the store revision for 'Get' request.
func WithGetRev(rev int64) rangeOption { return func(op *rangeOp) { op.rev = rev } }

// WithSerializable makes 'Get' request serializable. By default,
// it's linearizable. Serializable requests are better for lower latency
// requirement.
func WithGetSerializable() rangeOption {
	return func(op *rangeOp) { op.serializable = true }
}

// WithKeysOnly makes the 'Get' request return only the keys and the corresponding
// values will be omitted.
func WithGetKeysOnly() rangeOption {
	return func(op *rangeOp) { op.keysOnly = true }
}

// WithCountOnly makes the 'Get' request return only the count of keys.
func WithGetCountOnly() rangeOption {
	return func(op *rangeOp) { op.cntOnly = true }
}

// WithGetMinModRev filters out keys for Get with modification revisions less than the given revision.
func WithGetMinModRev(rev int64) rangeOption { return func(op *rangeOp) { op.minModRev = rev } }

// WithGetMaxModRev filters out keys for Get with modification revisions greater than the given revision.
func WithGetMaxModRev(rev int64) rangeOption { return func(op *rangeOp) { op.maxModRev = rev } }

// WithGetMinCreateRev filters out keys for Get with creation revisions less than the given revision.
func WithGetMinCreateRev(rev int64) rangeOption { return func(op *rangeOp) { op.minCreateRev = rev } }

// WithGetMaxCreateRev filters out keys for Get with creation revisions greater than the given revision.
func WithGetMaxCreateRev(rev int64) rangeOption { return func(op *rangeOp) { op.maxCreateRev = rev } }

// to be equal or greater than the key in the argument.
func WithGetFromKey() rangeOption {
	return func(op *rangeOp) {
		if len(op.key) == 0 {
			op.key = []byte{0}
		}
		op.end = []byte("\x00")
		op.isOptsWithFromKey = true
	}
}

// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithGetPrefix() rangeOption {
	return func(op *rangeOp) {
		op.isOptsWithPrefix = true
		if len(op.key) == 0 {
			op.key, op.end = []byte{0}, []byte{0}
			return
		}
		op.end = getPrefix(op.key)
	}
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

// isRangeOpWithPrefix returns true if WithPrefix option is called in the given opts.
func isRangeOpWithPrefix(opts []rangeOption) bool {
	ret := NewRangeOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithPrefix
}

// isGetOpWithFromKey returns true if WithFromKey option is called in the given opts.
func isGetOpWithFromKey(opts []rangeOption) bool {
	ret := NewRangeOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithFromKey
}

func NewRangeOp() *rangeOp {
	return &rangeOp{key: []byte("")}
}

type getResponse xlineapi.RangeResponse
