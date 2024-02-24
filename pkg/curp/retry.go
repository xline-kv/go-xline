package curp

import (
	"fmt"
	"time"

	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	xlinepb "github.com/xline-kv/go-xline/api/gen/xline"
	"github.com/xline-kv/go-xline/pkg/rpc"
	"go.uber.org/zap"
)

type backoffConfig = uint

// Backoff config
const (
	backoffFixed = iota
	backoffExponential
)

// Retry config to control the retry policy
type retryConfig struct {
	// Backoff config
	backoff backoffConfig
	// Initial delay
	delay time.Duration
	// Control the max delay of exponential
	maxDelay time.Duration
	// Retry count
	count uint
}

// Create a fixed retry config
func newFixedRetryConfig(delay time.Duration, count uint) *retryConfig {
	if count <= 0 {
		panic("retry count should be larger than 0")
	}
	return &retryConfig{
		backoff: backoffFixed,
		delay:   delay,
		count:   count,
	}
}

// Create a exponential retry config
func newExponentialRetryConfig(delay time.Duration, maxDelay time.Duration, count uint) *retryConfig {
	if count <= 0 {
		panic("retry count should be larger than 0")
	}
	return &retryConfig{
		backoff: backoffExponential,
		delay: delay,
		maxDelay: maxDelay,
		count: count,
	}
}

// Create a backoff process
func (c *retryConfig) initBackoff() *backoff {
	return &backoff{
		config:   c,
		curDelay: c.delay,
		count:    c.count,
	}
}

// Backoff tool
type backoff struct {
	// The retry config
	config *retryConfig
	// Current delay
	curDelay time.Duration
	// Total RPC count
	count uint
}

// Get the next delay duration, None means the end.
func (b *backoff) nextDelay() *time.Duration {
	if b.count == 0 {
		return nil
	}
	b.count--
	cur := b.curDelay
	if b.config.backoff == backoffExponential {
		b.curDelay *= 2
		b.curDelay = min(b.curDelay, b.config.maxDelay)
	}
	return &cur
}

// The retry client automatically retry the requests of the inner client api
type retry struct {
	// Inner client
	inner *unary
	// Retry config
	config *retryConfig
	// Logger
	logger *zap.SugaredLogger
}

// Create a retry client
func NewRetry(inner *unary, config *retryConfig) *retry {
	logger, err := zap.NewProduction()
	sugarLogger := logger.Sugar()
	if err != nil {
		panic(err)
	}
	return &retry{
		inner:  inner,
		config: config,
		logger: sugarLogger,
	}
}

func (r *retry) retryGenProposeId(f func() (*curppb.ProposeId, *rpc.CurpError)) (*curppb.ProposeId, *rpc.CurpError) {
	backoff := r.config.initBackoff()
	var lastErr *rpc.CurpError = nil

	for {
		delay := backoff.nextDelay()
		if delay == nil {
			break
		}

		res, err := f()
		if res != nil {
			return res, nil
		}

		switch err.Err.(type) {
		// some errors that should not retry
		case *curppb.CurpError_Duplicated,
			*curppb.CurpError_ShuttingDown,
			*curppb.CurpError_InvalidConfig,
			*curppb.CurpError_NodeNotExists,
			*curppb.CurpError_NodeAlreadyExists,
			*curppb.CurpError_LearnerNotCatchUp:
			return nil, err
		// some errors that could have a retry
		case *curppb.CurpError_ExpiredClientId,
			*curppb.CurpError_KeyConflict,
			*curppb.CurpError_Internal:
		// update leader state if we got a rpc transport error
		case *curppb.CurpError_RpcTransport:
			if _, err := r.inner.fetchLeaderId(); err != nil {
				r.logger.Warnf("fetch leader failed, error %v", err)
			}
		// update the cluster state if got WrongClusterVersion
		case *curppb.CurpError_WrongClusterVersion:
			// the inner client should automatically update cluster state when fetch_cluster
			if _, err := r.inner.fetchCluster(); err != nil {
				r.logger.Warnf("fetch leader failed, error %v", err)
			}
		// update the leader state if got Redirect
		case *curppb.CurpError_Redirect_:
			leaderId := err.GetRedirect().GetLeaderId()
			term := err.GetRedirect().GetTerm()
			r.inner.updateLeader(&leaderId, term)
		default:
			panic("unknown error type")
		}

		r.logger.Warnf("got error: %v, retry on %v seconds later", err, delay)
		lastErr = err
		time.Sleep(*delay)
	}

	return nil, &rpc.CurpError{
		CurpError: &curppb.CurpError{
			Err: &curppb.CurpError_Internal{
				Internal: fmt.Sprintf("request timeout, last error: %v", lastErr),
			},
		},
	}
}

func (r *retry) retryPropose(
	proposeId *curppb.ProposeId,
	cmd *xlinepb.Command,
	useFastPath bool,
	f func(proposeId *curppb.ProposeId, cmd *xlinepb.Command, useFastPath bool) (*rpc.ProposeResult_, *rpc.CurpError),
) (*rpc.ProposeResult_, *rpc.CurpError) {
	backoff := r.config.initBackoff()
	var lastErr *rpc.CurpError = nil

	for {
		delay := backoff.nextDelay()
		if delay == nil {
			break
		}

		res, err := f(proposeId, cmd, useFastPath)
		if res != nil {
			return res, nil
		}

		switch err.Err.(type) {
		// some errors that should not retry
		case *curppb.CurpError_Duplicated,
			*curppb.CurpError_ShuttingDown,
			*curppb.CurpError_InvalidConfig,
			*curppb.CurpError_NodeNotExists,
			*curppb.CurpError_NodeAlreadyExists,
			*curppb.CurpError_LearnerNotCatchUp:
			return nil, err
		// some errors that could have a retry
		case *curppb.CurpError_ExpiredClientId,
			*curppb.CurpError_KeyConflict,
			*curppb.CurpError_Internal:
		// update leader state if we got a rpc transport error
		case *curppb.CurpError_RpcTransport:
			if _, err := r.inner.fetchLeaderId(); err != nil {
				r.logger.Warnf("fetch leader failed, error %v", err)
			}
		// update the cluster state if got WrongClusterVersion
		case *curppb.CurpError_WrongClusterVersion:
			// the inner client should automatically update cluster state when fetch_cluster
			if _, err := r.inner.fetchCluster(); err != nil {
				r.logger.Warnf("fetch leader failed, error %v", err)
			}
		// update the leader state if got Redirect
		case *curppb.CurpError_Redirect_:
			leaderId := err.GetRedirect().GetLeaderId()
			term := err.GetRedirect().GetTerm()
			r.inner.updateLeader(&leaderId, term)
		default:
			panic("unknown error type")
		}

		r.logger.Warnf("got error: %v, retry on %v seconds later", err, delay)
		lastErr = err
		time.Sleep(*delay)
	}

	return nil, &rpc.CurpError{
		CurpError: &curppb.CurpError{
			Err: &curppb.CurpError_Internal{
				Internal: fmt.Sprintf("request timeout, last error: %v", lastErr),
			},
		},
	}
}

func (r *retry) propose(cmd *xlinepb.Command, useFastPath bool) (*rpc.ProposeResult_, *rpc.CurpError) {
	pid, err := r.retryGenProposeId(r.inner.genProposeID)
	if err != nil {
		return nil, err
	}
	res, err := r.retryPropose(pid, cmd, useFastPath, r.inner.repeatablePropose)
	if err != nil {
		return nil, err
	}
	return res, nil
}
