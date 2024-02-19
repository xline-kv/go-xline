package curp

import (
	"context"
	"time"

	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	xlinepb "github.com/xline-kv/go-xline/api/gen/xline"
	"github.com/xline-kv/go-xline/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CurpApi interface {
	// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
	// requests (event the requests are commutative).
	propose(cmd *xlinepb.Command, useFastPath bool) (*rpc.ProposeResult_, *rpc.CurpError)
}

// Leader state
type state_ struct {
	// server id
	id serverId
	// term
	term uint64
}

// Client builder to build a client
type clientBuilder struct {
	// initial cluster version
	clusterVersion *uint64
	// initial cluster members
	allMembers map[serverId][]string
	// initial leader state
	leaderState *state_
	// client configuration
	config *ClientConfig
}

// Create a client builder
func NewClientBuilder(config *ClientConfig) *clientBuilder {
	return &clientBuilder{
		clusterVersion: nil,
		allMembers:     map[serverId][]string{},
		leaderState:    nil,
		config:         config,
	}
}

// Set the initial cluster version
func (b *clientBuilder) SetClusterVersion(clusterVersion *uint64) *clientBuilder {
	b.clusterVersion = clusterVersion
	return b
}

// Set the initial all members
func (b *clientBuilder) SetAllMembers(allMembers map[serverId][]string) *clientBuilder {
	b.allMembers = allMembers
	return b
}

// Set the initial leader state
func (b *clientBuilder) SetLeaderState(leaderId serverId, term uint64) *clientBuilder {
	b.leaderState = &state_{id: leaderId, term: term}
	return b
}

// Discover the initial states from some endpoints
func (b *clientBuilder) DiscoveryFrom(addrs []string) *clientBuilder {
	proposeTimeout := b.config.proposeTimeout

	resCh := make(chan *curppb.FetchClusterResponse)
	errCh := make(chan error)

	for _, addr := range addrs {
		go func(addr string) {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				panic(err)
			}
			cli := curppb.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
			defer cancel()
			res, err := cli.FetchCluster(ctx, &curppb.FetchClusterRequest{})
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(addr)
	}

	select {
	case res := <-resCh:
		b.clusterVersion = &res.ClusterVersion
		if res.LeaderId != nil {
			id := *res.LeaderId
			b.leaderState = &state_{id: id, term: res.Term}
		}
		for _, member := range res.Members {
			b.allMembers[member.Id] = member.Addrs
		}
		return b
	case err := <-errCh:
		panic(err)
	}
}

// Init an unary builder
func (b *clientBuilder) initUnaryBuilder() *unaryBuilder {
	builder := newUnaryBuilder(
		b.allMembers,
		&unaryConfig{
			proposeTimeout:    b.config.proposeTimeout,
			waitSyncedTimeout: b.config.waitSyncedTimeout,
		},
	)
	if b.clusterVersion != nil {
		version := *b.clusterVersion
		builder.setClusterVersion(version)
	}
	if b.leaderState != nil {
		id := b.leaderState.id
		term := b.leaderState.term
		builder.setLeaderState(id, term)
	}
	return builder
}

// Init retry config
func (b *clientBuilder) initRetryConfig() *retryConfig {
	if b.config.useBackoff {
		return newExponentialRetryConfig(
			b.config.initialRetryTimeout,
			b.config.maxRetryTimeout,
			b.config.retryCount,
		)
	} else {
		return newFixedRetryConfig(
			b.config.initialRetryTimeout,
			b.config.retryCount,
		)
	}
}

// Build the client
func (b *clientBuilder) Build() (*retry, *rpc.CurpError) {
	unary, err := b.initUnaryBuilder().build()
	if err != nil {
		return nil, err
	}
	client := NewRetry(unary, b.initRetryConfig())
	return client, nil
}

// Curp client settings
type ClientConfig struct {
	// Curp client wait sync timeout
	waitSyncedTimeout time.Duration
	// Curp client propose request timeout
	proposeTimeout time.Duration
	// Curp client initial retry interval
	initialRetryTimeout time.Duration
	// Curp client max retry interval
	maxRetryTimeout time.Duration
	// Curp client retry interval
	retryCount uint
	// Whether to use exponential backoff in retries
	useBackoff bool
}

// Create a new client timeout
func NewClientConfig(
	waitSyncedTimeout time.Duration,
	proposeTimeout time.Duration,
	initialRetryTimeout time.Duration,
	maxRetryTimeout time.Duration,
	retryCount uint,
	useBackoff bool,
) *ClientConfig {
	if initialRetryTimeout <= maxRetryTimeout {
		panic("`initial_retry_timeout` should less or equal to `max_retry_timeout`")
	}
	return &ClientConfig{
		waitSyncedTimeout:   waitSyncedTimeout,
		proposeTimeout:      proposeTimeout,
		initialRetryTimeout: initialRetryTimeout,
		maxRetryTimeout:     maxRetryTimeout,
		retryCount:          retryCount,
		useBackoff:          useBackoff,
	}
}

func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		waitSyncedTimeout:   defaultClientWaitSyncedTimeout(),
		proposeTimeout:      defaultProposeTimeout(),
		initialRetryTimeout: defaultInitialRetryTimeout(),
		maxRetryTimeout:     defaultMaxRetryTimeout(),
		retryCount:          defaultRetryCount(),
		useBackoff:          defaultUseBackoff(),
	}
}

// default client wait synced timeout
func defaultClientWaitSyncedTimeout() time.Duration {
	return 2 * time.Second
}

// default client propose timeout
func defaultProposeTimeout() time.Duration {
	return 1 * time.Second
}

// default initial retry timeout
func defaultInitialRetryTimeout() time.Duration {
	return 50 * time.Millisecond
}

// default max retry timeout
func defaultMaxRetryTimeout() time.Duration {
	return 10000 * time.Millisecond
}

// default retry count
func defaultRetryCount() uint {
	return 3
}

// default use backoff
func defaultUseBackoff() bool {
	return true
}
