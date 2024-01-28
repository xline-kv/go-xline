package rpc

import (
	"context"
	"sync"
	"time"

	curppb "github.com/xline-kv/go-xline/api/gen/curp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server Id
type serverId = uint64

// The connection struct to hold the real rpc connections,
// it may failed to Connect, but it also retries the next time
type Connect struct {
	// Server serverId
	serverId serverId
	// The rpc connection
	RpcConnect []*grpc.ClientConn
	// The current rpc connection address, when the address is updated,
	// `addrs` will be used to remove previous connection
	addrs []string
	// Mutex
	mu sync.Mutex
}

// A wrapper of `connect_to`
func NewConnect(id serverId, addrs []string) (*Connect, error) {
	conn, err := connectTo(id, addrs)
	return conn, err
}

// Wrapper of `connect_all`
func NewConnects(member map[serverId][]string) (map[serverId]*Connect, error) {
	conns, err := connectAll(member)
	return conns, err
}

// Connect to a server
func connectTo(id serverId, addrs []string) (*Connect, error) {
	// TODO: load balancing
	// TODO: support TLS
	conns := []*grpc.ClientConn{}
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}
	return &Connect{
		serverId:   id,
		RpcConnect: conns,
		addrs:      addrs,
	}, nil
}

// Connect to a map of members
func connectAll(member map[serverId][]string) (map[serverId]*Connect, error) {
	conns := map[serverId]*Connect{}
	for id, addrs := range member {
		conn, err := connectTo(id, addrs)
		if err != nil {
			return nil, err
		}
		conns[conn.serverId] = conn
	}
	return conns, nil
}

// Update server addresses, the new addresses will override the old ones
func (c *Connect) UpdateAddrs(addrs []string) {
	c.innerUpdateAddrs(addrs)
}

func (c *Connect) innerUpdateAddrs(addrs []string) {
	c.mu.Lock()

	oldAddrs := map[string]struct{}{}
	newAddrs := map[string]struct{}{}
	for _, addr := range c.addrs {
		oldAddrs[addr] = struct{}{}
	}
	for _, addr := range addrs {
		newAddrs[addr] = struct{}{}
	}

	diffs := map[string]struct{}{}
	for old := range oldAddrs {
		if _, ok := newAddrs[old]; !ok {
			diffs[old] = struct{}{}
		}
	}
	for new := range newAddrs {
		if _, ok := oldAddrs[new]; !ok {
			diffs[new] = struct{}{}
		}
	}

	for diff := range diffs {
		if _, ok := newAddrs[diff]; ok {
			c.addrs = append(c.addrs, diff)
		} else {
			for i, addr := range c.addrs {
				if diff == addr {
					c.addrs = append(c.addrs[:i], c.addrs[i+1:]...)
				}
			}
		}
	}
	c.mu.Unlock()
}

// Send `ProposeRequest`
func (c *Connect) Propose(
	ctx context.Context,
	req *curppb.ProposeRequest,
	timeout time.Duration,
) (*curppb.ProposeResponse, error) {
	resCh := make(chan *curppb.ProposeResponse)
	errCh := make(chan error)

	for _, conn := range c.RpcConnect {
		go func(conn *grpc.ClientConn) {
			cli := curppb.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			res, err := cli.Propose(ctx, req)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(conn)
	}

	// find the first one return `ProposeResponse`
	select {
	case res := <-resCh:
		return res, nil
	case err := <-errCh:
		return nil, err
	}
}

// Send `WaitSyncedRequest`
func (c *Connect) WaitSynced(
	ctx context.Context,
	req *curppb.WaitSyncedRequest,
	timeout time.Duration,
) (*curppb.WaitSyncedResponse, error) {
	resCh := make(chan *curppb.WaitSyncedResponse)
	errCh := make(chan error)

	for _, conn := range c.RpcConnect {
		go func(conn *grpc.ClientConn) {
			cli := curppb.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			res, err := cli.WaitSynced(ctx, req)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(conn)
	}

	// find the first one return `ProposeConfChangeRequest`
	select {
	case res := <-resCh:
		return res, nil
	case err := <-errCh:
		return nil, err
	}
}

// Send `FetchClusterRequest`
func (c *Connect) FetchCluster(
	ctx context.Context,
	req *curppb.FetchClusterRequest,
	timeout time.Duration,
) (*curppb.FetchClusterResponse, error) {
	resCh := make(chan *curppb.FetchClusterResponse)
	errCh := make(chan error)

	for _, conn := range c.RpcConnect {
		go func(conn *grpc.ClientConn) {
			cli := curppb.NewProtocolClient(conn)
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			res, err := cli.FetchCluster(ctx, req)
			if err != nil {
				errCh <- err
				return
			}
			resCh <- res
		}(conn)
	}

	// find the first one return `ProposeConfChangeRequest`
	select {
	case res := <-resCh:
		return res, nil
	case err := <-errCh:
		return nil, err
	}
}
