package client

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Xline client
type client struct {
	// Kv client
	Kv kvClient
	// Auth client
	Auth authClient
	// Lease client
	Lease leaseClient
}

func Connect(allMembers []string, options ...ClientOptions) (*client, error) {
	name := "client"
	token := ""
	clientTimeout := newDefaultClientTimeout()
	idGen := newLeaseId()

	if len(options) != 0 {
		// get timeout
		if options[0].CurpTimeout != nil {
			clientTimeout = newClientTimeout(*options[0].CurpTimeout)
		}
		// get token
		if options[0].User != nil {
			conn, err := grpc.Dial(allMembers[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithIdleTimeout(clientTimeout.idleTimeout))
			if err != nil {
				return nil, fmt.Errorf("request token fail. %v", err)
			}
			authClient := xlineapi.NewAuthClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout.proposeTimeout)
			defer cancel()
			res, err := authClient.Authenticate(ctx, &xlineapi.AuthenticateRequest{Name: "name", Password: "password"})
			if err != nil {
				return nil, fmt.Errorf("request token fail. %v", err)
			} else {
				token = res.Token
			}
		}
	}

	curpClient, err := BuildCurpClientFromAddrs(allMembers, clientTimeout)
	if err != nil {
		return nil, err
	}
	leaseClient, err := buildLeaseClientFromAddrs(allMembers, clientTimeout)
	if err != nil {
		return nil, err
	}

	kv := newKvClient(name, *curpClient, token)
	auth := newAuthClient(name, *curpClient, token)
	lease := newLeaseClient(name, *curpClient, leaseClient, token, idGen)

	return &client{
		Kv:    kv,
		Auth:  auth,
		Lease: lease,
	}, nil
}

// Options for a client connection
type ClientOptions struct {
	// User is a pair values of name and password
	User *UserCredentials
	// Timeout settings for the curp client
	CurpTimeout *ClientTimeout
}

// Options for a user
type UserCredentials struct {
	// Username
	Name string
	// Password
	Password string
}

// Curp client settings
type ClientTimeout struct {
	// Curp client wait idle
	idleTimeout time.Duration
	// Curp client wait sync timeout
	waitSyncedTimeout time.Duration
	// Curp client propose request timeout
	proposeTimeout time.Duration
	// Curp client retry interval
	retryTimeout time.Duration
}

func newDefaultClientTimeout() ClientTimeout {
	return ClientTimeout{
		idleTimeout:       1 * time.Second,
		waitSyncedTimeout: 2 * time.Second,
		proposeTimeout:    2 * time.Second,
		retryTimeout:      50 * time.Millisecond,
	}
}

func newClientTimeout(options ClientTimeout) ClientTimeout {
	ct := newDefaultClientTimeout()

	if options.idleTimeout != 0 {
		ct.idleTimeout = options.idleTimeout
	}
	if options.waitSyncedTimeout != 0 {
		ct.waitSyncedTimeout = options.waitSyncedTimeout
	}
	if options.proposeTimeout != 0 {
		ct.proposeTimeout = options.proposeTimeout
	}
	if options.retryTimeout != 0 {
		ct.retryTimeout = options.retryTimeout
	}

	return ct
}

// Generator of unique lease id
// Note that this Lease Id generation method may cause collisions,
// the client should retry after informed by the server.
type leaseIdGenerator struct {
	// the current lease id
	id uint64
}

func newLeaseId() leaseIdGenerator {
	return leaseIdGenerator{id: rand.Uint64()}
}

// Generate a new `leaseId`
func (g *leaseIdGenerator) next() int64 {
	if g.id == 0 {
		return g.next()
	}
	return int64(g.id & 0x7FFF_FFFF_FFFF_FFFF)
}
