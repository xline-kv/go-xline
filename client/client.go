package client

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/xline-kv/go-xline/api/gen/xline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Xline client
type client struct {
	// Kv client
	KV
	// Auth client
	Auth
	// Lease client
	Lease
	// Watch client
	Watch
	// Lock client
	Lock
	// Maintenance client
	Maintenance
	// Cluster client
	Cluster
}

func Connect(allMembers []string, options ...ClientOptions) (*client, error) {
	var config ClientConfig
	var token string

	if len(options) > 1 {
		return nil, fmt.Errorf("to many options")
	}
	if len(options) == 1 {
		user := options[0].User
		config = options[0].CurpTimeout
		if user.Name != "" && user.Password != "" {
			conn, err := grpc.Dial(allMembers[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, fmt.Errorf("request token fail. %v", err)
			}
			authClient := xlineapi.NewAuthClient(conn)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			res, err := authClient.Authenticate(ctx, &xlineapi.AuthenticateRequest{Name: "name", Password: "password"})
			if err != nil {
				return nil, fmt.Errorf("request token fail. %v", err)
			} else {
				token = res.Token
			}
		}
	}
	newClientConfig(&config)
	idGen := newLeaseId()

	conn, err := grpc.Dial(allMembers[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	curpClient, err := BuildCurpClientFromAddrs(allMembers, &config)
	if err != nil {
		return nil, err
	}

	kv := NewKV(curpClient, token)
	auth := NewAuth(curpClient, token)
	lease := NewLease(curpClient, conn, token, idGen)
	watch := NewWatch(conn)
	lock := NewLock(curpClient, lease, watch, token)
	maintenance := NewMaintenance(conn)
	cluster := NewCluster(conn)

	return &client{
		KV:          kv,
		Auth:        auth,
		Lease:       lease,
		Watch:       watch,
		Lock:        lock,
		Maintenance: maintenance,
		Cluster:     cluster,
	}, nil
}

// Options for a client connection
type ClientOptions struct {
	// User is a pair values of name and password
	User UserCredentials
	// Timeout settings for the curp client
	CurpTimeout ClientConfig
}

// Options for a user
type UserCredentials struct {
	// Username
	Name string
	// Password
	Password string
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
	g.id = g.id + 1
	if g.id == 0 {
		return g.next()
	}
	return int64(g.id & 0x7FFF_FFFF_FFFF_FFFF)
}
