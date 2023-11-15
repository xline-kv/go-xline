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

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	LeaseGrantResponse      pb.LeaseGrantResponse
	LeaseRevokeResponse     pb.LeaseRevokeResponse
	LeaseKeepAliveResponse  pb.LeaseKeepAliveResponse
	LeaseTimeToLiveResponse pb.LeaseTimeToLiveResponse
	LeaseLeasesResponse     pb.LeaseLeasesResponse
)

type Lease interface {
	// Grant creates a new lease.
	// When passed WithID(), Grant will used to specify the lease ID, otherwise it is automatically generated.
	Grant(ttl int64, opts ...LeaseOption) (*LeaseGrantResponse, error)

	// Revoke revokes the given lease.
	Revoke(id int64) (*LeaseRevokeResponse, error)

	// KeepAlive attempts to keep the given lease alive forever.
	KeepAlive(ctx context.Context, id int64) (<-chan *LeaseKeepAliveResponse, error)

	// KeepAliveOnce renews the lease once. The response corresponds to the first message from calling KeepAlive.
	// In most of the cases, Keepalive should be used instead of KeepAliveOnce.
	KeepAliveOnce(ctx context.Context, id int64) (*LeaseKeepAliveResponse, error)

	// TimeToLive retrieves the lease information of the given lease ID.
	// When passed WithAttachedKeys(), TimeToLive will list the keys attached to the given lease ID.
	TimeToLive(id int64, opts ...LeaseOption) (*LeaseTimeToLiveResponse, error)

	// Leases retrieves all leases.
	Leases() (*LeaseLeasesResponse, error)
}

// Client for Lease operations
type leaseClient struct {
	// Name of the LeaseClient, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The lease RPC client, only communicate with one server at a time
	leaseClient pb.LeaseClient
	// Auth token
	token string
	// Lease Id generator
	idGen leaseIdGenerator
	// The logger for log
	logger *zap.Logger
}

const keepAliveInterval = 1 * time.Millisecond

// Creates a new `LeaseClient`
func newLeaseClient(
	name string,
	curpClient curpClient,
	conn *grpc.ClientConn,
	token string,
	idGen leaseIdGenerator,
) leaseClient {
	return leaseClient{
		name:        name,
		curpClient:  curpClient,
		leaseClient: pb.NewLeaseClient(conn),
		token:       token,
		idGen:       idGen,
		logger:      xlog.GetLogger(),
	}
}

// Creates a lease which expires if the server does not receive a keepAlive
// within a given time to live period. All keys attached to the lease will be expired and
// deleted if the lease expires. Each expired key generates a delete event in the event history.
func (c *leaseClient) Grant(ttl int64, opts ...LeaseOption) (*LeaseGrantResponse, error) {
	request := toGrantReq(ttl, opts...)
	if request.ID == 0 {
		request.ID = c.idGen.next()
	}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_LeaseGrantRequest{
			LeaseGrantRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := pb.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*LeaseGrantResponse)(res.CommandResp.GetLeaseGrantResponse()), err
}

// Revokes a lease. All keys attached to the lease will expire and be deleted.
func (c *leaseClient) Revoke(id int64) (*LeaseRevokeResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_LeaseRevokeRequest{
			LeaseRevokeRequest: &pb.LeaseRevokeRequest{ID: id},
		},
	}
	proposeId := c.generateProposeId()
	cmd := pb.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*LeaseRevokeResponse)(res.CommandResp.GetLeaseRevokeResponse()), err
}

// Keeps the lease alive by streaming keep alive requests from the client
// to the server and streaming keep alive responses from the server to the client.
func (c *leaseClient) KeepAlive(ctx context.Context, id int64) (<-chan *LeaseKeepAliveResponse, error) {
	kach := make(chan *LeaseKeepAliveResponse)

	stream, err := c.leaseClient.LeaseKeepAlive(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = stream.Send(&pb.LeaseKeepAliveRequest{ID: id})
				if err != nil {
					c.logger.Error("keep alive fail", zap.Error(err))
				}
				res, err := stream.Recv()
				if err != nil {
					c.logger.Error("keep alive fail", zap.Error(err))
				}
				kach <- (*LeaseKeepAliveResponse)(res)
			}
			time.Sleep(keepAliveInterval)
		}
	}()

	return kach, nil
}

func (c *leaseClient) KeepAliveOnce(id int64) (*LeaseKeepAliveResponse, error) {
	ctx := context.Background()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.leaseClient.LeaseKeepAlive(cctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := stream.CloseSend()
		if err != nil {
			panic("close stream fail")
		}
	}()

	err = stream.Send(&pb.LeaseKeepAliveRequest{ID: id})
	if err != nil {
		return nil, err
	}

	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	return (*LeaseKeepAliveResponse)(res), nil
}

// Retrieves lease information.
func (c *leaseClient) TimeToLive(id int64, opts ...LeaseOption) (*LeaseTimeToLiveResponse, error) {
	request := toTTLReq(id, opts...)
	res, err := c.leaseClient.LeaseTimeToLive(context.Background(), request)
	return (*LeaseTimeToLiveResponse)(res), err
}

// Lists all existing leases.
func (c *leaseClient) Leases() (*LeaseLeasesResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_LeaseLeasesRequest{
			LeaseLeasesRequest: &pb.LeaseLeasesRequest{},
		},
	}
	proposeId := c.generateProposeId()
	cmd := pb.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return (*LeaseLeasesResponse)(res.CommandResp.GetLeaseLeasesResponse()), err
}

// Generate a new `ProposeId`
func (c leaseClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}
