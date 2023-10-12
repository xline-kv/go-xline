package client

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
)

// Client for Lease operations
type leaseClient struct {
	// Name of the LeaseClient, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The lease RPC client, only communicate with one server at a time
	leaseClient xlineapi.LeaseClient
	// Auth token
	token string
	// Lease Id generator
	idGen leaseIdGenerator
}

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
		leaseClient: xlineapi.NewLeaseClient(conn),
		token:       token,
		idGen:       idGen,
	}
}

// Creates a lease which expires if the server does not receive a keepAlive
// within a given time to live period. All keys attached to the lease will be expired and
// deleted if the lease expires. Each expired key generates a delete event in the event history.
func (c *leaseClient) Grant(request *xlineapi.LeaseGrantRequest) (*xlineapi.LeaseGrantResponse, error) {
	if request.ID == 0 {
		request.ID = c.idGen.next()
	}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_LeaseGrantRequest{
			LeaseGrantRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return res.CommandResp.GetLeaseGrantResponse(), err
}

// Revokes a lease. All keys attached to the lease will expire and be deleted.
func (c *leaseClient) Revoke(request *xlineapi.LeaseRevokeRequest) (*xlineapi.LeaseRevokeResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_LeaseRevokeRequest{
			LeaseRevokeRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return res.CommandResp.GetLeaseRevokeResponse(), err
}

// Keeps the lease alive by streaming keep alive requests from the client
// to the server and streaming keep alive responses from the server to the client.
func (c *leaseClient) KeepAlive(request *xlineapi.LeaseKeepAliveRequest) (xlineapi.Lease_LeaseKeepAliveClient, error) {
	keeper, err := c.leaseClient.LeaseKeepAlive(context.Background())
	return keeper, err
}

// Retrieves lease information.
func (c *leaseClient) TimeToLive(request *xlineapi.LeaseTimeToLiveRequest) (*xlineapi.LeaseTimeToLiveResponse, error) {
	res, err := c.leaseClient.LeaseTimeToLive(context.Background(), request)
	return res, err
}

// Lists all existing leases.
func (c *leaseClient) Leases(request *xlineapi.LeaseLeasesRequest) (*xlineapi.LeaseLeasesResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_LeaseLeasesRequest{
			LeaseLeasesRequest: request,
		},
	}
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: &requestWithToken, ProposeId: proposeId}

	res, err := c.curpClient.propose(&cmd, true)
	if err != nil {
		return nil, err
	}
	return res.CommandResp.GetLeaseLeasesResponse(), err
}

// Generate a new `ProposeId`
func (c leaseClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}
