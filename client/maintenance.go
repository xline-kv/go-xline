package client

import (
	"context"

	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
)

// Client for Maintenance operations.
type maintenanceClient struct {
	// The maintenance RPC client, only communicate with one server at a time
	inner xlineapi.MaintenanceClient
}

// Creates a new maintenance client
func newMaintenanceClient(conn *grpc.ClientConn) maintenanceClient {
	return maintenanceClient{inner: xlineapi.NewMaintenanceClient(conn)}
}

// Gets a snapshot over a stream
func (c *maintenanceClient) Snapshot() (xlineapi.Maintenance_SnapshotClient, error) {
	client, err := c.inner.Snapshot(context.Background(), &xlineapi.SnapshotRequest{})
	return client, err
}
