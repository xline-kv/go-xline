package client

import (
	"context"

	"github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
)

type Maintenance interface {
	// Gets a snapshot over a stream
	Snapshot() (SnapshotClient, error)
}

type SnapshotClient xlineapi.Maintenance_SnapshotClient

// Client for Maintenance operations.
type maintenanceClient struct {
	// The maintenance RPC client, only communicate with one server at a time
	inner xlineapi.MaintenanceClient
}

// Creates a new maintenance client
func NewMaintenance(conn *grpc.ClientConn) Maintenance {
	return &maintenanceClient{inner: xlineapi.NewMaintenanceClient(conn)}
}

// Gets a snapshot over a stream
func (c *maintenanceClient) Snapshot() (SnapshotClient, error) {
	client, err := c.inner.Snapshot(context.Background(), &xlineapi.SnapshotRequest{})
	return (SnapshotClient)(client), err
}
