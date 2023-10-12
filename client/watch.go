package client

import (
	"context"

	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
)

// Client for Watch operations.
type watchClient struct {
	// The watch RPC client, only communicate with one server at a time
	watchClient xlineapi.WatchClient
}

// Creates a new maintenance client
func newWatchClient(conn *grpc.ClientConn) watchClient {
	return watchClient{watchClient: xlineapi.NewWatchClient(conn)}
}

func (c watchClient) Watch() (xlineapi.Watch_WatchClient, error) {
	res, err := c.watchClient.Watch(context.Background())
	if err != nil {
		return nil, err
	}
	return res, nil
}
