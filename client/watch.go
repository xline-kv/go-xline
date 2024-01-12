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
	"time"

	"github.com/xline-kv/go-xline/api/gen/xline"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Watch interface {
	// Watches for events happening or that have happened.
	Watch(ctx context.Context, key []byte) (<-chan *WatchResponse, error)
}

type WatchResponse xlineapi.WatchResponse

const watchInterval = 5 * time.Millisecond

// Client for Watch operations.
type watchClient struct {
	// The watch RPC client, only communicate with one server at a time
	watchClient xlineapi.WatchClient
	// The logger for log
	logger *zap.Logger
}

// Creates a new maintenance client
func NewWatch(conn *grpc.ClientConn) Watch {
	return &watchClient{watchClient: xlineapi.NewWatchClient(conn), logger: xlog.GetLogger()}
}

// Watches for events happening or that have happened.
func (c *watchClient) Watch(ctx context.Context, key []byte) (<-chan *WatchResponse, error) {
	wch := make(chan *WatchResponse)
	var watchID int64

	wc, err := c.watchClient.Watch(context.Background())
	if err != nil {
		return nil, err
	}
	r := &xlineapi.WatchRequest{
		RequestUnion: &xlineapi.WatchRequest_CreateRequest{
			CreateRequest: &xlineapi.WatchCreateRequest{
				Key: []byte(key),
			},
		},
	}
	err = wc.Send(r)
	if err != nil {
		panic("watch create fail")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				r := &xlineapi.WatchRequest{
					RequestUnion: &xlineapi.WatchRequest_CancelRequest{
						CancelRequest: &xlineapi.WatchCancelRequest{
							WatchId: watchID,
						},
					},
				}
				err := wc.Send(r)
				if err != nil {
					panic("watch cancel fail")
				}
				res, err := wc.Recv()
				if err != nil {
					panic("watch cancel fail")
				}
				wch <- (*WatchResponse)(res)
				close(wch)
				return
			default:
				res, err := wc.Recv()
				if err != nil {
					c.logger.Error("watch fail", zap.Error(err))
				}
				if res.Created {
					watchID = res.WatchId
				}
				wch <- (*WatchResponse)(res)
			}
			time.Sleep(watchInterval)
		}
	}()

	return wch, nil
}
