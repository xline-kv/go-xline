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

	"github.com/xline-kv/go-xline/api/xline"
	"google.golang.org/grpc"
)

type Cluster interface {
	// MemberList lists the current cluster membership.
	MemberList(ctx context.Context) (*MemberListResponse, error)

	// MemberAdd adds a new member into the cluster.
	MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error)

	// MemberAddAsLearner adds a new learner member into the cluster.
	MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error)

	// MemberRemove removes an existing member from the cluster.
	MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error)

	// MemberUpdate updates the peer addresses of the member.
	MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberUpdateResponse, error)

	// MemberPromote promotes a member from raft learner (non-voting) to raft voting member.
	MemberPromote(ctx context.Context, id uint64) (*MemberPromoteResponse, error)
}

type (
	Member                xlineapi.Member
	MemberListResponse    xlineapi.MemberListResponse
	MemberAddResponse     xlineapi.MemberAddResponse
	MemberRemoveResponse  xlineapi.MemberRemoveResponse
	MemberUpdateResponse  xlineapi.MemberUpdateResponse
	MemberPromoteResponse xlineapi.MemberPromoteResponse
)

// Client for Cluster operations.
type clusterClient struct {
	inner xlineapi.ClusterClient
}

// Create a new cluster client
func NewCluster(conn *grpc.ClientConn) *clusterClient {
	return &clusterClient{inner: xlineapi.NewClusterClient(conn)}
}

func (c *clusterClient) MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, false)
}

func (c *clusterClient) MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, true)
}

func (c *clusterClient) memberAdd(ctx context.Context, peerAddrs []string, isLearner bool) (*MemberAddResponse, error) {
	req := &xlineapi.MemberAddRequest{
		PeerURLs:  peerAddrs,
		IsLearner: isLearner,
	}
	res, err := c.inner.MemberAdd(ctx, req)
	if err != nil {
		return nil, err
	}
	return (*MemberAddResponse)(res), nil
}

func (c *clusterClient) MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error) {
	req := &xlineapi.MemberRemoveRequest{ID: id}
	res, err := c.inner.MemberRemove(ctx, req)
	if err != nil {
		return nil, err
	}
	return (*MemberRemoveResponse)(res), nil
}

func (c *clusterClient) MemberPromote(ctx context.Context, id uint64) (*MemberPromoteResponse, error) {
	req := &xlineapi.MemberPromoteRequest{ID: id}
	resp, err := c.inner.MemberPromote(ctx, req)
	if err != nil {
		return nil, err
	}
	return (*MemberPromoteResponse)(resp), nil
}

func (c *clusterClient) MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberUpdateResponse, error) {
	req := &xlineapi.MemberUpdateRequest{ID: id, PeerURLs: peerAddrs}
	resp, err := c.inner.MemberUpdate(ctx, req)
	if err != nil {
		return nil, err
	}
	return (*MemberUpdateResponse)(resp), nil
}

func (c *clusterClient) MemberList(ctx context.Context) (*MemberListResponse, error) {
	req := &xlineapi.MemberListRequest{Linearizable: true}
	resp, err := c.inner.MemberList(ctx, req)
	if err != nil {
		return nil, err
	}
	return (*MemberListResponse)(resp), nil
}
