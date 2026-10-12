// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package factory holds the shared GCS client factory registration for the
// api package dependency inversion. It is used by both the worker (internal/
// worker) and the dashboard head (internal/dashboard/head) when they
// initialize the Go runtime through the static base.Initialize path (which
// does not load go_runtime.so).
package native

import (
	"context"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/proto"
)

// gcsClientAdapter implements api.GCSClient by delegating to a gcs.Client.
// It lets the caller provide the concrete GCS client implementation to the api
// package without the api package importing go/internal/gcs/native directly.
type gcsClientAdapter struct {
	client gcs.Client
}

// GetNodeToConnect implements api.GCSClient.GetNodeToConnect.
func (a *gcsClientAdapter) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return a.client.GetNodeToConnect(ctx, nodeIpAddress)
}

// NextJobID implements api.GCSClient.NextJobID.
// Converts ids.JobID to hex string.
func (a *gcsClientAdapter) NextJobID(ctx context.Context) (string, error) {
	jobID, err := a.client.NextJobID(ctx)
	if err != nil {
		return "", err
	}
	return jobID.Hex(), nil
}

// Close implements api.GCSClient.Close.
func (a *gcsClientAdapter) Close() error {
	return a.client.Close()
}

// IsClosed implements api.GCSClient.IsClosed by delegating to the underlying client.
func (a *gcsClientAdapter) IsClosed() bool {
	if checker, ok := a.client.(interface{ IsClosed() bool }); ok {
		return checker.IsClosed()
	}
	return false
}

// GetPlacementGroupInfo implements api.GCSClient.GetPlacementGroupInfo.
func (a *gcsClientAdapter) GetPlacementGroupInfo(ctx context.Context, id ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return a.client.GetPlacementGroup(ctx, id)
}

// GetPlacementGroupInfoByName implements api.GCSClient.GetPlacementGroupInfoByName.
func (a *gcsClientAdapter) GetPlacementGroupInfoByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return a.client.GetPlacementGroupByName(ctx, name, namespace)
}

// GetAllPlacementGroupInfo implements api.GCSClient.GetAllPlacementGroupInfo.
func (a *gcsClientAdapter) GetAllPlacementGroupInfo(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return a.client.ListPlacementGroups(ctx)
}

// GetInternalKV implements api.GCSClient.GetInternalKV.
func (a *gcsClientAdapter) GetInternalKV(ctx context.Context, ns, key string) ([]byte, error) {
	return a.client.Get(ctx, ns, key)
}

// GetAllNodeInfo implements api.GCSClient.GetAllNodeInfo.
// A nil node list queries all nodes in the cluster.
func (a *gcsClientAdapter) GetAllNodeInfo(ctx context.Context) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return a.client.GetAll(ctx, nil)
}

// GetAllActorInfo implements api.GCSClient.GetAllActorInfo.
func (a *gcsClientAdapter) GetAllActorInfo(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return a.client.ListActorsByFilter(ctx, jobID, actorStateName)
}

type gcsClientFactory struct{}

// CreateClient implements api.GCSClientFactory.CreateClient().
func (f *gcsClientFactory) CreateClient(opts gcs.ClientOptions) (api.GCSClient, error) {
	client, err := ConnectClient(opts)
	if err != nil {
		return nil, err
	}
	return &gcsClientAdapter{client: client}, nil
}

// RegisterGCSClientFactory registers the GCS client factory with the api
// package. It must be called before api.WithCachedClient (or any api GCS
// operation) is used. Driver-mode initialization (worker and dashboard head)
// calls this once before fetching node info and JobID from GCS.
func RegisterGCSClientFactory() {
	api.RegisterGCSClientFactory(&gcsClientFactory{})
}
