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

// Package gcs provides the Go client for Ray Global Control Store (GCS).
package gcs

import (
	"context"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// InternalKVInterface provides the internal KV store interface.
type InternalKVInterface interface {
	Get(ctx context.Context, ns, key string) ([]byte, error)
	MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error)
	Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error)
	Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error)
	Keys(ctx context.Context, ns, prefix string) ([]string, error)
	Exists(ctx context.Context, ns, key string) (bool, error)
}

// NodeInfoInterface provides node information access.
type NodeInfoInterface interface {
	CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error)
	GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error)
	DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error)
	// GetNodeToConnect returns the node that a driver should connect to for the
	// given node IP address.
	GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error)
}

// NodeResourceInterface provides node resource access.
type NodeResourceInterface interface {
	GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error)
	GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error)
}

// ActorInfoInterface provides actor information access.
type ActorInfoInterface interface {
	GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error)
	// ListActors lists all actors, optionally filtered by job ID.
	ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error)
}

// JobInfoInterface provides job information access.
type JobInfoInterface interface {
	GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error)
	ListJobs(ctx context.Context) ([]*proto.JobTableData, error)
	NextJobID(ctx context.Context) (ids.JobID, error)
}

// WorkerInfoInterface provides worker information access.
type WorkerInfoInterface interface {
	GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error)
	ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error)
}

// PlacementGroupInterface provides placement group information access.
type PlacementGroupInterface interface {
	GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error)
	ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error)
}

// AutoscalerInterface exposes the autoscaler status.
type AutoscalerInterface interface {
	// GetAutoscalerStatus returns the deserialized protobuf autoscaler status.
	GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error)
}
