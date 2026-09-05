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

package contract

import (
	"github.com/ray-project/ray/go/pkg/ids"
)

// RunMode represents the runtime mode.
// Consistent with Java's RayNativeRuntime.isLocalMode().
type RunMode int

const (
	// RunModeCluster represents cluster mode.
	RunModeCluster RunMode = iota
	// RunModeLocal represents local mode (single process).
	RunModeLocal
)

// NodeState represents the state of a node.
type NodeState int

const (
	// NodeStateAlive represents a node that is alive.
	NodeStateAlive NodeState = iota
	// NodeStateDead represents a node that is dead.
	NodeStateDead
	// NodeStateDraining represents a node that is draining.
	NodeStateDraining
)

// ActorState represents the state of an actor.
type ActorState int

const (
	// ActorStatePending represents an actor in pending creation state.
	ActorStatePending ActorState = iota
	// ActorStateAlive represents an actor that is alive.
	ActorStateAlive
	// ActorStateDead represents an actor that is dead.
	ActorStateDead
	// ActorStateRestarting represents an actor that is restarting.
	ActorStateRestarting
	// ActorStateDraining represents an actor that is draining.
	ActorStateDraining
)

// NodeInfo represents information about a node in the cluster.
type NodeInfo struct {
	NodeID                ids.NodeID
	NodeManagerAddress    string
	NodeManagerPort       int
	ObjectManagerPort     int
	ObjectStoreSocketName string
	RayletSocketName      string
	Resources             map[string]float64
	State                 NodeState
	ClusterName           string
	Version               string
}

// ActorInfo represents information about an actor in the cluster.
type ActorInfo struct {
	ActorID        ids.ActorID
	JobID          ids.JobID
	OwnerWorkerID  ids.UniqueID
	OwnerIPAddress string
	OwnerPort      int
	ActorHandleID  ids.UniqueID
	Name           string
	Namespace      string
	MaxRestarts    int
	NumRestarts    int
	State          ActorState
	Address        string
	IsDetached     bool
	StartTime      int64
	EndTime        int64
	Resources      map[string]float64
}
