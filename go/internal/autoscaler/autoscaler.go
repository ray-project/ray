// Copyright 2026 The Ray Authors.
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

package autoscaler

import (
	"github.com/ray-project/ray/go/proto"
)

// This package is the shared dependency layer of autoscaler v2: the instance
// manager and its subpackages (reconciler/subscribers/cloud_providers) depend
// on the shared types and config utilities defined here, so this package must
// not import those subpackages back (one-way dependency, no cycles).
// The orchestrator Autoscaler, which assembles all long-lived collaborators
// and drives reconciliation, lives in the v2 package:
// internal/autoscaler/v2/autoscaler.go, mirroring the location of the Python
// side Autoscaler in ray/autoscaler/v2/autoscaler.py.

// CloudInstance holds the information of a cloud instance.
// It mirrors the core fields of the Python side node_provider.CloudInstance.
type CloudInstance struct {
	// CloudInstanceId is the cloud instance ID.
	CloudInstanceId string
	// NodeKind is the node kind (head/worker).
	NodeKind proto.NodeKind
	// NodeType is the node type name, matching available_node_types in the
	// autoscaling config.
	NodeType string
	// IsRunning reports whether the instance is in the running state.
	IsRunning bool
	// RequestId is the ID of the request that created the instance.
	RequestId string
}

// ICloudInstanceProvider is the cloud instance provider interface.
// It corresponds to the Python side node_provider.ICloudInstanceProvider.
// launch/terminate are asynchronous and non-blocking; the actual result
// (whether instance creation/termination took effect) is synced passively by
// later reconciliation rounds through GetNonTerminated; asynchronous failures
// are reported through PollErrors.
type ICloudInstanceProvider interface {
	// GetNonTerminated returns the non-terminated instances on the cloud,
	// keyed by cloud instance ID.
	GetNonTerminated() (map[string]CloudInstance, error)
	// Terminate asynchronously terminates cloud instances and is expected to
	// be idempotent (repeating the same requestId is a no-op).
	Terminate(ids []string, requestId string) error
	// Launch asynchronously launches cloud instances, where shape is
	// node type -> number to launch, and is expected to be idempotent.
	Launch(shape map[string]int, requestId string) error
	// PollErrors drains the asynchronous errors accumulated by the cloud
	// provider.
	PollErrors() []error
}

// ReconcileArgs holds all inputs required by one reconciliation round.
// It corresponds to the parameter list of the Python side Reconciler.reconcile.
type ReconcileArgs struct {
	// RayClusterResourceState is the current Ray cluster resource state
	// (from GCS).
	RayClusterResourceState *proto.ClusterResourceState
	// NonTerminatedCloudInstances are the non-terminated instances on the
	// cloud, keyed by cloud instance ID.
	NonTerminatedCloudInstances map[string]CloudInstance
	// CloudProviderErrors are the asynchronous errors accumulated by the
	// cloud provider.
	CloudProviderErrors []error
	// RayInstallErrors are the errors reported by the Ray installer
	// (ThreadedRayInstaller).
	RayInstallErrors []error
	// RayStopErrors are the errors reported by the Ray stopper (RayStopper).
	RayStopErrors []error
	// AutoscalingConfig is the autoscaling config (concretely
	// *instance_manager.AutoscalingConfig; instance_manager depends on this
	// package, so this package cannot reference its type and it is passed as
	// any, with the reconciler implementation responsible for the type
	// assertion).
	AutoscalingConfig any
}
