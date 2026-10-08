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

package cloud_providers

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
)

// clusterResourceStateTimeoutS is the timeout (seconds) of getting the
// cluster resource state.
// Corresponds to Python sdk.DEFAULT_RPC_TIMEOUT_S.
const clusterResourceStateTimeoutS = 10 * time.Second

// ReadOnlyProvider is the read-only cloud instance provider.
// Corresponds to Python
// cloud_providers/read_only/cloud_provider.py::ReadOnlyProvider:
// it treats the Ray node states reported by GCS as the "cloud instances", for
// the notebook mode/manually set up cluster mode, keeping the state reporting
// of those modes consistent with cloud clusters. Read-only mode does not
// support launching/terminating instances.
type ReadOnlyProvider struct {
	gcsClient gcs.Client
}

// NewReadOnlyProvider creates the read-only cloud instance provider.
func NewReadOnlyProvider(gcsClient gcs.Client) *ReadOnlyProvider {
	return &ReadOnlyProvider{gcsClient: gcsClient}
}

// GetNonTerminated returns the non-terminated "cloud instances" (i.e. the live
// nodes of the Ray cluster).
func (p *ReadOnlyProvider) GetNonTerminated() (map[string]autoscaler.CloudInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterResourceStateTimeoutS)
	defer cancel()

	reply, err := p.gcsClient.GetAutoscalerStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster resource state from GCS: %w", err)
	}
	if reply == nil || reply.ClusterResourceState == nil {
		return nil, fmt.Errorf("no cluster resource state available from GCS")
	}

	cloudInstances := make(map[string]autoscaler.CloudInstance)
	for _, nodeState := range reply.ClusterResourceState.NodeStates {
		// Skip the dead nodes.
		if nodeState.Status == proto.NodeStatus_DEAD {
			continue
		}

		// When the instance ID is unavailable, fall back to the hex
		// representation of the node ID.
		cloudInstanceId := nodeState.InstanceId
		if cloudInstanceId == "" {
			cloudInstanceId = hex.EncodeToString(nodeState.NodeId)
		}

		isHead := autoscaler.IsHeadNode(nodeState)
		nodeKind := proto.NodeKind_WORKER
		if isHead {
			nodeKind = proto.NodeKind_HEAD
		}

		cloudInstances[cloudInstanceId] = autoscaler.CloudInstance{
			CloudInstanceId: cloudInstanceId,
			NodeKind:        nodeKind,
			NodeType:        autoscaler.FormatReadonlyNodeType(hex.EncodeToString(nodeState.NodeId)),
			IsRunning:       true,
			RequestId:       "",
		}
	}
	return cloudInstances, nil
}

// PollErrors returns the asynchronous errors accumulated by the cloud
// provider.
// There are no cloud operations in read-only mode, so it always returns
// empty.
func (p *ReadOnlyProvider) PollErrors() []error {
	return nil
}

// Terminate asynchronously terminates the cloud instances.
// Cloud operations are not supported in read-only mode; Python raises
// NotImplementedError.
func (p *ReadOnlyProvider) Terminate(ids []string, requestId string) error {
	return fmt.Errorf("cannot terminate individuals in read-only mode")
}

// Launch asynchronously launches the cloud instances.
// Cloud operations are not supported in read-only mode; Python raises
// NotImplementedError.
func (p *ReadOnlyProvider) Launch(shape map[string]int, requestId string) error {
	return fmt.Errorf("cannot launch individuals in read-only mode")
}
