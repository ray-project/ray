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
	"errors"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// fakeGcsClient is a fake GCS client that only overrides GetAutoscalerStatus.
type fakeGcsClient struct {
	gcs.Client // embedded interface; the non-overridden methods panic only when called
	reply      *proto.GetClusterStatusReply
	err        error
}

func (f *fakeGcsClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return f.reply, f.err
}

// newFakeGcsReply builds a GCS reply with the cluster resource state.
func newFakeGcsReply(version int64, nodeStates ...*proto.NodeState) *fakeGcsClient {
	return &fakeGcsClient{
		reply: &proto.GetClusterStatusReply{
			ClusterResourceState: &proto.ClusterResourceState{
				ClusterResourceStateVersion: version,
				NodeStates:                  nodeStates,
			},
		},
	}
}

// TestReadOnlyProvider_GetNonTerminated builds the cloud instance table from
// the GCS node states:
// dead nodes are skipped, head/worker nodes are distinguished, and the node ID
// is the fallback when the instance ID is missing.
func TestReadOnlyProvider_GetNonTerminated(t *testing.T) {
	headNodeId := []byte{0x01, 0x02}
	workerNodeId := []byte{0x03, 0x04}
	workerNoInstanceIdNodeId := []byte{0x05, 0x06}
	gcsClient := newFakeGcsReply(1,
		&proto.NodeState{
			NodeId:         headNodeId,
			TotalResources: map[string]float64{"node:__internal_head__": 1.0},
			Status:         proto.NodeStatus_RUNNING,
		},
		&proto.NodeState{
			NodeId:     workerNodeId,
			InstanceId: "cloud-worker-1",
			Status:     proto.NodeStatus_RUNNING,
		},
		&proto.NodeState{
			NodeId: workerNoInstanceIdNodeId,
			Status: proto.NodeStatus_RUNNING,
		},
		&proto.NodeState{
			NodeId: []byte{0xff},
			Status: proto.NodeStatus_DEAD,
		},
	)

	provider := NewReadOnlyProvider(gcsClient)
	instances, err := provider.GetNonTerminated()

	assert.NoError(t, err)
	assert.Len(t, instances, 3)

	// Head node: the node ID hex is the fallback instance ID.
	head := instances["0102"]
	assert.Equal(t, proto.NodeKind_HEAD, head.NodeKind)
	assert.Equal(t, autoscaler.FormatReadonlyNodeType("0102"), head.NodeType)
	assert.True(t, head.IsRunning)

	// Worker node: has a cloud instance ID.
	worker := instances["cloud-worker-1"]
	assert.Equal(t, proto.NodeKind_WORKER, worker.NodeKind)
	assert.Equal(t, autoscaler.FormatReadonlyNodeType("0304"), worker.NodeType)

	// Worker node: no cloud instance ID, falling back to the node ID.
	workerFallback := instances["0506"]
	assert.Equal(t, proto.NodeKind_WORKER, workerFallback.NodeKind)
}

// TestReadOnlyProvider_GcsError returns an error when the GCS request fails.
func TestReadOnlyProvider_GcsError(t *testing.T) {
	provider := NewReadOnlyProvider(&fakeGcsClient{err: errors.New("gcs unavailable")})

	instances, err := provider.GetNonTerminated()

	assert.Error(t, err)
	assert.Nil(t, instances)
}

// TestReadOnlyProvider_PollErrors always returns an empty error list in
// read-only mode.
func TestReadOnlyProvider_PollErrors(t *testing.T) {
	provider := NewReadOnlyProvider(&fakeGcsClient{})

	assert.Empty(t, provider.PollErrors())
}
