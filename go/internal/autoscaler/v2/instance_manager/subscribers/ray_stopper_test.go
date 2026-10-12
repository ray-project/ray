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

package subscribers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// fakeGcsClientForRayStopper is a configurable fake GCS client for the
// RayStopper tests.
type fakeGcsClientForRayStopper struct {
	drainNodeFunc  func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error)
	drainNodesFunc func(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error)

	drainNodeCalls  []fakeDrainNodeCall
	drainNodesCalls [][]ids.NodeID
	mu              sync.Mutex
}

type fakeDrainNodeCall struct {
	nodeID    ids.NodeID
	reason    proto.DrainNodeReason
	reasonMsg string
	deadline  int64
}

func (f *fakeGcsClientForRayStopper) Address() string          { return "" }
func (f *fakeGcsClientForRayStopper) ClusterID() ids.ClusterID { return ids.ClusterID{} }
func (f *fakeGcsClientForRayStopper) Close() error             { return nil }
func (f *fakeGcsClientForRayStopper) IsClosed() bool           { return false }
func (f *fakeGcsClientForRayStopper) Get(ctx context.Context, ns, key string) ([]byte, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	return false, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	return 0, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) Exists(ctx context.Context, ns, key string) (bool, error) {
	return false, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	f.mu.Lock()
	f.drainNodesCalls = append(f.drainNodesCalls, nodeIDs)
	f.mu.Unlock()
	if f.drainNodesFunc != nil {
		return f.drainNodesFunc(ctx, nodeIDs)
	}
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	f.mu.Lock()
	f.drainNodeCalls = append(f.drainNodeCalls, fakeDrainNodeCall{nodeID, reason, reasonMessage, deadlineTimestampMs})
	f.mu.Unlock()
	if f.drainNodeFunc != nil {
		return f.drainNodeFunc(ctx, nodeID, reason, reasonMessage, deadlineTimestampMs)
	}
	return false, "", gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.JobID{}, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) PublishErrors(ctx context.Context) (<-chan gcs.ErrorData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) PublishLogs(ctx context.Context) (<-chan gcs.LogData, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return nil, gcs.ErrNotImplemented
}
func (f *fakeGcsClientForRayStopper) ReportAutoscalingState(autoscalingState string) error {
	return gcs.ErrNotImplemented
}

func (f *fakeGcsClientForRayStopper) getDrainNodeCalls() []fakeDrainNodeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	cps := make([]fakeDrainNodeCall, len(f.drainNodeCalls))
	copy(cps, f.drainNodeCalls)
	return cps
}

func (f *fakeGcsClientForRayStopper) getDrainNodesCalls() [][]ids.NodeID {
	f.mu.Lock()
	defer f.mu.Unlock()
	cps := make([][]ids.NodeID, len(f.drainNodesCalls))
	copy(cps, f.drainNodesCalls)
	return cps
}

// newStopEvent builds a RAY_STOP_REQUESTED event.
func newStopEvent(instanceID, rayNodeIDHex string, cause proto.TerminationRequest_Cause, idleDurationMs int64) *proto.InstanceUpdateEvent {
	return &proto.InstanceUpdateEvent{
		InstanceId:        instanceID,
		NewInstanceStatus: proto.Instance_RAY_STOP_REQUESTED,
		TerminationRequest: &proto.TerminationRequest{
			Cause:          cause,
			RayNodeId:      rayNodeIDHex,
			IdleDurationMs: u64Ptr(uint64(idleDurationMs)),
		},
	}
}

func u64Ptr(v uint64) *uint64 { return &v }

// TestRayStopper_DrainNodeIdleSuccess idle termination: no error when
// DrainNode is called and accepted=true.
func TestRayStopper_DrainNodeIdleSuccess(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodeFunc: func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
			return true, "", nil
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_IDLE, 30000),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Empty(t, errorQueue)
	calls := gcsClient.getDrainNodeCalls()
	assert.Len(t, calls, 1)
	assert.Equal(t, proto.DrainNodeReason_DRAIN_NODE_REASON_IDLE_TERMINATION, calls[0].reason)
	assert.Equal(t, int64(0), calls[0].deadline)
	assert.Contains(t, calls[0].reasonMsg, "idle for 30 seconds")
}

// TestRayStopper_DrainNodeIdleRejected an error is produced when DrainNode
// returns accepted=false.
func TestRayStopper_DrainNodeIdleRejected(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodeFunc: func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
			return false, "node still has actors", nil
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_IDLE, 30000),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Len(t, errorQueue, 1)
	err := <-errorQueue
	var rayStopErr RayStopError
	assert.ErrorAs(t, err, &rayStopErr)
	assert.Equal(t, "i-1", rayStopErr.ImInstanceId)
}

// TestRayStopper_DrainNodeError an error is produced when DrainNode returns an
// error.
func TestRayStopper_DrainNodeError(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodeFunc: func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
			return false, "", errors.New("rpc timeout")
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_IDLE, 30000),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Len(t, errorQueue, 1)
	err := <-errorQueue
	var rayStopErr RayStopError
	assert.ErrorAs(t, err, &rayStopErr)
	assert.Equal(t, "i-1", rayStopErr.ImInstanceId)
}

// TestRayStopper_DrainNodesSuccess non-idle termination: no error when
// DrainNodes returns drained nodes.
func TestRayStopper_DrainNodesSuccess(t *testing.T) {
	errorQueue := make(chan error, 10)
	expectedNodeID, _ := ids.NodeIDFromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c")
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodesFunc: func(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
			return []ids.NodeID{expectedNodeID}, nil
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_OUTDATED, 0),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Empty(t, errorQueue)
	calls := gcsClient.getDrainNodesCalls()
	assert.Len(t, calls, 1)
	assert.Len(t, calls[0], 1)
}

// TestRayStopper_DrainNodesError an error is produced when DrainNodes returns
// an error.
func TestRayStopper_DrainNodesError(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodesFunc: func(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
			return nil, errors.New("gcs unavailable")
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_OUTDATED, 0),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Len(t, errorQueue, 1)
	err := <-errorQueue
	var rayStopErr RayStopError
	assert.ErrorAs(t, err, &rayStopErr)
	assert.Equal(t, "i-1", rayStopErr.ImInstanceId)
}

// TestRayStopper_DrainNodesEmptyDrained an error is produced when DrainNodes
// returns an empty drained list.
func TestRayStopper_DrainNodesEmptyDrained(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodesFunc: func(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
			return []ids.NodeID{}, nil
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_MAX_NUM_NODES, 0),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Len(t, errorQueue, 1)
	err := <-errorQueue
	var rayStopErr RayStopError
	assert.ErrorAs(t, err, &rayStopErr)
	assert.Equal(t, "i-1", rayStopErr.ImInstanceId)
}

// TestRayStopper_InvalidNodeID an error is produced on an invalid Ray node ID.
func TestRayStopper_InvalidNodeID(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "not-a-valid-hex", proto.TerminationRequest_IDLE, 30000),
	})

	time.Sleep(100 * time.Millisecond)

	assert.Len(t, errorQueue, 1)
	err := <-errorQueue
	var rayStopErr RayStopError
	assert.ErrorAs(t, err, &rayStopErr)
	assert.Equal(t, "i-1", rayStopErr.ImInstanceId)
}

// TestRayStopper_IgnoresNonStopEvents non-RAY_STOP_REQUESTED events are
// ignored.
func TestRayStopper_IgnoresNonStopEvents(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	// Send events with other statuses.
	stopper.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_TERMINATING, "type-a", "", "cloud-1"),
		newEvent(proto.Instance_RAY_RUNNING, "type-a", "", "cloud-1"),
	})

	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, errorQueue)
	assert.Empty(t, gcsClient.getDrainNodeCalls())
	assert.Empty(t, gcsClient.getDrainNodesCalls())
}

// TestRayStopper_MissingTerminationRequest skips when the termination_request
// is missing.
func TestRayStopper_MissingTerminationRequest(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{}

	stopper := NewRayStopper(gcsClient, errorQueue)
	defer stopper.Close()

	event := &proto.InstanceUpdateEvent{
		InstanceId:        "i-1",
		NewInstanceStatus: proto.Instance_RAY_STOP_REQUESTED,
		// TerminationRequest is missing.
	}
	stopper.Notify([]*proto.InstanceUpdateEvent{event})

	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, errorQueue)
	assert.Empty(t, gcsClient.getDrainNodeCalls())
	assert.Empty(t, gcsClient.getDrainNodesCalls())
}

// TestRayStopper_Close waits for the tasks to finish on shutdown.
func TestRayStopper_Close(t *testing.T) {
	errorQueue := make(chan error, 10)
	gcsClient := &fakeGcsClientForRayStopper{
		drainNodeFunc: func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
			return true, "", nil
		},
	}

	stopper := NewRayStopper(gcsClient, errorQueue)

	// Shut down right after sending the request.
	stopper.Notify([]*proto.InstanceUpdateEvent{
		newStopEvent("i-1", "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c", proto.TerminationRequest_IDLE, 30000),
	})

	stopper.Close()
	// Close should block until the task completes, without panicking.
	assert.Len(t, gcsClient.getDrainNodeCalls(), 1)
}
