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
	"fmt"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// Compile-time assertion: RayStopper implements the instance update subscriber
// interface.
var _ instance_manager.InstanceUpdatedSubscriber = (*RayStopper)(nil)

// RayStopError is the Ray stop failure error.
// Corresponds to Python ray_stopper.RayStopError.
type RayStopError struct {
	ImInstanceId string // the instance manager's instance ID
}

func (e RayStopError) Error() string {
	return "ray stop failed for instance " + e.ImInstanceId
}

// RayStopper is the subscriber stopping the Ray process when an instance
// terminates.
// Corresponds to Python subscribers/ray_stopper.py:
// it listens to RAY_STOP_REQUESTED events and stops Ray via the GCS
// DrainNode/DrainNodes APIs; on failure it pushes a RayStopError into
// error_queue, which the Autoscaler drains every round and hands to the
// reconciler.
type RayStopper struct {
	gcsClient  gcs.Client
	errorQueue chan<- error
	shutdownCh chan struct{} // shutdown signal
	taskQueue  chan *proto.InstanceUpdateEvent
	done       chan struct{} // exit signal of the run goroutine
}

// NewRayStopper creates the Ray stop subscriber.
// errorQueue must be the same reference as the one passed to
// Autoscaler.NewAutoscaler.
func NewRayStopper(gcsClient gcs.Client, errorQueue chan<- error) *RayStopper {
	rs := &RayStopper{
		gcsClient:  gcsClient,
		errorQueue: errorQueue,
		shutdownCh: make(chan struct{}),
		taskQueue:  make(chan *proto.InstanceUpdateEvent, 100), // buffered queue
		done:       make(chan struct{}),
	}
	// Start the background goroutine processing the stop requests serially
	// (matching Python's single-thread ThreadPoolExecutor).
	go rs.run()
	return rs
}

// run is the background loop processing the stop requests serially.
func (rs *RayStopper) run() {
	defer close(rs.done)
	for {
		select {
		case <-rs.shutdownCh:
			// Drain the already submitted tasks on shutdown.
			rs.drainRemainingTasks()
			return
		case event, ok := <-rs.taskQueue:
			if !ok {
				return
			}
			rs.stopOrDrainRay(event)
		}
	}
}

// drainRemainingTasks drains the remaining tasks in the task queue without
// blocking.
func (rs *RayStopper) drainRemainingTasks() {
	for {
		select {
		case event, ok := <-rs.taskQueue:
			if !ok {
				return
			}
			rs.stopOrDrainRay(event)
		default:
			return
		}
	}
}

// Notify is the instance status update callback: only RAY_STOP_REQUESTED
// events are handled.
func (rs *RayStopper) Notify(events []*proto.InstanceUpdateEvent) {
	for _, event := range events {
		if event.NewInstanceStatus == proto.Instance_RAY_STOP_REQUESTED {
			// Submit asynchronously to the serial executor (matching Python's
			// ThreadPoolExecutor.submit).
			select {
			case rs.taskQueue <- event:
				// Submitted successfully.
			case <-rs.shutdownCh:
				// Already shut down; no new requests are accepted.
				return
			default:
				// The queue is full; drop and log.
				log.Log.Info("RayStopper task queue is full, dropping stop request.",
					"instanceId", event.GetInstanceId())
			}
		}
	}
}

// stopOrDrainRay performs the stop Ray operation.
// Corresponds to Python subscribers/ray_stopper.py's _stop_or_drain_ray:
//   - Idle termination (IDLE) calls DrainNode(reason=IDLE_TERMINATION,
//     deadline=0)
//   - Other terminations call DrainNodes (batch drain, without a reason)
//   - On failure a RayStopError is pushed into errorQueue
func (rs *RayStopper) stopOrDrainRay(event *proto.InstanceUpdateEvent) {
	if event.TerminationRequest == nil {
		log.Log.Info("TerminationRequest is missing, skip stopping ray.",
			"instanceId", event.GetInstanceId())
		return
	}

	tr := event.TerminationRequest
	rayNodeID := tr.RayNodeId
	instanceID := event.GetInstanceId()

	if tr.Cause == proto.TerminationRequest_IDLE {
		reasonStr := fmt.Sprintf("Termination of node that's idle for %d seconds.",
			tr.GetIdleDurationMs()/1000)
		rs.drainRayNode(rayNodeID, instanceID,
			proto.DrainNodeReason_DRAIN_NODE_REASON_IDLE_TERMINATION, reasonStr)
		return
	}

	rs.stopRayNode(rayNodeID, instanceID)
}

// drainRayNode drains a single node (with a reason, used for idle
// termination).
func (rs *RayStopper) drainRayNode(rayNodeID, instanceID string,
	reason proto.DrainNodeReason, reasonStr string) {
	nodeID, err := ids.NodeIDFromHex(rayNodeID)
	if err != nil {
		log.Log.Error(err, "Invalid ray node ID", "rayNodeId", rayNodeID)
		rs.sendError(instanceID)
		return
	}

	accepted, rejectMsg, err := rs.gcsClient.DrainNode(
		context.Background(), nodeID, reason, reasonStr, 0)
	if err != nil {
		log.Log.Error(err, "Error draining ray", "rayNodeId", rayNodeID)
		rs.sendError(instanceID)
		return
	}

	log.Log.Info("Drained ray", "rayNodeId", rayNodeID,
		"accepted", accepted, "msg", rejectMsg)
	if !accepted {
		rs.sendError(instanceID)
	}
}

// stopRayNode stops a node (non-idle termination, implemented via the batch
// DrainNodes).
func (rs *RayStopper) stopRayNode(rayNodeID, instanceID string) {
	nodeID, err := ids.NodeIDFromHex(rayNodeID)
	if err != nil {
		log.Log.Error(err, "Invalid ray node ID", "rayNodeId", rayNodeID)
		rs.sendError(instanceID)
		return
	}

	drained, err := rs.gcsClient.DrainNodes(
		context.Background(), []ids.NodeID{nodeID})
	if err != nil {
		log.Log.Error(err, "Error stopping ray",
			"rayNodeId", rayNodeID, "instanceId", instanceID)
		rs.sendError(instanceID)
		return
	}

	success := len(drained) > 0
	log.Log.Info("Stopping ray", "rayNodeId", rayNodeID,
		"instanceId", instanceID, "success", success)
	if !success {
		rs.sendError(instanceID)
	}
}

// sendError pushes a RayStopError into errorQueue (non-blocking).
func (rs *RayStopper) sendError(instanceID string) {
	select {
	case rs.errorQueue <- RayStopError{ImInstanceId: instanceID}:
	default:
		log.Log.Info("Error queue is full, dropping RayStopError",
			"instanceId", instanceID)
	}
}

// Close shuts the RayStopper down and waits for the submitted tasks to
// finish.
func (rs *RayStopper) Close() {
	close(rs.shutdownCh)
	<-rs.done
}
