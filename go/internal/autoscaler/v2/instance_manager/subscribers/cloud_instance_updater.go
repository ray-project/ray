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
	"github.com/google/uuid"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// Compile-time assertion: CloudInstanceUpdater implements the instance update
// subscriber interface.
var _ instance_manager.InstanceUpdatedSubscriber = (*CloudInstanceUpdater)(nil)

// CloudInstanceUpdater is the subscriber launching and terminating cloud
// instances.
// Corresponds to Python subscribers/cloud_instance_updater.py:
// when an instance enters REQUESTED (the reconciler decided to scale up), it
// asks the cloud provider to launch, grouped by launch request; when an
// instance enters TERMINATING (the reconciler decided to scale down/clean up),
// it asks the cloud provider to terminate.
// The cloud instance APIs are asynchronous and non-blocking; the actual result
// is later passively synced by the reconcile rounds via GetNonTerminated, and
// async failures are reported via the cloud provider's PollErrors.
type CloudInstanceUpdater struct {
	cloudProvider autoscaler.ICloudInstanceProvider
}

// NewCloudInstanceUpdater creates the cloud instance update subscriber.
func NewCloudInstanceUpdater(cloudProvider autoscaler.ICloudInstanceProvider) *CloudInstanceUpdater {
	return &CloudInstanceUpdater{cloudProvider: cloudProvider}
}

// Notify is the instance status update callback: dispatch REQUESTED/TERMINATING
// events to the corresponding cloud operations.
func (u *CloudInstanceUpdater) Notify(events []*proto.InstanceUpdateEvent) {
	newRequests := make([]*proto.InstanceUpdateEvent, 0)
	newTerminations := make([]*proto.InstanceUpdateEvent, 0)
	for _, event := range events {
		switch event.NewInstanceStatus {
		case proto.Instance_REQUESTED:
			newRequests = append(newRequests, event)
		case proto.Instance_TERMINATING:
			newTerminations = append(newTerminations, event)
		}
	}
	u.launchNewInstances(newRequests)
	u.terminateInstances(newTerminations)
}

// launchNewInstances groups the requests by launch request id and asks the
// cloud provider to launch.
func (u *CloudInstanceUpdater) launchNewInstances(newRequests []*proto.InstanceUpdateEvent) {
	if len(newRequests) == 0 {
		log.Log.V(1).Info("No instances to launch.")
		return
	}

	requestsByLaunchRequestId := make(map[string][]*proto.InstanceUpdateEvent)
	for _, event := range newRequests {
		if event.GetLaunchRequestId() == "" {
			// Python asserts here: the launch request id should be pre-filled
			// by the reconciler.
			log.Log.Info("Launch request id is empty, skip launching instance.",
				"instanceId", event.InstanceId)
			continue
		}
		requestsByLaunchRequestId[event.GetLaunchRequestId()] =
			append(requestsByLaunchRequestId[event.GetLaunchRequestId()], event)
	}

	for launchRequestId, events := range requestsByLaunchRequestId {
		// Aggregate the launch counts per node type under this request.
		shape := make(map[string]int)
		for _, event := range events {
			shape[event.GetInstanceType()]++
		}
		if err := u.cloudProvider.Launch(shape, launchRequestId); err != nil {
			log.Log.Error(err, "Failed to launch cloud instances.",
				"launchRequestId", launchRequestId, "shape", shape)
		}
	}
}

// terminateInstances collects the cloud instance IDs and asks the cloud
// provider to terminate them.
func (u *CloudInstanceUpdater) terminateInstances(newTerminations []*proto.InstanceUpdateEvent) {
	if len(newTerminations) == 0 {
		log.Log.V(1).Info("No instances to terminate.")
		return
	}

	cloudInstanceIds := make([]string, 0, len(newTerminations))
	for _, event := range newTerminations {
		cloudInstanceIds = append(cloudInstanceIds, event.GetCloudInstanceId())
	}

	if err := u.cloudProvider.Terminate(cloudInstanceIds, uuid.NewString()); err != nil {
		log.Log.Error(err, "Failed to terminate cloud instances.",
			"cloudInstanceIds", cloudInstanceIds)
	}
}
