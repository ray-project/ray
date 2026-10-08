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
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// IRayInstaller installs Ray on the target instance synchronously.
// Corresponds to Python ray_installer.RayInstaller.
type IRayInstaller interface {
	// InstallRay installs (starts) Ray on the given instance,
	// connecting the worker to the head node at headNodeIP.
	InstallRay(instance *proto.Instance, headNodeIP string) error
}

// Default retry/concurrency parameters, aligned with the Python
// ThreadedRayInstaller constructor defaults.
const (
	defaultMaxInstallAttempts    = 3
	defaultInstallRetryInterval  = 10 * time.Second
	defaultMaxConcurrentInstalls = 50
)

// InstallerOptions tunes ThreadedRayInstaller; zero values fall back to the
// defaults aligned with Python (attempts=3, interval=10s, concurrency=50).
type InstallerOptions struct {
	// MaxInstallAttempts is the max number of install attempts per instance.
	MaxInstallAttempts int
	// InstallRetryInterval is the base interval between retries; the actual
	// wait grows exponentially (interval * 1, 2, 4, ...).
	InstallRetryInterval time.Duration
	// MaxConcurrentInstalls bounds the concurrent installs.
	MaxConcurrentInstalls int
}

// ThreadedRayInstaller installs Ray on new nodes.
// Corresponds to Python subscribers/threaded_ray_installer.py: subscribes to
// RAY_INSTALLING events, fetches the instances from the instance storage and
// installs Ray asynchronously with bounded concurrency and exponential
// backoff retries. Final failures are reported as RayInstallError through
// the error queue, which the Autoscaler drains each reconcile round.
type ThreadedRayInstaller struct {
	headNodeIP           string
	instanceStorage      *instance_manager.InstanceStorage
	rayInstaller         IRayInstaller
	errorQueue           chan<- error
	maxInstallAttempts   int
	installRetryInterval time.Duration
	// installSemaphore bounds concurrent installs (Python: ThreadPoolExecutor
	// max_workers). Tasks queue up on the semaphore, matching the unbounded
	// executor queue.
	installSemaphore chan struct{}
	wg               sync.WaitGroup
	shutdownCh       chan struct{} // aborts pending retries on Close
}

// NewThreadedRayInstaller creates the Ray installer subscriber.
// errorQueue must be the same channel reference passed to
// Autoscaler.NewAutoscaler as rayInstallErrorsQueue.
func NewThreadedRayInstaller(
	headNodeIP string,
	instanceStorage *instance_manager.InstanceStorage,
	rayInstaller IRayInstaller,
	errorQueue chan<- error,
	options InstallerOptions,
) *ThreadedRayInstaller {
	maxInstallAttempts := options.MaxInstallAttempts
	if maxInstallAttempts <= 0 {
		maxInstallAttempts = defaultMaxInstallAttempts
	}
	installRetryInterval := options.InstallRetryInterval
	if installRetryInterval <= 0 {
		installRetryInterval = defaultInstallRetryInterval
	}
	maxConcurrentInstalls := options.MaxConcurrentInstalls
	if maxConcurrentInstalls <= 0 {
		maxConcurrentInstalls = defaultMaxConcurrentInstalls
	}
	return &ThreadedRayInstaller{
		headNodeIP:           headNodeIP,
		instanceStorage:      instanceStorage,
		rayInstaller:         rayInstaller,
		errorQueue:           errorQueue,
		maxInstallAttempts:   maxInstallAttempts,
		installRetryInterval: installRetryInterval,
		installSemaphore:     make(chan struct{}, maxConcurrentInstalls),
		shutdownCh:           make(chan struct{}),
	}
}

// Notify handles instance update events: only RAY_INSTALLING triggers an
// install, dispatched asynchronously.
func (i *ThreadedRayInstaller) Notify(events []*proto.InstanceUpdateEvent) {
	for _, event := range events {
		if event.NewInstanceStatus != proto.Instance_RAY_INSTALLING {
			continue
		}
		// Re-read the instance from the storage with the RAY_INSTALLING
		// filter (aligned with Python _install_ray_on_new_nodes).
		instances, _ := i.instanceStorage.GetInstances(
			[]string{event.GetInstanceId()},
			map[proto.Instance_InstanceStatus]bool{proto.Instance_RAY_INSTALLING: true})
		for _, instance := range instances {
			if instance.NodeKind != proto.NodeKind_WORKER {
				// Python asserts the worker kind here; skip with a log
				// instead of crashing the autoscaler.
				log.Log.Info("Skip installing ray on non-worker instance.",
					"instanceId", instance.InstanceId,
					"nodeKind", instance.NodeKind.String())
				continue
			}
			i.wg.Add(1)
			go i.installRayOnSingleNode(instance)
		}
	}
}

// installRayOnSingleNode installs Ray on a single node with exponential
// backoff retries; on final failure a RayInstallError is queued.
func (i *ThreadedRayInstaller) installRayOnSingleNode(instance *proto.Instance) {
	defer i.wg.Done()

	// Bounded concurrency: wait for a free slot (Python: executor max_workers
	// with an unbounded queue). Submitted tasks always run to completion;
	// Close drains them.
	i.installSemaphore <- struct{}{}
	defer func() { <-i.installSemaphore }()

	backoffFactor := 1
	var lastErr error
	for attempt := 0; attempt < i.maxInstallAttempts; attempt++ {
		if err := i.rayInstaller.InstallRay(instance, i.headNodeIP); err == nil {
			return
		} else {
			log.Log.Info("Ray installation failed on instance.",
				"instanceId", instance.InstanceId,
				"cloudInstanceId", instance.GetCloudInstanceId(), "error", err)
			lastErr = err
		}

		if attempt == i.maxInstallAttempts-1 {
			break
		}
		// Retry with exponential backoff.
		select {
		case <-time.After(i.installRetryInterval * time.Duration(backoffFactor)):
			backoffFactor *= 2
		case <-i.shutdownCh:
			return
		}
	}

	log.Log.Info("Failed to install ray after all attempts, reporting error.",
		"instanceId", instance.InstanceId,
		"attempts", i.maxInstallAttempts, "lastError", lastErr)
	select {
	case i.errorQueue <- RayInstallError{
		ImInstanceId: instance.InstanceId,
		Details:      lastErr.Error(),
	}:
	default:
		// Queue full: drop the error with a log (the Autoscaler drains the
		// queue each round, so this is a transient condition only).
		log.Log.Info("Ray install error queue is full, dropping error.",
			"instanceId", instance.InstanceId)
	}
}

// Close shuts the installer down, aborting pending retries and waiting for
// all submitted installs to complete.
func (i *ThreadedRayInstaller) Close() {
	close(i.shutdownCh)
	i.wg.Wait()
}
