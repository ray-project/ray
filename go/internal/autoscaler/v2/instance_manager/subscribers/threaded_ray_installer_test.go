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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// fakeRayInstaller is a fake installer recording calls with injectable
// failures.
type fakeRayInstaller struct {
	mu          sync.Mutex
	instanceIDs []string
	headNodeIPs []string
	failFirstN  int // the first N calls return an error
	calls       int
}

func (f *fakeRayInstaller) InstallRay(instance *proto.Instance, headNodeIP string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.instanceIDs = append(f.instanceIDs, instance.InstanceId)
	f.headNodeIPs = append(f.headNodeIPs, headNodeIP)
	if f.calls <= f.failFirstN {
		return errors.New("ssh failed")
	}
	return nil
}

func (f *fakeRayInstaller) stats() (int, []string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.instanceIDs, f.headNodeIPs
}

// blockingRayInstaller is a fake installer blocking until release is closed
// (used by the concurrency tests).
type blockingRayInstaller struct {
	started chan string // the instance IDs that entered InstallRay
	release chan struct{}
}

func (f *blockingRayInstaller) InstallRay(instance *proto.Instance, headNodeIP string) error {
	f.started <- instance.InstanceId
	<-f.release
	return nil
}

// newInstallerStorage builds an instance storage with the given instances.
func newInstallerStorage(t *testing.T, instances ...*proto.Instance) *instance_manager.InstanceStorage {
	t.Helper()
	storage := instance_manager.NewInstanceStorage("test", instance_manager.NewInMemoryStorage())
	for _, instance := range instances {
		status := storage.UpsertInstance(instance, nil, nil)
		if !status.Success {
			t.Fatalf("failed to upsert instance %s", instance.InstanceId)
		}
	}
	return storage
}

// newRayInstallingInstance builds a RAY_INSTALLING worker instance.
func newRayInstallingInstance(instanceID, cloudInstanceID string) *proto.Instance {
	return &proto.Instance{
		InstanceId:      instanceID,
		Status:          proto.Instance_RAY_INSTALLING,
		NodeKind:        proto.NodeKind_WORKER,
		InstanceType:    "local.cluster.node",
		CloudInstanceId: &cloudInstanceID,
	}
}

// newInstallingEvent builds a RAY_INSTALLING event.
func newInstallingEvent(instanceID string) *proto.InstanceUpdateEvent {
	return &proto.InstanceUpdateEvent{
		InstanceId:        instanceID,
		NewInstanceStatus: proto.Instance_RAY_INSTALLING,
	}
}

// fastInstallerOptions uses a very short retry interval to speed the tests up.
func fastInstallerOptions(maxConcurrentInstalls int) InstallerOptions {
	return InstallerOptions{
		InstallRetryInterval:  time.Millisecond,
		MaxConcurrentInstalls: maxConcurrentInstalls,
	}
}

// TestThreadedRayInstaller_InstallsOnRayInstallingEvent a RAY_INSTALLING event
// triggers the async install with the head node IP, and no error is produced on
// success.
func TestThreadedRayInstaller_InstallsOnRayInstallingEvent(t *testing.T) {
	storage := newInstallerStorage(t, newRayInstallingInstance("i-1", "127.0.0.2"))
	installer := &fakeRayInstaller{}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))

	threaded.Notify([]*proto.InstanceUpdateEvent{newInstallingEvent("i-1")})
	threaded.Close()

	calls, instanceIDs, headNodeIPs := installer.stats()
	assert.Equal(t, 1, calls)
	assert.Equal(t, []string{"i-1"}, instanceIDs)
	assert.Equal(t, []string{"127.0.0.1"}, headNodeIPs)
	assert.Empty(t, errorQueue)
}

// TestThreadedRayInstaller_RetriesWithBackoff succeeds on the third call after
// two failures: no error is reported and the total call count is 3.
func TestThreadedRayInstaller_RetriesWithBackoff(t *testing.T) {
	storage := newInstallerStorage(t, newRayInstallingInstance("i-1", "127.0.0.2"))
	installer := &fakeRayInstaller{failFirstN: 2}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))

	threaded.Notify([]*proto.InstanceUpdateEvent{newInstallingEvent("i-1")})

	// Wait for the retries to finish (3 calls in total) before closing, to
	// avoid racing with Close's retry abort.
	assert.Eventually(t, func() bool {
		calls, _, _ := installer.stats()
		return calls == 3
	}, 5*time.Second, 5*time.Millisecond)
	threaded.Close()

	assert.Empty(t, errorQueue)
}

// TestThreadedRayInstaller_FailsAfterMaxAttempts reports a RayInstallError
// after failing continuously up to the max attempt count.
func TestThreadedRayInstaller_FailsAfterMaxAttempts(t *testing.T) {
	storage := newInstallerStorage(t, newRayInstallingInstance("i-1", "127.0.0.2"))
	installer := &fakeRayInstaller{failFirstN: 100}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))

	threaded.Notify([]*proto.InstanceUpdateEvent{newInstallingEvent("i-1")})

	// The reported error is a RayInstallError carrying the instance ID and the
	// failure details.
	var queued error
	assert.Eventually(t, func() bool {
		select {
		case queued = <-errorQueue:
			return true
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond)
	threaded.Close()

	calls, _, _ := installer.stats()
	assert.Equal(t, defaultMaxInstallAttempts, calls)

	installErr, ok := queued.(RayInstallError)
	assert.True(t, ok)
	assert.Equal(t, "i-1", installErr.ImInstanceId)
	assert.Contains(t, installErr.Details, "ssh failed")
	assert.Empty(t, errorQueue)
}

// TestThreadedRayInstaller_IgnoresNonInstallingEvents non-RAY_INSTALLING
// events are ignored.
func TestThreadedRayInstaller_IgnoresNonInstallingEvents(t *testing.T) {
	storage := newInstallerStorage(t, newRayInstallingInstance("i-1", "127.0.0.2"))
	installer := &fakeRayInstaller{}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))
	defer threaded.Close()

	threaded.Notify([]*proto.InstanceUpdateEvent{
		{InstanceId: "i-1", NewInstanceStatus: proto.Instance_TERMINATING},
		{InstanceId: "i-1", NewInstanceStatus: proto.Instance_RAY_RUNNING},
	})

	calls, _, _ := installer.stats()
	assert.Equal(t, 0, calls)
	assert.Empty(t, errorQueue)
}

// TestThreadedRayInstaller_SkipsNonWorkerInstance skips the install on a
// non-worker instance
// (Python asserts there; Go skips with a log).
func TestThreadedRayInstaller_SkipsNonWorkerInstance(t *testing.T) {
	headInstance := newRayInstallingInstance("i-head", "127.0.0.1")
	headInstance.NodeKind = proto.NodeKind_HEAD
	storage := newInstallerStorage(t, headInstance)
	installer := &fakeRayInstaller{}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))

	threaded.Notify([]*proto.InstanceUpdateEvent{newInstallingEvent("i-head")})
	threaded.Close()

	calls, _, _ := installer.stats()
	assert.Equal(t, 0, calls)
}

// TestThreadedRayInstaller_BoundedConcurrency the number of concurrent
// installs is bounded by MaxConcurrentInstalls: the second install does not
// start before the first one finishes.
func TestThreadedRayInstaller_BoundedConcurrency(t *testing.T) {
	storage := newInstallerStorage(t,
		newRayInstallingInstance("i-1", "127.0.0.2"),
		newRayInstallingInstance("i-2", "127.0.0.3"))
	installer := &blockingRayInstaller{
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(1)) // a concurrency cap of 1

	threaded.Notify([]*proto.InstanceUpdateEvent{
		newInstallingEvent("i-1"),
		newInstallingEvent("i-2"),
	})

	// The first install starts.
	first := <-installer.started
	// With a concurrency cap of 1: the second install must wait for the first
	// one to finish.
	select {
	case second := <-installer.started:
		t.Fatalf("second install %s started before the first one %s finished", second, first)
	case <-time.After(100 * time.Millisecond):
	}

	// After the release, the second install starts.
	close(installer.release)
	second := <-installer.started
	assert.NotEqual(t, first, second)
	threaded.Close()
}

// TestThreadedRayInstaller_InstanceNotInStorage no install happens when the
// instance of the event is not in the storage
// (or has already left the RAY_INSTALLING status).
func TestThreadedRayInstaller_InstanceNotInStorage(t *testing.T) {
	storage := newInstallerStorage(t)
	installer := &fakeRayInstaller{}
	errorQueue := make(chan error, 10)
	threaded := NewThreadedRayInstaller("127.0.0.1", storage, installer, errorQueue,
		fastInstallerOptions(10))

	threaded.Notify([]*proto.InstanceUpdateEvent{newInstallingEvent("missing")})
	threaded.Close()

	calls, _, _ := installer.stats()
	assert.Equal(t, 0, calls)
	assert.Empty(t, errorQueue)
}
