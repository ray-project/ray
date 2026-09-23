// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// This file guards the task-return double-reference contract: task return
// ObjectRefs must NOT re-register a local reference on the Go side, because the
// C++ TaskManager::AddPendingTask already registered one
// (src/ray/core_worker/task_manager.cc: "the language bindings should set
// skip_adding_local_ref=True to avoid double referencing the object").
//
// Java follows this contract: AbstractRayRuntime wraps task return IDs with
// skipAddingLocalRef=true (AbstractRayRuntime.java:354,385). Go's
// createObjectRefWithFinalizer historically called objectStore.AddLocalReference
// unconditionally, so every task return ObjectRef incremented the C++
// local_ref_count twice while the finalizer and Release() decremented only
// once. The leaked reference pinned the object in the memory store / plasma
// indefinitely, which in turn kept stale objects reachable and contributed to
// the finalizer/CGO churn behind the reported driver crash.
//
// This test encodes the contract directly: after createObjectRefWithFinalizer,
// the object store must NOT have seen an AddLocalReference for the return ID.
// Note that the Go side must still call RemoveLocalReference on Release()
// (skipAddingLocalRef stays false) to balance the single C++ registration;
// unlike Java, Go's skipAddingLocalRef gates both add and remove.

// recordingObjectStore records whether AddLocalReference was called for each
// object ID. It is a minimal ObjectStore that lets the test observe the Go-side
// reference registration decisions without a live C++ CoreWorker.
type recordingObjectStore struct {
	mu      sync.Mutex
	added   map[string]int
	removed map[string]int
}

func newRecordingObjectStore() *recordingObjectStore {
	return &recordingObjectStore{
		added:   make(map[string]int),
		removed: make(map[string]int),
	}
}

func (s *recordingObjectStore) addCount(id ids.ObjectID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.added[id.Hex()]
}

func (s *recordingObjectStore) removedCount(id ids.ObjectID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removed[id.Hex()]
}

func (s *recordingObjectStore) PutRaw(obj *object.NativeRayObject) (*ids.ObjectID, error) {
	return nil, nil
}
func (s *recordingObjectStore) PutRawWithOwner(obj *object.NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error) {
	return nil, nil
}
func (s *recordingObjectStore) PutRawWithID(obj *object.NativeRayObject, objectID *ids.ObjectID) error {
	return nil
}
func (s *recordingObjectStore) GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return nil, nil
}
func (s *recordingObjectStore) GetRawWithContext(ctx context.Context, objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return nil, nil
}
func (s *recordingObjectStore) Wait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error) {
	return nil, nil
}
func (s *recordingObjectStore) WaitWithOptions(opts object.WaitOptions) ([]bool, error) {
	return nil, nil
}
func (s *recordingObjectStore) Delete(objectIDs []*ids.ObjectID, localOnly bool) error {
	return nil
}
func (s *recordingObjectStore) AddLocalReference(objectID *ids.ObjectID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.added[objectID.Hex()]++
	return nil
}
func (s *recordingObjectStore) RemoveLocalReference(objectID *ids.ObjectID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed[objectID.Hex()]++
	return nil
}
func (s *recordingObjectStore) GetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error) {
	return nil, nil
}
func (s *recordingObjectStore) RegisterOwnershipInfoAndResolveFuture(objectID, outerObjectID *ids.ObjectID, ownerAddress []byte) error {
	return nil
}
func (s *recordingObjectStore) GetOwnerAddress(objectID *ids.ObjectID) ([]byte, error) {
	return nil, nil
}
func (s *recordingObjectStore) GetAllReferenceCounts() (map[ids.ObjectID][2]int64, error) {
	return nil, nil
}
func (s *recordingObjectStore) CreateOwned(metadata *object.NativeRayObject, dataSize int) (*ids.ObjectID, uintptr, uint64, error) {
	return nil, 0, 0, nil
}
func (s *recordingObjectStore) SealOwned(objectID *ids.ObjectID, handle uint64) error {
	return nil
}
func (s *recordingObjectStore) CreateExisting(metadata *object.NativeRayObject, dataSize int, objectID *ids.ObjectID) (uintptr, uint64, error) {
	return 0, 0, nil
}
func (s *recordingObjectStore) SealExisting(objectID *ids.ObjectID, handle uint64) error {
	return nil
}

// recordingRuntime is a minimal contract.Runtime whose object store is the
// recording store above, so createObjectRefWithFinalizer can run against it.
type recordingRuntime struct {
	store object.ObjectStore
}

func (r recordingRuntime) Start() error                                 { return nil }
func (r recordingRuntime) Shutdown() error                              { return nil }
func (r recordingRuntime) Run() error                                   { return nil }
func (r recordingRuntime) IsInitialized() bool                          { return true }
func (r recordingRuntime) WorkerContext() contract.WorkerContext        { return nil }
func (r recordingRuntime) GetRunMode() contract.RunMode                 { return contract.RunModeCluster }
func (r recordingRuntime) IsLocalMode() bool                            { return false }
func (r recordingRuntime) WasCurrentActorRestarted() bool               { return false }
func (r recordingRuntime) GetAllNodeInfo() []contract.NodeInfo          { return nil }
func (r recordingRuntime) GetAllActorInfo() []contract.ActorInfo        { return nil }
func (r recordingRuntime) GetGpuIds() []string                          { return nil }
func (r recordingRuntime) GetCurrentActorHandle() submitter.ActorHandle { return nil }
func (r recordingRuntime) GetObjectStore() object.ObjectStore           { return r.store }
func (r recordingRuntime) GetTaskSubmitter() submitter.TaskSubmitter    { return nil }
func (r recordingRuntime) GetFunctionManager() function.Manager         { return nil }

// recordingHandle is a contract.RuntimeHandle exposing the recording runtime.
type recordingHandle struct {
	rt contract.Runtime
}

func (h recordingHandle) IsRuntimeHandle()          {}
func (h recordingHandle) Runtime() contract.Runtime { return h.rt }

// newRecordingHandle installs a recording handle as the current runtime and
// returns a handle bound to a fresh recording store, for tests that must
// observe Add/RemoveLocalReference calls.
func newRecordingHandle() (*recordingObjectStore, recordingHandle) {
	store := newRecordingObjectStore()
	handle := recordingHandle{rt: recordingRuntime{store: store}}
	setHandle(handle)
	// setHandle does not clear the shutdown flag left set by a previous
	// clearHandle; the release path under test is only reachable while shutdown
	// is pending.
	shutdownComplete.Store(false)
	return store, handle
}

// TestTaskReturnRefMustNotDoubleAddLocalReference is the regression guard for
// the task-return double-reference bug.
//
// Contract: createObjectRefWithFinalizer must NOT call AddLocalReference for a
// task return ID. The C++ side already registered the local reference when the
// task was submitted, so an extra Go-side registration over-increments
// local_ref_count and the reference is never fully released, pinning the object
// in the object store.
func TestTaskReturnRefMustNotDoubleAddLocalReference(t *testing.T) {
	store, _ := newRecordingHandle()
	defer clearHandle()

	returnID := ids.ObjectIDFromIndex(ids.TaskIDForDriverTask(ids.NilJobID()), 1)
	ref, err := createObjectRefWithFinalizer[int](returnID, "int")
	if err != nil {
		t.Fatalf("createObjectRefWithFinalizer failed: %v", err)
	}
	if ref == nil {
		t.Fatal("expected a non-nil ObjectRef")
	}
	if ref.skipAddingLocalRef {
		t.Fatal("task return ObjectRef should NOT require the Go side to add a local reference (C++ already did)")
	}

	// The double-reference bug: the Go side adds a local reference for a return
	// ID whose local reference was already registered by C++ AddPendingTask.
	if got := store.addCount(returnID); got != 0 {
		t.Fatalf("BUG REPRODUCED: createObjectRefWithFinalizer added %d local reference(s) for task return ID %s; "+
			"C++ AddPendingTask already registered one, so this double-counts and leaks the object",
			got, returnID.Hex())
	}
}

// TestTaskReturnRefSingleRemove verifies the symmetric half of the contract:
// releasing a task return ObjectRef must remove exactly one reference, and the
// finalizer path (releaseObjectRef) must not be skipped. It guards against a
// regression where skipAddingLocalRef silently disables cleanup.
func TestTaskReturnRefSingleRemove(t *testing.T) {
	store, _ := newRecordingHandle()
	defer clearHandle()

	returnID := ids.ObjectIDFromIndex(ids.TaskIDForDriverTask(ids.NilJobID()), 2)
	ref, err := createObjectRefWithFinalizer[int](returnID, "int")
	if err != nil {
		t.Fatalf("createObjectRefWithFinalizer failed: %v", err)
	}

	ref.Release()
	if got := store.addCount(returnID); got != 0 {
		t.Fatalf("task return ObjectRef added %d local reference(s); expected 0 (C++ already added it)", got)
	}
	if got := store.removedCount(returnID); got != 1 {
		t.Fatalf("Release() removed %d local reference(s); expected exactly 1", got)
	}
}
