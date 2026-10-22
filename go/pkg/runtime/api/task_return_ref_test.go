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
	"github.com/vmihailenco/msgpack/v5"
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

	// putRawCalls and putRawOwnerCalls count the Put paths taken so a test can
	// pin which one api.Put routed to. lastOwner records the actor ID forwarded
	// to PutRawWithOwner so the exact owner can be asserted.
	putRawCalls      int
	putRawOwnerCalls int
	lastOwner        *ids.ActorID

	deleteCalls []deleteCall
	// deleteErr, when non-nil, is returned by Delete to simulate a store failure.
	deleteErr error
}

// deleteCall records a single ObjectStore.Delete invocation.
type deleteCall struct {
	objectIDs []ids.ObjectID
	localOnly bool
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

// deleteCallCount returns the number of Delete invocations recorded.
func (s *recordingObjectStore) deleteCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.deleteCalls)
}

// lastDeleteCall returns the most recent recorded Delete invocation.
func (s *recordingObjectStore) lastDeleteCall() deleteCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteCalls[len(s.deleteCalls)-1]
}

func (s *recordingObjectStore) PutRaw(obj *object.NativeRayObject) (*ids.ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putRawCalls++
	oid := ids.NewObjectID()
	return &oid, nil
}
func (s *recordingObjectStore) PutRawWithOwner(obj *object.NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putRawOwnerCalls++
	// Copy the actor ID so the recorded value survives the caller's pointer.
	if ownerActorID != nil {
		id := *ownerActorID
		s.lastOwner = &id
	}
	oid := ids.NewObjectID()
	return &oid, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	idsCopy := make([]ids.ObjectID, 0, len(objectIDs))
	for _, oid := range objectIDs {
		if oid != nil {
			idsCopy = append(idsCopy, *oid)
		}
	}
	s.deleteCalls = append(s.deleteCalls, deleteCall{objectIDs: idsCopy, localOnly: localOnly})
	return s.deleteErr
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
func (r recordingRuntime) GetAllActorInfoByFilter(jobID ids.JobID, state contract.ActorState) []contract.ActorInfo {
	return nil
}
func (r recordingRuntime) GetGpuIds() []string { return nil }
func (r recordingRuntime) GetResourceIds() map[string][]contract.ResourceValue {
	return map[string][]contract.ResourceValue{
		"GPU": {{ResourceID: 0, Capacity: 1.0}},
		"CPU": {{ResourceID: 3, Capacity: 1.0}},
	}
}
func (r recordingRuntime) GetCurrentActorHandle() submitter.ActorHandle { return nil }
func (r recordingRuntime) GetObjectStore() object.ObjectStore           { return r.store }
func (r recordingRuntime) GetTaskSubmitter() submitter.TaskSubmitter    { return nil }
func (r recordingRuntime) GetFunctionManager() function.Manager         { return nil }
func (r recordingRuntime) CreateRuntimeEnv() contract.RuntimeEnv { return contract.RuntimeEnv{} }
func (r recordingRuntime) DeserializeRuntimeEnv(serializedRuntimeEnv string) (contract.RuntimeEnv, error) {
	return contract.UnmarshalRuntimeEnv(serializedRuntimeEnv)
}

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
	storeHandleLocked(handle)
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

// TestPutForwardsOwnerActorID verifies that api.Put rejects a non-nil owner
// with the owned-put removal error (the experimental `_owner` argument to
// ray.put was removed upstream in Ray 2.56, ray-project/ray#63520, and this
// bridge does not forward an owner address to the C++ core worker), and that a
// nil owner falls back to the plain PutRaw path (the current worker owns the
// object).
func TestPutForwardsOwnerActorID(t *testing.T) {
	store, _ := newRecordingHandle()
	defer clearHandle()

	// Put serializes via the global serializer, which is normally registered by
	// the runtime. Install a minimal serializer for this test and restore the
	// nil default afterwards so sibling tests are unaffected.
	object.SetSerializer(&putTestSerializer{})
	defer object.SetSerializer(nil)

	ownerID, err := ids.ActorIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("ActorIDFromHex failed: %v", err)
	}
	owner := NewActorHandleImpl[int](ownerID)

	// Owner present: must be rejected with the removal error, and the store
	// must not see any put (reject before serializing or storing).
	if _, err := Put(42, owner); err == nil {
		t.Fatal("Put with owner: expected an error, got nil")
	} else if err.Error() != errOwnedPutRemoved.Error() {
		t.Fatalf("Put with owner: got error %q, want %q", err, errOwnedPutRemoved)
	}
	if store.putRawCalls != 0 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Put with owner: putRawCalls=%d putRawOwnerCalls=%d; want 0/0 (rejected before the store)",
			store.putRawCalls, store.putRawOwnerCalls)
	}

	// Nil owner: must route through the plain PutRaw path.
	if _, err := Put(43, nil); err != nil {
		t.Fatalf("Put without owner failed: %v", err)
	}
	if store.putRawCalls != 1 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Put without owner: putRawCalls=%d putRawOwnerCalls=%d; want 1/0",
			store.putRawCalls, store.putRawOwnerCalls)
	}
}

// TestPutTypedNilOwnerBehavesLikeNoOwner verifies that a nil *ActorHandleImpl
// passed as the owner does not panic. At the package level a typed nil is
// indistinguishable from "no owner" (a nil concrete pointer), so Put routes it
// through the plain PutRaw path exactly like a nil owner; the owner rejection
// that closes the typed-nil panic path lives in the interface entry point
// Ray.PutWithOwner, covered by TestRayPutWithOwnerRejectsOwner.
func TestPutTypedNilOwnerBehavesLikeNoOwner(t *testing.T) {
	store, _ := newRecordingHandle()
	defer clearHandle()

	object.SetSerializer(&putTestSerializer{})
	defer object.SetSerializer(nil)

	var owner *ActorHandleImpl[int]
	if _, err := Put(44, owner); err != nil {
		t.Fatalf("Put with a typed-nil owner failed: %v", err)
	}
	if store.putRawCalls != 1 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Put with typed-nil owner: putRawCalls=%d putRawOwnerCalls=%d; want 1/0",
			store.putRawCalls, store.putRawOwnerCalls)
	}
}

// TestRayPutWithOwnerRejectsOwner pins the public Ray.PutWithOwner entry point:
// a non-nil owner is rejected with the removal error, a non-nil owner holding a
// nil *ActorHandleImpl (a typed nil stored in the ActorHandle interface) is
// also rejected instead of panicking, and a nil owner routes to the plain
// PutRaw path.
func TestRayPutWithOwnerRejectsOwner(t *testing.T) {
	store, _ := newRecordingHandle()
	defer clearHandle()

	object.SetSerializer(&putTestSerializer{})
	defer object.SetSerializer(nil)

	ownerID, err := ids.ActorIDFromHex("fedcba9876543210fedcba9876543210")
	if err != nil {
		t.Fatalf("ActorIDFromHex failed: %v", err)
	}
	owner := NewActorHandleImpl[int](ownerID)

	ray := &Ray{}
	if _, err := ray.PutWithOwner(7, owner); err == nil {
		t.Fatal("Ray.PutWithOwner with owner: expected an error, got nil")
	} else if err.Error() != errOwnedPutRemoved.Error() {
		t.Fatalf("Ray.PutWithOwner with owner: got error %q, want %q", err, errOwnedPutRemoved)
	}
	if store.putRawCalls != 0 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Ray.PutWithOwner with owner: putRawCalls=%d putRawOwnerCalls=%d; want 0/0 (rejected before the store)",
			store.putRawCalls, store.putRawOwnerCalls)
	}

	// Typed-nil in the ActorHandle interface: the old code panicked here
	// (NewActorHandleImpl(nil ID) is fine, but the type assertion on a nil
	// *ActorHandleImpl must not panic); the rejection path must return the same
	// removal error.
	var typedNil *ActorHandleImpl[interface{}]
	var typedNilOwner ActorHandle = typedNil
	if _, err := ray.PutWithOwner(9, typedNilOwner); err == nil {
		t.Fatal("Ray.PutWithOwner with typed-nil owner: expected an error, got nil")
	} else if err.Error() != errOwnedPutRemoved.Error() {
		t.Fatalf("Ray.PutWithOwner with typed-nil owner: got error %q, want %q", err, errOwnedPutRemoved)
	}
	if store.putRawCalls != 0 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Ray.PutWithOwner with typed-nil owner: putRawCalls=%d putRawOwnerCalls=%d; want 0/0 (rejected before the store)",
			store.putRawCalls, store.putRawOwnerCalls)
	}

	// Nil owner: must route through the plain PutRaw path.
	if _, err := ray.PutWithOwner(8, nil); err != nil {
		t.Fatalf("Ray.PutWithOwner without owner failed: %v", err)
	}
	if store.putRawCalls != 1 || store.putRawOwnerCalls != 0 {
		t.Fatalf("Ray.PutWithOwner without owner: putRawCalls=%d putRawOwnerCalls=%d; want 1/0",
			store.putRawCalls, store.putRawOwnerCalls)
	}
}

// putTestSerializer is a minimal object.Serializer for tests that drive api.Put
// without a running runtime. It encodes the value as its own msgpack bytes so
// Put can build a NativeRayObject without touching the real serializer package.
type putTestSerializer struct{}

func (s *putTestSerializer) Serialize(obj interface{}) (*object.NativeRayObject, error) {
	// Encode with vmihailenco/msgpack so the payload is a valid RAW object; this
	// test only checks routing, so a trivial int encoding is fine.
	data, err := msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return &object.NativeRayObject{Data: data, Metadata: []byte("RAW")}, nil
}
func (s *putTestSerializer) Deserialize(nativeObj *object.NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	return nil, nil
}
func (s *putTestSerializer) DeserializeTo(nativeObj *object.NativeRayObject, target interface{}) error {
	return nil
}
func (s *putTestSerializer) AddContainedObjectID(objectID ids.ObjectID)    {}
func (s *putTestSerializer) GetAndClearContainedObjectIDs() []ids.ObjectID { return nil }
func (s *putTestSerializer) SetOuterObjectID(objectID ids.ObjectID)        {}
func (s *putTestSerializer) GetOuterObjectID() ids.ObjectID                { return ids.NilObjectID() }
func (s *putTestSerializer) ResetOuterObjectID()                           {}
func (s *putTestSerializer) EstimateBufferSize(obj interface{}) int        { return 0 }
func (s *putTestSerializer) GetBuffer(size int) []byte                     { return nil }
func (s *putTestSerializer) PutBuffer(buf []byte)                          {}
func (s *putTestSerializer) IsCrossLanguageType(obj interface{}) bool      { return false }
