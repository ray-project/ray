//go:build apifree

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

// Unit tests for api.Free. These use a mock ObjectStore (recordingObjectStore)
// because go/pkg must not depend on go/internal (dependency inversion): the
// real local-mode release semantics are covered in
// go/internal/runtime/localstore/local_mode_object_store_test.go.
//
// Gated behind the "apifree" build tag because storeHandleLocked mutates
// package-global handle state (currentHandle) that the default api tests
// assume is unset (see the apilocalinit/apiinternal/apireinit precedents).
package api

import (
	"errors"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
)

// newFreeRecordingHandle installs a handle whose object store records Deletes.
// recordingObjectStore is defined in task_return_ref_test.go (which the
// api_free_test target compiles alongside this file).
func newFreeRecordingHandle() *recordingObjectStore {
	store := newRecordingObjectStore()
	handle := recordingHandle{rt: recordingRuntime{store: store}}
	storeHandleLocked(handle)
	return store
}

func TestFreeExtractsObjectIDs(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	id1 := ids.NewObjectID()
	id2 := ids.NewObjectID()
	ref1 := NewObjectRef[int](id1, "int", false)
	ref2 := NewObjectRef[int](id2, "int", false)

	if err := Free([]*ObjectRef[int]{ref1, ref2}, true); err != nil {
		t.Fatalf("Free failed: %v", err)
	}
	if got := store.deleteCallCount(); got != 1 {
		t.Fatalf("Delete called %d time(s); want 1", got)
	}
	call := store.lastDeleteCall()
	if !call.localOnly {
		t.Fatalf("localOnly = %v, want true", call.localOnly)
	}
	if len(call.objectIDs) != 2 || call.objectIDs[0] != id1 || call.objectIDs[1] != id2 {
		t.Fatalf("Delete got objectIDs %v, want [%s %s]", call.objectIDs, id1.Hex(), id2.Hex())
	}
}

func TestFreeForwardsLocalOnlyFalse(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	id := ids.NewObjectID()
	if err := Free([]*ObjectRef[string]{NewObjectRef[string](id, "string", false)}, false); err != nil {
		t.Fatalf("Free failed: %v", err)
	}
	call := store.lastDeleteCall()
	if call.localOnly {
		t.Fatalf("localOnly = true, want false")
	}
}

func TestFreeSkipsNilRefs(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	id1 := ids.NewObjectID()
	if err := Free([]*ObjectRef[int]{nil, NewObjectRef[int](id1, "int", false)}, true); err != nil {
		t.Fatalf("Free failed: %v", err)
	}
	call := store.lastDeleteCall()
	if len(call.objectIDs) != 1 || call.objectIDs[0] != id1 {
		t.Fatalf("Delete got objectIDs %v, want [%s]", call.objectIDs, id1.Hex())
	}
}

func TestFreeEmptyRefs(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	if err := Free([]*ObjectRef[int]{}, true); err != nil {
		t.Fatalf("Free on empty refs failed: %v", err)
	}
	if store.deleteCallCount() != 0 {
		t.Fatalf("Delete called %d time(s) for empty refs; want 0", store.deleteCallCount())
	}
}

func TestFreeGenericTypes(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	id1 := ids.NewObjectID()
	id2 := ids.NewObjectID()
	if err := Free([]*ObjectRef[string]{NewObjectRef[string](id1, "string", false)}, true); err != nil {
		t.Fatalf("Free[string] failed: %v", err)
	}
	if err := Free([]*ObjectRef[struct{ A int }]{NewObjectRef[struct{ A int }](id2, "struct", false)}, false); err != nil {
		t.Fatalf("Free[struct] failed: %v", err)
	}
	if store.deleteCallCount() != 2 {
		t.Fatalf("Delete called %d time(s); want 2", store.deleteCallCount())
	}
}

func TestFreeForwardsDeleteError(t *testing.T) {
	store := newFreeRecordingHandle()
	defer clearHandle()

	sentinel := errors.New("delete failed")
	store.mu.Lock()
	store.deleteErr = sentinel
	store.mu.Unlock()

	id := ids.NewObjectID()
	err := Free([]*ObjectRef[int]{NewObjectRef[int](id, "int", false)}, true)
	if err == nil {
		t.Fatal("Free on store Delete error: expected error, got nil")
	}
	// Free wraps the store error with context ("failed to free objects"); the
	// original error must remain reachable via errors.Is.
	if !errors.Is(err, sentinel) {
		t.Fatalf("Free returned %v, want the store's Delete error %v", err, sentinel)
	}
}
