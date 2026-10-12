//go:build apigetactorhandle

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

// Unit tests for api.GetActorHandle. Gated behind the "apigetactorhandle"
// build tag because storeHandleLocked mutates package-global handle state
// (currentHandle) that the default api tests assume is unset. Includes
// task_return_ref_test.go (via the api_get_actor_handle_test BUILD target) for
// the shared recordingRuntime/recordingHandle injection helpers.
package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// getActorHandleMockSubmitter is a minimal TaskSubmitter whose GetActorHandle returns a
// configurable handle/error, letting tests observe the api-layer wrapping
// without a live runtime.
type getActorHandleMockSubmitter struct {
	handle submitter.ActorHandle
	err    error
}

func (m *getActorHandleMockSubmitter) SubmitTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	return nil, nil
}

func (m *getActorHandleMockSubmitter) CreateActor(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	options *submitter.ActorCreationOptions,
) (ids.ActorID, error) {
	return ids.NilActorID(), nil
}

func (m *getActorHandleMockSubmitter) SubmitActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	return nil, nil
}

func (m *getActorHandleMockSubmitter) GetActor(name string, namespace string) (submitter.ActorHandle, error) {
	return nil, nil
}

func (m *getActorHandleMockSubmitter) GetActorHandle(actorID ids.ActorID) (submitter.ActorHandle, error) {
	return m.handle, m.err
}

func (m *getActorHandleMockSubmitter) KillActor(actorID ids.ActorID, noRestart bool) error {
	return nil
}

func (m *getActorHandleMockSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	return ids.NilPlacementGroupID(), nil
}

func (m *getActorHandleMockSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	return nil
}

func (m *getActorHandleMockSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	return nil
}

// getActorHandleMockRuntime wraps recordingRuntime to supply the mock submitter.
type getActorHandleMockRuntime struct {
	recordingRuntime
	submitter submitter.TaskSubmitter
}

func (r getActorHandleMockRuntime) GetTaskSubmitter() submitter.TaskSubmitter { return r.submitter }

// testActorHandle is a minimal submitter.ActorHandle used to exercise the
// non-*object.NativeActorHandle language-fallback path.
type testActorHandle struct {
	actorID ids.ActorID
}

func (h testActorHandle) ID() ids.ActorID { return h.actorID }

// newGetActorHandleMockRuntime installs a handle whose runtime returns the mock submitter.
func newGetActorHandleMockRuntime(ts submitter.TaskSubmitter) {
	storeHandleLocked(recordingHandle{rt: getActorHandleMockRuntime{submitter: ts}})
}

func TestGetActorHandleSuccess(t *testing.T) {
	id := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
	mock := &getActorHandleMockSubmitter{handle: object.NewNativeActorHandle(id, object.LanguagePython)}
	newGetActorHandleMockRuntime(mock)
	defer clearHandle()

	handle, err := GetActorHandle[string](id)
	if err != nil {
		t.Fatalf("GetActorHandle failed: %v", err)
	}
	if handle == nil {
		t.Fatal("GetActorHandle returned nil handle")
	}
	if handle.ID() != id {
		t.Fatalf("handle.ID() = %s, want %s", handle.ID().Hex(), id.Hex())
	}
	if handle.Language != object.LanguagePython {
		t.Fatalf("handle.Language = %s, want PYTHON (language must be preserved)", handle.Language)
	}
}

// errActorHandleNotFound is the sentinel the mock submitter returns so tests can
// verify that api.GetActorHandle wraps (rather than swallows) the submitter's
// error, matching it with errors.Is via the %w chain.
var errActorHandleNotFound = errors.New("actor handle not found")

func TestGetActorHandleNotFound(t *testing.T) {
	id := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 2)
	mock := &getActorHandleMockSubmitter{err: errActorHandleNotFound}
	newGetActorHandleMockRuntime(mock)
	defer clearHandle()

	_, err := GetActorHandle[int](id)
	if err == nil {
		t.Fatal("GetActorHandle on not-found: expected error, got nil")
	}
	if !errors.Is(err, errActorHandleNotFound) {
		t.Fatalf("GetActorHandle returned %v, want the submitter's error %v (wrapped via %%w)", err, errActorHandleNotFound)
	}
}

func TestGetActorHandleNilActorID(t *testing.T) {
	mock := &getActorHandleMockSubmitter{}
	newGetActorHandleMockRuntime(mock)
	defer clearHandle()

	_, err := GetActorHandle[int](ids.NilActorID())
	if err == nil {
		t.Fatal("GetActorHandle with nil actorID: expected error, got nil")
	}
}

func TestGetActorHandleLanguageFallback(t *testing.T) {
	id := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 4)
	// A handle that is NOT *object.NativeActorHandle (assertion-failure path).
	mock := &getActorHandleMockSubmitter{handle: testActorHandle{actorID: id}}
	newGetActorHandleMockRuntime(mock)
	defer clearHandle()

	handle, err := GetActorHandle[string](id)
	if err != nil {
		t.Fatalf("GetActorHandle fallback failed: %v", err)
	}
	if handle == nil {
		t.Fatal("GetActorHandle fallback returned nil handle")
	}
	if handle.ID() != id {
		t.Fatalf("handle.ID() = %s, want %s", handle.ID().Hex(), id.Hex())
	}
	if handle.Language != object.LanguageGo {
		t.Fatalf("handle.Language = %s, want GO (fallback to LanguageGo)", handle.Language)
	}
}
