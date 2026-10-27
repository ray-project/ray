//go:build apikill

// Copyright 2025 The Ray Authors.
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

// Unit tests for the ActorHandle.Kill / KillWithNoRestart instance methods.
// Gated behind the "apikill" build tag because storeHandleLocked mutates
// package-global handle state (currentHandle) that the default api tests assume
//
//	is unset; run it in isolation with:
//
//	go test -tags apikill ./pkg/runtime/api/ -run 'TestActorHandleImplKill|TestPythonActorHandleKill'
package api

import (
	"context"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// Compile-time verification that the ActorHandle implementations satisfy the
// interface after the Kill / KillWithNoRestart methods were added.
var (
	_ ActorHandle = (*ActorHandleImpl[int])(nil)
	_ ActorHandle = (*PythonActorHandle)(nil)
)

// killRecordingSubmitter is a TaskSubmitter that records the actorID, force,
// and noRestart passed to KillActor so tests can observe the api-layer
// forwarding.
type killRecordingSubmitter struct {
	killedActorID ids.ActorID
	force         bool
	noRestart     bool
	killed        bool
}

func (m *killRecordingSubmitter) SubmitTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	return nil, nil
}

func (m *killRecordingSubmitter) CreateActor(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	options *submitter.ActorCreationOptions,
) (ids.ActorID, error) {
	return ids.NilActorID(), nil
}

func (m *killRecordingSubmitter) SubmitActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	return nil, nil
}

func (m *killRecordingSubmitter) GetActor(name string, namespace string) (submitter.ActorHandle, error) {
	return nil, nil
}

func (m *killRecordingSubmitter) GetActorHandle(actorID ids.ActorID) (submitter.ActorHandle, error) {
	return nil, nil
}

func (m *killRecordingSubmitter) KillActor(actorID ids.ActorID, force, noRestart bool) error {
	m.killed = true
	m.killedActorID = actorID
	m.force = force
	m.noRestart = noRestart
	return nil
}

func (m *killRecordingSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	return ids.NilPlacementGroupID(), nil
}

func (m *killRecordingSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	return nil
}

func (m *killRecordingSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	return nil
}

// killTestHandle is a contract.RuntimeHandle exposing a runtime whose task
// submitter is the recording submitter above.
type killTestHandle struct {
	rt contract.Runtime
}

func (h killTestHandle) IsRuntimeHandle()          {}
func (h killTestHandle) Runtime() contract.Runtime { return h.rt }

// killTestRuntime is a minimal contract.Runtime that returns the recording
// submitter from GetTaskSubmitter. Only the methods required by contract.Runtime
// are implemented; the rest return zero values.
type killTestRuntime struct {
	submitter submitter.TaskSubmitter
}

func (r killTestRuntime) Start() error                          { return nil }
func (r killTestRuntime) Shutdown() error                       { return nil }
func (r killTestRuntime) Run() error                            { return nil }
func (r killTestRuntime) IsInitialized() bool                   { return true }
func (r killTestRuntime) WorkerContext() contract.WorkerContext { return nil }
func (r killTestRuntime) GetRunMode() contract.RunMode          { return contract.RunModeCluster }
func (r killTestRuntime) IsLocalMode() bool                     { return false }
func (r killTestRuntime) WasCurrentActorRestarted() bool        { return false }
func (r killTestRuntime) GetAllNodeInfo() []contract.NodeInfo   { return nil }
func (r killTestRuntime) GetAllActorInfo() []contract.ActorInfo { return nil }
func (r killTestRuntime) GetAllActorInfoByFilter(jobID ids.JobID, state contract.ActorState) []contract.ActorInfo {
	return nil
}
func (r killTestRuntime) GetGpuIds() []string { return nil }
func (r killTestRuntime) GetResourceIds() map[string][]contract.ResourceValue {
	return nil
}
func (r killTestRuntime) GetCurrentActorHandle() submitter.ActorHandle { return nil }
func (r killTestRuntime) GetObjectStore() object.ObjectStore           { return nil }
func (r killTestRuntime) GetTaskSubmitter() submitter.TaskSubmitter    { return r.submitter }
func (r killTestRuntime) GetFunctionManager() function.Manager         { return nil }
func (r killTestRuntime) CreateRuntimeEnv() contract.RuntimeEnv        { return contract.RuntimeEnv{} }
func (r killTestRuntime) DeserializeRuntimeEnv(serializedRuntimeEnv string) (contract.RuntimeEnv, error) {
	return contract.UnmarshalRuntimeEnv(serializedRuntimeEnv)
}

// newKillTestRuntime installs a handle whose runtime returns the recording
// submitter and returns the submitter for assertion.
func newKillTestRuntime() *killRecordingSubmitter {
	ts := &killRecordingSubmitter{}
	storeHandleLocked(killTestHandle{rt: killTestRuntime{submitter: ts}})
	return ts
}

// TestActorHandleImplKill verifies that ActorHandleImpl.Kill() forwards to
// submitter.KillActor with noRestart=false.
func TestActorHandleImplKill(t *testing.T) {
	ts := newKillTestRuntime()
	defer clearHandle()

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
	handle := NewActorHandleImpl[int](actorID)

	if err := handle.Kill(); err != nil {
		t.Fatalf("Kill() returned error: %v", err)
	}
	if !ts.killed {
		t.Fatal("Kill() did not call submitter.KillActor")
	}
	if ts.killedActorID != actorID {
		t.Fatalf("Kill() killed actor %s, want %s", ts.killedActorID.Hex(), actorID.Hex())
	}
	if ts.noRestart {
		t.Fatal("Kill() should pass noRestart=false, got true")
	}
}

// TestActorHandleImplKillWithNoRestart verifies that KillWithNoRestart always
// forwards noRestart=true to submitter.KillActor.
func TestActorHandleImplKillWithNoRestart(t *testing.T) {
	ts := newKillTestRuntime()
	defer clearHandle()

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 2)
	handle := NewActorHandleImpl[int](actorID)

	if err := handle.KillWithNoRestart(); err != nil {
		t.Fatalf("KillWithNoRestart() returned error: %v", err)
	}
	if !ts.killed {
		t.Fatal("KillWithNoRestart() did not call submitter.KillActor")
	}
	if ts.killedActorID != actorID {
		t.Fatalf("KillWithNoRestart() killed actor %s, want %s", ts.killedActorID.Hex(), actorID.Hex())
	}
	if !ts.noRestart {
		t.Fatal("KillWithNoRestart() should pass noRestart=true, got false")
	}
}

// TestPythonActorHandleKillWithNoRestart verifies that PythonActorHandle
// KillWithNoRestart always forwards noRestart=true to submitter.KillActor.
func TestPythonActorHandleKillWithNoRestart(t *testing.T) {
	ts := newKillTestRuntime()
	defer clearHandle()

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 3)
	handle := &PythonActorHandle{
		baseActorHandle: baseActorHandle{
			nativeHandle: object.NewNativeActorHandle(actorID, object.LanguagePython),
		},
		moduleName: "mymodule",
		className:  "MyActor",
	}

	if err := handle.KillWithNoRestart(); err != nil {
		t.Fatalf("KillWithNoRestart() returned error: %v", err)
	}
	if !ts.killed {
		t.Fatal("KillWithNoRestart() did not call submitter.KillActor")
	}
	if ts.killedActorID != actorID {
		t.Fatalf("KillWithNoRestart() killed actor %s, want %s", ts.killedActorID.Hex(), actorID.Hex())
	}
	if !ts.noRestart {
		t.Fatal("KillWithNoRestart() should pass noRestart=true, got false")
	}
}

// TestKillActorConvenienceWrappersDefaultToForce pins that the historical
// force=true kill semantics survive the force parameter being added to
// KillActor: the shared handle convenience wrappers (Kill / KillWithNoRestart)
// forward force=true, while an explicit force=false call reaches the submitter
// verbatim. This is api-layer parameter forwarding only — the cluster-side
// force semantics live in the matrix documented on KillActor, not in this
// test.
func TestKillActorConvenienceWrappersDefaultToForce(t *testing.T) {
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 5)

	// handle.Kill() must keep the historical semantics: force=true (a hard
	// kill, matching the pre-force C++ bridge behavior), noRestart=false.
	ts := newKillTestRuntime()
	defer clearHandle()
	handle := NewActorHandleImpl[int](actorID)

	if err := handle.Kill(); err != nil {
		t.Fatalf("Kill() returned error: %v", err)
	}
	if !ts.force {
		t.Fatal("Kill() should pass force=true by default, got false")
	}
	if ts.noRestart {
		t.Fatal("Kill() should pass noRestart=false, got true")
	}

	// handle.KillWithNoRestart() must keep force=true as well.
	ts = newKillTestRuntime()
	defer clearHandle()
	handle = NewActorHandleImpl[int](actorID)

	if err := handle.KillWithNoRestart(); err != nil {
		t.Fatalf("KillWithNoRestart() returned error: %v", err)
	}
	if !ts.force {
		t.Fatal("KillWithNoRestart() should pass force=true by default, got false")
	}
	if !ts.noRestart {
		t.Fatal("KillWithNoRestart() should pass noRestart=true, got false")
	}

	// An explicit force=false call (the graceful kill parameter shape) must
	// reach the submitter verbatim.
	ts = newKillTestRuntime()
	defer clearHandle()

	if err := KillActor(handle, false, true); err != nil {
		t.Fatalf("KillActor(force=false) returned error: %v", err)
	}
	if ts.force {
		t.Fatal("KillActor(force=false) forwarded force=true to the submitter")
	}
	if !ts.noRestart {
		t.Fatal("KillActor(force=false) should forward noRestart=true, got false")
	}
}

// TestActorHandleAdapterKill verifies that the actorHandleAdapter returned by
// RuntimeContext.GetCurrentActorHandle forwards Kill/KillWithNoRestart to
// submitter.KillActor with the correct noRestart flag.
func TestActorHandleAdapterKill(t *testing.T) {
	ts := newKillTestRuntime()
	defer clearHandle()

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 4)
	inner := object.NewNativeActorHandle(actorID, object.LanguageGo)
	handle := &actorHandleAdapter{handle: inner}

	if err := handle.Kill(); err != nil {
		t.Fatalf("Kill() returned error: %v", err)
	}
	if !ts.killed {
		t.Fatal("Kill() did not call submitter.KillActor")
	}
	if ts.killedActorID != actorID {
		t.Fatalf("Kill() killed actor %s, want %s", ts.killedActorID.Hex(), actorID.Hex())
	}
	if ts.noRestart {
		t.Fatal("Kill() should pass noRestart=false, got true")
	}

	ts.killed = false
	ts.killedActorID = ids.NilActorID()
	ts.noRestart = false

	if err := handle.KillWithNoRestart(); err != nil {
		t.Fatalf("KillWithNoRestart() returned error: %v", err)
	}
	if !ts.killed {
		t.Fatal("KillWithNoRestart() did not call submitter.KillActor")
	}
	if ts.killedActorID != actorID {
		t.Fatalf("KillWithNoRestart() killed actor %s, want %s", ts.killedActorID.Hex(), actorID.Hex())
	}
	if !ts.noRestart {
		t.Fatal("KillWithNoRestart() should pass noRestart=true, got false")
	}
}
