//go:build apirtcontext

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

// Unit tests for RuntimeContext delegation and contract-to-api mapping. Uses a
// mock contract.Runtime (no go/internal dependency, preserving dependency
// inversion). Gated behind the "apirtcontext" build tag like the other api
// runtime tests because storeHandleLocked mutates package-global handle state.
package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

type mockRTContextRuntime struct {
	recordingRuntime
	restarted bool
	nodes     []contract.NodeInfo
	actors    []contract.ActorInfo
	actorHdl  submitter.ActorHandle
	gpuIDs    []string
}

// rtContextTestHandle is a minimal submitter.ActorHandle for testing
// GetCurrentActorHandle delegation.
type rtContextTestHandle struct {
	actorID ids.ActorID
}

func (h *rtContextTestHandle) ID() ids.ActorID { return h.actorID }

func (m mockRTContextRuntime) WasCurrentActorRestarted() bool { return m.restarted }
func (m mockRTContextRuntime) GetAllNodeInfo() []contract.NodeInfo {
	return m.nodes
}
func (m mockRTContextRuntime) GetAllActorInfo() []contract.ActorInfo {
	return m.actors
}
func (m mockRTContextRuntime) GetCurrentActorHandle() submitter.ActorHandle { return m.actorHdl }
func (m mockRTContextRuntime) GetGpuIds() []string                          { return m.gpuIDs }

// newRuntimeContextWithMock installs the given mock runtime as the current
// runtime via the global handle (so RuntimeContext delegation methods, which
// route through internal(), resolve it) and returns a RuntimeContext snapshot.
func newRuntimeContextWithMock(m contract.Runtime) *RuntimeContext {
	storeHandleLocked(recordingHandle{rt: m})
	return NewRuntimeContext(ids.NilJobID(), ids.NilTaskID(), ids.NilActorID(), "", "", ids.NilNodeID(), false)
}

// newRuntimeContextNoRuntime installs a handle whose Runtime() is nil, so
// internal() reports "runtime instance not available" and delegation methods
// return their safe zero value. This exercises the uninitialized path
// deterministically, independent of any handle left behind by earlier tests
// (atomic.Value cannot store nil, so clearHandle cannot truly remove a handle).
func newRuntimeContextNoRuntime() *RuntimeContext {
	storeHandleLocked(recordingHandle{rt: nil})
	return NewRuntimeContext(ids.NilJobID(), ids.NilTaskID(), ids.NilActorID(), "", "", ids.NilNodeID(), false)
}

func TestRuntimeContextWasCurrentActorRestartedDelegates(t *testing.T) {
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{restarted: true})
	defer clearHandle()
	if !ctx.WasCurrentActorRestarted() {
		t.Fatal("WasCurrentActorRestarted = false, want true")
	}
}

func TestRuntimeContextWasCurrentActorRestartedNilRuntime(t *testing.T) {
	ctx := newRuntimeContextNoRuntime()
	defer clearHandle()
	if ctx.WasCurrentActorRestarted() {
		t.Fatal("WasCurrentActorRestarted with nil runtime = true, want false")
	}
}

func TestRuntimeContextGetAllNodeInfoMapping(t *testing.T) {
	nodes := []contract.NodeInfo{{NodeID: ids.NilNodeID(), NodeManagerAddress: "1.2.3.4", State: contract.NodeStateAlive}}
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{nodes: nodes})
	defer clearHandle()
	got := ctx.GetAllNodeInfo()
	if len(got) != 1 || got[0].NodeAddress != "1.2.3.4" || !got[0].IsAlive {
		t.Fatalf("GetAllNodeInfo = %+v, want mapped alive node", got)
	}
}

func TestRuntimeContextGetAllNodeInfoNilRuntime(t *testing.T) {
	ctx := newRuntimeContextNoRuntime()
	defer clearHandle()
	if got := ctx.GetAllNodeInfo(); len(got) != 0 {
		t.Fatalf("GetAllNodeInfo with nil runtime = %v, want empty", got)
	}
}

func TestRuntimeContextGetAllActorInfoMapping(t *testing.T) {
	actors := []contract.ActorInfo{{ActorID: ids.NilActorID(), Name: "a", State: contract.ActorStateAlive}}
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{actors: actors})
	defer clearHandle()
	got := ctx.GetAllActorInfo()
	if len(got) != 1 || got[0].Name != "a" || got[0].State != ActorStateAlive {
		t.Fatalf("GetAllActorInfo = %+v, want mapped actor", got)
	}
}

func TestRuntimeContextGetAllActorInfoNilRuntime(t *testing.T) {
	ctx := newRuntimeContextNoRuntime()
	defer clearHandle()
	if got := ctx.GetAllActorInfo(); len(got) != 0 {
		t.Fatalf("GetAllActorInfo with nil runtime = %v, want empty", got)
	}
}

func TestRuntimeContextGetCurrentActorHandleDelegates(t *testing.T) {
	want := &rtContextTestHandle{actorID: ids.NilActorID()}
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{actorHdl: want})
	defer clearHandle()
	got := ctx.GetCurrentActorHandle()
	if got == nil {
		t.Fatal("GetCurrentActorHandle returned nil, want non-nil delegated handle")
	}
	if got.ID() != want.ID() {
		t.Fatalf("GetCurrentActorHandle ID = %v, want delegated handle ID %v", got.ID(), want.ID())
	}
}

func TestRuntimeContextGetCurrentActorHandleNilRuntime(t *testing.T) {
	ctx := newRuntimeContextNoRuntime()
	defer clearHandle()
	if ctx.GetCurrentActorHandle() != nil {
		t.Fatal("GetCurrentActorHandle with nil runtime != nil")
	}
}

func TestRuntimeContextGetGpuIdsMapping(t *testing.T) {
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{gpuIDs: []string{"0", "2"}})
	defer clearHandle()
	got := ctx.GetGpuIds()
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("GetGpuIds = %v, want [0 2]", got)
	}
}

func TestRuntimeContextGetGpuIdsSkipsNonNumeric(t *testing.T) {
	ctx := newRuntimeContextWithMock(&mockRTContextRuntime{gpuIDs: []string{"0", "not-a-number"}})
	defer clearHandle()
	got := ctx.GetGpuIds()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("GetGpuIds = %v, want [0] (non-numeric skipped)", got)
	}
}

func TestRuntimeContextGetGpuIdsNilRuntime(t *testing.T) {
	ctx := newRuntimeContextNoRuntime()
	defer clearHandle()
	if got := ctx.GetGpuIds(); len(got) != 0 {
		t.Fatalf("GetGpuIds with nil runtime = %v, want empty", got)
	}
}
