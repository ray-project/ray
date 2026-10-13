//go:build apiresourceids

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

// Unit tests for api.GetAvailableResourceIds and RuntimeContext.GetGpuIds.
// Uses a mock contract.Runtime because go/pkg must not depend on go/internal
// (dependency inversion). Gated behind the "apiresourceids" build tag because
// storeHandleLocked mutates package-global handle state.
package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

func newResourceIdsRecordingHandle() recordingHandle {
	handle := recordingHandle{rt: recordingRuntime{store: newRecordingObjectStore()}}
	storeHandleLocked(handle)
	return handle
}

func TestGetAvailableResourceIdsConversion(t *testing.T) {
	_ = newResourceIdsRecordingHandle()
	defer clearHandle()

	got, err := GetAvailableResourceIds()
	if err != nil {
		t.Fatalf("GetAvailableResourceIds failed: %v", err)
	}
	if _, ok := got["GPU"]; !ok {
		t.Fatalf("GetAvailableResourceIds missing GPU: %v", got)
	}
	if len(got["GPU"]) != 1 || got["GPU"][0].ResourceID != 0 {
		t.Fatalf("GetAvailableResourceIds GPU values = %v, want [{0 1}]", got["GPU"])
	}
}

func TestRuntimeContextGetGpuIdsNilRuntime(t *testing.T) {
	// Install a handle whose Runtime() is nil so internal() reports the
	// runtime is unavailable, deterministically independent of any handle left
	// by earlier tests (atomic.Value cannot store nil, so clearHandle cannot
	// truly remove a handle).
	storeHandleLocked(recordingHandle{rt: nil})
	defer clearHandle()
	ctx := NewRuntimeContext(ids.NilJobID(), ids.NilTaskID(), ids.NilActorID(), "", "", ids.NilNodeID(), false)
	if got := ctx.GetGpuIds(); len(got) != 0 {
		t.Fatalf("GetGpuIds with nil runtime = %v, want empty", got)
	}
}

func TestRuntimeContextGetGpuIdsConversion(t *testing.T) {
	storeHandleLocked(recordingHandle{rt: mockGpuRuntime{ids: []string{"0", "abc", "2"}}})
	defer clearHandle()
	ctx := NewRuntimeContext(ids.NilJobID(), ids.NilTaskID(), ids.NilActorID(), "", "", ids.NilNodeID(), false)
	got := ctx.GetGpuIds()
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("GetGpuIds = %v, want [0 2]", got)
	}
}

// mockGpuRuntime implements contract.Runtime with a fixed GetGpuIds result.
type mockGpuRuntime struct {
	recordingRuntime
	ids []string
}

func (m mockGpuRuntime) GetGpuIds() []string { return m.ids }
func (m mockGpuRuntime) GetResourceIds() map[string][]contract.ResourceValue {
	return map[string][]contract.ResourceValue{}
}
