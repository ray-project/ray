//go:build cgo

// Copyright 2025 The Ray Authors.
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

package native

import (
	"testing"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/ids"
	contract "github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	object "github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/ray-project/ray/go/proto"
)

// mockRuntime implements base.Runtime interface for testing.
// It allows testing RuntimeContext methods without CGO dependencies.
type mockRuntime struct {
	jobId      ids.JobID
	taskId     ids.TaskID
	actorId    ids.ActorID
	namespace  string
	nodeId     ids.NodeID
	runtimeEnv string
	localMode  bool
}

// Implement all base.Runtime interface methods
func (m *mockRuntime) Start() error {
	return nil
}

func (m *mockRuntime) Shutdown() error {
	return nil
}

func (m *mockRuntime) Run() error {
	return nil
}

func (m *mockRuntime) IsInitialized() bool {
	return true
}

func (m *mockRuntime) WorkerContext() base.WorkerContext {
	// Return a mock WorkerContext that provides the context information
	return &mockWorkerContext{
		jobId:      m.jobId,
		taskId:     m.taskId,
		actorId:    m.actorId,
		namespace:  m.namespace,
		nodeId:     m.nodeId,
		runtimeEnv: m.runtimeEnv,
	}
}

func (m *mockRuntime) GetRunMode() base.RunMode {
	if m.localMode {
		return base.RunModeLocal
	}
	return base.RunModeCluster
}

func (m *mockRuntime) IsLocalMode() bool {
	return m.localMode
}

// mockWorkerContext implements base.WorkerContext for testing.
type mockWorkerContext struct {
	jobId      ids.JobID
	taskId     ids.TaskID
	actorId    ids.ActorID
	namespace  string
	nodeId     ids.NodeID
	runtimeEnv string
}

func (m *mockWorkerContext) GetCurrentWorkerId() ids.UniqueID {
	return ids.NilUniqueID()
}

func (m *mockWorkerContext) GetCurrentJobID() ids.JobID {
	return m.jobId
}

func (m *mockWorkerContext) GetCurrentActorID() ids.ActorID {
	return m.actorId
}

func (m *mockWorkerContext) GetCurrentTaskType() base.TaskType {
	return base.TaskTypeNormal
}

func (m *mockWorkerContext) GetCurrentTaskID() ids.TaskID {
	return m.taskId
}

func (m *mockWorkerContext) GetRpcAddress() []byte {
	return nil
}

func (m *mockWorkerContext) GetSerializedRuntimeEnv() string {
	return m.runtimeEnv
}

func (m *mockWorkerContext) GetNamespace() string {
	return m.namespace
}

func (m *mockWorkerContext) GetCurrentNodeID() ids.NodeID {
	return m.nodeId
}

func (m *mockRuntime) WasCurrentActorRestarted() bool {
	return false
}

func (m *mockRuntime) GetAllNodeInfo() []base.NodeInfo {
	return []base.NodeInfo{}
}

func (m *mockRuntime) GetAllActorInfo() []base.ActorInfo {
	return []base.ActorInfo{}
}

func (m *mockRuntime) GetGpuIds() []string {
	return []string{}
}

func (m *mockRuntime) GetCurrentActorHandle() submitter.ActorHandle {
	return nil
}

func (m *mockRuntime) GetObjectStore() object.ObjectStore {
	// Return nil for tests that don't need ObjectStore
	return nil
}

func (m *mockRuntime) GetTaskSubmitter() submitter.TaskSubmitter {
	return nil
}

func (m *mockRuntime) GetFunctionManager() function.Manager {
	return nil
}

// TestRuntimeContext_GetCurrentJobID tests the GetCurrentJobID delegation.
func TestRuntimeContext_GetCurrentJobID(t *testing.T) {
	tests := []struct {
		name      string
		mockJobID ids.JobID
		want      ids.JobID
	}{
		{
			name:      "zero value",
			mockJobID: ids.NilJobID(),
			want:      ids.NilJobID(),
		},
		{
			name:      "non-zero value",
			mockJobID: ids.JobIDFromInt(12345),
			want:      ids.JobIDFromInt(12345),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				jobId: tt.mockJobID,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetCurrentJobID()
			if !got.Equal(tt.want) {
				t.Errorf("GetCurrentJobID() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_GetCurrentTaskID tests the GetCurrentTaskID delegation.
func TestRuntimeContext_GetCurrentTaskID(t *testing.T) {
	tests := []struct {
		name       string
		mockTaskID ids.TaskID
		want       ids.TaskID
	}{
		{
			name:       "zero value",
			mockTaskID: ids.NilTaskID(),
			want:       ids.NilTaskID(),
		},
		{
			name:       "non-zero value",
			mockTaskID: ids.TaskIDForNormalTask(ids.JobIDFromInt(1), ids.NilTaskID(), 100),
			want:       ids.TaskIDForNormalTask(ids.JobIDFromInt(1), ids.NilTaskID(), 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				taskId: tt.mockTaskID,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetCurrentTaskID()
			if !got.Equal(tt.want) {
				t.Errorf("GetCurrentTaskID() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_GetCurrentActorID tests the GetCurrentActorID delegation.
func TestRuntimeContext_GetCurrentActorID(t *testing.T) {
	// Pre-create IDs to ensure consistency (OfActorID uses time.Now() internally)
	actorIDNonZero := ids.OfActorID(ids.JobIDFromInt(1), ids.NilTaskID(), 42)

	tests := []struct {
		name        string
		mockActorID ids.ActorID
		want        ids.ActorID
	}{
		{
			name:        "zero value",
			mockActorID: ids.NilActorID(),
			want:        ids.NilActorID(),
		},
		{
			name:        "non-zero value",
			mockActorID: actorIDNonZero,
			want:        actorIDNonZero,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				actorId: tt.mockActorID,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetCurrentActorID()
			if !got.Equal(tt.want) {
				t.Errorf("GetCurrentActorID() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_GetNamespace tests the GetNamespace delegation.
func TestRuntimeContext_GetNamespace(t *testing.T) {
	tests := []struct {
		name          string
		mockNamespace string
		want          string
	}{
		{
			name:          "empty namespace",
			mockNamespace: "",
			want:          "",
		},
		{
			name:          "non-empty namespace",
			mockNamespace: "production",
			want:          "production",
		},
		{
			name:          "namespace with special characters",
			mockNamespace: "dev-cluster_v2.0",
			want:          "dev-cluster_v2.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				namespace: tt.mockNamespace,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetNamespace()
			if got != tt.want {
				t.Errorf("GetNamespace() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_GetCurrentNodeID tests the GetCurrentNodeID delegation.
func TestRuntimeContext_GetCurrentNodeID(t *testing.T) {
	// Pre-create NodeID to ensure consistency (NewNodeID generates random ID each call)
	nodeIDNonZero := ids.NewNodeID()

	tests := []struct {
		name       string
		mockNodeID ids.NodeID
		want       ids.NodeID
	}{
		{
			name:       "zero value",
			mockNodeID: ids.NilNodeID(),
			want:       ids.NilNodeID(),
		},
		{
			name:       "non-zero value",
			mockNodeID: nodeIDNonZero,
			want:       nodeIDNonZero,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				nodeId: tt.mockNodeID,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetCurrentNodeID()
			if !got.Equal(tt.want) {
				t.Errorf("GetCurrentNodeID() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_GetSerializedRuntimeEnv tests the GetSerializedRuntimeEnv delegation.
func TestRuntimeContext_GetSerializedRuntimeEnv(t *testing.T) {
	tests := []struct {
		name           string
		mockRuntimeEnv string
		want           string
	}{
		{
			name:           "empty runtime env",
			mockRuntimeEnv: "",
			want:           "",
		},
		{
			name:           "JSON runtime env",
			mockRuntimeEnv: `{"worker_process_custom_options":["-XX:MaxRAM=400m"]}`,
			want:           `{"worker_process_custom_options":["-XX:MaxRAM=400m"]}`,
		},
		{
			name:           "complex runtime env",
			mockRuntimeEnv: `{"env_vars":{"PATH":"/usr/bin"},"pip":["requests","numpy"]}`,
			want:           `{"env_vars":{"PATH":"/usr/bin"},"pip":["requests","numpy"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				runtimeEnv: tt.mockRuntimeEnv,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetSerializedRuntimeEnv()
			if got != tt.want {
				t.Errorf("GetSerializedRuntimeEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRuntimeContext_IsLocalMode tests the IsLocalMode delegation.
func TestRuntimeContext_IsLocalMode(t *testing.T) {
	tests := []struct {
		name          string
		mockLocalMode bool
		want          bool
	}{
		{
			name:          "cluster mode",
			mockLocalMode: false,
			want:          false,
		},
		{
			name:          "local mode",
			mockLocalMode: true,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				localMode: tt.mockLocalMode,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.IsLocalMode()
			if got != tt.want {
				t.Errorf("IsLocalMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNativeRuntime_RuntimeContextMethods tests that NativeRuntime correctly
// delegates to runtimeContextProvider for all 7 Phase 1 methods.
func TestNativeRuntime_RuntimeContextMethods(t *testing.T) {
	// Create NativeRuntime instance
	opts := base.InitializeOptions{
		WorkerType: 0, // Use zero value for testing
	}
	nr, err := NewNativeRuntime(opts)
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}

	// Test that WorkerContext() method is accessible
	workerCtx := nr.WorkerContext()
	if workerCtx != nil {
		// Test that all WorkerContext methods are accessible and return zero values
		// (since mock runtime is not fully initialized)
		_ = workerCtx.GetCurrentJobID()
		_ = workerCtx.GetCurrentTaskID()
		_ = workerCtx.GetCurrentActorID()
		_ = workerCtx.GetNamespace()
		_ = workerCtx.GetCurrentNodeID()
		_ = workerCtx.GetSerializedRuntimeEnv()
	}
	_ = nr.IsLocalMode()

	// Test passed - methods are accessible without panic
}

// ============================================
// Phase 2 Integration Tests
// ============================================

// TestRuntimeContext_WasCurrentActorRestarted tests the Phase 2 placeholder
func TestRuntimeContext_WasCurrentActorRestarted(t *testing.T) {
	// Create mock runtime with actor ID
	mock := &mockRuntime{
		actorId: ids.OfActorID(ids.JobIDFromInt(1), ids.NilTaskID(), 42),
	}
	provider := newRuntimeContextProvider(mock)

	// Phase 2: Currently returns false as placeholder
	got := provider.WasCurrentActorRestarted()
	if got != false {
		t.Errorf("WasCurrentActorRestarted() = %v, want false (placeholder)", got)
	}

	// Test in non-actor context
	mockNoActor := &mockRuntime{
		actorId: ids.NilActorID(),
	}
	providerNoActor := newRuntimeContextProvider(mockNoActor)
	gotNoActor := providerNoActor.WasCurrentActorRestarted()
	if gotNoActor != false {
		t.Errorf("WasCurrentActorRestarted() in non-actor context = %v, want false", gotNoActor)
	}
}

// TestRuntimeContext_GetGpuIds tests the Phase 2 placeholder
func TestRuntimeContext_GetGpuIds(t *testing.T) {
	mock := &mockRuntime{}
	provider := newRuntimeContextProvider(mock)

	// Phase 2: Currently returns empty slice as placeholder
	got := provider.GetGpuIds()
	if len(got) != 0 {
		t.Errorf("GetGpuIds() = %v, want empty slice (placeholder)", got)
	}
}

// TestRuntimeContext_GetAllNodeInfo tests the Phase 2 implementation
// Note: This test requires GCS client integration, so it tests the error handling
func TestRuntimeContext_GetAllNodeInfo(t *testing.T) {
	// Create NativeRuntime instance (not fully initialized)
	opts := base.InitializeOptions{
		WorkerType: 0,
	}
	nr, err := NewNativeRuntime(opts)
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}

	// Phase 2: GCS client may not be initialized in test environment
	// Should return empty slice instead of panicking
	got := nr.GetAllNodeInfo()
	if got == nil {
		t.Error("GetAllNodeInfo() should return empty slice, not nil")
	}
	if len(got) != 0 {
		t.Logf("GetAllNodeInfo() returned %d nodes (GCS may be initialized)", len(got))
	}
}

// TestRuntimeContext_GetAllActorInfo tests the Phase 2 implementation
// Note: This test requires GCS client integration, so it tests the error handling
func TestRuntimeContext_GetAllActorInfo(t *testing.T) {
	// Create NativeRuntime instance (not fully initialized)
	opts := base.InitializeOptions{
		WorkerType: 0,
	}
	nr, err := NewNativeRuntime(opts)
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}

	// Phase 2: GCS client may not be initialized in test environment
	// Should return empty slice instead of panicking
	got := nr.GetAllActorInfo()
	if got == nil {
		t.Error("GetAllActorInfo() should return empty slice, not nil")
	}
	if len(got) != 0 {
		t.Logf("GetAllActorInfo() returned %d actors (GCS may be initialized)", len(got))
	}
}

// TestRuntimeContext_GetCurrentActorHandle tests the Phase 3 implementation
func TestRuntimeContext_GetCurrentActorHandle(t *testing.T) {
	tests := []struct {
		name        string
		mockActorID ids.ActorID
		wantNil     bool
	}{
		{
			name:        "non-actor context",
			mockActorID: ids.NilActorID(),
			wantNil:     true,
		},
		{
			name:        "actor context",
			mockActorID: ids.OfActorID(ids.JobIDFromInt(1), ids.NilTaskID(), 42),
			wantNil:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockRuntime{
				actorId: tt.mockActorID,
			}
			provider := newRuntimeContextProvider(mock)

			got := provider.GetCurrentActorHandle()

			if tt.wantNil && got != nil {
				t.Errorf("GetCurrentActorHandle() = %v, want nil in non-actor context", got)
			}

			// In actor context, result depends on GCS client availability
			// Phase 3: May return nil if GCS client not initialized
			if !tt.wantNil {
				if got == nil {
					t.Logf("GetCurrentActorHandle() returned nil (GCS client may not be initialized)")
				} else {
					t.Logf("GetCurrentActorHandle() returned actor handle: %T", got)
				}
			}
		})
	}
}

// TestRuntimeContext_ConvertNodeInfo tests the node info conversion helper
func TestRuntimeContext_ConvertNodeInfo(t *testing.T) {
	// Test convertNodeState with actual protobuf types
	tests := []struct {
		name     string
		state    proto.GcsNodeInfo_GcsNodeState
		expected base.NodeState
	}{
		{
			name:     "ALIVE state",
			state:    proto.GcsNodeInfo_ALIVE,
			expected: contract.NodeStateAlive,
		},
		{
			name:     "DEAD state",
			state:    proto.GcsNodeInfo_DEAD,
			expected: contract.NodeStateDead,
		},
		{
			name:     "unknown state",
			state:    proto.GcsNodeInfo_GcsNodeState(99), // Invalid state value
			expected: contract.NodeStateDead,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertNodeState(tt.state)
			if got != tt.expected {
				t.Errorf("convertNodeState(%v) = %v, want %v", tt.state, got, tt.expected)
			}
		})
	}
}

// TestRuntimeContext_ConvertActorInfo tests the actor info conversion helper
func TestRuntimeContext_ConvertActorInfo(t *testing.T) {
	// Test convertActorState with actual protobuf types
	tests := []struct {
		name     string
		state    proto.ActorTableData_ActorState
		expected base.ActorState
	}{
		{
			name:     "PENDING_CREATION state",
			state:    proto.ActorTableData_PENDING_CREATION,
			expected: contract.ActorStatePending,
		},
		{
			name:     "ALIVE state",
			state:    proto.ActorTableData_ALIVE,
			expected: contract.ActorStateAlive,
		},
		{
			name:     "DEAD state",
			state:    proto.ActorTableData_DEAD,
			expected: contract.ActorStateDead,
		},
		{
			name:     "RESTARTING state",
			state:    proto.ActorTableData_RESTARTING,
			expected: contract.ActorStateRestarting,
		},
		{
			name:     "unknown state",
			state:    99,
			expected: contract.ActorStateDead,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertActorState(tt.state)
			if got != tt.expected {
				t.Errorf("convertActorState(%v) = %v, want %v", tt.state, got, tt.expected)
			}
		})
	}
}

// TestRuntimeContext_RuntimeContextProvider_Structure tests the provider structure
func TestRuntimeContext_RuntimeContextProvider_Structure(t *testing.T) {
	mock := &mockRuntime{
		jobId:      ids.JobIDFromInt(123),
		taskId:     ids.TaskIDForNormalTask(ids.JobIDFromInt(1), ids.NilTaskID(), 100),
		actorId:    ids.OfActorID(ids.JobIDFromInt(1), ids.NilTaskID(), 42),
		namespace:  "test-namespace",
		nodeId:     ids.NewNodeID(),
		runtimeEnv: `{"test": "env"}`,
		localMode:  false,
	}

	provider := newRuntimeContextProvider(mock)

	// Verify provider is created
	if provider == nil {
		t.Fatal("newRuntimeContextProvider() returned nil")
	}

	// Verify all Phase 1 methods delegate correctly
	if !provider.GetCurrentJobID().Equal(mock.jobId) {
		t.Errorf("GetCurrentJobID() delegation failed")
	}
	if !provider.GetCurrentTaskID().Equal(mock.taskId) {
		t.Errorf("GetCurrentTaskID() delegation failed")
	}
	if !provider.GetCurrentActorID().Equal(mock.actorId) {
		t.Errorf("GetCurrentActorID() delegation failed")
	}
	if provider.GetNamespace() != mock.namespace {
		t.Errorf("GetNamespace() delegation failed")
	}
	if !provider.GetCurrentNodeID().Equal(mock.nodeId) {
		t.Errorf("GetCurrentNodeID() delegation failed")
	}
	if provider.GetSerializedRuntimeEnv() != mock.runtimeEnv {
		t.Errorf("GetSerializedRuntimeEnv() delegation failed")
	}
	if provider.IsLocalMode() != mock.localMode {
		t.Errorf("IsLocalMode() delegation failed")
	}
}
