//go:build apihandle

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

// This file verifies the worker handle-attach behavior introduced for
// api.ExitActor support on workers (go/internal/worker calls
// SetRuntimeHandleForWorker after base.Initialize). It is gated behind the
// "apihandle" build tag because setting the handle mutates package-global state
// that the default api tests assume is unset; run it in isolation with:
//
//	go test -tags apihandle ./pkg/runtime/api/ -run TestWorkerHandle
package api

import (
	"errors"
	"testing"

	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

type workerHandleTestWorkerContext struct{}

func (workerHandleTestWorkerContext) GetCurrentWorkerId() ids.UniqueID { return ids.NewUniqueID() }
func (workerHandleTestWorkerContext) GetCurrentJobID() ids.JobID       { return ids.NilJobID() }
func (workerHandleTestWorkerContext) GetCurrentActorID() ids.ActorID {
	return ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 42)
}
func (workerHandleTestWorkerContext) GetCurrentTaskType() contract.TaskType {
	return contract.TaskTypeActorTask
}
func (workerHandleTestWorkerContext) GetCurrentTaskID() ids.TaskID    { return ids.NilTaskID() }
func (workerHandleTestWorkerContext) GetRpcAddress() []byte           { return nil }
func (workerHandleTestWorkerContext) GetSerializedRuntimeEnv() string { return "" }
func (workerHandleTestWorkerContext) GetNamespace() string            { return "" }
func (workerHandleTestWorkerContext) GetCurrentNodeID() ids.NodeID    { return ids.NilNodeID() }

type workerHandleTestRuntime struct{}

func (workerHandleTestRuntime) Start() error        { return nil }
func (workerHandleTestRuntime) Shutdown() error     { return nil }
func (workerHandleTestRuntime) Run() error          { return nil }
func (workerHandleTestRuntime) IsInitialized() bool { return true }
func (workerHandleTestRuntime) WorkerContext() contract.WorkerContext {
	return workerHandleTestWorkerContext{}
}
func (workerHandleTestRuntime) GetRunMode() contract.RunMode          { return contract.RunModeCluster }
func (workerHandleTestRuntime) IsLocalMode() bool                     { return false }
func (workerHandleTestRuntime) WasCurrentActorRestarted() bool        { return false }
func (workerHandleTestRuntime) GetAllNodeInfo() []contract.NodeInfo   { return nil }
func (workerHandleTestRuntime) GetAllActorInfo() []contract.ActorInfo { return nil }
func (workerHandleTestRuntime) GetGpuIds() []string                   { return nil }
func (workerHandleTestRuntime) GetCurrentActorHandle() submitter.ActorHandle {
	return nil
}
func (workerHandleTestRuntime) GetObjectStore() object.ObjectStore        { return nil }
func (workerHandleTestRuntime) GetTaskSubmitter() submitter.TaskSubmitter { return nil }
func (workerHandleTestRuntime) GetFunctionManager() function.Manager      { return nil }
func (workerHandleTestRuntime) GetAllActorInfoByFilter(jobID ids.JobID, state contract.ActorState) []contract.ActorInfo {
	return nil
}
func (workerHandleTestRuntime) GetResourceIds() map[string][]contract.ResourceValue {
	return nil
}
func (workerHandleTestRuntime) CreateRuntimeEnv() contract.RuntimeEnv { return contract.RuntimeEnv{} }
func (workerHandleTestRuntime) DeserializeRuntimeEnv(serializedRuntimeEnv string) (contract.RuntimeEnv, error) {
	return contract.UnmarshalRuntimeEnv(serializedRuntimeEnv)
}

type workerHandleTestHandle struct{}

func (workerHandleTestHandle) IsRuntimeHandle()          {}
func (workerHandleTestHandle) Runtime() contract.Runtime { return workerHandleTestRuntime{} }

// TestWorkerHandleAttach verifies that after SetRuntimeHandleForWorker,
// handle-dependent functions (ExitActor) work, and that nil is a no-op.
func TestWorkerHandleAttach(t *testing.T) {
	// Nil handle must not initialize the api handle.
	SetRuntimeHandleForWorker(nil)
	if IsInitialized() {
		t.Fatal("SetRuntimeHandleForWorker(nil) must be a no-op")
	}

	SetRuntimeHandleForWorker(workerHandleTestHandle{})
	if !IsInitialized() {
		t.Fatal("SetRuntimeHandleForWorker should mark the runtime initialized")
	}

	// ExitActor must now return an ActorExitError (the fake worker context
	// reports a current actor), which is what drives the worker's intentional
	// exit path on the executor side.
	err := ExitActor()
	if err == nil {
		t.Fatal("ExitActor with a set handle should return a non-nil error")
	}
	var exitErr *rayerrors.ActorExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("ExitActor should return ActorExitError, got %T: %v", err, err)
	}

	// A second call must remain a no-op (idempotent).
	SetRuntimeHandleForWorker(workerHandleTestHandle{})
}
