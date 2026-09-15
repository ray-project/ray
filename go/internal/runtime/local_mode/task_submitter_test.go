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

package local_mode

import (
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalModeTaskSubmitter(t *testing.T) {
	t.Run("CreateSubmitter", func(t *testing.T) {
		objectStore := objectstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)
		require.NotNil(t, taskSubmitter)
	})

	t.Run("SubmitNormalTask", func(t *testing.T) {
		objectStore := objectstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)

		// Create a simple function descriptor
		funcDesc := function.NewGoFunctionDescriptorOrUnknown("test", "", "TestMethod")

		// Submit task
		returnIds, err := taskSubmitter.SubmitTask(funcDesc, nil, 1, nil)
		require.NoError(t, err)
		assert.Len(t, returnIds, 1)

		taskSubmitter.Shutdown()
	})

	t.Run("CreateActor", func(t *testing.T) {
		objectStore := objectstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("test", "", "TestActor", "<init>")

		actorID, err := taskSubmitter.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{
			MaxConcurrency: 2,
		})
		require.NoError(t, err)
		assert.NotEqual(t, ids.NilActorID(), actorID)

		// Verify actor is registered - access internal manager through taskSubmitter
		group := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID)
		assert.NotNil(t, group)

		taskSubmitter.Shutdown()
	})

	t.Run("GetNamedActor", func(t *testing.T) {
		objectStore := objectstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("test", "", "TestActor", "<init>")

		actorID, err := taskSubmitter.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{
			Name: "TestActorName",
		})
		require.NoError(t, err)

		// Get actor by name
		handle, err := taskSubmitter.GetActor("TestActorName", "")
		require.NoError(t, err)
		assert.NotNil(t, handle)
		assert.Equal(t, actorID, handle.ID())

		// Non-existent actor
		_, err = taskSubmitter.GetActor("NonExistent", "")
		assert.Error(t, err)

		taskSubmitter.Shutdown()
	})

	t.Run("SubmitActorTask", func(t *testing.T) {
		objectStore := objectstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)

		// Create actor first
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("test", "", "TestActor", "<init>")

		actorID, _ := taskSubmitter.CreateActor(funcDesc, nil, nil)

		// Submit actor task
		methodDesc := function.NewGoActorMethodDescriptorOrUnknown("test", "", "TestActor", "TestMethod")

		returnIds, err := taskSubmitter.SubmitActorTask(actorID, methodDesc, nil, 1, nil)
		require.NoError(t, err)
		assert.Len(t, returnIds, 1)

		taskSubmitter.Shutdown()
	})
}

// TestDependentTaskNoDeadlock reproduces the deadlock that occurred when a
// task dependent on an unready ObjectRef was executed from checkWaitingTasks
// while holding the non-reentrant taskAndObjectLock. Submitting the dependent
// task must return, and putting the dependency must not deadlock.
func TestDependentTaskNoDeadlock(t *testing.T) {
	objectStore := objectstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr)
	defer taskSubmitter.Shutdown()

	funcDesc := function.NewGoFunctionDescriptorOrUnknown("test", "", "TestMethod")

	// A freshly created object ID is not in the store yet, so it is unready.
	depRef, err := objectStore.PutRaw(object.NewNativeRayObject([]byte{1}, []byte{}))
	require.NoError(t, err)

	// This task depends on depRef, which is not ready yet -> submitted but
	// parked in waitingTasks.
	arg := function.NewFunctionArgByRef(*depRef, nil)
	done := make(chan struct{})
	go func() {
		_, _ = taskSubmitter.SubmitTask(funcDesc, []function.FunctionArg{arg}, 1, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SubmitTask blocked: possible deadlock")
	}

	// Making the dependency ready triggers checkWaitingTasks.
	depData := object.NewNativeRayObject([]byte{2}, []byte{})
	require.NoError(t, objectStore.PutRawWithID(depData, depRef))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not progress after dependency became ready: possible deadlock")
	}
}
