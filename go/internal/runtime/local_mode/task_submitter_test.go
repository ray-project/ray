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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/runtime/actor"
	"github.com/ray-project/ray/go/internal/runtime/localstore"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testActor is a concrete actor type used by the actor lifecycle tests. Local
// mode resolves methods dynamically from the live instance (like native/Java),
// so the tests register the constructor in the executor's actor manager and the
// methods are dispatched by reflection on this type.
type testActor struct{}

func newTestActor() *testActor { return &testActor{} }

// TestMethod is a no-op method with no returns, used to exercise routing.
func (a *testActor) TestMethod() {}

// Exit returns an intentional exit error so the actor is torn down.
func (a *testActor) Exit() error { return rayerrors.NewActorExitError("local actor exiting") }

// Boom returns a normal (non-exit) error so the actor survives the task failure.
func (a *testActor) Boom() error { return fmt.Errorf("boom") }

// registerTestActorConstructor registers the testActor constructor with the
// executor's actor manager, the same registration path taken by
// registerUserFunctions for "<init>" entries.
func registerTestActorConstructor(t *testing.T, executor *LocalModeTaskExecutor) {
	t.Helper()
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	executor.RegisterActorConstructor(initDesc, actor.WrapActorConstructor(newTestActor))
}

func TestLocalModeTaskSubmitter(t *testing.T) {
	t.Run("CreateSubmitter", func(t *testing.T) {
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
		require.NotNil(t, taskSubmitter)
	})

	t.Run("SubmitNormalTask", func(t *testing.T) {
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

		// Create a simple function descriptor
		funcDesc := function.NewGoFunctionDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestMethod")

		// Submit task
		returnIds, err := taskSubmitter.SubmitTask(funcDesc, nil, 1, nil)
		require.NoError(t, err)
		assert.Len(t, returnIds, 1)

		taskSubmitter.Shutdown()
	})

	t.Run("CreateActor", func(t *testing.T) {
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		registerTestActorConstructor(t, taskExecutor)

		actorID, err := taskSubmitter.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{
			MaxConcurrency:  2,
			MaxPendingCalls: -1,
		})
		require.NoError(t, err)
		assert.NotEqual(t, ids.NilActorID(), actorID)

		// Verify actor is registered - access internal manager through taskSubmitter
		group := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID)
		assert.NotNil(t, group)

		// The actor instance must have been constructed and stored.
		_, ok := taskExecutor.actorManager.GetInstance(actorID)
		assert.True(t, ok, "actor instance should be stored after successful creation")

		taskSubmitter.Shutdown()
	})

	t.Run("GetNamedActor", func(t *testing.T) {
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		registerTestActorConstructor(t, taskExecutor)

		actorID, err := taskSubmitter.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{
			Name:            "TestActorName",
			MaxPendingCalls: -1,
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
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

		// Create actor first
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		registerTestActorConstructor(t, taskExecutor)

		actorID, _ := taskSubmitter.CreateActor(funcDesc, nil, nil)

		// Submit actor task (dispatched by reflection on the stored instance)
		methodDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", "TestMethod")

		returnIds, err := taskSubmitter.SubmitActorTask(actorID, methodDesc, nil, 1, nil)
		require.NoError(t, err)
		assert.Len(t, returnIds, 1)

		taskSubmitter.Shutdown()
	})
}

func TestLocalModeExitActor(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	// Create the actor. The constructor must be registered so the actor
	// creation task completes and the actor becomes available.
	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, nil)
	require.NoError(t, err)
	taskExecutor.RegisterActorContext(actorID, NewLocalActorContext(ids.NewUniqueID()))

	// Submitting the "Exit" task (dispatched by reflection on the live instance)
	// must tear the actor down.
	exitDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", "Exit")
	_, err = taskSubmitter.SubmitActorTask(actorID, exitDesc, nil, 1, nil)
	require.NoError(t, err)

	// Concurrency group and context must be removed; later calls must fail.
	if group := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID); group != nil {
		t.Error("expected actor concurrency group to be removed after exit")
	}
	if _, ok := taskExecutor.GetActorContextByID(actorID); ok {
		t.Error("expected actor context to be removed after exit")
	}

	taskSubmitter.Shutdown()
}

func TestLocalModeKillActor(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	// Create the actor. The constructor must be registered so the actor
	// creation task completes and the actor becomes available.
	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, nil)
	require.NoError(t, err)
	taskExecutor.RegisterActorContext(actorID, NewLocalActorContext(ids.NewUniqueID()))

	// Kill the actor from the driver side (noRestart=true).
	require.NoError(t, taskSubmitter.KillActor(actorID, true))

	// Concurrency group and context must be removed; later calls must fail.
	if group := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID); group != nil {
		t.Error("expected actor concurrency group to be removed after kill")
	}
	if _, ok := taskExecutor.GetActorContextByID(actorID); ok {
		t.Error("expected actor context to be removed after kill")
	}

	taskSubmitter.Shutdown()
}

func TestLocalModeActorTask_NamedConcurrencyGroup(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, &submitter.ActorCreationOptions{
		MaxConcurrency:  1,
		MaxPendingCalls: -1,
		ConcurrencyGroups: []submitter.ConcurrencyGroup{
			{Name: "cg1", MaxCalls: 2},
		},
	})
	require.NoError(t, err)
	taskExecutor.RegisterActorContext(actorID, NewLocalActorContext(ids.NewUniqueID()))

	methodDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", "TestMethod")

	// The declared group is pre-registered at actor creation time.
	namedGroup := taskSubmitter.actorConcurrencyGroupMgr.GetOrCreateNamedGroup(actorID, "cg1", 2)
	require.NotNil(t, namedGroup)

	// With a concurrency group name, the task routes to the named group.
	_, err = taskSubmitter.SubmitActorTask(actorID, methodDesc, nil, 1, &submitter.TaskOptions{
		ConcurrencyGroupName: "cg1",
	})
	require.NoError(t, err)

	// Without a group name, the task routes to the default group.
	_, err = taskSubmitter.SubmitActorTask(actorID, methodDesc, nil, 1, nil)
	require.NoError(t, err)

	defaultGroup := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID)
	assert.NotNil(t, defaultGroup)
	assert.NotEqual(t, namedGroup, defaultGroup, "named group must be distinct from the default group")

	taskSubmitter.Shutdown()
}

// TestLocalModeActorTask_ConcurrencyGroupMaxCallsFallback verifies that
// ConcurrencyGroup.MaxCalls <= 0 (0 or -1) resolves to the actor's
// MaxConcurrency at actor creation. This mirrors the native (cgo) backend
// (TestConvertActorCreationOptionsToCMaxCallsFallback in
// internal/runtime/cgo/native_submitter_test.go), so cluster and local mode
// resolve the same group max concurrency for the same API input and the two
// fallback rules cannot silently drift apart.
func TestLocalModeActorTask_ConcurrencyGroupMaxCallsFallback(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, &submitter.ActorCreationOptions{
		MaxConcurrency:  4,
		MaxPendingCalls: -1,
		ConcurrencyGroups: []submitter.ConcurrencyGroup{
			{Name: "cg0", MaxCalls: 0},
			{Name: "cgn1", MaxCalls: -1},
			{Name: "cgPos", MaxCalls: 3},
		},
	})
	require.NoError(t, err)

	// MaxCalls=0 and MaxCalls=-1 both resolve to the actor's MaxConcurrency (4),
	// matching the cgo fallback; a positive MaxCalls (3) passes through.
	assert.Equal(t, 4, taskSubmitter.actorConcurrencyGroupMgr.GetNamedGroup(actorID, "cg0").maxConcurrency)
	assert.Equal(t, 4, taskSubmitter.actorConcurrencyGroupMgr.GetNamedGroup(actorID, "cgn1").maxConcurrency)
	assert.Equal(t, 3, taskSubmitter.actorConcurrencyGroupMgr.GetNamedGroup(actorID, "cgPos").maxConcurrency)

	taskSubmitter.Shutdown()
}

func TestLocalModeActorTask_SystemGroupAndUndeclaredGroup(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, &submitter.ActorCreationOptions{MaxConcurrency: 1, MaxPendingCalls: -1})
	require.NoError(t, err)

	methodDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", "TestMethod")

	// "_ray_system" is auto-created with concurrency 1 even when the actor did
	// not declare it (mirrors C++ ConcurrencyGroupManager::GetExecutor).
	_, err = taskExecutor.ExecuteActorTaskInGroup(actorID, systemConcurrencyGroupName, methodDesc, nil)
	require.NoError(t, err)
	sysGroup := actorMgr.GetNamedGroup(actorID, systemConcurrencyGroupName)
	require.NotNil(t, sysGroup, "_ray_system group should be auto-created")

	// An undeclared non-system group fails fast instead of silently falling
	// back to the default group (which would mask misconfiguration).
	_, err = taskExecutor.ExecuteActorTaskInGroup(actorID, "undeclared_group", methodDesc, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "undeclared_group")

	taskSubmitter.Shutdown()
}

func TestLocalModeNormalErrorKeepsActor(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	registerTestActorConstructor(t, taskExecutor)
	initDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
	actorID, err := taskSubmitter.CreateActor(initDesc, nil, nil)
	require.NoError(t, err)
	taskExecutor.RegisterActorContext(actorID, NewLocalActorContext(ids.NewUniqueID()))

	boomDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", "Boom")

	_, err = taskSubmitter.SubmitActorTask(actorID, boomDesc, nil, 1, nil)
	require.NoError(t, err)

	if group := taskSubmitter.actorConcurrencyGroupMgr.GetGroup(actorID); group == nil {
		t.Error("expected actor to survive a normal error")
	}

	taskSubmitter.Shutdown()
}

// lateRegisteredAdd mirrors the "remote registered after Init" driver pattern:
// api.RegisterFunction (the path taken by api.Remote at call time) runs after
// the runtime Start()ed, so the function is not in the local FunctionManager
// when the submitter is built. Submitting a task for it must still succeed
// because SubmitTask re-synchronizes the global registry before execution.
func lateRegisteredAdd(a, b int) int { return a + b }

// concurrentSyncAdd is a distinct function from lateRegisteredAdd so the
// concurrent test registers its own unique global entry, avoiding collision
// with registrations from other tests in this binary.
func concurrentSyncAdd(a, b int) int { return a + b }

func TestLocalModeSubmitTask_SyncsLateRegisteredFunction(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)

	// Register the function AFTER the submitter (and runtime) were created,
	// simulating api.Remote called after Init. The global registry is shared
	// across tests in this binary, so use a unique function name to avoid
	// colliding with other registrations.
	require.NoError(t, api.RegisterFunction(lateRegisteredAdd))

	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	// The function must not be visible in the local manager before submission.
	desc := function.ExtractFunctionDescriptor(lateRegisteredAdd)
	_, err := functionMgr.GetFunction(desc)
	require.Error(t, err, "function must not be registered before SubmitTask re-syncs")

	// Submit a task; SubmitTask re-syncs, the executor resolves the function
	// and runs it, and the result is stored in the object store.
	ser := object.GetSerializer()
	in1, err := ser.Serialize(4)
	require.NoError(t, err)
	in2, err := ser.Serialize(5)
	require.NoError(t, err)
	args := []function.FunctionArg{
		function.NewFunctionArgByValue(in1.Data, in1.Metadata),
		function.NewFunctionArgByValue(in2.Data, in2.Metadata),
	}

	returnIds, err := taskSubmitter.SubmitTask(desc, args, 1, nil)
	require.NoError(t, err)
	require.Len(t, returnIds, 1)

	// The executed result must be readable from the object store.
	got, err := objectStore.GetRaw([]*ids.ObjectID{&returnIds[0]}, 0, "test")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got, 1)

	var sum int
	require.NoError(t, ser.DeserializeTo(got[0], &sum))
	assert.Equal(t, 9, sum)

	taskSubmitter.Shutdown()
}

// TestLocalModeByRefArgMaterialization verifies that a pass-by-reference
// argument (produced by convertArgToFunctionArg for large objects) is
// materialized from the in-memory object store before execution, mirroring the
// C++ LocalDependencyResolver. Without materialization the function receives an
// unsupported by-ref FunctionArg and DeserializeArgs fails; the test would then
// observe a missing return object instead of the computed value.
func TestLocalModeByRefArgMaterialization(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	desc := function.NewGoFunctionDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "Identity")
	require.NoError(t, functionMgr.RegisterFunction(desc, func(args []function.FunctionArg) ([]function.SerializedObject, error) {
		// The registered function must only ever observe value arguments; a
		// by-ref arg that leaked through would carry ObjectRef and no Data.
		require.Len(t, args, 1)
		arg := args[0]
		require.True(t, arg.IsPassByValue(), "arg must be materialized to by-value, got %+v", arg)
		require.NotNil(t, arg.Data, "arg data must be present")
		// The materialized arg carries the same serialized format (with the
		// cross-language length header) as a by-value arg, so decode it the same
		// way DeserializeArgs would.
		var in string
		if err := object.GetSerializer().DeserializeTo(&object.NativeRayObject{
			Data:     arg.Data.Data,
			Metadata: arg.Data.Metadata,
		}, &in); err != nil {
			return nil, err
		}
		nativeObj, err := object.GetSerializer().Serialize(in)
		if err != nil {
			return nil, err
		}
		return []function.SerializedObject{function.SerializedObjectFromNative(nativeObj)}, nil
	}))

	ser := object.GetSerializer()
	// Put a large (by-ref trigger sized) string into the store as the argument
	// object, the way convertArgToFunctionArg's PutRawWithID does.
	in1, err := ser.Serialize("hello-")
	require.NoError(t, err)
	argOID := ids.NewObjectID()
	require.NoError(t, objectStore.PutRawWithID(in1, &argOID))
	ownerAddr, err := objectStore.GetOwnerAddress(&argOID)
	require.NoError(t, err)
	argRef := function.NewFunctionArgByRef(argOID, ownerAddr)

	returnIds, err := taskSubmitter.SubmitTask(desc, []function.FunctionArg{argRef}, 1, nil)
	require.NoError(t, err)
	require.Len(t, returnIds, 1)

	// The return object must be produced; a deadlock would instead block here.
	got, err := objectStore.GetRaw([]*ids.ObjectID{&returnIds[0]}, 1000, "test")
	require.NoError(t, err)
	require.Len(t, got, 1)

	var out string
	require.NoError(t, ser.DeserializeTo(got[0], &out))
	assert.Equal(t, "hello-", out)

	taskSubmitter.Shutdown()
}

// TestLocalModeTaskFailurePutsErrorObject verifies that a task whose execution
// fails still puts an error object for every return ID. This is the backstop
// against the by-ref deadlock: without it, a failed task silently returned with
// no return objects, and a driver Get()ing the result blocked forever in
// waitForObjects. The error object must decode as a RayTaskExecutionException.
func TestLocalModeTaskFailurePutsErrorObject(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	desc := function.NewGoFunctionDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "Boom")
	require.NoError(t, functionMgr.RegisterFunction(desc, func(args []function.FunctionArg) ([]function.SerializedObject, error) {
		return nil, fmt.Errorf("boom")
	}))

	returnIds, err := taskSubmitter.SubmitTask(desc, nil, 2, nil)
	require.NoError(t, err)
	require.Len(t, returnIds, 2)

	// Both return IDs must resolve to error objects, never to nothing.
	got, err := objectStore.GetRaw([]*ids.ObjectID{&returnIds[0], &returnIds[1]}, 1000, "test")
	require.NoError(t, err)
	require.Len(t, got, 2)

	for i, nativeObj := range got {
		require.NotNil(t, nativeObj, "return %d must be present", i)
		exc, ok := object.ErrorObjectFromNative(nativeObj)
		require.True(t, ok, "return %d must be an error object", i)
		_, isTaskExc := exc.(*object.RayTaskExecutionException)
		require.True(t, isTaskExc, "return %d must carry a RayTaskExecutionException, got %T", i, exc)
	}

	taskSubmitter.Shutdown()
}

// TestLocalModeSubmitTask_ConcurrentSync verifies the double-checked sync in
// SubmitTask under concurrent submission: when a function is registered after
// the submitter exists, concurrent Submits that observe the stale registry
// version must all succeed (the sync is idempotent) and the late-registered
// function must be visible to the executor. A data race in the sync would be
// caught by running under the race detector.
func TestLocalModeSubmitTask_ConcurrentSync(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)

	// Register the function after the submitter exists, then submit from many
	// goroutines at once so several observe the stale version concurrently.
	require.NoError(t, api.RegisterFunction(concurrentSyncAdd))
	desc := function.ExtractFunctionDescriptor(concurrentSyncAdd)

	ser := object.GetSerializer()
	arg1Data, err := ser.Serialize(3)
	require.NoError(t, err)
	arg2Data, err := ser.Serialize(6)
	require.NoError(t, err)
	args := []function.FunctionArg{
		function.NewFunctionArgByValue(arg1Data.Data, arg1Data.Metadata),
		function.NewFunctionArgByValue(arg2Data.Data, arg2Data.Metadata),
	}

	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			returnIds, err := taskSubmitter.SubmitTask(desc, args, 1, nil)
			if err != nil {
				errCh <- err
				return
			}
			if len(returnIds) != 1 {
				errCh <- fmt.Errorf("expected 1 return id, got %d", len(returnIds))
				return
			}
			// Wait for the result object to be produced.
			got, err := objectStore.GetRaw([]*ids.ObjectID{&returnIds[0]}, 1000, "test")
			if err != nil {
				errCh <- err
				return
			}
			if len(got) != 1 {
				errCh <- fmt.Errorf("expected 1 result object, got %d", len(got))
				return
			}
			var sum int
			if err := ser.DeserializeTo(got[0], &sum); err != nil {
				errCh <- err
				return
			}
			if sum != 9 {
				errCh <- fmt.Errorf("expected sum 9, got %d", sum)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent SubmitTask failed: %v", err)
	}

	taskSubmitter.Shutdown()
}

// TestCreateActorRejectsInvalidMaxPendingCalls verifies that CreateActor
// validates the new MaxPendingCalls field (aligned with Java LocalMode) before
// any other processing, so an invalid value returns an error without needing
// the full executor/object-store setup to succeed. The validation runs at the
// very start of CreateActor, before syncUserFunctions, so the minimal submitter
// constructed here is sufficient to trigger the branch.
func TestCreateActorRejectsInvalidMaxPendingCalls(t *testing.T) {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	taskSubmitter := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
	defer taskSubmitter.Shutdown()

	funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)

	testCases := []struct {
		name            string
		maxPendingCalls int
		wantErr         bool
	}{
		{name: "zero is unset (unlimited)", maxPendingCalls: 0, wantErr: false},
		{name: "less than -1 is invalid", maxPendingCalls: -2, wantErr: true},
		{name: "negative one is valid (unlimited)", maxPendingCalls: -1, wantErr: false},
		{name: "positive is valid", maxPendingCalls: 1, wantErr: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := taskSubmitter.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{
				MaxPendingCalls: tc.maxPendingCalls,
				MaxConcurrency:  1,
			})
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "maxPendingCalls")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLocalModeTaskSubmitterGetActorHandle(t *testing.T) {
	newSubmitter := func() (*LocalModeTaskSubmitter, *localstore.LocalModeObjectStore) {
		objectStore := localstore.NewLocalModeObjectStore()
		workerContext := NewLocalModeWorkerContext()
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		// The constructor must be registered so CreateActor actually constructs
		// the instance; GetActorHandle resolves live instances only.
		registerTestActorConstructor(t, taskExecutor)
		ts := NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
		return ts, objectStore
	}

	t.Run("GetActorHandleByID", func(t *testing.T) {
		ts, _ := newSubmitter()
		defer ts.Shutdown()
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		actorID, err := ts.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{MaxConcurrency: 2, MaxPendingCalls: -1})
		require.NoError(t, err)
		assert.NotEqual(t, ids.NilActorID(), actorID)

		handle, err := ts.GetActorHandle(actorID)
		require.NoError(t, err)
		assert.NotNil(t, handle)
		assert.Equal(t, actorID, handle.ID())
	})

	t.Run("GetActorHandleUnnamedRegistered", func(t *testing.T) {
		ts, _ := newSubmitter()
		defer ts.Shutdown()
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		// No Name in options: registration must still happen (Java registers unconditionally).
		actorID, err := ts.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{MaxPendingCalls: -1})
		require.NoError(t, err)

		handle, err := ts.GetActorHandle(actorID)
		require.NoError(t, err)
		assert.Equal(t, actorID, handle.ID())
	})

	t.Run("GetActorHandleNotFound", func(t *testing.T) {
		ts, _ := newSubmitter()
		defer ts.Shutdown()
		_, err := ts.GetActorHandle(ids.NilActorID())
		assert.Error(t, err)
	})

	t.Run("GetActorHandleKillCleans", func(t *testing.T) {
		ts, _ := newSubmitter()
		defer ts.Shutdown()
		funcDesc := function.NewGoActorMethodDescriptorOrUnknown("github.com/ray-project/ray/go/internal/runtime/local_mode", "", "TestActor", function.ConstructorName)
		actorID, err := ts.CreateActor(funcDesc, nil, &submitter.ActorCreationOptions{MaxConcurrency: 2, MaxPendingCalls: -1})
		require.NoError(t, err)

		require.NoError(t, ts.KillActor(actorID, true))
		_, err = ts.GetActorHandle(actorID)
		assert.Error(t, err)
	})
}

// newTestSubmitter constructs a LocalModeTaskSubmitter with fresh supporting
// components, mirroring the setup used by the other submitter tests.
func newTestSubmitter() *LocalModeTaskSubmitter {
	objectStore := localstore.NewLocalModeObjectStore()
	workerContext := NewLocalModeWorkerContext()
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	taskExecutor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
	return NewLocalModeTaskSubmitter(objectStore, workerContext, taskExecutor, functionMgr, actorMgr)
}

// TestLocalModePlacementGroupLifecycle verifies that the local-mode submitter
// simulates a placement group lifecycle with an in-process map: creation
// always returns a non-nil id, waiting on it succeeds immediately, removal
// deletes it, and waiting on an unknown group fails.
func TestLocalModePlacementGroupLifecycle(t *testing.T) {
	ts := newTestSubmitter()
	defer ts.Shutdown()
	ctx := context.Background()

	id, err := ts.CreatePlacementGroup(ctx, &submitter.PlacementGroupCreationOptions{
		Name:    "pg-1",
		Bundles: []map[string]float64{{"CPU": 1}},
	})
	require.NoError(t, err)
	assert.False(t, id.IsNil(), "expected non-nil placement group id")

	require.NoError(t, ts.WaitPlacementGroupReady(ctx, id, time.Second))
	require.NoError(t, ts.RemovePlacementGroup(ctx, id))

	// Removing an unknown group is a no-op success in local mode.
	require.NoError(t, ts.RemovePlacementGroup(ctx, ids.OfPlacementGroupID(ids.NewJobID())))

	// Waiting on an unknown group must fail.
	require.Error(t, ts.WaitPlacementGroupReady(ctx, ids.OfPlacementGroupID(ids.NewJobID()), time.Second))
}

// TestLocalModePlacementGroupValidation verifies that invalid creation options
// are rejected before any in-process state is recorded.
func TestLocalModePlacementGroupValidation(t *testing.T) {
	ts := newTestSubmitter()
	defer ts.Shutdown()
	ctx := context.Background()

	_, err := ts.CreatePlacementGroup(ctx, &submitter.PlacementGroupCreationOptions{})
	require.Error(t, err)

	_, err = ts.CreatePlacementGroup(ctx, &submitter.PlacementGroupCreationOptions{
		Name:     "pg-2",
		Bundles:  []map[string]float64{{"CPU": 1}},
		Strategy: 99,
	})
	require.Error(t, err)
}
