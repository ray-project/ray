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

	"github.com/ray-project/ray/go/internal/runtime/localstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLocalAdd is a plain Go function used to exercise registration.
func testLocalAdd(a, b int) int { return a + b }

// newTestExecutor builds a LocalModeTaskExecutor wired to the given FunctionManager
// with a fresh object store and concurrency group manager.
func newTestExecutor(functionMgr *function.FunctionManager) *LocalModeTaskExecutor {
	return NewLocalModeTaskExecutor(
		functionMgr,
		NewActorConcurrencyGroupManager(),
		localstore.NewLocalModeObjectStore(),
	)
}

func TestRegisterUserFunctionsFromRegistry(t *testing.T) {
	// Register a plain function into the global registry via the public API.
	// This is the same path taken by api.Remote at call time.
	require.NoError(t, api.RegisterFunction(testLocalAdd))

	functionMgr := function.NewFunctionManager(nil)
	executor := newTestExecutor(functionMgr)

	require.NoError(t, registerUserFunctions(executor))

	// The function must now be resolvable from the local FunctionManager.
	desc := function.ExtractFunctionDescriptor(testLocalAdd)
	registered, err := functionMgr.GetFunction(desc)
	require.NoError(t, err, "function should be registered in local function manager")
	require.NotNil(t, registered)

	// Execute the registered function through the FunctionManager to verify the
	// wrapper can deserialize args and serialize results.
	ser := object.GetSerializer()
	in1, err := ser.Serialize(2)
	require.NoError(t, err)
	in2, err := ser.Serialize(3)
	require.NoError(t, err)
	args := []function.FunctionArg{
		function.NewFunctionArgByValue(in1.Data, in1.Metadata),
		function.NewFunctionArgByValue(in2.Data, in2.Metadata),
	}
	results, err := registered(args)
	require.NoError(t, err)
	require.Len(t, results, 1)

	var got int
	require.NoError(t, ser.DeserializeTo(&object.NativeRayObject{
		Data:     results[0].Data,
		Metadata: results[0].Metadata,
	}, &got))
	assert.Equal(t, 5, got)
}

func TestRegisterUserFunctionsNoRegistry(t *testing.T) {
	// An empty registry must not error and must leave the manager empty.
	executor := newTestExecutor(function.NewFunctionManager(nil))
	require.NoError(t, registerUserFunctions(executor))
}

// registrationTestActor is a stateful actor type used to verify that the "<init>"
// constructor is now synchronized into the local actor manager (mirroring Java,
// where the constructor is always resolvable when an ACTOR_CREATION_TASK runs).
type registrationTestActor struct {
	value int
}

func (a *registrationTestActor) Add(delta int) int {
	a.value += delta
	return a.value
}

func newRegistrationTestActor(initial int) *registrationTestActor {
	return &registrationTestActor{value: initial}
}

func TestRegisterUserFunctionsIncludesActorConstructor(t *testing.T) {
	// Register the actor constructor via the public API, the same path taken by
	// userfuncs.registerActors at package init.
	require.NoError(t, api.RegisterActorClass((*registrationTestActor)(nil), newRegistrationTestActor))

	executor := newTestExecutor(function.NewFunctionManager(nil))
	require.NoError(t, registerUserFunctions(executor))

	// The "<init>" constructor must be registered in the executor's actor
	// manager so an ACTOR_CREATION_TASK can build the actor. Before the fix it
	// was skipped entirely and actor creation failed. The descriptor is rebuilt
	// exactly as RegisterActorClass derives it, so the lookup key matches the
	// registration key.
	moduleName, packagePath := function.SplitModuleAndPackage("github.com/ray-project/ray/go/internal/runtime/local_mode")
	desc := function.NewGoActorMethodDescriptorOrUnknown(moduleName, packagePath, "registrationTestActor", function.ConstructorName)
	require.Equal(t, function.ConstructorName, desc.MethodName())

	// Actor creation must construct a live instance (NOT serialize it) and store
	// it under the actor ID, mirroring actor.ActorManager.
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
	ser := object.GetSerializer()
	in, err := ser.Serialize(42)
	require.NoError(t, err)
	args := []function.FunctionArg{
		function.NewFunctionArgByValue(in.Data, in.Metadata),
	}
	require.NoError(t, executor.ExecuteActorCreation(actorID, desc, args))
	instance, ok := executor.actorManager.GetInstance(actorID)
	require.True(t, ok, "actor instance should be stored after creation")
	require.NotNil(t, instance)

	// The stored instance must be able to serve a dynamic method call.
	methodDesc := function.NewGoActorMethodDescriptorOrUnknown(moduleName, packagePath, "registrationTestActor", "Add")
	results, err := executor.ExecuteActorMethodByID(actorID, methodDesc, args)
	require.NoError(t, err, "actor method dispatch through the stored instance should succeed")
	require.Len(t, results, 1)
	var got int
	require.NoError(t, ser.DeserializeTo(&object.NativeRayObject{
		Data:     results[0].Data,
		Metadata: results[0].Metadata,
	}, &got))
	// The instance was created with 42 and Add(42) ran on it: 42 + 42 = 84.
	assert.Equal(t, 84, got)
}
