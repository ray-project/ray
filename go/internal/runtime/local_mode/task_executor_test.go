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

	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalActorContext(t *testing.T) {
	t.Run("CreateActorContext", func(t *testing.T) {
		workerID := ids.NewUniqueID()
		ctx := NewLocalActorContext(workerID)
		require.NotNil(t, ctx)
		assert.Equal(t, workerID, ctx.GetWorkerID())
	})
}

func TestLocalModeTaskExecutor(t *testing.T) {
	t.Run("CreateExecutor", func(t *testing.T) {
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		objectStore := objectstore.NewLocalModeObjectStore()

		executor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)
		require.NotNil(t, executor)
	})

	t.Run("SetAndGetActorContext", func(t *testing.T) {
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		objectStore := objectstore.NewLocalModeObjectStore()

		executor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)

		workerID := ids.NewUniqueID()
		actorContext := NewLocalActorContext(workerID)

		executor.SetActorContext(workerID, actorContext)
		retrieved := executor.GetActorContext()

		assert.Equal(t, actorContext, retrieved)
	})

	t.Run("RegisterAndGetActorContext", func(t *testing.T) {
		functionMgr := function.NewFunctionManager(nil)
		actorMgr := NewActorConcurrencyGroupManager()
		objectStore := objectstore.NewLocalModeObjectStore()

		executor := NewLocalModeTaskExecutor(functionMgr, actorMgr, objectStore)

		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
		workerID := ids.NewUniqueID()
		actorContext := NewLocalActorContext(workerID)

		executor.RegisterActorContext(actorID, actorContext)

		retrieved, ok := executor.GetActorContextByID(actorID)
		assert.True(t, ok)
		assert.Equal(t, actorContext, retrieved)

		// Non-existent actor
		nonExistentID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 2)
		_, ok = executor.GetActorContextByID(nonExistentID)
		assert.False(t, ok)
	})
}

// TestResolveByRefArgs verifies that pass-by-reference arguments are inlined
// from the object store into pass-by-value arguments before execution, which is
// what allows a by-ref argument (an object larger than the by-value threshold)
// to be deserialized by the wrapped function.
func TestResolveByRefArgs(t *testing.T) {
	functionMgr := function.NewFunctionManager(nil)
	actorMgr := NewActorConcurrencyGroupManager()
	store := objectstore.NewLocalModeObjectStore()
	executor := NewLocalModeTaskExecutor(functionMgr, actorMgr, store)

	payload := []byte("payload-larger-than-the-by-value-threshold")
	storedID, err := store.PutRaw(object.NewNativeRayObject(payload, nil))
	require.NoError(t, err)

	byRefArg := function.NewFunctionArgByRef(*storedID, nil)
	byValueArg := function.NewFunctionArgByValue([]byte{1}, []byte{2})

	resolved, err := executor.resolveByRefArgs([]function.FunctionArg{byRefArg, byValueArg})
	require.NoError(t, err)
	require.Len(t, resolved, 2)

	// The by-ref argument is now a by-value argument carrying the stored bytes.
	require.True(t, resolved[0].IsPassByValue())
	require.NotNil(t, resolved[0].Data)
	assert.Equal(t, payload, resolved[0].Data.Data)

	// Pass-by-value arguments are forwarded untouched.
	assert.Equal(t, byValueArg, resolved[1])
}
