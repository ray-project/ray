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
