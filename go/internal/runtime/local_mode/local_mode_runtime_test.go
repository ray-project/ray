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

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalModeRuntime(t *testing.T) {
	t.Run("CreateRuntime", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)
		require.NotNil(t, runtime)
		assert.False(t, runtime.IsInitialized())
	})

	t.Run("StartRuntime", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)
		assert.True(t, runtime.IsInitialized())

		// Second start should fail
		err = runtime.Start()
		assert.Error(t, err)
	})

	t.Run("RunNoOp", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		// Run should be no-op
		err = runtime.Run()
		require.NoError(t, err)
	})

	t.Run("GetWorkerContext", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		ctx := runtime.WorkerContext()
		require.NotNil(t, ctx)

		// Check it's the local mode context
		localCtx, ok := ctx.(*LocalModeWorkerContext)
		assert.True(t, ok)
		assert.NotNil(t, localCtx)
	})

	t.Run("GetRunMode", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		assert.True(t, runtime.IsLocalMode())
	})

	t.Run("WasCurrentActorRestarted", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		// Should always return false in local mode
		assert.False(t, runtime.WasCurrentActorRestarted())
	})

	t.Run("GetAllNodeInfo", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		nodeInfos := runtime.GetAllNodeInfo()
		assert.Len(t, nodeInfos, 1)
		assert.Equal(t, "127.0.0.1", nodeInfos[0].NodeManagerAddress)
	})

	t.Run("GetAllActorInfo", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		actorInfos := runtime.GetAllActorInfo()
		assert.Empty(t, actorInfos)
	})

	t.Run("GetGpuIds", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		gpuIds := runtime.GetGpuIds()
		assert.Empty(t, gpuIds)
	})

	t.Run("GetCurrentActorHandle", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		handle := runtime.GetCurrentActorHandle()
		assert.Nil(t, handle)
	})

	t.Run("GetObjectStore", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		store := runtime.GetObjectStore()
		assert.NotNil(t, store)

		// Should be LocalModeObjectStore from objectstore package
		_, ok := store.(*objectstore.LocalModeObjectStore)
		assert.True(t, ok)
	})

	t.Run("GetTaskSubmitter", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		submitter := runtime.GetTaskSubmitter()
		assert.NotNil(t, submitter)
	})

	t.Run("Shutdown", func(t *testing.T) {
		opts := base.InitializeOptions{}
		runtime, err := NewLocalModeRuntime(opts)
		require.NoError(t, err)

		err = runtime.Start()
		require.NoError(t, err)

		err = runtime.Shutdown()
		require.NoError(t, err)
		assert.False(t, runtime.IsInitialized())

		// Second shutdown should be safe
		err = runtime.Shutdown()
		require.NoError(t, err)
	})
}
