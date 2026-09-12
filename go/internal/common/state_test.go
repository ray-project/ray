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

package common

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

func TestNewGlobalState(t *testing.T) {
	state := NewGlobalState()

	assert.NotNil(t, state)
	assert.Nil(t, state.gcsOptions)
	assert.Nil(t, state.globalStateAccessor)
}

func TestGlobalState_InitializeGlobalState(t *testing.T) {
	t.Run("normal initialization", func(t *testing.T) {
		state := NewGlobalState()

		opts := &gcs.ClientOptions{
			Address:   "127.0.0.1:6379",
			ClusterID: ids.NewClusterID(),
			TimeoutMs: 10000,
		}

		state.InitializeGlobalState(opts)

		assert.Equal(t, opts, state.gcsOptions)
	})

	t.Run("repeated initialization", func(t *testing.T) {
		state := NewGlobalState()

		opts1 := &gcs.ClientOptions{
			Address:   "127.0.0.1:6379",
			ClusterID: ids.NewClusterID(),
		}
		opts2 := &gcs.ClientOptions{
			Address:   "192.168.1.1:6379",
			ClusterID: ids.NewClusterID(),
		}

		state.InitializeGlobalState(opts1)
		assert.Equal(t, opts1, state.gcsOptions)

		state.InitializeGlobalState(opts2)
		assert.Equal(t, opts2, state.gcsOptions)
	})

	t.Run("nil argument", func(t *testing.T) {
		state := NewGlobalState()
		state.InitializeGlobalState(nil)
		assert.Nil(t, state.gcsOptions)
	})
}

func TestGlobalState_ConnectAndGetAccessor(t *testing.T) {
	t.Run("call returns error when not initialized", func(t *testing.T) {
		state := NewGlobalState()

		accessor, err := state.ConnectAndGetAccessor()

		assert.Error(t, err)
		assert.Nil(t, accessor)
		assert.Contains(t, err.Error(), "Ray has not been started yet")
	})

	t.Run("get accessor fails (global accessor not set)", func(t *testing.T) {
		state := NewGlobalState()
		opts := &gcs.ClientOptions{
			Address:   "127.0.0.1:6379",
			ClusterID: ids.NewClusterID(),
		}
		state.InitializeGlobalState(opts)

		// Without a global accessor set, GetGlobalStateAccessor returns ErrNotImplemented.
		accessor, err := state.ConnectAndGetAccessor()
		assert.Error(t, err)
		assert.Nil(t, accessor)
		assert.Equal(t, gcs.ErrNotImplemented, err)
	})
}

func TestGlobalState_Disconnect(t *testing.T) {
	t.Run("disconnect when not initialized", func(t *testing.T) {
		state := NewGlobalState()

		err := state.Disconnect()

		assert.NoError(t, err)
		assert.Nil(t, state.gcsOptions)
		assert.Nil(t, state.globalStateAccessor)
	})
}

func TestGlobalState_AddWorker(t *testing.T) {
	t.Run("get accessor fails", func(t *testing.T) {
		state := NewGlobalState()

		workerID := ids.NewWorkerID()
		workerType := proto.WorkerType_DRIVER
		workerInfo := map[string]string{"key": "value"}

		err := state.AddWorker(workerID, workerType, workerInfo)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get GCS accessor")
	})
}

func TestGlobalState_Concurrency(t *testing.T) {
	t.Run("concurrent InitializeGlobalState calls", func(t *testing.T) {
		state := NewGlobalState()

		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(id int) {
				opts := &gcs.ClientOptions{
					Address:   "127.0.0.1:6379",
					ClusterID: ids.NewClusterID(),
				}
				state.InitializeGlobalState(opts)
				done <- true
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}

		assert.NotNil(t, state.gcsOptions)
	})
}

func BenchmarkGlobalState_InitializeGlobalState(b *testing.B) {
	state := NewGlobalState()
	opts := &gcs.ClientOptions{
		Address:   "127.0.0.1:6379",
		ClusterID: ids.NewClusterID(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.InitializeGlobalState(opts)
	}
}
