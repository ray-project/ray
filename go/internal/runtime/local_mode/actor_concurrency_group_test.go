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
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActorConcurrencyGroup(t *testing.T) {
	t.Run("CreateWithDefaultConcurrency", func(t *testing.T) {
		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
		group := NewActorConcurrencyGroup(actorID, 0)
		require.NotNil(t, group)
		assert.Equal(t, 1, group.maxConcurrency)
		group.Shutdown()
	})

	t.Run("CreateWithCustomConcurrency", func(t *testing.T) {
		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 2)
		group := NewActorConcurrencyGroup(actorID, 3)
		require.NotNil(t, group)
		assert.Equal(t, 3, group.maxConcurrency)
		group.Shutdown()
	})

	t.Run("SubmitAndExecute", func(t *testing.T) {
		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 3)
		group := NewActorConcurrencyGroup(actorID, 1)

		var executed bool
		var mu sync.Mutex

		group.Submit(func() {
			mu.Lock()
			defer mu.Unlock()
			executed = true
		})

		// Wait for execution
		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		assert.True(t, executed)
		mu.Unlock()

		group.Shutdown()
	})

	t.Run("MultipleTasksSerialExecution", func(t *testing.T) {
		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 4)
		group := NewActorConcurrencyGroup(actorID, 1)

		var executionOrder []int
		var mu sync.Mutex

		for i := 0; i < 5; i++ {
			taskNum := i
			group.Submit(func() {
				mu.Lock()
				defer mu.Unlock()
				executionOrder = append(executionOrder, taskNum)
				time.Sleep(1 * time.Millisecond)
			})
		}

		// Wait for all tasks to complete
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		assert.Len(t, executionOrder, 5)
		// With concurrency 1, tasks should execute in order
		assert.Equal(t, []int{0, 1, 2, 3, 4}, executionOrder)
		mu.Unlock()

		group.Shutdown()
	})

	t.Run("Shutdown", func(t *testing.T) {
		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 5)
		group := NewActorConcurrencyGroup(actorID, 2)

		// Submit a task
		executed := make(chan bool, 1)
		group.Submit(func() {
			executed <- true
		})

		// Wait for task to execute
		select {
		case <-executed:
			// Task executed
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Task did not execute")
		}

		// Shutdown should not panic
		group.Shutdown()

		// Second shutdown should be safe (idempotent)
		group.Shutdown()
	})
}

func TestActorConcurrencyGroupManager(t *testing.T) {
	t.Run("CreateManager", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()
		require.NotNil(t, mgr)
	})

	t.Run("GetOrCreateGroup", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()

		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 10)
		group1 := mgr.GetOrCreateGroup(actorID, 2)
		require.NotNil(t, group1)

		// Get the same group again
		group2 := mgr.GetOrCreateGroup(actorID, 2)
		assert.Equal(t, group1, group2)

		mgr.Shutdown()
	})

	t.Run("GetNonExistentGroup", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()

		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 11)
		group := mgr.GetGroup(actorID)
		assert.Nil(t, group)

		mgr.Shutdown()
	})

	t.Run("RemoveGroup", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()

		actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 12)
		group := mgr.GetOrCreateGroup(actorID, 2)
		require.NotNil(t, group)

		// Remove the group
		mgr.RemoveGroup(actorID)

		// Group should be gone
		group2 := mgr.GetGroup(actorID)
		assert.Nil(t, group2)

		mgr.Shutdown()
	})

	t.Run("Shutdown", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()

		// Create multiple groups
		for i := 0; i < 5; i++ {
			actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), uint64(13+i))
			mgr.GetOrCreateGroup(actorID, 2)
		}

		// Shutdown should not panic
		mgr.Shutdown()

		// All groups should be removed
		assert.Empty(t, mgr.groups)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		mgr := NewActorConcurrencyGroupManager()

		var wg sync.WaitGroup
		numGoroutines := 10

		// Concurrently create groups
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), uint64(100+i))
				group := mgr.GetOrCreateGroup(actorID, 2)
				require.NotNil(t, group)
			}(i)
		}

		wg.Wait()

		// Should have created numGoroutines groups
		mgr.mu.RLock()
		assert.Len(t, mgr.groups, numGoroutines)
		mgr.mu.RUnlock()

		mgr.Shutdown()
	})
}
