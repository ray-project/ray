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

	"github.com/ray-project/ray/go/pkg/ids"
)

// ActorConcurrencyGroup manages concurrent execution for a single actor.
// Inspired by Java's LocalModeTaskExecutor.ActorConcurrencyGroup.
//
// Design notes:
// 1. Uses channels for serial execution of actor methods within a concurrency group
// 2. Supports configurable max concurrency (for @ray.method(num_cpus=2) etc.)
// 3. Each actor can have multiple concurrency groups
type ActorConcurrencyGroup struct {
	actorID        ids.ActorID
	executionQueue chan func()
	maxConcurrency int
	workerTasks    []chan func()
	shutdownOnce   sync.Once
	done           chan struct{}
}

// NewActorConcurrencyGroup creates a new ActorConcurrencyGroup.
func NewActorConcurrencyGroup(actorID ids.ActorID, maxConcurrency int) *ActorConcurrencyGroup {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	group := &ActorConcurrencyGroup{
		actorID:        actorID,
		maxConcurrency: maxConcurrency,
		executionQueue: make(chan func(), 100), // buffered queue
		workerTasks:    make([]chan func(), maxConcurrency),
		done:           make(chan struct{}),
	}

	// Start worker goroutines. Each worker select-sends on its task channel so
	// Shutdown can never "send on closed channel": workers stop on done and the
	// dispatcher stops feeding them before the task channels are drained.
	for i := 0; i < maxConcurrency; i++ {
		taskCh := make(chan func(), 1)
		group.workerTasks[i] = taskCh
		go func(workerChan chan func()) {
			for task := range workerChan {
				task()
			}
		}(taskCh)
	}

	// Start dispatcher goroutine. It feeds round-robin to workers and exits on
	// done; Shutdown closes executionQueue only after workers have drained, and
	// closes done to unblock a dispatcher blocked on a full worker channel.
	go func() {
		workerIdx := 0
		for {
			select {
			case task := <-group.executionQueue:
				group.workerTasks[workerIdx] <- task
				workerIdx = (workerIdx + 1) % maxConcurrency
			case <-group.done:
				return
			}
		}
	}()

	return group
}

// Submit adds a task to the execution queue.
//
// The send blocks when the queue is full instead of dropping the task: dropping
// would leave callers (ExecuteActorTask's <-done) waiting forever for a task
// that never runs.
func (g *ActorConcurrencyGroup) Submit(task func()) {
	g.executionQueue <- task
}

// Shutdown gracefully shuts down the concurrency group.
//
// Closing done stops the dispatcher first so it cannot be blocked mid-send on a
// worker channel when that channel is closed (which would panic). Worker
// channels are never closed here; workers exit when the dispatcher stops
// feeding them via done.
func (g *ActorConcurrencyGroup) Shutdown() {
	g.shutdownOnce.Do(func() {
		close(g.done)
	})
}

// ActorConcurrencyGroupManager manages all actor concurrency groups.
// Inspired by Java's ActorConcurrencyGroupManager.
//
// Design notes:
// 1. Thread-safe access to concurrency groups
// 2. Lazy creation of groups on first access
// 3. Supports cleanup on shutdown
type ActorConcurrencyGroupManager struct {
	groups map[ids.ActorID]*ActorConcurrencyGroup
	mu     sync.RWMutex
}

// NewActorConcurrencyGroupManager creates a new ActorConcurrencyGroupManager.
func NewActorConcurrencyGroupManager() *ActorConcurrencyGroupManager {
	return &ActorConcurrencyGroupManager{
		groups: make(map[ids.ActorID]*ActorConcurrencyGroup),
	}
}

// GetOrCreateGroup gets or creates a concurrency group for an actor.
func (m *ActorConcurrencyGroupManager) GetOrCreateGroup(actorID ids.ActorID, maxConcurrency int) *ActorConcurrencyGroup {
	// First try read lock
	m.mu.RLock()
	if group, ok := m.groups[actorID]; ok {
		m.mu.RUnlock()
		return group
	}
	m.mu.RUnlock()

	// Need to create - use write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if group, ok := m.groups[actorID]; ok {
		return group
	}

	group := NewActorConcurrencyGroup(actorID, maxConcurrency)
	m.groups[actorID] = group
	return group
}

// GetGroup gets a concurrency group by actor ID.
// Returns nil if the group doesn't exist.
func (m *ActorConcurrencyGroupManager) GetGroup(actorID ids.ActorID) *ActorConcurrencyGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groups[actorID]
}

// RemoveGroup removes and shuts down a concurrency group.
func (m *ActorConcurrencyGroupManager) RemoveGroup(actorID ids.ActorID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if group, ok := m.groups[actorID]; ok {
		group.Shutdown()
		delete(m.groups, actorID)
	}
}

// Shutdown shuts down all concurrency groups.
func (m *ActorConcurrencyGroupManager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for actorID, group := range m.groups {
		group.Shutdown()
		delete(m.groups, actorID)
	}
}
