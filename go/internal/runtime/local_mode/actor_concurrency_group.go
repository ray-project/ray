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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
)

// submitRetryBackoff is the pause between retries when the execution queue is
// full. It bounds the backoff for submitter goroutines while letting the
// dispatcher drain, without burning CPU.
const submitRetryBackoff = time.Millisecond

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
	workers        []chan func()
	shutdownOnce   sync.Once
	stopped        atomic.Bool
	submitMu       sync.Mutex
	dispatcherDone chan struct{}
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
		workers:        make([]chan func(), maxConcurrency),
		dispatcherDone: make(chan struct{}),
	}

	// Start worker goroutines
	for i := 0; i < maxConcurrency; i++ {
		group.workers[i] = make(chan func(), 1)
		go func(workerChan chan func()) {
			for task := range workerChan {
				task()
			}
		}(group.workers[i])
	}

	// Start dispatcher goroutine. It drains the execution queue, then closes
	// the worker channels itself so no goroutine can send to a worker channel
	// that has already been closed (send-on-closed-channel race with Shutdown).
	go func() {
		defer close(group.dispatcherDone)
		workerIdx := 0
		for task := range group.executionQueue {
			// Round-robin dispatch to worker goroutines
			group.workers[workerIdx] <- task
			workerIdx = (workerIdx + 1) % maxConcurrency
		}
		for _, workerChan := range group.workers {
			close(workerChan)
		}
	}()

	return group
}

// Submit enqueues a task for execution. It blocks until the dispatcher
// accepts the task so tasks are never silently dropped, and reports false
// only when the group is shutting down (the caller must not wait on the
// task's completion signal in that case). When the buffered queue is full the
// caller sleeps briefly and retries (bounded backoff) instead of spinning, so
// the dispatcher can drain without burning CPU. The lock is never held while
// blocking: each send attempt is non-blocking and the lock is released between
// attempts, so a full queue cannot deadlock a concurrent Shutdown (which
// needs the lock to close the queue) or another Submit.
func (g *ActorConcurrencyGroup) Submit(task func()) bool {
	if g.stopped.Load() {
		return false
	}
	for {
		g.submitMu.Lock()
		if g.stopped.Load() {
			g.submitMu.Unlock()
			return false
		}
		select {
		case g.executionQueue <- task:
			g.submitMu.Unlock()
			return true
		default:
			// Queue is full: release the lock and back off so the dispatcher
			// can drain and a concurrent Shutdown can proceed, then retry.
			g.submitMu.Unlock()
			time.Sleep(submitRetryBackoff)
		}
	}
}

// Shutdown gracefully shuts down the concurrency group. Tasks already queued
// are drained and executed before the worker goroutines exit. Concurrent
// Submit calls either complete their send before the queue is closed or are
// rejected (Submit returns false) once the group is marked stopped.
func (g *ActorConcurrencyGroup) Shutdown() {
	g.shutdownOnce.Do(func() {
		g.stopped.Store(true)
		g.submitMu.Lock()
		close(g.executionQueue)
		g.submitMu.Unlock()
		<-g.dispatcherDone
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
	groups map[string]*ActorConcurrencyGroup
	mu     sync.RWMutex
}

// groupKeySeparator joins the actor ID and a named group in the flat map key.
// It is shared between groupKey (key construction) and RemoveGroup (prefix
// matching) so the key format has a single source of truth.
const groupKeySeparator = "/"

// groupKey builds the map key for a concurrency group. The default group of an
// actor is keyed by the actor ID alone; a named group is keyed by the synthetic
// composite "actorID/groupName". This expresses named groups on the existing
// single-level map without restructuring into a per-actor multi-group map.
func groupKey(actorID ids.ActorID, groupName string) string {
	if groupName == "" {
		return actorID.String()
	}
	return actorID.String() + groupKeySeparator + groupName
}

// NewActorConcurrencyGroupManager creates a new ActorConcurrencyGroupManager.
func NewActorConcurrencyGroupManager() *ActorConcurrencyGroupManager {
	return &ActorConcurrencyGroupManager{
		groups: make(map[string]*ActorConcurrencyGroup),
	}
}

// GetOrCreateGroup gets or creates the default concurrency group for an actor.
func (m *ActorConcurrencyGroupManager) GetOrCreateGroup(actorID ids.ActorID, maxConcurrency int) *ActorConcurrencyGroup {
	return m.GetOrCreateNamedGroup(actorID, "", maxConcurrency)
}

// GetOrCreateNamedGroup gets or creates the named concurrency group for an
// actor with the given max concurrency. An empty groupName resolves to the
// actor's default group. Per-actor multi-group scheduling can be extended in
// the future by routing more callers through this entry point while the
// underlying storage stays a single map.
func (m *ActorConcurrencyGroupManager) GetOrCreateNamedGroup(actorID ids.ActorID, groupName string, maxConcurrency int) *ActorConcurrencyGroup {
	return m.getOrCreate(groupKey(actorID, groupName), actorID, maxConcurrency)
}

func (m *ActorConcurrencyGroupManager) getOrCreate(key string, actorID ids.ActorID, maxConcurrency int) *ActorConcurrencyGroup {
	// First try read lock
	m.mu.RLock()
	if group, ok := m.groups[key]; ok {
		m.mu.RUnlock()
		return group
	}
	m.mu.RUnlock()

	// Need to create - use write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if group, ok := m.groups[key]; ok {
		return group
	}

	group := NewActorConcurrencyGroup(actorID, maxConcurrency)
	m.groups[key] = group
	return group
}

// GetGroup gets the default concurrency group by actor ID.
// Returns nil if the group doesn't exist.
func (m *ActorConcurrencyGroupManager) GetGroup(actorID ids.ActorID) *ActorConcurrencyGroup {
	return m.GetNamedGroup(actorID, "")
}

// GetNamedGroup looks up the named concurrency group for an actor without
// creating it. An empty groupName resolves to the actor's default group.
// Returns nil if the group doesn't exist.
func (m *ActorConcurrencyGroupManager) GetNamedGroup(actorID ids.ActorID, groupName string) *ActorConcurrencyGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groups[groupKey(actorID, groupName)]
}

// RemoveGroup removes and shuts down all concurrency groups of an actor
// (both the default group and any named groups derived from it).
func (m *ActorConcurrencyGroupManager) RemoveGroup(actorID ids.ActorID) {
	actorIDStr := actorID.String()
	prefix := actorIDStr + groupKeySeparator

	// Collect the groups to shut down and drop them from the map under the
	// lock, then call Shutdown outside the lock so a draining group does not
	// block concurrent lookups.
	var toShutdown []*ActorConcurrencyGroup
	m.mu.Lock()
	for key, group := range m.groups {
		if key == actorIDStr || strings.HasPrefix(key, prefix) {
			toShutdown = append(toShutdown, group)
			delete(m.groups, key)
		}
	}
	m.mu.Unlock()

	for _, group := range toShutdown {
		group.Shutdown()
	}
}

// Shutdown shuts down all concurrency groups.
func (m *ActorConcurrencyGroupManager) Shutdown() {
	m.mu.Lock()
	groups := make([]*ActorConcurrencyGroup, 0, len(m.groups))
	for key, group := range m.groups {
		groups = append(groups, group)
		delete(m.groups, key)
	}
	m.mu.Unlock()

	for _, group := range groups {
		group.Shutdown()
	}
}
