// Copyright 2026 The Ray Authors.
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

package instance_manager

import (
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestInstanceUtil_IsRayPending tests the IsRayPending method.
func TestInstanceUtil_IsRayPending(t *testing.T) {
	iu := &instanceUtil{}

	t.Run("QUEUED status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayPending(proto.Instance_QUEUED))
	})

	t.Run("REQUESTED status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayPending(proto.Instance_REQUESTED))
	})

	t.Run("ALLOCATED status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayPending(proto.Instance_ALLOCATED))
	})

	t.Run("RAY_INSTALLING status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayPending(proto.Instance_RAY_INSTALLING))
	})

	t.Run("RAY_RUNNING status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_RAY_RUNNING))
	})

	t.Run("RAY_STOP_REQUESTED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_RAY_STOP_REQUESTED))
	})

	t.Run("RAY_STOPPING status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_RAY_STOPPING))
	})

	t.Run("RAY_STOPPED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_RAY_STOPPED))
	})

	t.Run("TERMINATING status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_TERMINATING))
	})

	t.Run("TERMINATED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_TERMINATED))
	})

	t.Run("ALLOCATION_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_ALLOCATION_FAILED))
	})

	t.Run("ALLOCATION_TIMEOUT status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_ALLOCATION_TIMEOUT))
	})

	t.Run("RAY_INSTALL_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_RAY_INSTALL_FAILED))
	})

	t.Run("TERMINATION_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_TERMINATION_FAILED))
	})

	t.Run("UNKNOWN status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayPending(proto.Instance_UNKNOWN))
	})
}

// TestInstanceUtil_IsRayRunning tests the IsRayRunning method.
func TestInstanceUtil_IsRayRunning(t *testing.T) {
	iu := &instanceUtil{}

	t.Run("RAY_RUNNING status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayRunning(proto.Instance_RAY_RUNNING))
	})

	t.Run("RAY_STOP_REQUESTED status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayRunning(proto.Instance_RAY_STOP_REQUESTED))
	})

	t.Run("RAY_STOPPING status returns true", func(t *testing.T) {
		assert.True(t, iu.IsRayRunning(proto.Instance_RAY_STOPPING))
	})

	t.Run("QUEUED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_QUEUED))
	})

	t.Run("REQUESTED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_REQUESTED))
	})

	t.Run("ALLOCATED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_ALLOCATED))
	})

	t.Run("RAY_INSTALLING status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_RAY_INSTALLING))
	})

	t.Run("RAY_STOPPED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_RAY_STOPPED))
	})

	t.Run("TERMINATING status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_TERMINATING))
	})

	t.Run("TERMINATED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_TERMINATED))
	})

	t.Run("ALLOCATION_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_ALLOCATION_FAILED))
	})

	t.Run("ALLOCATION_TIMEOUT status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_ALLOCATION_TIMEOUT))
	})

	t.Run("RAY_INSTALL_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_RAY_INSTALL_FAILED))
	})

	t.Run("TERMINATION_FAILED status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_TERMINATION_FAILED))
	})

	t.Run("UNKNOWN status returns false", func(t *testing.T) {
		assert.False(t, iu.IsRayRunning(proto.Instance_UNKNOWN))
	})
}

// TestInstanceUtil_getValidTransitions tests the getValidTransitions method.
func TestInstanceUtil_getValidTransitions(t *testing.T) {
	iu := &instanceUtil{}
	transitions := iu.getValidTransitions()

	t.Run("QUEUED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_QUEUED], proto.Instance_REQUESTED)
		assert.Len(t, transitions[proto.Instance_QUEUED], 1)
	})

	t.Run("REQUESTED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_REQUESTED], proto.Instance_ALLOCATED)
		assert.Contains(t, transitions[proto.Instance_REQUESTED], proto.Instance_QUEUED)
		assert.Contains(t, transitions[proto.Instance_REQUESTED], proto.Instance_ALLOCATION_FAILED)
		assert.Len(t, transitions[proto.Instance_REQUESTED], 3)
	})

	t.Run("ALLOCATED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_RAY_INSTALLING)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_RAY_RUNNING)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_ALLOCATION_TIMEOUT)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_RAY_STOPPING)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_RAY_STOPPED)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_ALLOCATED], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_ALLOCATED], 7)
	})

	t.Run("RAY_INSTALLING status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALLING], proto.Instance_RAY_RUNNING)
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALLING], proto.Instance_RAY_INSTALL_FAILED)
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALLING], proto.Instance_RAY_STOPPED)
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALLING], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALLING], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_INSTALLING], 5)
	})

	t.Run("RAY_RUNNING status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_RUNNING], proto.Instance_RAY_STOP_REQUESTED)
		assert.Contains(t, transitions[proto.Instance_RAY_RUNNING], proto.Instance_RAY_STOPPING)
		assert.Contains(t, transitions[proto.Instance_RAY_RUNNING], proto.Instance_RAY_STOPPED)
		assert.Contains(t, transitions[proto.Instance_RAY_RUNNING], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_RAY_RUNNING], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_RUNNING], 5)
	})

	t.Run("RAY_STOP_REQUESTED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_STOP_REQUESTED], proto.Instance_RAY_STOPPING)
		assert.Contains(t, transitions[proto.Instance_RAY_STOP_REQUESTED], proto.Instance_RAY_STOPPED)
		assert.Contains(t, transitions[proto.Instance_RAY_STOP_REQUESTED], proto.Instance_RAY_RUNNING)
		assert.Contains(t, transitions[proto.Instance_RAY_STOP_REQUESTED], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_STOP_REQUESTED], 4)
	})

	t.Run("ALLOCATION_TIMEOUT status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_ALLOCATION_TIMEOUT], proto.Instance_TERMINATING)
		assert.Len(t, transitions[proto.Instance_ALLOCATION_TIMEOUT], 1)
	})

	t.Run("RAY_STOPPING status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_STOPPING], proto.Instance_RAY_STOPPED)
		assert.Contains(t, transitions[proto.Instance_RAY_STOPPING], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_RAY_STOPPING], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_STOPPING], 3)
	})

	t.Run("RAY_STOPPED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_STOPPED], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_RAY_STOPPED], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_STOPPED], 2)
	})

	t.Run("TERMINATING status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_TERMINATING], proto.Instance_TERMINATED)
		assert.Contains(t, transitions[proto.Instance_TERMINATING], proto.Instance_TERMINATION_FAILED)
		assert.Len(t, transitions[proto.Instance_TERMINATING], 2)
	})

	t.Run("TERMINATION_FAILED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_TERMINATION_FAILED], proto.Instance_TERMINATING)
		assert.Len(t, transitions[proto.Instance_TERMINATION_FAILED], 1)
	})

	t.Run("TERMINATED status transitions", func(t *testing.T) {
		assert.Empty(t, transitions[proto.Instance_TERMINATED])
	})

	t.Run("ALLOCATION_FAILED status transitions", func(t *testing.T) {
		assert.Empty(t, transitions[proto.Instance_ALLOCATION_FAILED])
	})

	t.Run("RAY_INSTALL_FAILED status transitions", func(t *testing.T) {
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALL_FAILED], proto.Instance_TERMINATING)
		assert.Contains(t, transitions[proto.Instance_RAY_INSTALL_FAILED], proto.Instance_TERMINATED)
		assert.Len(t, transitions[proto.Instance_RAY_INSTALL_FAILED], 2)
	})

	t.Run("UNKNOWN status transitions", func(t *testing.T) {
		assert.Empty(t, transitions[proto.Instance_UNKNOWN])
	})
}

// TestInstanceUtil_computeReachable tests the computeReachable method.
func TestInstanceUtil_computeReachable(t *testing.T) {
	iu := &instanceUtil{}
	iu.computeReachable()

	t.Run("QUEUED status reaches all non-terminal statuses", func(t *testing.T) {
		reachable := iu.reachableFrom[proto.Instance_QUEUED]
		assert.True(t, reachable[proto.Instance_REQUESTED])
		assert.True(t, reachable[proto.Instance_ALLOCATED])
		assert.True(t, reachable[proto.Instance_RAY_INSTALLING])
		assert.True(t, reachable[proto.Instance_RAY_RUNNING])
		assert.True(t, reachable[proto.Instance_RAY_STOP_REQUESTED])
		assert.True(t, reachable[proto.Instance_RAY_STOPPING])
		assert.True(t, reachable[proto.Instance_RAY_STOPPED])
		assert.True(t, reachable[proto.Instance_TERMINATING])
		assert.True(t, reachable[proto.Instance_TERMINATED])
		assert.True(t, reachable[proto.Instance_ALLOCATION_FAILED])
		assert.True(t, reachable[proto.Instance_ALLOCATION_TIMEOUT])
		assert.True(t, reachable[proto.Instance_RAY_INSTALL_FAILED])
		assert.True(t, reachable[proto.Instance_TERMINATION_FAILED])
	})

	t.Run("RAY_RUNNING status reaches the stop and terminate related statuses", func(t *testing.T) {
		reachable := iu.reachableFrom[proto.Instance_RAY_RUNNING]
		assert.True(t, reachable[proto.Instance_RAY_STOP_REQUESTED])
		assert.True(t, reachable[proto.Instance_RAY_STOPPING])
		assert.True(t, reachable[proto.Instance_RAY_STOPPED])
		assert.True(t, reachable[proto.Instance_TERMINATING])
		assert.True(t, reachable[proto.Instance_TERMINATED])
		assert.False(t, reachable[proto.Instance_QUEUED])
		assert.False(t, reachable[proto.Instance_REQUESTED])
		assert.False(t, reachable[proto.Instance_ALLOCATED])
		assert.False(t, reachable[proto.Instance_RAY_INSTALLING])
	})

	t.Run("TERMINATED status reaches no other status", func(t *testing.T) {
		reachable := iu.reachableFrom[proto.Instance_TERMINATED]
		assert.Empty(t, reachable)
	})

	t.Run("ALLOCATION_FAILED status reaches no other status", func(t *testing.T) {
		reachable := iu.reachableFrom[proto.Instance_ALLOCATION_FAILED]
		assert.Empty(t, reachable)
	})

	t.Run("TERMINATION_FAILED status reaches TERMINATING and TERMINATED", func(t *testing.T) {
		reachable := iu.reachableFrom[proto.Instance_TERMINATION_FAILED]
		assert.True(t, reachable[proto.Instance_TERMINATING])
		assert.True(t, reachable[proto.Instance_TERMINATED])
	})
}

// TestInstanceUtil_getReachableStatuses tests the getReachableStatuses method.
func TestInstanceUtil_getReachableStatuses(t *testing.T) {
	iu := &instanceUtil{}

	t.Run("the first call triggers computeReachable", func(t *testing.T) {
		// Make sure the cache is not initialized.
		iu.reachableFrom = nil

		// The first call should trigger computeReachable and initialize the
		// cache.
		reachable := iu.getReachableStatuses(proto.Instance_QUEUED)
		assert.NotNil(t, reachable)
		assert.NotEmpty(t, reachable)

		// Verify the cache is initialized.
		assert.NotNil(t, iu.reachableFrom)
	})

	t.Run("repeated calls reuse the cache", func(t *testing.T) {
		// First call.
		reachable1 := iu.getReachableStatuses(proto.Instance_RAY_RUNNING)

		// The second call should return the same cached result.
		reachable2 := iu.getReachableStatuses(proto.Instance_RAY_RUNNING)
		assert.Equal(t, reachable1, reachable2)
	})

	t.Run("UNKNOWN status returns an empty set", func(t *testing.T) {
		reachable := iu.getReachableStatuses(proto.Instance_UNKNOWN)
		assert.Empty(t, reachable)
	})

	t.Run("TERMINATED status returns an empty set", func(t *testing.T) {
		reachable := iu.getReachableStatuses(proto.Instance_TERMINATED)
		assert.Empty(t, reachable)
	})
}

// BenchmarkInstanceUtil_IsRayPending benchmarks the IsRayPending performance.
func BenchmarkInstanceUtil_IsRayPending(b *testing.B) {
	iu := &instanceUtil{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iu.IsRayPending(proto.Instance_QUEUED)
	}
}

// BenchmarkInstanceUtil_IsRayRunning benchmarks the IsRayRunning performance.
func BenchmarkInstanceUtil_IsRayRunning(b *testing.B) {
	iu := &instanceUtil{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iu.IsRayRunning(proto.Instance_RAY_RUNNING)
	}
}

// BenchmarkInstanceUtil_getValidTransitions benchmarks the getValidTransitions
// performance.
func BenchmarkInstanceUtil_getValidTransitions(b *testing.B) {
	iu := &instanceUtil{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iu.getValidTransitions()
	}
}

// BenchmarkInstanceUtil_computeReachable benchmarks the computeReachable
// performance.
func BenchmarkInstanceUtil_computeReachable(b *testing.B) {
	iu := &instanceUtil{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iu.computeReachable()
	}
}
