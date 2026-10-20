// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actor

import (
	"sync/atomic"
	"testing"
)

// TestRunActorShutdownCallbackUnsetNoop verifies that running the global actor
// shutdown callback with no registration is a safe no-op (the default for the
// Go worker, which has no actor shutdown hook registered).
func TestRunActorShutdownCallbackUnsetNoop(t *testing.T) {
	SetActorShutdownCallback(nil)
	RunActorShutdownCallback() // must not panic or block
}

// TestRunActorShutdownCallbackInvokesRegistered verifies that a registered
// callback is dispatched exactly once by RunActorShutdownCallback.
func TestRunActorShutdownCallbackInvokesRegistered(t *testing.T) {
	var calls atomic.Int32
	SetActorShutdownCallback(func() { calls.Add(1) })

	RunActorShutdownCallback()
	RunActorShutdownCallback()

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected registered actor shutdown callback to run twice, got %d", got)
	}
}

// TestRunActorShutdownCallbackClearedNoop verifies that clearing the
// registration with nil turns RunActorShutdownCallback back into a no-op:
// the callback is not invoked and no panic is raised (a nil function pointer
// must never be dispatched).
func TestRunActorShutdownCallbackClearedNoop(t *testing.T) {
	var calls atomic.Int32
	SetActorShutdownCallback(func() { calls.Add(1) })
	SetActorShutdownCallback(nil)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("dispatch after clear must not panic, got %v", r)
			}
		}()
		RunActorShutdownCallback()
	}()

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected cleared actor shutdown callback to be a no-op, got %d calls", got)
	}
}

// TestRunActorShutdownCallbackPanicRecovered verifies that a panicking
// callback is recovered and does not propagate across the C++/cgo boundary
// (which would abort the worker during graceful shutdown).
func TestRunActorShutdownCallbackPanicRecovered(t *testing.T) {
	SetActorShutdownCallback(func() { panic("shutdown hook failure") })
	RunActorShutdownCallback() // must not panic
}
