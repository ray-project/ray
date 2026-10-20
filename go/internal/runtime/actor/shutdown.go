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

	"github.com/ray-project/ray/go/pkg/log"
)

// Global actor-exit teardown hook used by frameworks running Go actors.
//
// The C++ core worker wires its actor_shutdown_callback (CoreWorkerOptions) to
// GoTriggerActorShutdown, which is exported from the cgo package and dispatches
// the callback registered here. This mirrors the C++ worker's global actor exit
// callback pattern: a framework (e.g. a
// serve-like replica) can register its death-path teardown here so it runs on
// the core worker main thread during the graceful-exit sequence, BEFORE the
// core worker tears down its io_service / gRPC / GCS.
//
// Exactly one registrar is expected per process (a Go worker runs at most one
// actor manager); last-write-wins is acceptable. Clearing with nil is a
// defensive no-op on dispatch.

// actorShutdownCallback holds the registered global actor-exit teardown hook.
// It is a single-slot, last-write-wins registry: a Go worker runs at most one
// actor manager, so an atomic pointer is sufficient (see native_runtime.go).
var actorShutdownCallback atomic.Pointer[func()]

// SetActorShutdownCallback registers the global actor-exit teardown callback.
// Passing nil clears the registration (dispatch becomes a no-op).
func SetActorShutdownCallback(callback func()) {
	if callback == nil {
		actorShutdownCallback.Store(nil)
		return
	}
	actorShutdownCallback.Store(&callback)
}

// RunActorShutdownCallback dispatches the registered callback, if any. It is
// invoked from the C++ actor_shutdown_callback on the core worker main thread
// during graceful shutdown. It is a no-op when no callback is registered. A
// panicking callback is recovered and logged so a teardown failure cannot abort
// the worker's graceful-shutdown path.
//
// The registered callback runs synchronously on the core worker main thread
// and must return quickly without blocking (no unbounded waits on locks or
// remote calls), otherwise it delays the worker's graceful exit.
func RunActorShutdownCallback() {
	callback := actorShutdownCallback.Load()
	if callback == nil || *callback == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Log.Error(nil, "actor shutdown callback panicked", "panic", r)
		}
	}()
	(*callback)()
}
