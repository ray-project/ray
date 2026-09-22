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

package local_mode

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// actorConstructorMethodName is the reserved method name of an actor
// constructor descriptor (see function.NewGoActorMethodDescriptorOrUnknown).
// Local mode does not implement the actor-constructor registrar contract, so
// constructors are skipped while synchronizing user functions.
const actorConstructorMethodName = "<init>"

// registerUserFunctions synchronizes functions registered in the global
// function.Registry (via api.RegisterFunction / api.Remote in the same process)
// into the local runtime's FunctionManager, so task execution can look them up.
// Local mode runs driver and tasks in the same process, so no plugin loading is
// needed. It is safe to call repeatedly: RegisterFunction overwrites by
// descriptor key, so a function registered after a previous sync is added and
// already-known functions are re-wrapped at their current registration.
func registerUserFunctions(funcMgr *function.FunctionManager) error {
	if funcMgr == nil {
		return fmt.Errorf("function manager is nil")
	}

	entries, hasFuncs := function.Registry.ListEntries()
	if !hasFuncs {
		return nil
	}

	for _, entry := range entries {
		desc := entry.Descriptor()
		// Skip actor "<init>" constructors; local mode does not support actor
		// creation through the function registry.
		if desc.MethodName() == actorConstructorMethodName {
			continue
		}
		wrapped := function.WrapGoFunction(entry.Function())
		if err := funcMgr.RegisterFunction(desc, wrapped); err != nil {
			return fmt.Errorf("failed to register function %s: %w", desc.String(), err)
		}
	}
	return nil
}

// syncFunctionsFromRegistry re-synchronizes user functions registered since the
// last sync, right before a task spec is submitted. api.Remote registers lazily
// at call time, so a function registered after the runtime Start()ed must become
// visible before that function's task executes.
//
// The registry version only advances when a function is actually (re)registered,
// so the common path (no new registrations) skips the sync entirely instead of
// re-wrapping every function on every submission. The version is captured before
// the sync and stored after it returns: if a function is registered concurrently
// while this sync runs, the stored version stays stale and the next submission
// re-syncs (idempotent) instead of permanently missing the new function.
func (s *LocalModeTaskSubmitter) syncFunctionsFromRegistry() {
	if s.functionMgr == nil {
		return
	}

	version := function.Registry.Version()
	if version == s.lastSyncedVersion.Load() {
		return
	}

	if err := registerUserFunctions(s.functionMgr); err != nil {
		// Registration failure is not fatal: this submission still proceeds and
		// task execution surfaces a clearer "function not registered" error if
		// the function is really needed. The version is deliberately not
		// recorded so the next submission retries the sync.
		return
	}
	s.lastSyncedVersion.Store(version)
}
