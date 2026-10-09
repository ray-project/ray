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
	"fmt"

	"github.com/ray-project/ray/go/internal/runtime/actor"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// registerUserFunctions synchronizes functions registered in the global
// function.Registry (via api.Remote / api.RegisterFunction / api.RegisterActorClass
// in the same process) into the local runtime, so task execution can look them
// up. Local mode runs driver and tasks in the same process, so no plugin
// loading is needed. It is safe to call repeatedly: RegisterFunction overwrites
// by descriptor key, so a function registered after a previous sync is added
// and already-known functions are re-wrapped at their current registration.
//
// Actor constructors (descriptor method name == function.ConstructorName) are
// registered into the executor's actor manager (NOT the FunctionManager),
// mirroring actor.ActorManager: a constructor returns a live actor instance
// rather than serialized data, so it is not wrapped by WrapGoFunction (which
// would try to serialize the returned struct). The constructor must be
// resolvable when an ACTOR_CREATION_TASK is executed, otherwise the actor is
// never created and dependent actor tasks wait forever.
func registerUserFunctions(executor *LocalModeTaskExecutor) error {
	if executor == nil || executor.functionMgr == nil {
		return fmt.Errorf("executor or function manager is nil")
	}

	entries, hasFuncs := api.GetRegisteredFunctions()
	if !hasFuncs {
		return nil
	}

	for _, entry := range entries {
		desc := entry.Descriptor()
		if desc.MethodName() == function.ConstructorName {
			executor.RegisterActorConstructor(desc, actor.WrapActorConstructor(entry.Function()))
			continue
		}
		wrapped := function.WrapGoFunction(entry.Function())
		if err := executor.functionMgr.RegisterFunction(desc, wrapped); err != nil {
			return fmt.Errorf("failed to register function %s: %w", desc.String(), err)
		}
	}
	return nil
}
