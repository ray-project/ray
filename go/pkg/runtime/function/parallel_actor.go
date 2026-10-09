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

package function

// ParallelActorExecutor is the fixed actor-type identifier for the parallel
// actor wrapper executor.
//
// It is registered as the wrapper actor's actor type. When the worker receives
// an ACTOR_CREATION task for its <init> descriptor, it constructs a
// ParallelActorExecutor (which internally constructs N user instances), and all
// method calls are dispatched to its Execute method. This mirrors Java's
// ParallelActorExecutorImpl (the wrapper actor type is the executor itself).
//
// Both go/pkg (driver) and go/internal (worker) must share these constants and
// constructors so that the descriptor key built on the submitting side exactly
// matches the key recognized on the worker side.
const (
	// ParallelActorExecutorModule is the module path of the package that holds
	// ParallelActorExecutor.
	ParallelActorExecutorModule = "github.com/ray-project/ray/go/internal/runtime/actor"
	// ParallelActorExecutorPackage is the package name that holds
	// ParallelActorExecutor.
	ParallelActorExecutorPackage = "actor"
	// ParallelActorExecutorActorType is the actor type name of
	// ParallelActorExecutor.
	ParallelActorExecutorActorType = "ParallelActorExecutor"
)

// NewParallelActorExecutorDescriptor returns the fixed <init> descriptor of the
// parallel actor wrapper executor. The submitting side
// (go/pkg/runtime/api.ParallelActor) uses it to create the wrapper actor, and
// the worker side (go/internal/runtime/actor.ActorManager) uses it to recognize
// and construct the executor.
func NewParallelActorExecutorDescriptor() *GoFunctionDescriptor {
	return NewGoActorMethodDescriptorOrUnknown(
		ParallelActorExecutorModule,
		ParallelActorExecutorPackage,
		ParallelActorExecutorActorType,
		ConstructorName,
	)
}
