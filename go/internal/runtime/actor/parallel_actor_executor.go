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
	"fmt"
	"sync"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// ParallelActorExecutor is the wrapper actor executed on the worker side for a
// parallel actor. It is registered as an actor class whose actor type is the
// fixed "ParallelActorExecutor" descriptor (aligned with Java's
// ParallelActorExecutorImpl: the wrapper actor's type is the executor itself).
//
// At ACTOR_CREATION it constructs N user instances (keyed by instanceId); method
// calls are routed by instanceId through the PARALLEL_INSTANCE_<i> concurrency
// group and dispatched to the target instance via its Execute method.
type ParallelActorExecutor struct {
	instances []interface{}
	mu        sync.RWMutex
}

// NewParallelActorExecutor creates an empty executor. Instances are built later
// by ConstructInstances once the user constructor and parallelism are known.
func NewParallelActorExecutor() *ParallelActorExecutor {
	return &ParallelActorExecutor{}
}

// ConstructInstances builds N user instances by invoking userCtor once per
// instance with the given ctorArgs, storing them in a slice indexed by
// instanceId. userCtor must be the wrapped user actor constructor (the same
// func(args []function.FunctionArg) (interface{}, error) produced by
// WrapActorConstructor).
func (e *ParallelActorExecutor) ConstructInstances(
	n int,
	userCtor func(args []function.FunctionArg) (interface{}, error),
	ctorArgs []function.FunctionArg,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if n < 1 {
		return fmt.Errorf("parallel actor instance count must be >= 1, got %d", n)
	}
	if userCtor == nil {
		return fmt.Errorf("parallel actor user constructor is nil")
	}
	instances := make([]interface{}, n)
	for i := 0; i < n; i++ {
		inst, err := userCtor(ctorArgs)
		if err != nil {
			return fmt.Errorf("construct instance %d: %w", i, err)
		}
		if function.IsNilInstance(inst) {
			return fmt.Errorf("parallel actor constructor returned nil instance for instance %d", i)
		}
		instances[i] = inst
	}
	e.instances = instances
	return nil
}

// Execute dispatches a method call to instance instanceId, reusing
// CallActorMethod for the full actor-method semantics (parameter validation,
// deserialization, trailing-error convention and result serialization). The
// user method is resolved dynamically from the live instance.
func (e *ParallelActorExecutor) Execute(
	instanceId int,
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if instanceId < 0 || instanceId >= len(e.instances) {
		return nil, fmt.Errorf("parallel actor instance %d out of range [0,%d)", instanceId, len(e.instances))
	}
	inst := e.instances[instanceId]
	if function.IsNilInstance(inst) {
		return nil, fmt.Errorf("parallel actor instance %d is nil", instanceId)
	}
	return CallActorMethod(inst, desc, args)
}
