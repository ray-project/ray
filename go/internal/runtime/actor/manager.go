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

	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// ActorManager manages actor instances and constructor registration for the
// current worker process.
//
// Design notes:
//  1. This is the Go counterpart of Java's TaskExecutor.ActorContext (which holds
//     the currentActor instance) and C++'s ActorManager. Because a single Go
//     worker process can serve multiple actors, instances are keyed by ActorID.
//  2. Actor constructors must be registered explicitly (via worker startup) since
//     Go has no runtime type lookup by name. Each constructor is keyed by the
//     actor's "<init>" descriptor key.
//  3. All maps are concurrency-safe: task execution can happen on multiple
//     goroutines (C++ calls GoExecuteTask concurrently, and local mode may run
//     actor tasks on different goroutines).
//
// It is shared by the native (cluster) and local runtimes so actor dispatch
// logic is maintained in one place.

// parallelActorExecutorDescKey is the precomputed String() key of the
// parallel-actor wrapper descriptor, compared against every actor creation
// descriptor in ConstructActor. It is built once at package init so the common
// (non-parallel) actor creation path does not rebuild the descriptor and
// format a key on every call.
var parallelActorExecutorDescKey = function.NewParallelActorExecutorDescriptor().String()

type ActorManager struct {
	// instances maps ActorID to the live actor instance.
	instances sync.Map // map[ids.ActorID]interface{}

	// constructors maps an actor "<init>" descriptor key to a constructor that
	// returns a fresh actor instance. Constructors live outside the regular
	// FunctionManager namespace because a constructor returns an actor
	// instance, not serialized data.
	constructors sync.Map // map[string]func(args []function.FunctionArg) (interface{}, error)
}

// NewActorManager creates a new ActorManager.
func NewActorManager() *ActorManager {
	return &ActorManager{}
}

// RegisterConstructor registers the constructor for an actor type, keyed by its
// "<init>" descriptor key.
func (m *ActorManager) RegisterConstructor(descKey string, ctor func(args []function.FunctionArg) (interface{}, error)) {
	if ctor == nil {
		return
	}
	m.constructors.Store(descKey, ctor)
}

// GetConstructor returns the constructor registered for an actor descriptor.
func (m *ActorManager) GetConstructor(desc *function.GoFunctionDescriptor) (func(args []function.FunctionArg) (interface{}, error), bool) {
	if desc == nil || desc.MethodName() != function.ConstructorName {
		return nil, false
	}
	ctor, ok := m.constructors.Load(desc.String())
	if !ok {
		return nil, false
	}
	fn, ok := ctor.(func(args []function.FunctionArg) (interface{}, error))
	return fn, ok
}

// SetInstance stores a constructed actor instance under its ActorID.
func (m *ActorManager) SetInstance(actorID ids.ActorID, instance interface{}) {
	m.instances.Store(actorID, instance)
}

// GetInstance returns the actor instance for an ActorID, if present.
func (m *ActorManager) GetInstance(actorID ids.ActorID) (interface{}, bool) {
	instance, ok := m.instances.Load(actorID)
	return instance, ok
}

// RemoveInstance removes an actor instance (e.g. on actor exit).
func (m *ActorManager) RemoveInstance(actorID ids.ActorID) {
	m.instances.Delete(actorID)
}

// ConstructActor invokes the registered constructor for the given actor
// descriptor and returns a fresh actor instance. The constructor arguments are
// passed by value (serialized), matching the pass-by-value convention used by
// the driver for actor creation.
func (m *ActorManager) ConstructActor(
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) (interface{}, error) {
	if desc == nil {
		return nil, fmt.Errorf("actor descriptor is nil")
	}
	// A parallel-actor wrapper actor is constructed by the executor, which builds
	// N user instances from the creation args ([parallelism, userCtorDesc, ...]).
	if desc.String() == parallelActorExecutorDescKey {
		return m.constructParallelActorExecutor(desc, args)
	}
	ctor, ok := m.GetConstructor(desc)
	if !ok {
		return nil, fmt.Errorf("actor constructor not registered: %s", desc.String())
	}
	instance, err := ctor(args)
	if err != nil {
		return nil, fmt.Errorf("failed to construct actor %s: %w", desc.String(), err)
	}
	if function.IsNilInstance(instance) {
		return nil, fmt.Errorf("actor constructor returned nil instance for %s", desc.String())
	}
	return instance, nil
}

// constructParallelActorExecutor builds a ParallelActorExecutor for a parallel
// actor's wrapper actor. The creation args carry
// [parallelism, userCtorDesc, userArgs...] (aligned with Java's
// ParallelActorExecutorImpl(int parallelism, JavaFunctionDescriptor ctorDesc)):
// parallelism and the user actor's <init> descriptor are deserialized from the
// first two args, then N user instances are constructed via the registered user
// constructor.
func (m *ActorManager) constructParallelActorExecutor(
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parallel actor creation expects [parallelism, userCtorDesc, userArgs...], got %d args", len(args))
	}
	var parallelism int
	if err := deserializeFunctionArg(args[0], &parallelism); err != nil {
		return nil, fmt.Errorf("parallel actor parallelism: %w", err)
	}
	var userCtorDescList []string
	if err := deserializeFunctionArg(args[1], &userCtorDescList); err != nil {
		return nil, fmt.Errorf("parallel actor user constructor descriptor: %w", err)
	}
	userCtorDesc, err := goActorMethodDescriptorFromList(userCtorDescList)
	if err != nil {
		return nil, fmt.Errorf("parallel actor user constructor descriptor: %w", err)
	}
	userCtor, ok := m.GetConstructor(userCtorDesc)
	if !ok {
		return nil, fmt.Errorf("parallel actor user constructor not registered: %s", userCtorDesc.String())
	}
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(parallelism, userCtor, args[2:]); err != nil {
		return nil, err
	}
	return ex, nil
}

// ExecuteActorMethod looks up the stored instance for actorID and invokes the
// method identified by goDesc, mirroring the per-actor task execution path.
//
// An intentional exit (ActorExitError, produced by api.ExitActor) releases the
// actor instance here; the worker is going down (C++ receives
// Status::IntentionalSystemExit and marks the worker exiting without treating
// the actor as failed), and in local mode the caller tears down the remaining
// actor state. Any other error leaves the instance in place.
func (m *ActorManager) ExecuteActorMethod(
	actorID ids.ActorID,
	goDesc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	instance, ok := m.GetInstance(actorID)
	if !ok || function.IsNilInstance(instance) {
		return nil, fmt.Errorf("actor instance not found or nil for actor %s", actorID.Hex())
	}
	// A parallel-actor wrapper actor routes instanceId-routed calls to the
	// executor's Execute method, which dispatches to the target user instance.
	if pe, isPA := instance.(*ParallelActorExecutor); isPA {
		return m.executeParallelActor(pe, goDesc, args)
	}
	results, callErr := CallActorMethod(instance, goDesc, args)
	if callErr != nil {
		if rayerrors.IsActorExitError(callErr) {
			m.RemoveInstance(actorID)
			log.Log.Info("actor instance removed on intentional exit",
				"actorID", actorID.Hex(), "error", callErr.Error())
		}
		return nil, callErr
	}
	return results, nil
}

// executeParallelActor dispatches an instance-routed parallel actor method call.
// The task args carry [instanceId, userMethodDesc, userArgs...] (aligned with
// Java's ParallelActorExecutorImpl.execute(int instanceId, JavaFunctionDescriptor,
// Object[] args)): instanceId and the user method's descriptor are deserialized
// from the first two args, then the call is dispatched to that instance.
func (m *ActorManager) executeParallelActor(
	pe *ParallelActorExecutor,
	goDesc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parallel actor task expects [instanceId, userMethodDesc, userArgs...], got %d args", len(args))
	}
	var instanceId int
	if err := deserializeFunctionArg(args[0], &instanceId); err != nil {
		return nil, fmt.Errorf("parallel actor instance id: %w", err)
	}
	var userMethodDescList []string
	if err := deserializeFunctionArg(args[1], &userMethodDescList); err != nil {
		return nil, fmt.Errorf("parallel actor user method descriptor: %w", err)
	}
	userMethodDesc, err := goActorMethodDescriptorFromList(userMethodDescList)
	if err != nil {
		return nil, fmt.Errorf("parallel actor user method descriptor: %w", err)
	}
	return pe.Execute(instanceId, userMethodDesc, args[2:])
}

// deserializeFunctionArg deserializes a single pass-by-value FunctionArg into
// out. It is used by the parallel-actor dispatch paths to decode the leading
// instanceId/parallelism (int) and descriptor list ([]string) arguments.
func deserializeFunctionArg(arg function.FunctionArg, out interface{}) error {
	if arg.IsPassByRef() || arg.Data == nil {
		return fmt.Errorf("expected a pass-by-value argument")
	}
	ser := object.GetSerializer()
	return ser.DeserializeTo(&object.NativeRayObject{Data: arg.Data.Data, Metadata: arg.Data.Metadata}, out)
}

// goActorMethodDescriptorFromList builds a GoFunctionDescriptor from a 4-element
// [module, package, function, method] list, as carried in parallel actor args.
func goActorMethodDescriptorFromList(list []string) (*function.GoFunctionDescriptor, error) {
	fd, err := function.FunctionDescriptorFromList(list)
	if err != nil {
		return nil, err
	}
	goDesc, ok := fd.(*function.GoFunctionDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a Go function descriptor")
	}
	return goDesc, nil
}
