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

package native

import (
	"fmt"
	"reflect"
	"sync"

	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// actorConstructorKeyPrefix is prepended to the "<init>" descriptor key that is
// registered by api.RegisterActorClass. It keeps actor constructors separate
// from regular functions in the FunctionManager namespace.
const actorConstructorKeyPrefix = "actor:" + function.ConstructorName + ":"

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
//     goroutines (C++ calls GoExecuteTask concurrently).
type ActorManager struct {
	// instances maps ActorID to the live actor instance.
	instances sync.Map // map[ids.ActorID]interface{}

	// constructors maps an actor constructor key (actorConstructorKeyPrefix +
	// descriptor key) to a constructor that returns a fresh actor instance.
	constructors sync.Map // map[string]func(args []function.FunctionArg) (interface{}, error)

	// NOTE: there are deliberately no per-actor execution locks here.
	// Concurrency control is owned by the C++ ConcurrencyGroupManager
	// <BoundedExecutor>, which schedules GoExecuteTask according to the actor's
	// max_concurrency and concurrency groups (matching Java's TaskExecutor,
	// which holds no per-actor lock either).
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
	m.constructors.Store(actorConstructorKeyPrefix+descKey, ctor)
}

// GetConstructor returns the constructor registered for an actor descriptor.
func (m *ActorManager) GetConstructor(desc *function.GoFunctionDescriptor) (func(args []function.FunctionArg) (interface{}, error), bool) {
	if desc == nil || desc.MethodName() != function.ConstructorName {
		return nil, false
	}
	ctor, ok := m.constructors.Load(actorConstructorKeyPrefix + desc.String())
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

// ExecuteActorMethod looks up the stored instance for actorID and invokes the
// method identified by goDesc, mirroring the per-actor task execution path.
//
// An intentional exit (ActorExitError, produced by api.ExitActor) releases the
// actor instance here; the worker is going down (C++ receives
// Status::IntentionalSystemExit and marks the worker exiting without treating
// the actor as failed). Any other error leaves the instance in place.
func (m *ActorManager) ExecuteActorMethod(
	actorID ids.ActorID,
	goDesc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	instance, ok := m.GetInstance(actorID)
	if !ok || function.IsNilInstance(instance) {
		return nil, fmt.Errorf("actor instance not found or nil for actor %s", actorID)
	}
	results, callErr := CallActorMethod(instance, goDesc, args)
	if callErr != nil {
		if rayerrors.IsActorExitError(callErr) {
			m.RemoveInstance(actorID)
			logger.Info("actor instance removed on intentional exit",
				"actorID", actorID.Hex(), "error", callErr.Error())
		}
		return nil, callErr
	}
	return results, nil
}

// ConstructActor invokes the registered constructor for the given actor
// descriptor and returns a fresh actor instance. The constructor arguments are
// passed by value (serialized), matching the pass-by-value convention used by
// the driver for actor creation.
func (m *ActorManager) ConstructActor(
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) (interface{}, error) {
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

// CallActorMethod invokes an actor method on the given instance using reflection.
// The method name comes from the function descriptor's MethodName().
//
// Parameters are deserialized into the method's parameter types and the return
// values are serialized back to SerializedObject, mirroring how
// function.WrapGoFunction handles regular functions but with the actor instance as
// the receiver (matching Java's RayFunction.getMethod().invoke(actor, args)).
func CallActorMethod(
	instance interface{},
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	if instance == nil {
		return nil, fmt.Errorf("actor instance is nil")
	}
	methodName := desc.MethodName()
	if methodName == "" || methodName == function.ConstructorName {
		return nil, fmt.Errorf("invalid actor method name %q", methodName)
	}

	value := reflect.ValueOf(instance)
	// For pointer instances (the common actor case) MethodByName resolves both
	// value- and pointer-receiver methods in a single lookup.
	method := value.MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("actor method %s not found on %T", methodName, instance)
	}
	methodType := method.Type()

	// Validate the argument count up front so a driver that submitted the wrong
	// number of arguments gets a clear error instead of a reflect panic.
	if len(args) != methodType.NumIn() {
		return nil, fmt.Errorf("actor method %s on %T expects %d argument(s), got %d",
			methodName, instance, methodType.NumIn(), len(args))
	}

	in, err := function.DeserializeArgs(args, function.ParamTypesOf(methodType, len(args)))
	if err != nil {
		return nil, err
	}

	out := method.Call(in)

	// Honor the trailing-error convention. An ActorExitError produced by
	// api.ExitActor must flow through the execution error channel so the worker
	// can exit intentionally; any other non-nil error marks the task as failed.
	exitErr, rest := function.ExtractErrorReturn(out)
	if exitErr != nil {
		return nil, exitErr
	}

	ser := object.GetSerializer()
	results := make([]function.SerializedObject, 0, len(rest))
	for _, val := range rest {
		nativeObj, err := ser.Serialize(val.Interface())
		if err != nil {
			return nil, fmt.Errorf("failed to serialize return value: %w", err)
		}
		results = append(results, function.SerializedObjectFromNative(nativeObj))
	}
	return results, nil
}

// WrapActorConstructor builds a constructor that deserializes the construction
// arguments, invokes the provided factory function and returns the resulting
// actor instance. The factory must be a Go function whose first return value is
// the actor instance (an optional trailing error return is treated as a
// construction failure).
func WrapActorConstructor(factory interface{}) func(args []function.FunctionArg) (interface{}, error) {
	funcValue := reflect.ValueOf(factory)
	funcType := funcValue.Type()
	if funcType.NumOut() < 1 {
		log.Log.Error(fmt.Errorf("invalid constructor signature"), "actor constructor must return at least the actor instance", "signature", funcType)
		return func(args []function.FunctionArg) (interface{}, error) {
			return nil, fmt.Errorf("invalid actor constructor signature %s", funcType)
		}
	}
	// The factory's parameter types are fixed at registration time, so compute
	// them once and reuse them on every construction instead of rebuilding the
	// slice per task.
	paramTypes := function.ParamTypesOf(funcType, funcType.NumIn())
	return func(args []function.FunctionArg) (interface{}, error) {
		in, err := function.DeserializeArgs(args, paramTypes)
		if err != nil {
			return nil, err
		}

		out := funcValue.Call(in)
		ctorErr, rest := function.ExtractErrorReturn(out)
		if ctorErr != nil {
			return nil, ctorErr
		}
		var instance interface{}
		if len(rest) > 0 {
			instance = rest[0].Interface()
		}
		if function.IsNilInstance(instance) {
			return nil, fmt.Errorf("actor constructor returned nil instance")
		}
		return instance, nil
	}
}
