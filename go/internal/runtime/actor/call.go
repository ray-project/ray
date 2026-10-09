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

// Package actor provides cgo-free actor execution primitives shared by the
// native (cluster) and local runtimes, so actor dispatch logic is maintained
// in a single place instead of being duplicated.
package actor

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// methodParamTypesKey identifies the (instance type, method name) pair whose
// parameter types are stable for the lifetime of the type.
type methodParamTypesKey struct {
	typ  reflect.Type
	name string
}

// methodParamTypes caches each actor method's parameter types so repeated
// method calls do not rebuild the []reflect.Type slice on every invocation
// (mirroring how WrapActorConstructor caches constructor parameter types).
var methodParamTypes sync.Map // map[methodParamTypesKey][]reflect.Type

// cachedMethodParamTypes returns the parameter types of methodType, computing
// and caching them on first use for the (type, method name) pair.
func cachedMethodParamTypes(methodType reflect.Type, methodName string) []reflect.Type {
	key := methodParamTypesKey{typ: methodType, name: methodName}
	if cached, ok := methodParamTypes.Load(key); ok {
		return cached.([]reflect.Type)
	}
	paramTypes := function.ParamTypesOf(methodType, methodType.NumIn())
	methodParamTypes.Store(key, paramTypes)
	return paramTypes
}

// CallActorMethod invokes an actor method on the given instance using
// reflection. The method name comes from the function descriptor's MethodName().
//
// Parameters are deserialized into the method's parameter types and the return
// values are serialized back to SerializedObject, mirroring how
// function.WrapGoFunction handles regular functions but with the actor instance
// as the receiver (matching Java's RayFunction.getMethod().invoke(actor, args)
// and C++'s actor method dispatch). Actor methods are resolved dynamically from
// the live instance, so they do not need to be pre-registered like regular
// functions.
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

	in, err := function.DeserializeArgs(args, cachedMethodParamTypes(methodType, methodName))
	if err != nil {
		return nil, err
	}

	out := method.Call(in)

	// Honor the trailing-error convention. An ActorExitError produced by
	// api.ExitActor must flow through the execution error channel so the actor
	// is torn down; any other non-nil error marks the task as failed.
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
//
// The actor instance is returned as a value, NOT serialized (unlike
// WrapGoFunction, which would try to serialize the returned struct and fail on
// unexported fields).
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
