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

// Package cgo provides CGO bindings for Ray runtime.
// This package is organized into subdirectories by function:
//   - boundary/: CGO boundary handling (CoreWorker lifecycle)
//   - interfaces/: Interface implementations (WorkerContext, TaskExecutor, TaskSubmitter)
//   - memory/: Memory management (object allocation)
//   - callback/: Callback functions (called from C++)
//   - utils/: Shared utilities (type conversion)
package cgo

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "src/ray/core_worker/lib/go/native_runtime.h"
#include "src/ray/core_worker/lib/go/native_task_executor.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// NativeTaskExecutor implements TaskExecutor for cluster mode.
// Similar to LocalModeTaskExecutor, it has a function manager for looking up Go functions.
type NativeTaskExecutor struct {
	functionManager *function.FunctionManager
}

// NewNativeTaskExecutor creates a new NativeTaskExecutor instance.
func NewNativeTaskExecutor(functionManager *function.FunctionManager) *NativeTaskExecutor {
	return &NativeTaskExecutor{
		functionManager: functionManager,
	}
}

// actorConstructors maps an actor type name to its constructor. Both the driver
// and worker processes load the same plugin, so the class is registered wherever
// the actor is created or its methods execute.
var actorConstructors sync.Map // typeName -> func(args []function.FunctionArg) (interface{}, error)

// actorInstances maps a running actor's ID to its Go instance. The instance is
// created when the worker executes the actor creation task (<init>) and is
// reused for every subsequent actor method task on the same actor.
var actorInstances sync.Map // actorID.Hex() -> interface{}

// RegisterActorClass registers an actor constructor under the actor type name.
// The worker invokes the constructor when it receives the actor creation task.
func RegisterActorClass(typeName string, constructor func(args []function.FunctionArg) (interface{}, error)) {
	actorConstructors.Store(typeName, constructor)
}

// executeTask is the common implementation for both Execute and ExecuteActorTask.
// If actorID is nil, executes a normal task; otherwise executes an actor task.
// Similar to LocalModeTaskExecutor.executeFunction, it looks up and executes Go functions.
func (e *NativeTaskExecutor) executeTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	actorID *ids.ActorID,
) ([]function.SerializedObject, error) {
	// Actor task: create the instance on <init>, then run methods on it.
	if actorID != nil {
		return e.executeClusterActorTask(functionDescriptor, args, numReturns, *actorID)
	}

	// Get the function from the manager
	// Similar to LocalModeTaskExecutor.executeFunction
	goDesc, err := function.FromBaseFunctionDescriptor(functionDescriptor)
	if err != nil {
		return nil, fmt.Errorf("invalid function descriptor: %w", err)
	}

	rayFunc, err := e.functionManager.GetFunction(goDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to get function: %w", err)
	}

	// Execute the function - it returns SerializedObject directly
	return rayFunc(args)
}

// executeClusterActorTask executes an actor creation task (methodName "<init>")
// or an actor method task on the actor instance stored for actorID.
func (e *NativeTaskExecutor) executeClusterActorTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	actorID ids.ActorID,
) ([]function.SerializedObject, error) {
	goDesc, err := function.FromBaseFunctionDescriptor(functionDescriptor)
	if err != nil {
		return nil, fmt.Errorf("invalid function descriptor: %w", err)
	}

	if goDesc.MethodName() == "<init>" {
		// Actor creation task: construct the instance and store it under the
		// actor ID so subsequent method tasks can find it.
		factory, ok := actorConstructors.Load(goDesc.FunctionName)
		if !ok {
			return nil, fmt.Errorf("actor class %q is not registered", goDesc.FunctionName)
		}
		instance, cerr := factory.(func(args []function.FunctionArg) (interface{}, error))(args)
		if cerr != nil {
			return nil, fmt.Errorf("actor constructor failed: %w", cerr)
		}
		actorInstances.Store(actorID.Hex(), instance)
		return nil, nil
	}

	// Actor method task: run the method on the stored instance.
	inst, ok := actorInstances.Load(actorID.Hex())
	if !ok {
		return nil, fmt.Errorf("actor instance not found: %s", actorID.Hex())
	}
	method := reflect.ValueOf(inst).MethodByName(goDesc.MethodName())
	if !method.IsValid() {
		return nil, fmt.Errorf("actor %s has no method %q", actorID.Hex(), goDesc.MethodName())
	}

	mType := method.Type()
	if mType.NumIn() != len(args) {
		return nil, fmt.Errorf("method %s expects %d args, got %d", goDesc.MethodName(), mType.NumIn(), len(args))
	}
	ser := object.GetSerializer()
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		if !arg.IsPassByValue() || arg.Data == nil {
			return nil, fmt.Errorf("actor method argument %d must be pass-by-value", i)
		}
		nativeObj := &object.NativeRayObject{Data: arg.Data.Data, Metadata: arg.Data.Metadata}
		deserialized := reflect.New(mType.In(i)).Interface()
		if err := ser.DeserializeTo(nativeObj, deserialized); err != nil {
			return nil, fmt.Errorf("failed to deserialize argument %d: %w", i, err)
		}
		in[i] = reflect.ValueOf(deserialized).Elem()
	}

	out := method.Call(in)
	results := make([]function.SerializedObject, len(out))
	for i, val := range out {
		nativeObj, serr := ser.Serialize(val.Interface())
		if serr != nil {
			return nil, fmt.Errorf("failed to serialize result %d: %w", i, serr)
		}
		results[i] = function.SerializedObject{Data: nativeObj.Data, Metadata: nativeObj.Metadata}
	}
	return results, nil
}

// Execute executes a normal task and returns the results.
func (e *NativeTaskExecutor) Execute(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
) ([]function.SerializedObject, error) {
	return e.executeTask(functionDescriptor, args, numReturns, nil)
}

// ExecuteActorTask executes an actor task and returns the results.
func (e *NativeTaskExecutor) ExecuteActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
) ([]function.SerializedObject, error) {
	return e.executeTask(functionDescriptor, args, numReturns, &actorID)
}

// convertCSerializedObjectArrayToGo converts C.CSerializedObjectArray to Go []function.SerializedObject.
func convertCSerializedObjectArrayToGo(cArray *C.CSerializedObjectArray) ([]function.SerializedObject, error) {
	if cArray == nil || cArray.count <= 0 {
		return nil, nil
	}

	result := make([]function.SerializedObject, cArray.count)

	// Convert C array to Go slice using pointer arithmetic
	objectsPtr := uintptr(unsafe.Pointer(cArray.objects))
	for i := 0; i < int(cArray.count); i++ {
		// Get pointer to i-th element
		objPtr := (*C.CSerializedObject)(unsafe.Pointer(objectsPtr + uintptr(i)*unsafe.Sizeof(*cArray.objects)))

		// Get data
		var data []byte
		if objPtr.data != nil && objPtr.data_size > 0 {
			data = C.GoBytes(unsafe.Pointer(objPtr.data), objPtr.data_size)
		}

		// Get metadata
		var metadata []byte
		if objPtr.metadata != nil && objPtr.metadata_size > 0 {
			metadata = C.GoBytes(unsafe.Pointer(objPtr.metadata), objPtr.metadata_size)
		}

		result[i] = function.SerializedObject{
			Data:     data,
			Metadata: metadata,
		}

		// Note: Don't free data and metadata here individually, as the unified free function will handle it
	}

	// Use unified free function instead of scattered C.free calls
	C.CNativeCommon_FreeCSerializedObjectArray(cArray)

	return result, nil
}
