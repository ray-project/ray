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
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
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

// executeTask is the common implementation for both Execute and ExecuteActorTask.
// If actorID is nil, executes a normal task; otherwise executes an actor task.
// Similar to LocalModeTaskExecutor.executeFunction, it looks up and executes Go functions.
func (e *NativeTaskExecutor) executeTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	actorID *ids.ActorID,
) ([]function.SerializedObject, error) {
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
