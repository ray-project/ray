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
#include <string.h>
#include "src/ray/core_worker/lib/go/native_task_executor.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

var taskExecutorLogger = log.WithName("task_executor")

// TaskExecutorFunc is the Go-side task executor function type.
// This is set during initialization and called by C++ when a task is received.
type TaskExecutorFunc func(
	taskType int,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	actorID ids.ActorID,
) ([]function.SerializedObject, error)

// taskExecutorRegistry holds the Go-side task executor function.
var taskExecutor TaskExecutorFunc

// convertCFunctionArgToBase converts C.CFunctionArg to function.FunctionArg.
func convertCFunctionArgToBase(cArg C.CFunctionArg) function.FunctionArg {
	argType := int(C.CFunctionArg_GetType(&cArg))

	if argType == int(C.FUNCTION_ARG_TYPE_REFERENCE) {
		// Pass by reference (ObjectRef)
		var objectRef *function.ObjectRefData
		objectIDData := C.CFunctionArg_GetReferenceObjectIdData(&cArg)
		objectIDSize := C.CFunctionArg_GetReferenceObjectIdSize(&cArg)
		if objectIDData != nil && objectIDSize > 0 {
			objectIDBytes := C.GoBytes(unsafe.Pointer(objectIDData), objectIDSize)
			objectID, _ := ids.ObjectIDFromBinary(objectIDBytes)
			objectRef = &function.ObjectRefData{
				ObjectID: objectID,
			}
		}

		var ownerAddress []byte
		ownerAddr := C.CFunctionArg_GetReferenceOwnerAddress(&cArg)
		ownerAddrSize := C.CFunctionArg_GetReferenceOwnerAddressSize(&cArg)
		if ownerAddr != nil && ownerAddrSize > 0 {
			ownerAddress = C.GoBytes(unsafe.Pointer(ownerAddr), ownerAddrSize)
		}

		return function.FunctionArg{
			ObjectRef:    objectRef,
			OwnerAddress: ownerAddress,
		}
	} else if argType == int(C.FUNCTION_ARG_TYPE_VALUE) {
		// Pass by value (serialized data)
		data := &function.SerializedData{} // Always create empty data object
		valueData := C.CFunctionArg_GetValueData(&cArg)
		valueDataSize := C.CFunctionArg_GetValueDataSize(&cArg)
		if valueData != nil && valueDataSize > 0 {
			data.Data = C.GoBytes(unsafe.Pointer(valueData), valueDataSize)
		}
		valueMetadata := C.CFunctionArg_GetValueMetadata(&cArg)
		valueMetadataSize := C.CFunctionArg_GetValueMetadataSize(&cArg)
		if valueMetadata != nil && valueMetadataSize > 0 {
			data.Metadata = C.GoBytes(unsafe.Pointer(valueMetadata), valueMetadataSize)
		}

		return function.FunctionArg{
			Data:         data,
			ObjectRef:    nil,
			OwnerAddress: nil,
		}
	}

	// Default to value type with empty data
	return function.FunctionArg{
		Data:      &function.SerializedData{},
		ObjectRef: nil,
	}
}

// allocateCSerializedObject allocates and initializes a single CSerializedObject.
//
// Error Handling Strategy:
//   - Single allocation failure: return false immediately (no cleanup needed)
//   - Multiple allocations: if later allocation fails, free previously allocated resources
//     to prevent memory leaks. This follows the principle that the caller should not
//     be responsible for cleaning up partial allocations from a failed function call.
//
// Memory Management:
//   - All allocated memory (data, metadata) is owned by the C++ caller after successful return
//   - The C++ caller must free the entire CSerializedObject using
//     CNativeCommon_FreeCSerializedObjectArray (not individual C.free calls)
//   - Do NOT use defer for cleanup in CGO context - manual cleanup is clearer and
//     avoids potential issues with CGO memory management across language boundaries
//
// Parameters:
//   - cObj: Pointer to CSerializedObject structure to initialize
//   - data: Object data bytes (may be empty)
//   - metadata: Object metadata bytes (may be empty)
//
// Returns:
//   - true: Allocation successful, cObj is fully initialized
//   - false: Allocation failed, cObj is left in zero state (all fields nil/0)
//
// The caller (C++ side) is responsible for freeing the allocated memory using
// CNativeCommon_FreeCSerializedObjectArray.
func allocateCSerializedObject(cObj *C.CSerializedObject, data []byte, metadata []byte) bool {
	// Initialize to zero state
	cObj.data = nil
	cObj.data_size = 0
	cObj.metadata = nil
	cObj.metadata_size = 0

	// Allocate and copy data
	if len(data) > 0 {
		cObj.data = (*C.char)(C.malloc(C.size_t(len(data))))
		if cObj.data == nil {
			taskExecutorLogger.Error(fmt.Errorf("malloc failed"), "Failed to allocate object data")
			return false
		}
		C.memcpy(unsafe.Pointer(cObj.data), unsafe.Pointer(&data[0]), C.size_t(len(data)))
		cObj.data_size = C.int(len(data))
		taskExecutorLogger.Info("Allocated object data",
			"dataSize", len(data))
	}

	// Allocate and copy metadata
	if len(metadata) > 0 {
		cObj.metadata = (*C.char)(C.malloc(C.size_t(len(metadata))))
		if cObj.metadata == nil {
			// Metadata allocation failed - free previously allocated data to prevent leak
			// This ensures the caller doesn't receive a partially allocated object
			if cObj.data != nil {
				C.free(unsafe.Pointer(cObj.data))
				cObj.data = nil
				cObj.data_size = 0
			}
			taskExecutorLogger.Error(fmt.Errorf("malloc failed"), "Failed to allocate object metadata")
			return false
		}
		C.memcpy(unsafe.Pointer(cObj.metadata), unsafe.Pointer(&metadata[0]), C.size_t(len(metadata)))
		cObj.metadata_size = C.int(len(metadata))
		taskExecutorLogger.Info("Allocated object metadata",
			"metadataSize", len(metadata))
	}

	return true
}

// convertGoResultToC converts Go execution result to C.CSerializedObjectArray.
// On error, serializes the error and returns an array with numReturns error objects.
// The C++ caller is responsible for freeing the returned array
// using CNativeCommon_FreeCSerializedObjectArray (defined in cgo_wrapper.cc).
//
// Parameters:
//   - results: Task execution results (may be nil on error)
//   - err: Execution error (if any)
//   - numReturns: Number of return values expected by the caller
//
// Returns:
//   - *C.CSerializedObjectArray: C array containing results or error objects
func convertGoResultToC(results []function.SerializedObject, err error, numReturns int) *C.CSerializedObjectArray {
	if err != nil {
		taskExecutorLogger.Error(err, "Task execution failed, serializing error to C++")

		// Serialize the error to JSON
		// Check if error is already TaskExecutionError, otherwise wrap it
		var rayErr *errors.TaskExecutionError
		if taskErr, ok := err.(*errors.TaskExecutionError); ok {
			rayErr = taskErr
		} else {
			// Wrap as TaskExecutionError if not already
			rayErr = errors.NewTaskExecutionError(
				ids.NilTaskID(),
				ids.NilJobID(),
				err,
				err.Error(),
			)
		}

		// Serialize error to JSON bytes using ToSerializedForm() method
		serializedErr, marshalErr := json.Marshal(rayErr.ToSerializedForm())
		if marshalErr != nil {
			taskExecutorLogger.Error(marshalErr, "Failed to serialize error, using fallback")
			// Fallback: serialize error message as string
			serializedErr = []byte(fmt.Sprintf(`{"error_type":"RayTaskException","error_message":"%s"}`, err.Error()))
		}

		// Create error objects for all return values
		// This ensures the C++ side receives the expected number of results
		errorResults := make([]function.SerializedObject, numReturns)
		for i := 0; i < numReturns; i++ {
			errorResults[i] = function.SerializedObject{
				Data:     serializedErr,
				Metadata: []byte(`{"type":"error"}`),
			}
		}

		// Convert error results to C format
		return convertGoResultToC(errorResults, nil, numReturns)
	}

	if len(results) == 0 {
		taskExecutorLogger.Info("Task execution returned empty results")
		return nil
	}

	// Allocate C array structure
	cArray := (*C.CSerializedObjectArray)(C.malloc(C.size_t(unsafe.Sizeof(C.CSerializedObjectArray{}))))
	if cArray == nil {
		taskExecutorLogger.Error(fmt.Errorf("malloc failed"), "Failed to allocate result array")
		return nil
	}
	taskExecutorLogger.Info("Allocated result array",
		"count", int(cArray.count))

	cArray.count = C.int(len(results))

	// Allocate objects array
	cArray.objects = (*C.CSerializedObject)(C.malloc(C.size_t(len(results)) * C.size_t(unsafe.Sizeof(C.CSerializedObject{}))))
	if cArray.objects == nil {
		C.free(unsafe.Pointer(cArray))
		taskExecutorLogger.Error(fmt.Errorf("malloc failed"), "Failed to allocate result objects")
		return nil
	}
	taskExecutorLogger.Info("Allocated result objects",
		"objectCount", len(results),
		"totalSize", len(results)*int(unsafe.Sizeof(C.CSerializedObject{})))

	// Use slice for safer access instead of pointer arithmetic
	objs := unsafe.Slice((*C.CSerializedObject)(cArray.objects), int(cArray.count))

	// Copy each result to C memory
	for i, result := range results {
		if !allocateCSerializedObject(&objs[i], result.Data, result.Metadata) {
			// Allocation failed - free everything allocated so far
			for j := 0; j < i; j++ {
				if objs[j].data != nil {
					C.free(unsafe.Pointer(objs[j].data))
				}
				if objs[j].metadata != nil {
					C.free(unsafe.Pointer(objs[j].metadata))
				}
			}
			C.free(unsafe.Pointer(cArray.objects))
			C.free(unsafe.Pointer(cArray))
			taskExecutorLogger.Error(fmt.Errorf("failed to allocate object %d", i), "Object allocation failed")
			return nil
		}
	}

	// P0 refactoring: Add memory allocation statistics
	atomic.AddInt64(&cgoMemoryStats.allocatedBytes, int64(cArray.count)*int64(unsafe.Sizeof(C.CSerializedObject{})))
	atomic.AddInt64(&cgoMemoryStats.allocationCount, 1)

	taskExecutorLogger.Info("Returning result array",
		"count", cArray.count,
		"objectCount", cArray.count)
	return cArray
}

// cgoMemoryStats tracks CGO memory allocation statistics
var cgoMemoryStats struct {
	allocatedBytes  int64
	allocationCount int64
	freedBytes      int64
	freedCount      int64
}

// GoExecuteTask is the Go-side task executor callback registered with C++.
// It is called by C++ when a task is received during task execution loop.
//
// Parameters:
//   - task_type: Type of task (matches ray::rpc::TaskType enum values)
//   - function_descriptor: Array of function descriptor strings
//   - function_descriptor_count: Number of elements in function_descriptor array
//   - args: Array of function arguments
//   - args_count: Number of arguments
//   - num_returns: Number of expected return values
//   - actor_id_data: Binary data of actor ID (for actor tasks, NULL for normal tasks)
//   - actor_id_size: Size of actor ID binary data
//
// Returns:
//   - *C.CSerializedObjectArray containing task execution results, or serialized error
//   - Caller (C++ side) is responsible for freeing the returned CSerializedObjectArray
//
//export GoExecuteTask
func GoExecuteTask(
	taskType C.int,
	functionDescriptor **C.char,
	functionDescriptorCount C.int,
	args *C.CFunctionArg,
	argsCount C.int,
	numReturns C.int,
	actorIDData *C.char,
	actorIDSize C.int,
) *C.CSerializedObjectArray {
	var funcDescList []string
	var panicErr error
	defer func() {
		if r := recover(); r != nil {
			// Capture panic with full stack trace
			stackTrace := make([]byte, 1<<16)
			n := runtime.Stack(stackTrace, false)

			panicErr = errors.NewTaskExecutionError(
				ids.NilTaskID(),
				ids.NilJobID(),
				fmt.Errorf("panic: %v", r),
				fmt.Sprintf("Task execution panicked: %v", r),
			)

			taskExecutorLogger.Error(panicErr,
				"Task execution panicked",
				"taskType", taskType,
				"functionDescriptor", funcDescList,
				"stackTrace", string(stackTrace[:n]))
		}
	}()

	// Convert function descriptor from C string array to Go slice
	funcDescList = make([]string, int(functionDescriptorCount))
	for i := 0; i < int(functionDescriptorCount); i++ {
		cStr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(functionDescriptor)) + uintptr(i)*unsafe.Sizeof((*C.char)(nil))))
		funcDescList[i] = C.GoString(cStr)
	}

	// Create FunctionDescriptor from list
	funcDesc, err := function.FunctionDescriptorFromList(funcDescList)
	if err != nil {
		taskExecutorLogger.Error(err, "Failed to create function descriptor",
			"functionDescriptor", funcDescList)
		return nil
	}

	// Convert args from C array to Go slice
	goArgs := make([]function.FunctionArg, int(argsCount))
	for i := 0; i < int(argsCount); i++ {
		cArg := (*C.CFunctionArg)(unsafe.Pointer(uintptr(unsafe.Pointer(args)) + uintptr(i)*unsafe.Sizeof(C.CFunctionArg{})))
		goArgs[i] = convertCFunctionArgToBase(*cArg)
	}

	// Parse actor ID (if provided)
	var actorID ids.ActorID
	if actorIDData != nil && actorIDSize > 0 {
		actorIDBytes := C.GoBytes(unsafe.Pointer(actorIDData), actorIDSize)
		actorID, _ = ids.ActorIDFromBinary(actorIDBytes)
	}

	taskExecutorLogger.Info("Executing task",
		"taskType", taskType,
		"functionDescriptor", funcDescList,
		"argsCount", argsCount,
		"numReturns", numReturns,
		"actorID", actorID,
	)

	// Check if task executor is registered
	if taskExecutor == nil {
		taskExecutorLogger.Error(fmt.Errorf("task executor not registered"), "Cannot execute task")
		return nil
	}

	// Execute the task
	results, err := taskExecutor(
		int(taskType),
		funcDesc,
		goArgs,
		int(numReturns),
		actorID,
	)

	// Check for panic error captured in defer
	if panicErr != nil {
		// Convert panic error to C format
		cResult := convertGoResultToC(nil, panicErr, int(numReturns))
		taskExecutorLogger.Error(fmt.Errorf("returning panic error result"),
			"count", cResult.count)
		return cResult
	}

	// Convert result to C format
	cResult := convertGoResultToC(results, err, int(numReturns))
	if cResult != nil && cResult.count > 0 && cResult.objects != nil {
		objs := unsafe.Slice((*C.CSerializedObject)(cResult.objects), int(cResult.count))
		taskExecutorLogger.V(1).Info("First result object details",
			"dataSize", objs[0].data_size,
			"metadataSize", objs[0].metadata_size)
	}
	return cResult
}

// RegisterTaskExecutorCallback registers the Go task executor callback with C++.
// This must be called once during runtime initialization, before RunTaskExecutionLoop().
func RegisterTaskExecutorCallback() {
	C.RegisterGoTaskExecutorCallback()
	taskExecutorLogger.Info("Task executor callback registered")
}

// SetTaskExecutor sets the Go-side task executor function.
// This should be called before RegisterTaskExecutorCallback().
func SetTaskExecutor(executor TaskExecutorFunc) {
	taskExecutor = executor
}

// GoTriggerGC is exported for C++ to trigger Go garbage collection.
// This is called from C++ when memory management is needed.
//
//export GoTriggerGC
func GoTriggerGC() {
	// Force garbage collection
	// Go's runtime.GC() is thread-safe and can be called concurrently
	runtime.GC()
}
