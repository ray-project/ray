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
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/internal/proctitle"
	"github.com/ray-project/ray/go/internal/runtime/actor"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
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
// On error, serializes the error and returns an array with numReturns error
// objects, or null when numReturns is zero.
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

		// Serialize the error under the cross-language numeric convention:
		// metadata is the decimal rpc::ErrorType and the payload is a
		// msgpack-wrapped RayException protobuf, which Python's integer
		// error parse reads natively. The previous JSON convention
		// ({"type":"error"} metadata) was unreadable to Python; the Go
		// reader keeps accepting it for objects written before this change
		// (see ErrorObjectFromNative in go/pkg/runtime/object).
		//
		// Spec 3.1 requires FormattedExceptionString to carry both the error
		// string and its stack trace, so append the stack the JSON convention
		// used to carry (TaskExecutionError.StackTrace) on a new line.
		msg := rayErr.Error()
		if stack := rayErr.StackTrace(); stack != "" {
			msg += "\n" + stack
		}
		errorData, dataErr := object.MarshalTaskExecutionErrorData(msg)
		if dataErr != nil {
			taskExecutorLogger.Error(dataErr, "Failed to marshal task error payload, using fallback message")
			errorData, dataErr = object.MarshalTaskExecutionErrorData("task execution failed: " + err.Error())
			if dataErr != nil {
				// The payload builder cannot fail on a plain string; if it
				// ever does, return null so C++ surfaces the failure through
				// its own error path rather than emitting a malformed object.
				return nil
			}
		}

		// Create error objects for all return values
		// This ensures the C++ side receives the expected number of results
		errorResults := make([]function.SerializedObject, numReturns)
		for i := 0; i < numReturns; i++ {
			errorResults[i] = function.SerializedObject{
				Data:     errorData,
				Metadata: object.TaskExecutionErrorMetadata(),
			}
		}
		if numReturns == 0 {
			// No return slots to carry an error object. Return null so the
			// C++ caller treats the task as failed (nullptr distinguishes an
			// error from an empty success result).
			return nil
		}

		// Convert error results to C format
		return convertGoResultToC(errorResults, nil, numReturns)
	}

	// Pad a short result array up to numReturns. A cross-language caller
	// (Python/C++) always expects exactly numReturns results for a task or actor
	// method (DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS = 1), even when the Go method
	// is logically void and produced zero SerializedObjects (e.g. CheckHealth()).
	// Without padding, the C++ return slot in runtime_callbacks.cc stays nullptr
	// and CoreWorker::ExecuteTask CHECK-fails on objects_valid in task_receiver.cc,
	// crashing the worker. Each padded slot carries a valid cross-language nil
	// object (9-byte length header + msgpack nil) whose metadata is
	// MetadataTypeCrossLanguage (aka XLANG): Python's _deserialize_object rejects
	// the Go metadata a plain Serialize(nil) would emit. Actor creation tasks
	// (numReturns == 0) are untouched: the count == 0 array below already
	// satisfies the C++ success path (no return slots are expected).
	if len(results) < numReturns {
		if len(results) > 0 {
			// A non-void method produced fewer results than numReturns: silently
			// padding would mask a Go-side defect (the original C++ objects_valid
			// CHECK surfaced it explicitly). Keep the default log level so the
			// anomaly is always visible (logr has no Warn level; plain Info is
			// the always-on severity here).
			taskExecutorLogger.Info("Task returned fewer results than numReturns; padding the remaining slots",
				"produced", len(results), "numReturns", numReturns)
		} else {
			taskExecutorLogger.V(1).Info("Padding task results up to numReturns",
				"produced", len(results), "numReturns", numReturns)
		}
		paddedResults := make([]function.SerializedObject, numReturns)
		copy(paddedResults, results)
		// The cross-language nil payload is a fixed byte pattern, so it is built
		// once and shared across every missing slot. Sharing is safe: the bytes
		// are read-only and allocateCSerializedObject deep-copies each slot into
		// C memory (the error-result path above shares one byte slice across all
		// slots the same way). Building it directly also avoids the global
		// serializer and cannot fail, so this path never hands C++ a nil array.
		for i := len(results); i < numReturns; i++ {
			paddedResults[i] = function.SerializedObject{
				Data:     crossLanguageNilData,
				Metadata: crossLanguageNilMetadata,
			}
		}
		results = paddedResults
	}

	if len(results) == 0 {
		// Empty success (an actor creation task with numReturns == 0: any other
		// task is padded to numReturns above, so it reaches here with a non-empty
		// result array). Return a zero-count array rather than null: null is
		// reserved for execution errors and makes the C++ caller fail the task.
		taskExecutorLogger.Info("Task execution returned empty results")
		cArray := (*C.CSerializedObjectArray)(C.malloc(C.size_t(unsafe.Sizeof(C.CSerializedObjectArray{}))))
		if cArray == nil {
			taskExecutorLogger.Error(fmt.Errorf("malloc failed"), "Failed to allocate empty result array")
			return nil
		}
		cArray.count = 0
		cArray.objects = nil
		return cArray
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

// crossLanguageNilPayload returns the fixed byte layout of a cross-language nil
// object: a 9-byte msgpack length header encoding a 1-byte payload (compact int
// 0x01, zero-padded) followed by the msgpack nil byte (0xc0). This is the exact
// layout the serializer's EncodeToBuffer produces for a nil value and what
// Python's _deserialize_object expects for a cross-language nil result. It is
// constant, so it is built once and reused for every padded result slot.
func crossLanguageNilPayload() []byte {
	payload := make([]byte, 10) // 9-byte length header + msgpack nil
	payload[0] = 0x01
	payload[9] = 0xc0
	return payload
}

// crossLanguageNilData and crossLanguageNilMetadata together form the
// cross-language nil object's data and metadata, built once at package init
// and shared across every padded result slot in convertGoResultToC. Sharing is
// safe: allocateCSerializedObject deep-copies each slot into C memory.
var (
	crossLanguageNilData     = crossLanguageNilPayload()
	crossLanguageNilMetadata = []byte(object.MetadataTypeCrossLanguage)
)

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
//   - language: Language of the task (ray::rpc::Language enum value). The C++
//     side passes the task spec's language so the descriptor is built without
//     guessing it from the descriptor contents.
//   - task_type: Type of task (matches ray::rpc::TaskType enum values)
//   - function_descriptor: Array of function descriptor strings
//   - function_descriptor_count: Number of elements in function_descriptor array
//   - args: Array of function arguments
//   - args_count: Number of arguments
//   - num_returns: Number of expected return values
//   - actor_id_data: Binary data of actor ID (for actor tasks, NULL for normal tasks)
//   - actor_id_size: Size of actor ID binary data
//   - error_code: Out parameter for a task execution status code (0 = success).
//     The Go side writes GO_TASK_ERROR_INTENTIONAL_SYSTEM_EXIT when the task
//     raised an intentional actor exit, so C++ returns Status::IntentionalSystemExit
//     (mirroring how the Java worker throws RayIntentionalSystemExitException).
//
// Returns:
//   - *C.CSerializedObjectArray containing task execution results, or serialized error
//   - Caller (C++ side) is responsible for freeing the returned CSerializedObjectArray
//
//export GoExecuteTask
func GoExecuteTask(
	language C.int,
	taskType C.int,
	functionDescriptor **C.char,
	functionDescriptorCount C.int,
	args *C.CFunctionArg,
	argsCount C.int,
	numReturns C.int,
	actorIDData *C.char,
	actorIDSize C.int,
	errorCode *C.int,
) (cResult *C.CSerializedObjectArray) {
	var funcDescList []string
	defer func() {
		if r := recover(); r != nil {
			// Capture panic with full stack trace
			stackTrace := make([]byte, 1<<16)
			n := runtime.Stack(stackTrace, false)

			panicErr := errors.NewTaskExecutionError(
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

			// Serialize the panic as a task execution error object and return it through the
			// named return value. This must happen here: after a panic is recovered, execution
			// never resumes below the panic point, so a check on panicErr after the task
			// executor call would never run and the C++ caller would receive nullptr (which
			// makes CoreWorker::ExecuteTask CHECK-fail and crash the worker).
			cResult = convertGoResultToC(nil, panicErr, int(numReturns))
			if cResult != nil {
				taskExecutorLogger.Info("returning panic error result", "count", cResult.count)
			}
		}
	}()

	// Convert function descriptor from C string array to Go slice
	cFuncDescs := unsafe.Slice(functionDescriptor, functionDescriptorCount)
	funcDescList = make([]string, len(cFuncDescs))
	for i, cStr := range cFuncDescs {
		funcDescList[i] = C.GoString(cStr)
	}

	// Create FunctionDescriptor from list using the explicit task language. The
	// language comes from ray_function.GetLanguage() on the C++ side and is
	// passed verbatim here, so the descriptor is never built from a guess about
	// the language based on the descriptor contents.
	funcDesc, err := function.FunctionDescriptorFromListWithLanguage(
		function.Language(language), funcDescList)
	if err != nil {
		taskExecutorLogger.Error(err, "Failed to create function descriptor",
			"language", language,
			"functionDescriptor", funcDescList)
		return nil
	}

	// Convert args from C array to Go slice
	cArgsSlice := unsafe.Slice(args, argsCount)
	goArgs := make([]function.FunctionArg, len(cArgsSlice))
	for i := range cArgsSlice {
		goArgs[i] = convertCFunctionArgToBase(cArgsSlice[i])
	}

	// Parse actor ID (if provided). Default to the nil actor ID so tasks
	// without one (normal tasks pass null from C++) take the non-actor path:
	// the Go zero-value ActorID is all zeroes, which ActorID.IsNil() (all
	// 0xff) does not recognize as nil.
	var actorID ids.ActorID = ids.NilActorID()
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

	// Process-title change mirroring Python's _changeproctitle(title,
	// next_title) and C++ TaskExecutor::ExecuteTask: title = "ray::" +
	// (task_name if given else the function name), then restore to
	// "ray::IDLE" for normal tasks or "ray::<actor class>" for actor tasks.
	// The restore title is derived from the descriptor (field layout matches
	// Python _raylet.pyx "ray::<class_name>" and C++ ClassName()).
	title, nextTitle := proctitleTitles(contract.TaskType(taskType), funcDesc)
	proctitleGuard := proctitle.Begin(title, nextTitle)
	defer proctitleGuard.End()

	// Execute the task
	results, err := taskExecutor(
		int(taskType),
		funcDesc,
		goArgs,
		int(numReturns),
		actorID,
	)

	// An actor-exit error is not a task failure: it drives CoreWorker::ExecuteTask's
	// Exit(INTENDED_USER_EXIT) path and marks the worker exiting without treating
	// the actor as failed, mirroring how the Java worker throws
	// RayIntentionalSystemExitException. The exit is carried through the
	// error_code out parameter instead of being serialized into a result object.
	if err != nil && rayerrors.IsActorExitError(err) {
		if errorCode != nil {
			*errorCode = C.int(C.GO_TASK_ERROR_INTENTIONAL_SYSTEM_EXIT)
		}
		taskExecutorLogger.Info("task raised intentional actor exit",
			"taskType", taskType, "error", err.Error())
		// Return nil results: the actor is exiting, there is nothing to deliver.
		return nil
	}

	// Convert result to C format
	cResult = convertGoResultToC(results, err, int(numReturns))
	if cResult != nil && cResult.count > 0 && cResult.objects != nil {
		objs := unsafe.Slice((*C.CSerializedObject)(cResult.objects), int(cResult.count))
		taskExecutorLogger.V(1).Info("First result object details",
			"dataSize", objs[0].data_size,
			"metadataSize", objs[0].metadata_size)
	}
	return cResult
}

// proctitleTitles derives the (title, nextTitle) pair for the process-title
// guard, mirroring:
//   - Python _raylet.pyx: title = f"ray::{task_name}"; normal task ->
//     next_title = "ray::IDLE"; actor task -> next_title = f"ray::{class_name}".
//   - C++ TaskExecutor::ExecuteTask: title = "ray::" + (task_name or
//     func_name); normal -> kIdleProcessTitle; else ClassName() or func_name,
//     prefixed with "ray::" unless already prefixed.
//
// The function descriptor fields match the Go layout
// [module, package, function=actor type, method] (see NewGoActorMethodDescriptor);
// for normal Go functions FunctionName is the function name. Cross-language
// (Python/Cpp) descriptors carry class/function names in their ClassName /
// FunctionName fields. Only the three concrete descriptor kinds are possible
// here (they come from FunctionDescriptorFromListWithLanguage), so a single
// type-switch derives both names.
func proctitleTitles(taskType contract.TaskType, funcDesc function.FunctionDescriptor) (string, string) {
	var taskName, className, funcName string
	switch desc := funcDesc.(type) {
	case *function.GoFunctionDescriptor:
		// Actor type or plain function name; method name adds ".Method".
		taskName, className, funcName = desc.FunctionName, desc.FunctionName, desc.FunctionName
		if desc.HasMethodName() {
			taskName = desc.FunctionName + "." + desc.MethodName()
		}
	case *function.PythonFunctionDescriptor:
		taskName, className, funcName = desc.FunctionName, desc.ClassName, desc.FunctionName
		if desc.ClassName != "" {
			taskName = desc.ClassName + "." + desc.FunctionName
		}
	case *function.CppFunctionDescriptor:
		taskName, className, funcName = desc.CreateFunctionName, desc.ClassName, desc.CreateFunctionName
		if desc.ClassName != "" {
			taskName = desc.ClassName + "." + desc.CreateFunctionName
		}
	}
	title := "ray::" + taskName
	if taskType == contract.TaskTypeNormal {
		return title, proctitle.IdleTitle()
	}
	// Actor creation and actor method tasks: restore to the actor class name,
	// mirroring Python's f"ray::{class_name}" (fall back to the plain function
	// name when the descriptor carries no class name).
	class := className
	if class == "" {
		class = funcName
	}
	if !strings.HasPrefix(class, "ray::") {
		class = "ray::" + class
	}
	return title, class
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

// GoTriggerActorShutdown is exported for C++ to trigger the Go-side actor
// shutdown hook during a core worker's graceful actor exit. It is wired to
// CoreWorkerOptions.actor_shutdown_callback by RuntimeOperations::InitializeCoreWorker so the
// core worker dispatches the Go-side teardown hook during a worker's graceful
// actor exit (the shutdown executor invokes the callback when present). The
// dispatch is a no-op when no shutdown hook is registered.
//
//export GoTriggerActorShutdown
func GoTriggerActorShutdown() {
	actor.RunActorShutdownCallback()
}
