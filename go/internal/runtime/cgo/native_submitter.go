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
#include "src/ray/core_worker/lib/go/native_task_submitter.h"
*/
import "C"
import (
	"fmt"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// NativeTaskSubmitter implements TaskSubmitter for cluster mode.
type NativeTaskSubmitter struct {
	functionManager *function.FunctionManager
}

// NewNativeTaskSubmitter creates a new NativeTaskSubmitter instance.
func NewNativeTaskSubmitter(functionManager *function.FunctionManager) *NativeTaskSubmitter {
	return &NativeTaskSubmitter{
		functionManager: functionManager,
	}
}

// SubmitTask submits a normal task to be executed.
func (s *NativeTaskSubmitter) SubmitTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	cFuncDescArray, freeFuncDesc := CStringSlice(functionDescriptor.ToList())
	defer freeFuncDesc()

	// Convert args to C array using shared utility function
	cArgs := make([]C.CFunctionArg, len(args))
	for i, arg := range args {
		cArgs[i] = ConvertFunctionArgToC(arg)
	}

	// Convert options to C struct
	var cOptions *C.CTaskOptions
	if options != nil {
		cOptions = convertTaskOptionsToC(options)
		defer freeTaskOptions(cOptions)
	}

	// Call CGO function
	cResult := C.CNativeTaskSubmitter_SubmitTask(
		(**C.char)(unsafe.Pointer(argPtr(cFuncDescArray))),
		C.int(len(cFuncDescArray)),
		(*C.CFunctionArg)(unsafe.Pointer(argPtr(cArgs))),
		C.int(len(args)),
		C.int(numReturns),
		cOptions,
	)

	if cResult == nil {
		return nil, nil
	}

	// Convert result to Go ObjectID array
	return convertCObjectIdArrayToGo(cResult)
}

// CreateActor creates a new actor.
func (s *NativeTaskSubmitter) CreateActor(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	options *submitter.ActorCreationOptions,
) (ids.ActorID, error) {
	cFuncDescArray, freeFuncDesc := CStringSlice(functionDescriptor.ToList())
	defer freeFuncDesc()

	// Convert args to C array using shared utility function
	cArgs := make([]C.CFunctionArg, len(args))
	for i, arg := range args {
		cArgs[i] = ConvertFunctionArgToC(arg)
	}

	// Convert options to C struct
	var cOptions *C.CActorCreationOptions
	if options != nil {
		cOptions = convertActorCreationOptionsToC(options)
		defer freeActorCreationOptions(cOptions)
	}

	// Call CGO function
	cResult := C.CNativeTaskSubmitter_CreateActor(
		(**C.char)(unsafe.Pointer(argPtr(cFuncDescArray))),
		C.int(len(cFuncDescArray)),
		(*C.CFunctionArg)(unsafe.Pointer(argPtr(cArgs))),
		C.int(len(args)),
		cOptions,
	)

	if cResult == nil {
		return ids.NilActorID(), nil
	}

	// Free C string array is now handled by defer

	// Convert result to Go ActorID
	data := C.GoBytes(unsafe.Pointer(cResult.data), C.int(cResult.size))
	C.CNativeCommon_FreeCByteArray(cResult)

	actorID, err := ids.ActorIDFromBinary(data)
	if err != nil {
		return ids.NilActorID(), err
	}

	return actorID, nil
}

// SubmitActorTask submits a task to be executed by an actor.
func (s *NativeTaskSubmitter) SubmitActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	cFuncDescArray, freeFuncDesc := CStringSlice(functionDescriptor.ToList())
	defer freeFuncDesc()

	// Convert args to C array using shared utility function
	cArgs := make([]C.CFunctionArg, len(args))
	for i, arg := range args {
		cArgs[i] = ConvertFunctionArgToC(arg)
	}

	// Convert options to C struct
	var cOptions *C.CTaskOptions
	if options != nil {
		cOptions = convertTaskOptionsToC(options)
		defer freeTaskOptions(cOptions)
	}

	// Get actor ID binary data
	actorIDBinary := actorID.Binary()

	// Call CGO function
	cResult := C.CNativeTaskSubmitter_SubmitActorTask(
		byteSlicePtr(actorIDBinary),
		C.int(len(actorIDBinary)),
		(**C.char)(unsafe.Pointer(argPtr(cFuncDescArray))),
		C.int(len(cFuncDescArray)),
		(*C.CFunctionArg)(unsafe.Pointer(argPtr(cArgs))),
		C.int(len(args)),
		C.int(numReturns),
		cOptions,
	)

	if cResult == nil {
		return nil, nil
	}

	// Free C string array is now handled by defer

	// Convert result to Go ObjectID array
	return convertCObjectIdArrayToGo(cResult)
}

// GetActor retrieves a named actor by its name and namespace.
// This implementation calls CGO to query GCS for the actor ID.
func (s *NativeTaskSubmitter) GetActor(name string, namespace string) (submitter.ActorHandle, error) {
	// Convert Go strings to C strings
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cNamespace *C.char
	if namespace != "" {
		cNamespace = C.CString(namespace)
		defer C.free(unsafe.Pointer(cNamespace))
	}

	// Call CGO function
	var cActorID *C.CByteArray
	var cError *C.char

	success := C.CNativeTaskSubmitter_GetActor(
		cName,
		cNamespace,
		&cActorID,
		&cError,
	)

	// Handle error
	if cError != nil {
		errMsg := C.GoString(cError)
		C.free(unsafe.Pointer(cError))
		if cActorID != nil {
			C.CNativeCommon_FreeCByteArray(cActorID)
		}
		return nil, fmt.Errorf("GetActor failed: %s", errMsg)
	}

	if success == 0 {
		return nil, fmt.Errorf("GetActor CGO call failed")
	}

	// Convert CByteArray to ActorID
	if cActorID == nil || cActorID.size <= 0 {
		return nil, fmt.Errorf("actor not found")
	}

	data := C.GoBytes(unsafe.Pointer(cActorID.data), cActorID.size)
	C.CNativeCommon_FreeCByteArray(cActorID)

	actorID, err := ids.ActorIDFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse actor ID: %w", err)
	}

	// Check if actor ID is nil (actor not found)
	if actorID.IsNil() {
		return nil, nil
	}

	// Create NativeActorHandle with the actor ID
	return &object.NativeActorHandle{
		ActorID:  actorID,
		Language: object.LanguageGo,
	}, nil
}

// convertTaskOptionsToC converts Go TaskOptions to C.CTaskOptions.
func convertTaskOptionsToC(opts *submitter.TaskOptions) *C.CTaskOptions {
	if opts == nil {
		return nil
	}

	cOpts := &C.CTaskOptions{}

	// Convert resources (includes GPU if set via Resources map)
	if len(opts.Resources) > 0 {
		resourceStr := ResourcesToString(opts.Resources)
		cOpts.resources = C.CString(resourceStr)
	}

	if opts.RuntimeEnv != "" {
		cOpts.runtime_env = C.CString(opts.RuntimeEnv)
	}

	// Convert placement group
	if opts.PlacementGroup != nil {
		// Convert PlacementGroup ID to hex string
		pgIDHex := opts.PlacementGroup.ID.Hex()
		cOpts.placement_group_id = C.CString(pgIDHex)
		cOpts.placement_group_id_size = C.int(len(pgIDHex))
		cOpts.bundle_index = C.int(opts.PlacementGroup.BundleIndex)
	}

	// Convert retry policy
	if opts.RetryPolicy != nil {
		cOpts.max_retries = C.int(opts.RetryPolicy.MaxRetries)
	}

	return cOpts
}

// convertActorCreationOptionsToC converts Go ActorCreationOptions to C.CActorCreationOptions.
func convertActorCreationOptionsToC(opts *submitter.ActorCreationOptions) *C.CActorCreationOptions {
	if opts == nil {
		return nil
	}

	cOpts := &C.CActorCreationOptions{}

	if len(opts.Resources) > 0 {
		resourceStr := ResourcesToString(opts.Resources)
		cOpts.resources = C.CString(resourceStr)
	}
	if opts.RuntimeEnv != "" {
		cOpts.runtime_env = C.CString(opts.RuntimeEnv)
	}

	if opts.Name != "" {
		cOpts.name = C.CString(opts.Name)
	}

	if opts.Namespace != "" {
		cOpts.namespace_ = C.CString(opts.Namespace)
	}

	if opts.MaxRestarts > 0 {
		cOpts.max_restarts = C.int(opts.MaxRestarts)
	}

	if opts.MaxTaskRetries > 0 {
		cOpts.max_task_retries = C.int(opts.MaxTaskRetries)
	}

	return cOpts
}

// freeTaskOptions frees memory allocated for CTaskOptions.
// Note: The CTaskOptions struct itself is stack-allocated in Go (via &C.CTaskOptions{}),
// so we only free the C.CString fields which are malloc-allocated by CGO.
func freeTaskOptions(opts *C.CTaskOptions) {
	if opts == nil {
		return
	}
	// Free all allocated strings
	if opts.placement_group_id != nil {
		C.free(unsafe.Pointer(opts.placement_group_id))
	}
	if opts.resources != nil {
		C.free(unsafe.Pointer(opts.resources))
	}
	if opts.runtime_env != nil {
		C.free(unsafe.Pointer(opts.runtime_env))
	}
	// Do NOT free(opts) - the struct itself is stack-allocated in Go
}

// freeActorCreationOptions frees memory allocated for CActorCreationOptions.
// Note: The CActorCreationOptions struct itself is stack-allocated in Go (via &C.CActorCreationOptions{}),
// so we only free the C.CString fields which are malloc-allocated by CGO.
func freeActorCreationOptions(opts *C.CActorCreationOptions) {
	if opts == nil {
		return
	}
	// Free all allocated strings
	if opts.name != nil {
		C.free(unsafe.Pointer(opts.name))
	}
	if opts.namespace_ != nil {
		C.free(unsafe.Pointer(opts.namespace_))
	}
	if opts.resources != nil {
		C.free(unsafe.Pointer(opts.resources))
	}
	if opts.runtime_env != nil {
		C.free(unsafe.Pointer(opts.runtime_env))
	}
	// Do NOT free(opts) - the struct itself is stack-allocated in Go
}

// convertCObjectIdArrayToGo converts C.CObjectIdArray to Go []ids.ObjectID.
func convertCObjectIdArrayToGo(cArray *C.CObjectIdArray) ([]ids.ObjectID, error) {
	if cArray == nil || cArray.count <= 0 {
		return nil, nil
	}

	result := make([]ids.ObjectID, int(cArray.count))

	// Convert C array to Go slice of CByteArray
	objectIdsSlice := unsafe.Slice(cArray.object_ids, int(cArray.count))

	for i := 0; i < int(cArray.count); i++ {
		// Each element is a CByteArray with data and size fields
		dataBytes := C.GoBytes(unsafe.Pointer(objectIdsSlice[i].data), objectIdsSlice[i].size)
		objectID, err := ids.ObjectIDFromBinary(dataBytes)
		if err != nil {
			return nil, err
		}
		result[i] = objectID
	}

	// Free the entire CObjectIdArray
	// This properly releases all nested CByteArray elements
	C.CNativeCommon_FreeCObjectIdArray(cArray)

	return result, nil
}
