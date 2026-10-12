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

// The prototypes below mirror the extern "C" block of
// src/ray/core_worker/lib/go/placement_group_ops.h. They are declared directly
// (instead of #include-ing that C++ header) to keep the cgo preamble C-only;
// the symbols are resolved at link time against the placement_group_ops
// library.
//
// Return convention (from placement_group_ops.h): 1 on success (pg_id_hex or
// ready set), 0 on failure with *error set. All output strings are
// caller-freed with free().
int ray_runtime_create_placement_group(const char* name,
                                       const char* bundles_json,
                                       int strategy,
                                       char** pg_id_hex,
                                       char** error);

int ray_runtime_remove_placement_group(const char* pg_id_hex, char** error);

int ray_runtime_wait_placement_group_ready(const char* pg_id_hex,
                                           int timeout_seconds,
                                           int* ready,
                                           char** error);
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"
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

	// Call CGO function. The function's language is passed explicitly so the
	// submitted descriptor is not labelled with the wrong language.
	cResult := C.CNativeTaskSubmitter_SubmitTask(
		C.int(functionDescriptor.GetLanguage()),
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

	// Validate options before conversion, then resolve the "unset" zero value to
	// the unlimited sentinel. Passing a raw 0 straight through to C++ would be
	// silently treated as "unlimited", so NormalizeMaxPendingCalls makes the
	// zero-value struct valid while keeping behavior consistent with local_mode.
	if options != nil {
		if err := options.ValidateMaxPendingCalls(); err != nil {
			return ids.NilActorID(), err
		}
		options.NormalizeMaxPendingCalls()
	}

	// Convert options to C struct
	var cOptions *C.CActorCreationOptions
	if options != nil {
		cOptions = convertActorCreationOptionsToC(options, functionDescriptor.ToList())
		defer freeActorCreationOptions(cOptions)
	}

	// Call CGO function
	cResult := C.CNativeTaskSubmitter_CreateActor(
		C.int(functionDescriptor.GetLanguage()),
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

	// Only register successfully created actors. The C++ ActorManager keeps
	// handles until process exit, so entries are never removed (kill is only
	// a state marker, consistent with actor_manager.cc).
	createdActorIDSet.add(actorID)
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
		C.int(functionDescriptor.GetLanguage()),
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

// GetActorHandle retrieves an actor handle by its actor ID.
//
// This implementation deviates from us-design-get-actor-handle.md (AC4/Q2):
// the C++ CoreWorker's ActorManager (core_worker.cc is frozen) is not
// consulted; instead any non-nil actor ID resolves to a handle whose language
// is carried on the handle itself. The returned handle always carries
// LanguageGo.
//
// An unknown actor ID is still a valid handle (matching Java's
// NativeTaskSubmitter.getActor(ActorId) -> NativeActorHandle.create): the
// actual method call surfaces a genuine error if the actor does not exist, so
// handles obtained by name, handles for actors created elsewhere, and
// worker-side lookups all work.
func (s *NativeTaskSubmitter) GetActorHandle(actorID ids.ActorID) (submitter.ActorHandle, error) {
	if actorID.IsNil() {
		return nil, fmt.Errorf("get actor handle: actor ID is nil")
	}

	return object.NewNativeActorHandle(actorID, object.LanguageGo), nil
}

// KillActor kills an actor from the driver side.
// This implementation calls CGO to invoke the C++ CoreWorker::KillActor with
// force_kill fixed to true (matching the Java runtime semantics: kill equals a
// crash, pending tasks fail).
func (s *NativeTaskSubmitter) KillActor(actorID ids.ActorID, noRestart bool) error {
	if actorID.IsNil() {
		return fmt.Errorf("kill actor: actor ID is nil")
	}

	actorIDBinary := actorID.Binary()
	var cError *C.char

	success := C.CNativeTaskSubmitter_KillActor(
		byteSlicePtr(actorIDBinary),
		C.int(len(actorIDBinary)),
		C.bool(noRestart),
		&cError,
	)

	if cError != nil {
		errMsg := C.GoString(cError)
		C.free(unsafe.Pointer(cError))
		return fmt.Errorf("kill actor failed: %s", errMsg)
	}

	if success == 0 {
		return fmt.Errorf("kill actor CGO call failed")
	}

	return nil
}

// Compile-time check that NativeTaskSubmitter satisfies the structural
// actorKiller capability probed by api.KillActor. Without it, a signature drift
// would silently turn the public API back into a kill_actor_not_supported error.
var _ interface {
	KillActor(ids.ActorID, bool) error
} = (*NativeTaskSubmitter)(nil)

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

	if opts.Name != "" {
		cOpts.name = C.CString(opts.Name)
	}

	if opts.ConcurrencyGroupName != "" {
		cOpts.concurrency_group_name = C.CString(opts.ConcurrencyGroupName)
	}

	return cOpts
}

// convertActorCreationOptionsToC converts Go ActorCreationOptions to C.CActorCreationOptions.
func convertActorCreationOptionsToC(opts *submitter.ActorCreationOptions, actorDesc []string) *C.CActorCreationOptions {
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

	if opts.MaxRestarts != 0 {
		// max_restarts: 0 means the C++ default (no restarts), -1 means
		// unlimited restarts (aligned with the Python semantics used by the
		// original actors, e.g. ServeController max_restarts=-1). Only skip
		// the zero value so an explicit -1 propagates through.
		cOpts.max_restarts = C.int(opts.MaxRestarts)
	}

	if opts.MaxTaskRetries != 0 {
		// max_task_retries: 0 means the C++ default, -1 means unlimited
		// retries (aligned with the Python semantics, e.g. ServeController
		// max_task_retries=-1). Only skip the zero value so an explicit -1
		// propagates through.
		cOpts.max_task_retries = C.int(opts.MaxTaskRetries)
	}

	// max_concurrency: 0 means "use the default" (1 = serialized), -1 means
	// unlimited, and values >= 1 pass through directly. The builder default is
	// already 1; 0 only occurs from a zero-value struct and must also resolve
	// to the serialized default (not unlimited) to avoid silent data races.
	switch {
	case opts.MaxConcurrency > 0:
		cOpts.max_concurrency = C.int(opts.MaxConcurrency)
	case opts.MaxConcurrency < 0:
		cOpts.max_concurrency = -1
	default:
		cOpts.max_concurrency = 1
	}

	// lifetime -> is_detached (1 when detached)
	if opts.Lifetime == submitter.ActorLifetimeDetached {
		cOpts.is_detached = 1
	}

	// isAsync -> is_asyncio
	if opts.IsAsync {
		cOpts.is_asyncio = 1
	}

	// maxPendingCalls -> max_pending_calls (default -1 unlimited)
	cOpts.max_pending_calls = C.int(opts.MaxPendingCalls)

	// Convert concurrency groups to C flat arrays. Each group has a name, a
	// max concurrency, and a set of 4-element GoFunctionDescriptor string
	// arrays (module/package/actorType/method). cg_count=0 means no groups
	// (backward compatible with existing callers). The module/package/actorType
	// prefix of every method descriptor comes from actorDesc[0..2] (the actor
	// descriptor's first three elements); methodName comes from each group's
	// Methods entry.
	prefix := []string{"", "", ""}
	if len(actorDesc) >= 3 {
		prefix[0], prefix[1], prefix[2] = actorDesc[0], actorDesc[1], actorDesc[2]
	}
	// MaxCalls <= 0 (0 or -1) resolves to the actor's effective max_concurrency,
	// mirroring local_mode's CreateActor (task_submitter.go) so cluster and local
	// behave identically. A non-positive actor max_concurrency falls back to 1
	// (serialized); a concurrency group has no "unlimited" mode (its C++
	// max_concurrency is a positive uint32), matching Java.
	groupMaxCallsFallback := 1
	if opts.MaxConcurrency > 0 {
		groupMaxCallsFallback = opts.MaxConcurrency
	}
	if len(opts.ConcurrencyGroups) > 0 {
		cgCount := len(opts.ConcurrencyGroups)
		cOpts.cg_count = C.int(cgCount)
		cOpts.cg_names = (**C.char)(C.malloc(C.size_t(unsafe.Sizeof((*C.char)(nil))) * C.size_t(cgCount)))
		cOpts.cg_max_concurrency = (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(C.int(0))) * C.size_t(cgCount)))
		cOpts.cg_fd_counts = (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(C.int(0))) * C.size_t(cgCount)))
		cOpts.cg_fds = (***C.char)(C.malloc(C.size_t(unsafe.Sizeof((**C.char)(nil))) * C.size_t(cgCount)))

		names := (*[1 << 20]*C.char)(unsafe.Pointer(cOpts.cg_names))[:cgCount:cgCount]
		maxConc := (*[1 << 20]C.int)(unsafe.Pointer(cOpts.cg_max_concurrency))[:cgCount:cgCount]
		fdCounts := (*[1 << 20]C.int)(unsafe.Pointer(cOpts.cg_fd_counts))[:cgCount:cgCount]
		fdArrays := (*[1 << 20]**C.char)(unsafe.Pointer(cOpts.cg_fds))[:cgCount:cgCount]

		for i, g := range opts.ConcurrencyGroups {
			maxCalls := g.MaxCalls
			if maxCalls <= 0 {
				maxCalls = groupMaxCallsFallback
			}
			names[i] = C.CString(g.Name)
			maxConc[i] = C.int(maxCalls)
			fdCounts[i] = C.int(len(g.Methods))
			if len(g.Methods) == 0 {
				fdArrays[i] = nil
				continue
			}
			// Each method -> 4-element GoFunctionDescriptor string array
			// [module, package, actorType, methodName].
			fdArray := (**C.char)(C.malloc(C.size_t(unsafe.Sizeof((*C.char)(nil))) * C.size_t(len(g.Methods))))
			methodPtrs := (*[1 << 20]*C.char)(unsafe.Pointer(fdArray))[:len(g.Methods):len(g.Methods)]
			for j, m := range g.Methods {
				parts := []string{prefix[0], prefix[1], prefix[2], m}
				inner := (**C.char)(C.malloc(C.size_t(unsafe.Sizeof((*C.char)(nil))) * 4))
				innerPtrs := (*[4]*C.char)(unsafe.Pointer(inner))[:4:4]
				for k, p := range parts {
					innerPtrs[k] = C.CString(p)
				}
				methodPtrs[j] = (*C.char)(unsafe.Pointer(inner))
			}
			fdArrays[i] = fdArray
		}
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
	if opts.name != nil {
		C.free(unsafe.Pointer(opts.name))
	}
	if opts.concurrency_group_name != nil {
		C.free(unsafe.Pointer(opts.concurrency_group_name))
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
	// Free concurrency group flat arrays: each group's descriptor array
	// (per-method inner 4-element arrays), then the per-group outer arrays.
	if opts.cg_count > 0 {
		cgCount := int(opts.cg_count)
		names := (*[1 << 20]*C.char)(unsafe.Pointer(opts.cg_names))[:cgCount:cgCount]
		fdArrays := (*[1 << 20]**C.char)(unsafe.Pointer(opts.cg_fds))[:cgCount:cgCount]
		fdCounts := (*[1 << 20]C.int)(unsafe.Pointer(opts.cg_fd_counts))[:cgCount:cgCount]
		for i := 0; i < cgCount; i++ {
			C.free(unsafe.Pointer(names[i]))
			innerCount := int(fdCounts[i])
			if fdArrays[i] != nil && innerCount > 0 {
				methodPtrs := (*[1 << 20]*C.char)(unsafe.Pointer(fdArrays[i]))[:innerCount:innerCount]
				for j := 0; j < innerCount; j++ {
					inner := (*[4]*C.char)(unsafe.Pointer(methodPtrs[j]))[:4:4]
					for k := 0; k < 4; k++ {
						C.free(unsafe.Pointer(inner[k]))
					}
					C.free(unsafe.Pointer(methodPtrs[j]))
				}
				C.free(unsafe.Pointer(fdArrays[i]))
			}
		}
		C.free(unsafe.Pointer(opts.cg_names))
		C.free(unsafe.Pointer(opts.cg_max_concurrency))
		C.free(unsafe.Pointer(opts.cg_fd_counts))
		C.free(unsafe.Pointer(opts.cg_fds))
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

// cError converts a C string (allocated by the C++ boundary, caller-freed with
// free()) into a Go error, freeing the C string. It is only invoked on failure
// paths, so a nil or empty C string still yields a non-nil error rather than
// silently turning a failed cgo call into success.
func cError(cStr *C.char) error {
	if cStr == nil {
		return fmt.Errorf("cgo call failed")
	}
	msg := C.GoString(cStr)
	C.free(unsafe.Pointer(cStr))
	if msg == "" {
		return fmt.Errorf("cgo call failed")
	}
	return fmt.Errorf("%s", msg)
}

// serializeBundlesToJSON encodes resource bundles as a JSON array for the C++
// bridge. The C++ side (PlacementGroupOperations::ParseBundlesJson) decodes the
// same format.
func serializeBundlesToJSON(bundles []map[string]float64) (string, error) {
	b, err := json.Marshal(bundles)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// CreatePlacementGroup creates a placement group and returns its id.
// The bundles are serialized to JSON and handed to the C++ boundary, which
// parses them back into per-bundle resource maps.
func (s *NativeTaskSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	if err := opts.Validate(); err != nil {
		return ids.NilPlacementGroupID(), err
	}
	bundlesJSON, err := serializeBundlesToJSON(opts.Bundles)
	if err != nil {
		return ids.NilPlacementGroupID(), err
	}
	cName := C.CString(opts.Name)
	defer C.free(unsafe.Pointer(cName))
	cBundles := C.CString(bundlesJSON)
	defer C.free(unsafe.Pointer(cBundles))

	var pgIDHex *C.char
	var errMsg *C.char
	status := C.ray_runtime_create_placement_group(cName, cBundles, C.int(opts.Strategy), &pgIDHex, &errMsg)
	if status == 0 {
		return ids.NilPlacementGroupID(), cError(errMsg)
	}
	defer C.free(unsafe.Pointer(pgIDHex))
	id, err := ids.PlacementGroupIDFromHex(C.GoString(pgIDHex))
	if err != nil {
		return ids.NilPlacementGroupID(), err
	}
	return id, nil
}

// RemovePlacementGroup removes an existing placement group by id.
func (s *NativeTaskSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	cID := C.CString(id.Hex())
	defer C.free(unsafe.Pointer(cID))
	var errMsg *C.char
	status := C.ray_runtime_remove_placement_group(cID, &errMsg)
	if status == 0 {
		return cError(errMsg)
	}
	return nil
}

// WaitPlacementGroupReady blocks until the placement group is ready or the
// timeout expires, honouring the caller's context: a cancellation or deadline
// on ctx aborts the wait (returning ctx.Err()) even though the underlying
// blocking C++ call may still be running in the background, matching how the
// GCS client threads cancellation through its runAsync/waitContext helpers.
func (s *NativeTaskSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("wait placement group: timeout must be positive, got %v", timeout)
	}
	// The C++ boundary takes an int seconds value; round sub-second timeouts up
	// (500ms -> 1s instead of a truncated 0) and clamp the upper bound to the
	// C int range.
	seconds := int64(math.Ceil(timeout.Seconds()))
	if seconds < 1 {
		seconds = 1
	} else if seconds > math.MaxInt32 {
		seconds = math.MaxInt32
	}

	type waitResult struct {
		ready int
		ok    bool
		err   error
	}
	// Check for cancellation before starting the blocking call.
	if err := ctx.Err(); err != nil {
		return err
	}
	resultCh := make(chan waitResult, 1)
	go func() {
		// Allocate the C string inside the goroutine so the sole consumer owns
		// it for its whole lifetime: the outer function returns (and would free
		// it) on ctx.Done() while this goroutine may still be inside the
		// blocking C call, which reads the pointer. Owning it here removes that
		// use-after-free race.
		cID := C.CString(id.Hex())
		defer C.free(unsafe.Pointer(cID))

		var ready C.int
		var errMsg *C.char
		status := C.ray_runtime_wait_placement_group_ready(cID, C.int(seconds), &ready, &errMsg)
		var err error
		if status == 0 {
			err = cError(errMsg)
		}
		select {
		case <-ctx.Done():
			// Cancelled; drop the result so the caller observes ctx.Err().
			return
		case resultCh <- waitResult{ready: int(ready), ok: status != 0, err: err}:
		}
	}()

	select {
	case result := <-resultCh:
		if !result.ok {
			return result.err
		}
		if result.ready == 0 {
			return fmt.Errorf("placement group %s not ready within %v: %w", id, timeout, submitter.ErrPlacementGroupNotReady)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
