// Copyright 2025 The Ray Authors.
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

// Package api provides the public API for Ray Go Runtime.
// This package is designed to be consistent with the Java Ray API.
package api

import (
	"fmt"
	"reflect"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// ============================================================================
// Task Caller Builder
// ============================================================================

// TaskCaller provides a builder for configuring and submitting tasks.
// Consistent with Java's io.ray.api.call.TaskCaller.
//
// Type parameter T is the return type of the task.
type TaskCaller[T any] struct {
	// functionDescriptor describes the function to call.
	functionDescriptor *function.GoFunctionDescriptor
	// args are the function arguments.
	args []function.FunctionArg
	// options are the task options.
	options *submitter.TaskOptions
	// numReturns is the number of return values.
	numReturns int
}

// Remote sets a remote function to be called.
// This is the entry point for the task caller builder.
//
// Parameters:
//   - fn: The remote function to call.
//
// Returns:
//   - *TaskCaller[T]: A task caller builder.
//
// Note: The function is automatically registered with the global registry
// if not already registered. This ensures the function is available for
// worker-side lookup during task execution.
func Remote[T any](fn interface{}) *TaskCaller[T] {
	// Register the function with the global registry
	// This is the key step that enables worker-side function lookup
	if err := RegisterFunction(fn); err != nil {
		// Ignore errors - the function may already be registered
		// Task submission will still work with the extracted descriptor
	}

	// Extract function descriptor - this never fails, but may return a fallback descriptor
	// for plugin-loaded functions or other edge cases
	funcDesc := function.ExtractFunctionDescriptor(fn)

	return &TaskCaller[T]{
		functionDescriptor: funcDesc,
		args:               make([]function.FunctionArg, 0),
		options:            &submitter.TaskOptions{},
		numReturns:         1,
	}
}

// RemoteVoid sets a remote function with no return value to be called.
//
// Parameters:
//   - fn: The remote function to call.
//
// Returns:
//   - *TaskCaller[struct{}]: A task caller builder for void functions.
//
// Note: The function is automatically registered with the global registry
// if not already registered. This ensures the function is available for
// worker-side lookup during task execution.
func RemoteVoid(fn interface{}) *TaskCaller[struct{}] {
	// Register the function with the global registry
	if err := RegisterFunction(fn); err != nil {
		// Ignore errors - the function may already be registered
	}

	// Extract function descriptor - this never fails, but may return a fallback descriptor
	funcDesc := function.ExtractFunctionDescriptor(fn)

	return &TaskCaller[struct{}]{
		functionDescriptor: funcDesc,
		args:               make([]function.FunctionArg, 0),
		options:            &submitter.TaskOptions{},
		numReturns:         0,
	}
}

// WithResources sets the resource requirements for the task.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5}).
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithResources(resources map[string]float64) *TaskCaller[T] {
	c.options.Resources = resources
	return c
}

// WithNumReturns sets the number of return values for the task.
//
// Parameters:
//   - numReturns: The number of return values.
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithNumReturns(numReturns int) *TaskCaller[T] {
	c.numReturns = numReturns
	return c
}

// WithMaxRetries sets the maximum number of retries for the task.
//
// Parameters:
//   - maxRetries: The maximum number of retries.
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithMaxRetries(maxRetries int) *TaskCaller[T] {
	c.options.RetryPolicy = &submitter.RetryPolicy{
		MaxRetries: maxRetries,
	}
	return c
}

// WithRuntimeEnv sets the runtime environment for the task.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string.
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithRuntimeEnv(runtimeEnv string) *TaskCaller[T] {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithName sets the name for the task.
//
// Parameters:
//   - name: The task name (used for monitoring and debugging).
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithName(name string) *TaskCaller[T] {
	c.options.Name = name
	return c
}

// Call submits the task with the provided arguments.
//
// Parameters:
//   - args: The function arguments (can be values or ObjectRefs).
//
// Returns:
//   - *ObjectRef[T]: A reference to the task result.
//   - error: Any error encountered during submission.
func (c *TaskCaller[T]) Call(args ...interface{}) (*ObjectRef[T], error) {
	// Convert arguments to FunctionArg format
	functionArgs := make([]function.FunctionArg, len(args))
	for i, arg := range args {
		functionArgs[i] = convertArgToFunctionArg(arg)
	}
	// Release the PutWithID local reference of internal pass-by-reference
	// arguments on every exit path (successful submit, submit failure, or an
	// unavailable submitter): once submitted, the C++ reference counter tracks
	// the argument object; otherwise it would stay pinned in the object store.
	defer releaseInternalByRefArgRefs(functionArgs)

	// Get task submitter
	submitter := getTaskSubmitter()
	if submitter == nil {
		// Task submitter is nil even though runtime may be initialized.
		// This indicates an internal inconsistency rather than "runtime not initialized".
		return nil, errors.NewRuntimeError("submit_task", "submitter_not_available")
	}

	// Submit task
	returnIDs, err := submitter.SubmitTask(
		c.functionDescriptor,
		functionArgs,
		c.numReturns,
		c.options,
	)

	if err != nil {
		// Convert internal error to public error
		return nil, errors.ConvertToPublic(err)
	}

	// Create ObjectRef for the first return value
	if len(returnIDs) > 0 {
		return createObjectRefWithFinalizer[T](returnIDs[0], "")
	}

	return nil, nil
}

// releaseInternalByRefArgRefs releases the local reference that PutWithID added
// for the internal pass-by-reference arguments (marked ReleaseAfterSubmit). It is
// deferred so it runs on every exit path: after a successful submit the C++
// reference counter tracks the argument object (submitted-task reference plus the
// worker's borrow), and on a failed submit or an unavailable submitter the argument
// was never used. Either way the reference is dropped and the object is not pinned
// in the object store forever.
func releaseInternalByRefArgRefs(args []function.FunctionArg) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return
	}
	runtime := handle.Runtime()
	if runtime == nil || runtime.GetObjectStore() == nil {
		return
	}
	objectStore := runtime.GetObjectStore()
	for _, arg := range args {
		if arg.IsPassByRef() && arg.ObjectRef != nil && arg.ObjectRef.ReleaseAfterSubmit {
			objectID := arg.ObjectRef.ObjectID
			_ = objectStore.RemoveLocalReference(&objectID)
		}
	}
}

// ============================================================================
// Actor Creator Builder
// ============================================================================

// ActorCreator provides a builder for creating actors.
// Consistent with Java's io.ray.api.call.ActorCreator.
//
// Type parameter T is the actor type.
type ActorCreator[T any] struct {
	// functionDescriptor describes the actor class.
	functionDescriptor *function.GoFunctionDescriptor
	// args are the constructor arguments.
	args []function.FunctionArg
	// options are the actor creation options.
	options *submitter.ActorCreationOptions
}

// Actor sets an actor class to be created.
// This is the entry point for the actor creator builder.
//
// Parameters:
//   - actorClass: The actor class to create.
//
// Returns:
//   - *ActorCreator[T]: An actor creator builder.
func Actor[T any](actorClass interface{}) *ActorCreator[T] {
	// Extract function descriptor from the actor class
	funcDesc := extractActorFunctionDescriptor(actorClass)

	return &ActorCreator[T]{
		functionDescriptor: funcDesc,
		args:               make([]function.FunctionArg, 0),
		options:            &submitter.ActorCreationOptions{},
	}
}

// WithName sets the name for the actor.
//
// Parameters:
//   - name: The actor name.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithName(name string) *ActorCreator[T] {
	c.options.Name = name
	return c
}

// WithNamespace sets the namespace for the actor.
//
// Parameters:
//   - namespace: The actor namespace.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithNamespace(namespace string) *ActorCreator[T] {
	c.options.Namespace = namespace
	return c
}

// WithResources sets the resource requirements for the actor.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5}).
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithResources(resources map[string]float64) *ActorCreator[T] {
	c.options.Resources = resources
	return c
}

// WithMaxRestarts sets the maximum number of restarts for the actor.
//
// Parameters:
//   - maxRestarts: The maximum number of restarts.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithMaxRestarts(maxRestarts int) *ActorCreator[T] {
	c.options.MaxRestarts = maxRestarts
	return c
}

// WithMaxTaskRetries sets the maximum number of task retries for the actor.
//
// Parameters:
//   - maxTaskRetries: The maximum number of task retries.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithMaxTaskRetries(maxTaskRetries int) *ActorCreator[T] {
	c.options.MaxTaskRetries = maxTaskRetries
	return c
}

// WithRuntimeEnv sets the runtime environment for the actor.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithRuntimeEnv(runtimeEnv string) *ActorCreator[T] {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithMaxConcurrency sets the maximum number of concurrent calls for the actor.
//
// Parameters:
//   - maxConcurrency: The maximum number of concurrent calls.
//
// Returns:
//   - *ActorCreator[T]: The same actor creator for chaining.
func (c *ActorCreator[T]) WithMaxConcurrency(maxConcurrency int) *ActorCreator[T] {
	c.options.MaxConcurrency = maxConcurrency
	return c
}

// Create creates the actor with the provided constructor arguments.
//
// Parameters:
//   - args: The constructor arguments (can be values or ObjectRefs).
//
// Returns:
//   - *ActorHandleImpl[T]: A handle to the created actor.
//   - error: Any error encountered during actor creation.
func (c *ActorCreator[T]) Create(args ...interface{}) (*ActorHandleImpl[T], error) {
	// Convert arguments to FunctionArg format
	functionArgs := make([]function.FunctionArg, len(args))
	for i, arg := range args {
		functionArgs[i] = convertArgToFunctionArg(arg)
	}
	// Release the PutWithID local reference of internal pass-by-reference
	// arguments on every exit path (see releaseInternalByRefArgRefs).
	defer releaseInternalByRefArgRefs(functionArgs)

	// Get task submitter
	submitter := getTaskSubmitter()
	if submitter == nil {
		// Task submitter is nil even though runtime may be initialized.
		// This indicates an internal inconsistency rather than "runtime not initialized".
		return nil, errors.NewRuntimeError("create_actor", "submitter_not_available")
	}

	// Create actor
	actorID, err := submitter.CreateActor(
		c.functionDescriptor,
		functionArgs,
		c.options,
	)
	if err != nil {
		// Convert internal error to public error
		return nil, errors.ConvertToPublic(err)
	}

	// Create actor handle
	return NewActorHandleImpl[T](actorID), nil
}

// ============================================================================
// Helper Functions (moved to object.go for better organization)
// ============================================================================

// Note: setupObjectRefFinalizer, releaseObjectRef, and createObjectRefWithFinalizer
// have been moved to object.go as they are ObjectRef lifecycle management functions,
// not specific to task calling. They are still accessible from call.go since both
// files are in the same package.

// extractActorFunctionDescriptor extracts a FunctionDescriptor from an actor class.
func extractActorFunctionDescriptor(actorClass interface{}) *function.GoFunctionDescriptor {
	actorType := reflect.TypeOf(actorClass)
	if actorType == nil {
		return function.NewGoActorMethodDescriptorOrUnknown("unknown", "unknown", "unknown", "")
	}
	// Actors are passed as pointers (&MyActor{}); dereference so Name() and
	// PkgPath() reflect the underlying type (a pointer type has empty Name and
	// PkgPath, which previously degraded the descriptor to all-"unknown").
	if actorType.Kind() == reflect.Ptr {
		actorType = actorType.Elem()
	}

	typeName := actorType.Name()
	if typeName == "" {
		typeName = actorType.String()
	}

	packagePath := actorType.PkgPath()
	if packagePath == "" {
		packagePath = "unknown"
	}

	// Split module/package with the same heuristic as the function registry so
	// the actor constructor descriptor matches registered functions.
	moduleName, pkgPath := function.SplitModuleAndPackage(packagePath)
	if moduleName == "" {
		moduleName = "unknown"
	}

	// "<init>" is the reserved method name for actor constructors.
	return function.NewGoActorMethodDescriptorOrUnknown(moduleName, pkgPath, typeName, "<init>")
}

// convertArgToFunctionArg converts an interface{} argument to a FunctionArg.
// This function implements object passing threshold control:
// - Objects smaller than threshold (100KB) are passed by value (serialized directly)
// - Objects larger than threshold are passed by reference (stored in object store)
func convertArgToFunctionArg(arg interface{}) function.FunctionArg {
	if objRef, ok := arg.(*ObjectRef[any]); ok {
		return function.NewFunctionArgByRef(objRef.ObjectID(), nil)
	}

	// Use the global serializer from object package
	ser := object.GetSerializer()
	nativeObj, err := ser.Serialize(arg)
	if err != nil {
		return function.NewFunctionArgByValue(nil, nil)
	}
	defer nativeObj.Close()

	// Check if object should be passed by value or by reference based on size
	// This aligns with Java's implementation in SystemConfig.java
	if object.ShouldPassByValue(len(nativeObj.Data), object.GetIsLocalMode()) {
		// Small object: pass by value (serialize directly)
		data := make([]byte, len(nativeObj.Data))
		copy(data, nativeObj.Data)
		return function.NewFunctionArgByValue(data, nil)
	} else {
		// Large object: pass by reference (store in object store)
		// Generate a new ObjectID for this argument
		objectID := ids.NewObjectID()

		// Store the object in the object store
		handle, ok := tryGetHandle()
		if ok && handle != nil {
			runtime := handle.Runtime()
			if runtime != nil {
				objectStore := runtime.GetObjectStore()
				if objectStore != nil {
					// Put the object into the object store with the generated ID
					err := objectStore.PutRawWithID(nativeObj, &objectID)
					if err == nil {
						// Return pass-by-reference argument, marked for release once
						// the task is submitted so the PutWithID local reference does
						// not pin the object in the object store forever.
						arg := function.NewFunctionArgByRef(objectID, nil)
						arg.ObjectRef.ReleaseAfterSubmit = true
						return arg
					}
				}
			}
		}

		// Fallback: pass by value if object store is not available
		data := make([]byte, len(nativeObj.Data))
		copy(data, nativeObj.Data)
		return function.NewFunctionArgByValue(data, nil)
	}
}

// getTaskSubmitter returns the current task submitter.
// Deprecated: Use tryGetTaskSubmitter() instead, which returns (submitter, ok).
func getTaskSubmitter() submitter.TaskSubmitter {
	submitter, _ := tryGetTaskSubmitter()
	return submitter
}

// ============================================================================
// Actor Handle Types
// ============================================================================

// ActorHandleImpl is a typed actor handle implementation.
// Consistent with Java's io.ray.api.BaseActorHandle.
//
// Type parameter T is the actor type.
// This implementation embeds NativeActorHandle for cross-language compatibility.
type ActorHandleImpl[T any] struct {
	*object.NativeActorHandle
	methodExtractor *MethodExtractor
}

// NewActorHandleImpl creates a new ActorHandleImpl instance.
func NewActorHandleImpl[T any](actorID ids.ActorID) *ActorHandleImpl[T] {
	return &ActorHandleImpl[T]{
		NativeActorHandle: &object.NativeActorHandle{
			ActorID:  actorID,
			Language: object.LanguageGo,
		},
		methodExtractor: NewMethodExtractor(),
	}
}

// ID returns the actor ID.
// This method delegates to the embedded NativeActorHandle.
func (a *ActorHandleImpl[T]) ID() ids.ActorID {
	return a.NativeActorHandle.ActorID
}

// Task creates a task caller for an actor method.
// This is the primary way to call actor methods.
//
// Parameters:
//   - method: The actor method to call (method expression or method value)
//   - args: Optional method arguments
//
// Returns:
//   - *ActorTaskCaller[T]: A task caller builder for the actor method
//
// Example:
//
//	// Method expression
//	resultRef, err := actor.Task((*MyActor).MethodName, arg1, arg2).Remote()
//
//	// Method value
//	resultRef, err := actor.Task(myActorInstance.MethodName, arg1, arg2).Remote()
func (a *ActorHandleImpl[T]) Task(method interface{}, args ...interface{}) *ActorTaskCaller[T] {
	// Extract method descriptor
	methodDesc, err := a.methodExtractor.ExtractActorMethodDescriptor(method)
	if err != nil {
		// Return a caller that will fail on Remote()
		return &ActorTaskCaller[T]{
			actorID: a.NativeActorHandle.ActorID,
			err:     fmt.Errorf("failed to extract method descriptor: %w", err),
		}
	}

	// Convert arguments to FunctionArg format
	functionArgs := make([]function.FunctionArg, len(args))
	for i, arg := range args {
		functionArgs[i] = convertArgToFunctionArg(arg)
	}

	return &ActorTaskCaller[T]{
		actorID:          a.NativeActorHandle.ActorID,
		methodDescriptor: methodDesc,
		args:             functionArgs,
		options:          &submitter.TaskOptions{},
		numReturns:       1,
	}
}

// ActorTaskCaller is a task caller specifically for actor method calls.
// Consistent with Java's actor task caller pattern.
//
// Type parameter T is the return type of the actor method.
type ActorTaskCaller[T any] struct {
	actorID          ids.ActorID
	methodDescriptor *function.GoFunctionDescriptor
	args             []function.FunctionArg
	options          *submitter.TaskOptions
	numReturns       int
	err              error // Deferred error reporting
}

// Remote submits the actor method call and returns an ObjectRef.
//
// Returns:
//   - *ObjectRef[T]: A reference to the task result
//   - error: Any error encountered during submission
func (c *ActorTaskCaller[T]) Remote() (*ObjectRef[T], error) {
	if c.err != nil {
		return nil, c.err
	}

	// Release the PutWithID local reference of internal pass-by-reference
	// arguments on every exit path (successful submit, submit failure, or an
	// unavailable submitter): once submitted, the C++ reference counter tracks
	// the argument object; otherwise it would stay pinned in the object store.
	defer releaseInternalByRefArgRefs(c.args)

	submitter := getTaskSubmitter()
	if submitter == nil {
		// Task submitter is nil even though runtime may be initialized.
		// This indicates an internal inconsistency rather than "runtime not initialized".
		return nil, errors.NewRuntimeError("submit_actor_task", "submitter_not_available")
	}

	// Submit actor task
	returnIDs, err := submitter.SubmitActorTask(
		c.actorID,
		c.methodDescriptor,
		c.args,
		c.numReturns,
		c.options,
	)
	if err != nil {
		// Convert internal error to public error
		return nil, errors.ConvertToPublic(err)
	}

	if len(returnIDs) > 0 {
		return createObjectRefWithFinalizer[T](returnIDs[0], "")
	}

	return nil, nil
}

// RemoteVoid submits the actor method call without returning a result.
// Use this for actor methods that don't return a value.
//
// Returns:
//   - error: Any error encountered during submission
func (c *ActorTaskCaller[T]) RemoteVoid() error {
	_, err := c.Remote()
	return err
}

// WithResources sets the resource requirements for the actor method call.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5})
//
// Returns:
//   - *ActorTaskCaller[T]: The same caller for chaining
func (c *ActorTaskCaller[T]) WithResources(resources map[string]float64) *ActorTaskCaller[T] {
	c.options.Resources = resources
	return c
}

// WithRuntimeEnv sets the runtime environment for the actor method call.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string
//
// Returns:
//   - *ActorTaskCaller[T]: The same caller for chaining
func (c *ActorTaskCaller[T]) WithRuntimeEnv(runtimeEnv string) *ActorTaskCaller[T] {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithName sets the name for the actor method call.
//
// Parameters:
//   - name: The task name
//
// Returns:
//   - *ActorTaskCaller[T]: The same caller for chaining
func (c *ActorTaskCaller[T]) WithName(name string) *ActorTaskCaller[T] {
	return c
}

// WithConcurrencyGroup sets the concurrency group for the actor method call.
//
// Parameters:
//   - groupName: The concurrency group name
//
// Returns:
//   - *ActorTaskCaller[T]: The same caller for chaining
func (c *ActorTaskCaller[T]) WithConcurrencyGroup(groupName string) *ActorTaskCaller[T] {
	return c
}

// WithNumReturns sets the number of return values for the actor method call.
//
// Parameters:
//   - numReturns: The number of return values
//
// Returns:
//   - *ActorTaskCaller[T]: The same caller for chaining
func (c *ActorTaskCaller[T]) WithNumReturns(numReturns int) *ActorTaskCaller[T] {
	c.numReturns = numReturns
	return c
}
