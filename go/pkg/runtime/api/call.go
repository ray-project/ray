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
	"bytes"
	"fmt"
	"reflect"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
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

// WithPlacementGroup binds the task to the given placement group bundle.
//
// Parameters:
//   - group: The placement group to bind to.
//   - bundleIndex: The index of the bundle to use.
//
// Returns:
//   - *TaskCaller[T]: The same task caller for chaining.
func (c *TaskCaller[T]) WithPlacementGroup(group *PlacementGroup, bundleIndex int) *TaskCaller[T] {
	if group == nil {
		return c
	}
	c.options.PlacementGroup = &submitter.PlacementGroupOptions{
		ID:          group.ID(),
		BundleIndex: bundleIndex,
	}
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
	functionArgs := convertArgs(args...)
	return submitTaskRef[T]("submit_task", functionArgs, func(s submitter.TaskSubmitter) ([]ids.ObjectID, error) {
		return s.SubmitTask(c.functionDescriptor, functionArgs, c.numReturns, c.options)
	})
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
// Actor Options Builder
// ============================================================================

// actorOptions encapsulates the actor creation option state shared by
// ActorCreator and PythonActorCreator so that the builder methods are
// implemented once instead of being duplicated across the creator types.
//
// Type parameter T is the concrete creator type; builder methods return T so
// chained calls keep their concrete type.
type actorOptions[T any] struct {
	// options are the actor creation options.
	options *submitter.ActorCreationOptions
	// self points to the owning creator so builder methods can return the
	// concrete creator type for chaining.
	self T
	// err is a deferred error from a builder call that requested an option the
	// backends cannot honor. It is surfaced by Create so an unsupported option
	// fails loudly instead of silently becoming a no-op.
	err error
}

// newActorOptions creates actor options with a default empty ActorCreationOptions
// and a pointer back to the owning creator.
func newActorOptions[T any](self T) actorOptions[T] {
	return actorOptions[T]{
		// MaxPendingCalls defaults to unlimited (MaxPendingCallsUnlimited),
		// matching ActorCreationOptionsBuilder's default.
		options: &submitter.ActorCreationOptions{MaxPendingCalls: submitter.MaxPendingCallsUnlimited},
		self:    self,
	}
}

// WithName sets the name for the actor.
//
// Parameters:
//   - name: The actor name.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithName(name string) T {
	o.options.Name = name
	return o.self
}

// WithNamespace sets the namespace for the actor.
//
// Parameters:
//   - namespace: The actor namespace.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithNamespace(namespace string) T {
	o.options.Namespace = namespace
	return o.self
}

// WithPlacementGroup binds the actor to the given placement group bundle.
//
// Actor-level placement-group binding is not supported by any backend in this
// tree: the native path's C++ ActorCreationOptions has no placement-group
// fields (task_submitter_ops.cc), and local mode does not schedule against
// bundles. Instead of silently ignoring the request, the creator records an
// error that Create surfaces so the caller learns the option was not applied.
// Note: the internal tree has the same gap (its actor creation path does not
// consume PlacementGroup either); this loud failure is an OSS-side correction.
//
// Parameters:
//   - group: The placement group to bind to.
//   - bundleIndex: The index of the bundle to use.
//
// Returns:
//   - T: The same actor creator for chaining; Create will fail if the binding
//     is unsupported.
func (o *actorOptions[T]) WithPlacementGroup(group *PlacementGroup, bundleIndex int) T {
	if group == nil {
		return o.self
	}
	if o.err == nil {
		o.err = errors.NewRuntimeError("create_actor", "actor placement-group binding is not supported on this path")
	}
	return o.self
}

// WithResources sets the resource requirements for the actor.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5}).
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithResources(resources map[string]float64) T {
	o.options.Resources = resources
	return o.self
}

// WithMaxRestarts sets the maximum number of restarts for the actor.
//
// Parameters:
//   - maxRestarts: The maximum number of restarts.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithMaxRestarts(maxRestarts int) T {
	o.options.MaxRestarts = maxRestarts
	return o.self
}

// WithMaxTaskRetries sets the maximum number of task retries for the actor.
//
// Parameters:
//   - maxTaskRetries: The maximum number of task retries.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithMaxTaskRetries(maxTaskRetries int) T {
	o.options.MaxTaskRetries = maxTaskRetries
	return o.self
}

// WithRuntimeEnv sets the runtime environment for the actor.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithRuntimeEnv(runtimeEnv string) T {
	o.options.RuntimeEnv = runtimeEnv
	return o.self
}

// WithLifetime sets the actor lifetime (detached or non-detached).
//
// Parameters:
//   - lifetime: The actor lifetime.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithLifetime(lifetime submitter.ActorLifetime) T {
	o.options.Lifetime = lifetime
	return o.self
}

// WithAsync sets whether the actor uses async direct call mode.
//
// Parameters:
//   - async: Whether to use async direct call mode.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithAsync(async bool) T {
	o.options.IsAsync = async
	return o.self
}

// WithMaxPendingCalls sets the maximum pending calls (-1 means unlimited).
//
// Parameters:
//   - maxPendingCalls: The maximum number of pending calls.
//
// Returns:
//   - T: The same actor creator for chaining.
func (o *actorOptions[T]) WithMaxPendingCalls(maxPendingCalls int) T {
	o.options.MaxPendingCalls = maxPendingCalls
	return o.self
}

// WithConcurrencyGroups sets the actor's concurrency groups.
// Can be called multiple times to append groups. Each group is validated
// (non-empty name, MaxCalls >= -1) and an invalid group panics, mirroring the
// other builder methods' fail-fast behavior (e.g. SetParallelism).
func (o *actorOptions[T]) WithConcurrencyGroups(groups ...ConcurrencyGroup) T {
	for _, g := range groups {
		if err := g.Validate(); err != nil {
			panic(fmt.Sprintf("invalid concurrency group %q: %v", g.Name, err))
		}
		o.options.ConcurrencyGroups = append(o.options.ConcurrencyGroups, submitter.ConcurrencyGroup{
			Name:     g.Name,
			MaxCalls: g.MaxCalls,
			Methods:  g.Methods,
		})
	}
	return o.self
}

// WithDefaultConcurrencyGroup sets the default concurrency group.
// The default group contains all methods not explicitly assigned to other
// groups. It is registered as a NAMED group "default" (key=actorID/"default"),
// distinct from the actor's implicit empty-name default group (key=actorID),
// matching Java and local_mode's existing behavior.
func (o *actorOptions[T]) WithDefaultConcurrencyGroup(maxCalls int) T {
	return o.WithConcurrencyGroups(ConcurrencyGroup{
		Name:     "default",
		MaxCalls: maxCalls,
		Methods:  []string{},
	})
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
	// actorOptions are the shared actor creation options and builder state.
	actorOptions[*ActorCreator[T]]
}

// Actor sets an actor class to be created.
// This is the entry point for the actor creator builder.
//
// Parameters:
//   - actorClass: The actor class to create. This can be either:
//   - a constructor factory function (e.g. `func() *MyActor` or
//     `func(x int) *MyActor`), which is registered as the actor's "<init>"
//     constructor so the worker can build instances; or
//   - a pointer/instance of the actor type (e.g. `(*MyActor)(nil)`), in which
//     case a zero-value constructor is registered automatically on the typed
//     path. Actors with constructor arguments should use the factory-function
//     form (or register the constructor explicitly via api.RegisterActorClass).
//     Note that the untyped convenience form (Ray.Actor, where T is interface{})
//     never auto-registers: it preserves OSS's original behavior and relies on
//     an explicit api.RegisterActorClass call.
//
// Returns:
//   - *ActorCreator[T]: An actor creator builder.
func Actor[T any](actorClass interface{}) *ActorCreator[T] {
	// The untyped convenience path (Ray.Actor / api.Instance().Actor, where T is
	// interface{}) keeps OSS's original behavior: it derives the "<init>"
	// descriptor from the actorClass argument and registers no constructor (the
	// constructor is expected to be registered explicitly via api.RegisterActorClass,
	// exactly as the OSS examples do). The typed path (api.Actor[*T], INT-aligned)
	// registers the constructor and derives the descriptor from the type parameter
	// T, which is the type-parameter form Java's Ray.actor(Class) maps to.
	funcDesc := actorDescriptorFor[T](actorClass)
	if !isInterfaceType[T]() {
		registerActorConstructor[T](actorClass)
	}

	creator := &ActorCreator[T]{
		functionDescriptor: funcDesc,
		args:               make([]function.FunctionArg, 0),
	}
	creator.actorOptions = newActorOptions[*ActorCreator[T]](creator)
	return creator
}

// isInterfaceType reports whether the type parameter T is an interface type
// (e.g. interface{}). It is used to distinguish the untyped convenience entry
// point (Ray.Actor / api.Actor[interface{}]) from the typed INT-aligned form
// (api.Actor[*MyActor]). The zero value of T has no type identity, so T is
// inspected through a pointer to it: (*interface{}) is a pointer whose element
// kind is Interface, whereas (*MyActor) is a pointer whose element kind is the
// concrete kind.
func isInterfaceType[T any]() bool {
	t := reflect.TypeOf((*T)(nil))
	return t != nil && t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Interface
}

// actorDescriptorFor builds the "<init>" descriptor for an actor creation.
//
// When T is a concrete actor type (the INT-aligned typed form) the descriptor
// is derived from the type parameter so it always matches the constructor that
// registerActorConstructor[T] registered under the same type. When T is an
// interface (the OSS untyped convenience path, e.g. Ray.Actor / api.Instance().
// Actor(&MyActor{})), the descriptor must instead come from the actorClass
// argument: deriving it from interface{} would degrade the module/package/type
// names to "unknown", so the worker could never resolve the constructor. This
// preserves OSS's original behavior for the untyped entry point.
func actorDescriptorFor[T any](actorClass interface{}) *function.GoFunctionDescriptor {
	if isInterfaceType[T]() {
		return extractActorFunctionDescriptor(actorClass)
	}
	return extractActorTypeDescriptor[T]()
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
	if c.err != nil {
		return nil, c.err
	}

	functionArgs := convertArgs(args...)

	actorID, err := createActorWithSubmitter("create_actor", functionArgs, func(s submitter.TaskSubmitter) (ids.ActorID, error) {
		return s.CreateActor(c.functionDescriptor, functionArgs, c.options)
	})
	if err != nil {
		return nil, err
	}

	return NewActorHandleImpl[T](actorID), nil
}

// ============================================================================
// Python Task Caller Builder (cross-language)
// ============================================================================

// pythonDummyType is the marker inserted before each argument of a Python call,
// matching Java's ArgumentsBuilder.wrap() and CPP's Arguments::WrapArgsImpl.
// Python's recover_args expects the [DUMMY_TYPE, value, ...] layout.
var pythonDummyType = []byte("__RAY_DUMMY__")

// pythonDummyRawMetadata is the metadata attached to each DUMMY_TYPE marker,
// matching CPP's METADATA_STR_RAW.
var pythonDummyRawMetadata = []byte(object.MetadataTypeRaw)

// pythonInitFunctionName is the Python actor constructor name. Python actors are
// created via a task whose function name is "__init__" (see FunctionActorManager).
const pythonInitFunctionName = "__init__"

// wrapPythonArgs converts raw call arguments into the FunctionArg sequence used
// by Python workers, inserting a DUMMY_TYPE marker before every value so that
// recover_args can restore the original argument list.
func wrapPythonArgs(args []interface{}) []function.FunctionArg {
	functionArgs := make([]function.FunctionArg, 0, len(args)*2)
	for _, arg := range args {
		functionArgs = append(functionArgs, function.NewFunctionArgByValue(pythonDummyType, pythonDummyRawMetadata))
		functionArgs = append(functionArgs, convertArgToFunctionArg(arg))
	}
	return functionArgs
}

// PythonTaskCaller provides a builder for configuring and submitting Python tasks.
// This is the cross-language counterpart to TaskCaller, allowing Go code to invoke
// Python functions (module-level or class methods) on a Python worker.
//
// Type parameter T is the return type of the task.
type PythonTaskCaller[T any] struct {
	// functionDescriptor describes the Python function to call.
	functionDescriptor *function.PythonFunctionDescriptor
	// options are the task options.
	options *submitter.TaskOptions
	// numReturns is the number of return values.
	numReturns int
	// err is a deferred error from descriptor validation.
	err error
}

// RemotePython sets a Python function to be called remotely.
// This is the cross-language entry point for calling Python functions.
//
// Parameters:
//   - moduleName: The Python module path (e.g., "my_module").
//   - functionName: The Python function name (e.g., "add").
//   - className: The Python class name for class methods. Empty for module-level functions.
//
// The argument order is (module, function, class): the optional className comes
// last because module-level functions (the common case) pass an empty class.
// This differs from the internal NewPythonFunctionDescriptor(module, class,
// function, hash) order, so callers should rely on the documented parameter
// names rather than positional correspondence.
//
// Returns:
//   - *PythonTaskCaller[T]: A task caller builder.
func RemotePython[T any](moduleName, functionName, className string) *PythonTaskCaller[T] {
	desc, err := function.NewPythonFunctionDescriptor(moduleName, className, functionName, "")
	return &PythonTaskCaller[T]{
		functionDescriptor: desc,
		options:            &submitter.TaskOptions{},
		numReturns:         1,
		err:                err,
	}
}

// RemotePythonVoid sets a Python function with no return value to be called.
//
// Parameters:
//   - moduleName: The Python module path (e.g., "my_module").
//   - functionName: The Python function name (e.g., "log_event").
//   - className: The Python class name for class methods. Empty for module-level functions.
//
// Argument order is (module, function, class); see RemotePython for details.
//
// Returns:
//   - *PythonTaskCaller[struct{}]: A task caller builder for void functions.
func RemotePythonVoid(moduleName, functionName, className string) *PythonTaskCaller[struct{}] {
	desc, err := function.NewPythonFunctionDescriptor(moduleName, className, functionName, "")
	return &PythonTaskCaller[struct{}]{
		functionDescriptor: desc,
		options:            &submitter.TaskOptions{},
		numReturns:         0,
		err:                err,
	}
}

// WithResources sets the resource requirements for the Python task.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5}).
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithResources(resources map[string]float64) *PythonTaskCaller[T] {
	c.options.Resources = resources
	return c
}

// WithNumReturns sets the number of return values for the Python task.
//
// Parameters:
//   - numReturns: The number of return values.
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithNumReturns(numReturns int) *PythonTaskCaller[T] {
	c.numReturns = numReturns
	return c
}

// WithMaxRetries sets the maximum number of retries for the Python task.
//
// Parameters:
//   - maxRetries: The maximum number of retries.
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithMaxRetries(maxRetries int) *PythonTaskCaller[T] {
	c.options.RetryPolicy = &submitter.RetryPolicy{
		MaxRetries: maxRetries,
	}
	return c
}

// WithRuntimeEnv sets the runtime environment for the Python task.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string.
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithRuntimeEnv(runtimeEnv string) *PythonTaskCaller[T] {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithName sets the name for the Python task.
//
// Parameters:
//   - name: The task name (used for monitoring and debugging).
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithName(name string) *PythonTaskCaller[T] {
	c.options.Name = name
	return c
}

// WithPlacementGroup binds the Python task to the given placement group bundle.
//
// Parameters:
//   - group: The placement group to bind to.
//   - bundleIndex: The index of the bundle to use.
//
// Returns:
//   - *PythonTaskCaller[T]: The same task caller for chaining.
func (c *PythonTaskCaller[T]) WithPlacementGroup(group *PlacementGroup, bundleIndex int) *PythonTaskCaller[T] {
	if group == nil {
		return c
	}
	c.options.PlacementGroup = &submitter.PlacementGroupOptions{
		ID:          group.ID(),
		BundleIndex: bundleIndex,
	}
	return c
}

// Call submits the Python task with the provided arguments.
//
// Parameters:
//   - args: The function arguments (can be values or ObjectRefs).
//
// Returns:
//   - *ObjectRef[T]: A reference to the task result.
//   - error: Any error encountered during submission.
func (c *PythonTaskCaller[T]) Call(args ...interface{}) (*ObjectRef[T], error) {
	if c.err != nil {
		return nil, c.err
	}

	functionArgs := wrapPythonArgs(args)
	return submitTaskRef[T]("submit_task", functionArgs, func(s submitter.TaskSubmitter) ([]ids.ObjectID, error) {
		return s.SubmitTask(c.functionDescriptor, functionArgs, c.numReturns, c.options)
	})
}

// ============================================================================
// Python Actor Creator / Handle (cross-language)
// ============================================================================

// PythonActorCreator provides a builder for creating Python actors.
// This is the cross-language counterpart to ActorCreator, allowing Go code to
// instantiate a Python class as a Ray actor on a Python worker.
type PythonActorCreator struct {
	// functionDescriptor describes the actor constructor ("__init__").
	functionDescriptor *function.PythonFunctionDescriptor
	// options are the actor creation options.
	options *submitter.ActorCreationOptions
	// err is a deferred error from descriptor validation.
	err error
}

// RemotePythonActor sets a Python actor class to be created remotely.
// This is the cross-language entry point for creating Python actors.
//
// Parameters:
//   - moduleName: The Python module path (e.g., "my_module").
//   - className: The Python actor class name (e.g., "Calculator").
//
// Returns:
//   - *PythonActorCreator: An actor creator builder.
func RemotePythonActor(moduleName, className string) *PythonActorCreator {
	// Python actors are created via a task whose function name is pythonInitFunctionName
	// and whose class name is the actor class (see FunctionActorManager).
	desc, err := function.NewPythonFunctionDescriptor(moduleName, className, pythonInitFunctionName, "")
	return &PythonActorCreator{
		functionDescriptor: desc,
		options:            &submitter.ActorCreationOptions{},
		err:                err,
	}
}

// WithName sets the name for the Python actor.
//
// Parameters:
//   - name: The actor name.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithName(name string) *PythonActorCreator {
	c.options.Name = name
	return c
}

// WithNamespace sets the namespace for the Python actor.
//
// Parameters:
//   - namespace: The actor namespace.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithNamespace(namespace string) *PythonActorCreator {
	c.options.Namespace = namespace
	return c
}

// WithResources sets the resource requirements for the Python actor.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5}).
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithResources(resources map[string]float64) *PythonActorCreator {
	c.options.Resources = resources
	return c
}

// WithMaxRestarts sets the maximum number of restarts for the Python actor.
//
// Parameters:
//   - maxRestarts: The maximum number of restarts.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithMaxRestarts(maxRestarts int) *PythonActorCreator {
	c.options.MaxRestarts = maxRestarts
	return c
}

// WithMaxTaskRetries sets the maximum number of task retries for the Python actor.
//
// Parameters:
//   - maxTaskRetries: The maximum number of task retries.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithMaxTaskRetries(maxTaskRetries int) *PythonActorCreator {
	c.options.MaxTaskRetries = maxTaskRetries
	return c
}

// WithRuntimeEnv sets the runtime environment for the Python actor.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithRuntimeEnv(runtimeEnv string) *PythonActorCreator {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithMaxConcurrency sets the maximum number of concurrent calls for the Python actor.
//
// Parameters:
//   - maxConcurrency: The maximum number of concurrent calls.
//
// Returns:
//   - *PythonActorCreator: The same actor creator for chaining.
func (c *PythonActorCreator) WithMaxConcurrency(maxConcurrency int) *PythonActorCreator {
	c.options.MaxConcurrency = maxConcurrency
	return c
}

// Create creates the Python actor with the provided constructor arguments.
//
// Parameters:
//   - args: The constructor arguments (can be values or ObjectRefs).
//
// Returns:
//   - *PythonActorHandle: A handle to the created actor.
//   - error: Any error encountered during actor creation.
func (c *PythonActorCreator) Create(args ...interface{}) (*PythonActorHandle, error) {
	if c.err != nil {
		return nil, c.err
	}

	functionArgs := wrapPythonArgs(args)

	actorID, err := createActorWithSubmitter("create_actor", functionArgs, func(s submitter.TaskSubmitter) (ids.ActorID, error) {
		return s.CreateActor(c.functionDescriptor, functionArgs, c.options)
	})
	if err != nil {
		return nil, err
	}

	return &PythonActorHandle{
		nativeHandle: object.NewNativeActorHandle(actorID, object.LanguagePython),
		moduleName:   c.functionDescriptor.ModuleName,
		className:    c.functionDescriptor.ClassName,
	}, nil
}

// PythonActorHandle represents a handle to a Python actor.
// Method calls are dispatched by method name (Go cannot reflect over Python
// methods, so the method name is passed as a string).
type PythonActorHandle struct {
	// nativeHandle is the underlying cross-language actor handle.
	nativeHandle *object.NativeActorHandle
	// moduleName is the Python module path of the actor class.
	moduleName string
	// className is the Python actor class name.
	className string
}

// ID returns the actor ID.
func (h *PythonActorHandle) ID() ids.ActorID {
	return h.nativeHandle.ActorID
}

// ActorTask creates a task caller for a Python actor method with an explicit
// return type. Go does not support method-level type parameters, so the caller
// passes the handle explicitly instead of invoking a method on it.
//
// Parameters:
//   - handle: The Python actor handle.
//   - methodName: The name of the Python actor method (e.g., "add").
//   - args: Optional method arguments.
//
// Returns:
//   - *PythonActorTaskCaller: A task caller builder for the actor method.
//
// Example:
//
//	ref, err := api.ActorTask[int](handle, "add", 5).Remote()
func ActorTask[T any](handle *PythonActorHandle, methodName string, args ...interface{}) *PythonActorTaskCaller[T] {
	desc, err := function.NewPythonFunctionDescriptor(handle.moduleName, handle.className, methodName, "")
	return &PythonActorTaskCaller[T]{
		actorID:          handle.nativeHandle.ActorID,
		methodDescriptor: desc,
		args:             wrapPythonArgs(args),
		options:          &submitter.TaskOptions{},
		numReturns:       1,
		err:              err,
	}
}

// PythonActorTaskCaller is a task caller specifically for Python actor method calls.
// Type parameter T is the return type of the actor method.
type PythonActorTaskCaller[T any] struct {
	actorID          ids.ActorID
	methodDescriptor *function.PythonFunctionDescriptor
	args             []function.FunctionArg
	options          *submitter.TaskOptions
	numReturns       int
	err              error // Deferred error reporting
}

// Remote submits the Python actor method call and returns an ObjectRef.
//
// Returns:
//   - *ObjectRef[T]: A reference to the task result
//   - error: Any error encountered during submission
func (c *PythonActorTaskCaller[T]) Remote() (*ObjectRef[T], error) {
	if c.err != nil {
		return nil, c.err
	}

	return submitTaskRef[T]("submit_actor_task", c.args, func(s submitter.TaskSubmitter) ([]ids.ObjectID, error) {
		return s.SubmitActorTask(c.actorID, c.methodDescriptor, c.args, c.numReturns, c.options)
	})
}

// WithResources sets the resource requirements for the Python actor method call.
//
// Parameters:
//   - resources: A map of resource name to quantity (e.g., {"CPU": 1.0, "GPU": 0.5})
//
// Returns:
//   - *PythonActorTaskCaller[T]: The same caller for chaining
func (c *PythonActorTaskCaller[T]) WithResources(resources map[string]float64) *PythonActorTaskCaller[T] {
	c.options.Resources = resources
	return c
}

// WithRuntimeEnv sets the runtime environment for the Python actor method call.
//
// Parameters:
//   - runtimeEnv: The runtime environment JSON string
//
// Returns:
//   - *PythonActorTaskCaller[T]: The same caller for chaining
func (c *PythonActorTaskCaller[T]) WithRuntimeEnv(runtimeEnv string) *PythonActorTaskCaller[T] {
	c.options.RuntimeEnv = runtimeEnv
	return c
}

// WithName sets the name for the Python actor method call.
//
// Parameters:
//   - name: The task name
//
// Returns:
//   - *PythonActorTaskCaller[T]: The same caller for chaining
func (c *PythonActorTaskCaller[T]) WithName(name string) *PythonActorTaskCaller[T] {
	c.options.Name = name
	return c
}

// WithConcurrencyGroup sets the concurrency group for the Python actor method call.
//
// Parameters:
//   - groupName: The concurrency group name
//
// Returns:
//   - *PythonActorTaskCaller[T]: The same caller for chaining
func (c *PythonActorTaskCaller[T]) WithConcurrencyGroup(groupName string) *PythonActorTaskCaller[T] {
	c.options.ConcurrencyGroupName = groupName
	return c
}

// WithNumReturns sets the number of return values for the Python actor method call.
//
// Parameters:
//   - numReturns: The number of return values
//
// Returns:
//   - *PythonActorTaskCaller[T]: The same caller for chaining
func (c *PythonActorTaskCaller[T]) WithNumReturns(numReturns int) *PythonActorTaskCaller[T] {
	c.numReturns = numReturns
	return c
}

// ============================================================================
// Helper Functions (moved to object.go for better organization)
// ============================================================================

// Note: setupObjectRefFinalizer, releaseObjectRef, and createObjectRefWithFinalizer
// have been moved to object.go as they are ObjectRef lifecycle management functions,
// not specific to task calling. They are still accessible from call.go since both
// files are in the same package.

// registerActorConstructor registers the actor's "<init>" constructor with the
// global registry so that a Go worker can construct actor instances when it
// receives an ACTOR_CREATION_TASK.
//
// If actorClass is a function, it is treated as the constructor factory and
// registered as-is (any reflection-visible type info is derived from it). If it
// is a type (pointer/instance), a zero-value constructor returning a fresh
// instance is registered.
func registerActorConstructor[T any](actorClass interface{}) {
	if actorClass == nil {
		return
	}

	// Factory-function form: a func (of any arity) is the constructor factory.
	// The actor type is derived from T's zero value rather than from the factory
	// (a func value carries no reflect-visible actor type) so RegisterActorClass
	// can reach the underlying struct type.
	if reflect.TypeOf(actorClass).Kind() == reflect.Func {
		var zero T
		if err := RegisterActorClass(zero, actorClass); err != nil {
			log.Log.Error(err, "failed to register actor class for factory constructor")
		}
		return
	}

	// Type form: actorClass is a pointer/instance of the actor type. Register a
	// zero-value constructor returning a fresh instance, UNLESS the class's
	// "<init>" constructor was already registered explicitly via
	// api.RegisterActorClass. A class has a single declared constructor (Java:
	// Ray.actor(Counter.class) never redefines the class's constructor), so the
	// explicit registration wins. Overwriting it with a zero-arg fallback would
	// silently break actors that need constructor arguments whenever the type
	// form is used in the same process as an explicit registration (e.g. a
	// driver that both registers a class and creates it in local mode).
	actorType := reflect.TypeOf(actorClass)
	if actorType == nil {
		return
	}
	if actorType.Kind() == reflect.Ptr {
		actorType = actorType.Elem()
	}
	typeName, moduleName, packagePath := function.DescriptorPartsFromType(actorType)
	ctorDesc, descErr := function.NewGoActorMethodDescriptor(
		moduleName, packagePath, typeName, function.ConstructorName,
	)
	if descErr != nil {
		// Log rather than silently skipping registration: the type form used to
		// always register a zero-value constructor, and a descriptor that fails
		// to build would otherwise leave the actor class silently unregistered.
		log.Log.Error(descErr, "failed to build actor constructor descriptor for type form",
			"type", actorType.String())
		return
	}
	if _, err := function.Registry.Get(ctorDesc); err == nil {
		// An explicit constructor registration already exists; keep it.
		return
	}
	constructor := func() T {
		v := reflect.New(actorType)
		return v.Interface().(T)
	}
	if err := RegisterActorClass(actorClass, constructor); err != nil {
		log.Log.Error(err, "failed to register actor class")
	}
}

// extractActorTypeDescriptor builds the "<init>" descriptor for the actor type
// identified by the type parameter T.
//
// T is conventionally a pointer to the actor struct type (e.g. *MyActor), so the
// pointer layers are dereferenced to reach the struct type whose Name()/PkgPath()
// identify the actor class. Without the dereference the module/package/type name
// would degrade to "unknown".
//
// The returned descriptor carries the special "<init>" method name, matching
// Java's FunctionManager.CONSTRUCTOR_NAME, so the worker's actor execution path can
// distinguish actor construction from regular actor method calls.
func extractActorTypeDescriptor[T any]() *function.GoFunctionDescriptor {
	actorType := reflect.TypeOf((*T)(nil))
	if actorType == nil {
		return function.NewGoActorMethodDescriptorOrUnknown("unknown", "unknown", "unknown", function.ConstructorName)
	}

	typeName, modulePath, packagePath := function.DescriptorPartsFromType(actorType)

	return function.NewGoActorMethodDescriptorOrUnknown(modulePath, packagePath, typeName, function.ConstructorName)
}

// extractActorFunctionDescriptor extracts a FunctionDescriptor from an actor class.
// It is used by the untyped convenience entry point (Ray.Actor / api.Actor[interface{}],
// where the descriptor must be derived from the actorClass argument rather than from
// the type parameter T=interface{}). This preserves OSS's original behavior for that
// entry point.
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
	return function.NewGoActorMethodDescriptorOrUnknown(moduleName, pkgPath, typeName, function.ConstructorName)
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
		return functionArgByValue(nativeObj)
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
						// not pin the object in the object store forever. The owner
						// address (rpc address with worker_id) is required for the
						// raylet to locate the owner when it pulls the object to
						// schedule the task; an empty owner crashes the raylet in
						// CoreWorkerClientPool::GetOrConnect.
						arg := function.NewFunctionArgByRef(objectID, getCurrentWorkerRpcAddress(runtime))
						arg.ObjectRef.ReleaseAfterSubmit = true
						return arg
					}
				}
			}
		}

		// Fallback: pass by value if object store is not available
		return functionArgByValue(nativeObj)
	}
}

// convertArgs converts raw interface{} arguments to a FunctionArg slice.
// Shared by TaskCaller.Call, ActorCreator.Create and ActorHandleImpl.Task.
func convertArgs(args ...interface{}) []function.FunctionArg {
	functionArgs := make([]function.FunctionArg, len(args))
	for i, arg := range args {
		functionArgs[i] = convertArgToFunctionArg(arg)
	}
	return functionArgs
}

// functionArgByValue deep-copies the serialized payload out of a NativeRayObject
// and returns a pass-by-value FunctionArg. The copy is required because the
// NativeRayObject may be returned to the buffer pool (Close) after the caller
// returns, invalidating its backing slices. Metadata is carried so the receiving
// runtime can deserialize cross-language arguments (e.g. Python requires a
// non-empty metadata for non-null objects).
func functionArgByValue(nativeObj *object.NativeRayObject) function.FunctionArg {
	data := bytes.Clone(nativeObj.Data)
	metadata := bytes.Clone(nativeObj.Metadata)
	return function.NewFunctionArgByValue(data, metadata)
}

// getCurrentWorkerRpcAddress returns the worker's own RPC address as bytes. It
// is used as the owner address for pass-by-reference arguments so the raylet can
// locate the owning worker when it pulls the object to schedule the task; an
// empty owner address crashes the raylet in CoreWorkerClientPool::GetOrConnect.
func getCurrentWorkerRpcAddress(runtime contract.Runtime) []byte {
	if runtime == nil {
		return nil
	}
	wc := runtime.WorkerContext()
	if wc == nil {
		return nil
	}
	return wc.GetRpcAddress()
}

// submitTaskRef submits any task-like operation that returns a list of ObjectIDs
// (a normal task or an actor task) and wraps the first return ID into an
// ObjectRef[T]. It centralizes the pass-by-reference argument release, the
// submitter availability check, the error translation and the result wrapping
// that used to be duplicated across TaskCaller/PythonTaskCaller/ActorTaskCaller.
//
// Parameters:
//   - action: The operation name used in the runtime error when the submitter is
//     unavailable (e.g. "submit_task", "submit_actor_task").
//   - args: The FunctionArg list of the call. Pass-by-reference arguments marked
//     ReleaseAfterSubmit are released on every exit path.
//   - submit: The actual submission closure.
func submitTaskRef[T any](action string, args []function.FunctionArg,
	submit func(submitter.TaskSubmitter) ([]ids.ObjectID, error)) (*ObjectRef[T], error) {
	// Release the PutWithID local reference of internal pass-by-reference
	// arguments on every exit path (see releaseInternalByRefArgRefs).
	defer releaseInternalByRefArgRefs(args)

	taskSubmitter := getTaskSubmitter()
	if taskSubmitter == nil {
		return nil, errors.NewRuntimeError(action, submitterNotAvailable)
	}
	returnIDs, err := submit(taskSubmitter)
	if err != nil {
		return nil, errors.ConvertToPublic(err)
	}
	if len(returnIDs) > 0 {
		return createObjectRefWithFinalizer[T](returnIDs[0], "")
	}
	return nil, nil
}

// createActorWithSubmitter creates an actor through the current task submitter
// and returns its ActorID. It centralizes the pass-by-reference argument
// release, the submitter availability check and the error translation shared
// by ActorCreator and PythonActorCreator.
//
// Parameters:
//   - action: The operation name used in the runtime error when the submitter is
//     unavailable (e.g. "create_actor").
//   - args: The constructor FunctionArg list. Pass-by-reference arguments marked
//     ReleaseAfterSubmit are released on every exit path.
//   - create: The actual actor creation closure.
func createActorWithSubmitter(action string, args []function.FunctionArg,
	create func(submitter.TaskSubmitter) (ids.ActorID, error)) (ids.ActorID, error) {
	defer releaseInternalByRefArgRefs(args)

	taskSubmitter := getTaskSubmitter()
	if taskSubmitter == nil {
		return ids.NilActorID(), errors.NewRuntimeError(action, submitterNotAvailable)
	}
	actorID, err := create(taskSubmitter)
	if err != nil {
		return ids.NilActorID(), errors.ConvertToPublic(err)
	}
	return actorID, nil
}

// getTaskSubmitter returns the current task submitter via the internal()
// single entry point. It returns nil when the runtime is not initialized or
// has no submitter; callers (submitTaskRef / createActorWithSubmitter) map
// nil to the submitter_not_available runtime error.
func getTaskSubmitter() submitter.TaskSubmitter {
	rt, err := internal()
	if err != nil {
		return nil
	}
	return rt.GetTaskSubmitter()
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

	return submitTaskRef[T]("submit_actor_task", c.args, func(s submitter.TaskSubmitter) ([]ids.ObjectID, error) {
		return s.SubmitActorTask(c.actorID, c.methodDescriptor, c.args, c.numReturns, c.options)
	})
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
	c.options.Name = name
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
	c.options.ConcurrencyGroupName = groupName
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
