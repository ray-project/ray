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

package api

import (
	"fmt"
	"sync"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// Ray is the unified entry point for Ray Go runtime.
// Consistent with Java's io.ray.api.Ray
//
// Usage examples:
//
//	// Initialize Ray
//	api.Instance().Init()
//	defer api.Instance().Shutdown()
//
//	// Submit a task
//	result, err := api.Instance().Remote(myFunction, arg1, arg2).Call()
//
//	// Get an object
//	value, err := api.Instance().Get(objectRef)
//
//	// Create an actor
//	actorHandle, err := api.Instance().Actor(MyActor{}).Create()
//
//	// Get runtime context
//	ctx, err := api.Instance().GetRuntimeContext()
type Ray struct {
	// initOnce ensures initialization happens only once
	initOnce sync.Once

	// shutdownHook for cleanup
	shutdownHook func()
}

// rayInstance is the singleton Ray instance
var rayInstance *Ray
var rayInitMutex sync.Mutex

// Instance returns the singleton Ray instance.
// This is the main entry point for all Ray operations.
// Consistent with Java's Ray class static methods.
// Note: Unlike Java which uses static methods on the Ray class,
// Go uses a separate function named Instance() to return the singleton.
func Instance() *Ray {
	rayInitMutex.Lock()
	defer rayInitMutex.Unlock()

	if rayInstance == nil {
		rayInstance = &Ray{}
	}
	return rayInstance
}

// Init initializes Ray runtime with default options.
// Consistent with Java's Ray.init()
func (r *Ray) Init() error {
	return r.InitWithOptions(nil)
}

// InitWithOptions initializes Ray runtime with custom options.
// Consistent with Java's Ray.init(factory)
func (r *Ray) InitWithOptions(opts *options.InitializeOptions) error {
	var initErr error

	r.initOnce.Do(func() {
		// Delegate to existing InitWithOptions
		initErr = InitWithOptions(opts)

		if initErr == nil {
			// Register shutdown hook
			r.registerShutdownHook()
		}
	})

	return initErr
}

// registerShutdownHook registers a shutdown hook for cleanup.
// Note: Go doesn't have direct shutdown hooks like Java,
// users should manually call Shutdown() or use defer.
func (r *Ray) registerShutdownHook() {
	r.shutdownHook = func() {
		r.Shutdown()
	}
	// In Go, we rely on users to call defer api.Instance().Shutdown()
}

// Shutdown shuts down Ray runtime.
// Consistent with Java's Ray.shutdown()
func (r *Ray) Shutdown() {
	Shutdown() // Delegate to existing Shutdown function
}

// IsInitialized checks if Ray is initialized.
// Consistent with Java's Ray.isInitialized()
func (r *Ray) IsInitialized() bool {
	return IsInitialized()
}

// Put puts an object into the object store.
// Consistent with Java's Ray.put(obj)
//
// Parameters:
//   - obj: the object to put
//
// Returns:
//   - *ObjectRef[T]: a reference to the put object
//   - error: any error encountered during the put operation
//
// Note: This is a convenience wrapper around the package-level Put function.
// For type-safe operations, use api.Put(obj) directly.
func (r *Ray) Put(obj interface{}) (*ObjectRef[interface{}], error) {
	return Put(obj, nil)
}

// PutWithOwner puts an object into the object store; a non-nil owner is
// rejected.
//
// The owner argument is rejected when non-nil: owned puts (the experimental
// `_owner` argument to ray.put) were removed upstream in Ray 2.56
// (ray-project/ray#63520), and this bridge does not forward an owner address to
// the C++ core worker. A nil owner keeps the object owned by the current worker.
// Any non-nil owner is rejected before dereferencing, so a typed-nil handle
// stored in the ActorHandle interface (owner != nil at the interface level)
// returns the removal error instead of panicking.
//
// Parameters:
//   - obj: the object to put
//   - owner: must be nil; a non-nil owner is rejected
//
// Returns:
//   - *ObjectRef[interface{}]: a reference to the put object
//   - error: any error encountered during the put operation
func (r *Ray) PutWithOwner(obj interface{}, owner ActorHandle) (*ObjectRef[interface{}], error) {
	if owner != nil {
		return nil, errOwnedPutRemoved
	}
	return PutWithOwner(obj, nil)
}

// Get fetches an object from the object store.
// Consistent with Java's Ray.get(objectRef)
//
// Parameters:
//   - objectRef: the object reference
//
// Returns:
//   - interface{}: the object value
//   - error: any error encountered during the get operation
//
// Note: This is a convenience wrapper around the package-level Get function.
// For type-safe operations, use api.Get(ref) directly.
func (r *Ray) Get(objectRef *ObjectRef[interface{}]) (interface{}, error) {
	return Get(objectRef)
}

// GetList fetches a list of objects from the object store.
// Consistent with Java's Ray.get(objectList)
//
// Parameters:
//   - objectRefs: the list of object references
//
// Returns:
//   - []interface{}: the list of object values
//   - error: any error encountered during the get operation
func (r *Ray) GetList(objectRefs []*ObjectRef[interface{}]) ([]interface{}, error) {
	return GetList(objectRefs)
}

// Wait waits for objects to be locally available.
// Consistent with Java's Ray.wait(waitList, numReturns, timeoutMs, fetchLocal)
//
// Parameters:
//   - objectRefs: the list of object references to wait for
//   - numReturns: the number of objects that need to be available
//   - timeoutMs: the maximum time in milliseconds to wait
//   - fetchLocal: whether to fetch the objects locally
//
// Returns:
//   - *WaitResult[interface{}]: the wait result containing ready and unready objects
//   - error: any error encountered during the wait operation
func (r *Ray) Wait(objectRefs []*ObjectRef[interface{}], numReturns int,
	timeoutMs int64, fetchLocal bool) (*WaitResult[interface{}], error) {
	return Wait(objectRefs, numReturns, timeoutMs, fetchLocal)
}

// Remote creates a remote task caller.
// Consistent with Java's Ray.remote(fn)
//
// Parameters:
//   - fn: the remote function to call
//
// Returns:
//   - *TaskCaller[interface{}]: a task caller builder
func (r *Ray) Remote(fn interface{}) *TaskCaller[interface{}] {
	return Remote[interface{}](fn)
}

// Actor creates an actor creator.
// Consistent with Java's Ray.actor(actorClass)
//
// This is the OSS-only untyped convenience form: T is interface{}, so the
// actor "<init>" descriptor is derived from the actorClass argument (the typed
// api.Actor[*T]((*T)(nil)) form is the INT-aligned equivalent and derives the
// descriptor from T). Like Java's Ray.actor(Class), the actor class's
// constructor is expected to be registered explicitly via api.RegisterActorClass.
//
// Parameters:
//   - actorClass: the actor class to create (a pointer/instance of the actor
//     type, or a constructor factory function)
//
// Returns:
//   - *ActorCreator[interface{}]: an actor creator builder
func (r *Ray) Actor(actorClass interface{}) *ActorCreator[interface{}] {
	return Actor[interface{}](actorClass)
}

// GetActor retrieves a named actor.
// Consistent with Java's Ray.getActor(name)
//
// Parameters:
//   - name: the name of the actor
//
// Returns:
//   - *ActorHandleImpl[interface{}]: a handle to the actor
//   - error: any error encountered during retrieval
func (r *Ray) GetActor(name string) (*ActorHandleImpl[interface{}], error) {
	return GetActor[interface{}](name)
}

// GetActorWithNamespace retrieves a named actor with namespace.
// Consistent with Java's Ray.getActor(name, namespace)
//
// Parameters:
//   - name: the name of the actor
//   - namespace: the namespace of the actor
//
// Returns:
//   - *ActorHandleImpl[interface{}]: a handle to the actor
//   - error: any error encountered during retrieval
func (r *Ray) GetActorWithNamespace(name string, namespace string) (*ActorHandleImpl[interface{}], error) {
	return GetActorWithNamespace[interface{}](name, namespace)
}

// GetRuntimeContext gets the runtime context.
// Consistent with Java's Ray.getRuntimeContext()
//
// Returns:
//   - *RuntimeContext: the current runtime context
//   - error: any error encountered during retrieval
func (r *Ray) GetRuntimeContext() (*RuntimeContext, error) {
	return GetRuntimeContext()
}

// ExitActor exits the current actor.
// Consistent with Java's Ray.exitActor()
//
// Returns:
//   - error: any error encountered during exit
func (r *Ray) ExitActor() error {
	return ExitActor()
}

// Internal returns the underlying runtime handle (for internal use).
// Consistent with Java's Ray.internal()
//
// Returns:
//   - contract.RuntimeHandle: the runtime handle
//
// Panics:
//   - if the runtime is not initialized - this is an internal API and callers
//     must ensure Init() has been called
func (r *Ray) Internal() contract.RuntimeHandle {
	handle, ok := tryGetHandle()
	if !ok {
		panic("Ray runtime not initialized. Call api.Init() or api.InitWithOptions() first.")
	}
	return handle
}

// InitializeWithJobConfig initializes Ray with a JobConfig builder.
// This is a convenience method for setting job-level configuration.
//
// The builder is mutated: the job-injected runtime_env and metadata (from
// RAY_JOB_CONFIG_JSON_ENV_VAR) are merged into it before the job options are
// built.
//
// Parameters:
//   - builder: JobConfigBuilder to configure job settings
//
// Returns:
//   - error: any error encountered during initialization
//
// Example:
//
//	jobConfig, err := options.NewJobConfigBuilder().
//	    WithCodeSearchPath("./userfuncs.so").
//	    WithNamespace("my-job").
//	    BuildToJobOptions()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	opts := &options.InitializeOptions{
//	    WorkerType: options.WorkerTypeDriver,
//	    Job: jobConfig,
//	}
//	if err := api.Instance().InitWithOptions(opts); err != nil {
//	    log.Fatal(err)
//	}
func (r *Ray) InitializeWithJobConfig(builder *options.JobConfigBuilder) error {
	jobOpts, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		return err
	}

	return r.InitWithOptions(&options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Job:        jobOpts,
	})
}

// buildJobOptionsFromBuilder merges the job-injected configuration
// (runtime_env and metadata from RAY_JOB_CONFIG_JSON_ENV_VAR) into a copy of the
// caller's builder and then builds the resulting JobOptions.
//
// The merge mirrors the default path in InitWithOptions: when a job is
// submitted through the job supervisor (for example, `ray job submit` or the
// dashboard job API), the job's runtime_env - including its working_dir - is
// delivered to the driver only through RAY_JOB_CONFIG_JSON_ENV_VAR. Ray's core
// worker derives a driver's task and actor runtime_env from the job config, so
// a driver that does not merge it in leaves every task and actor worker on
// other nodes without the job's working_dir or env_vars. Merging here keeps the
// explicit-job-config initialization path consistent with the default path.
//
// The merge is additive and conflict-checked by JobConfigBuilder.MergeFromEnv;
// the caller's explicit settings win on conflicts unless
// RAY_OVERRIDE_JOB_RUNTIME_ENV=1 is required to permit them.
//
// Merging into a copy rather than into the caller's builder keeps this function
// repeatable: MergeFromEnv writes the job-injected runtime_env into the builder
// when the caller set none, and a second call on the same builder (a retry
// after a failed initialization, for example) would then find a runtime_env
// already there and merge the job-injected value with itself. checkConflict
// reports a shared key as a conflict whatever its value, matching Python's
// _merge_runtime_env, so that retry would fail with a conflict the caller never
// created.
func buildJobOptionsFromBuilder(builder *options.JobConfigBuilder) (options.JobOptions, error) {
	merged := builder.Clone()
	if err := merged.MergeFromEnv(); err != nil {
		return options.JobOptions{}, fmt.Errorf("failed to merge job config from environment: %w", err)
	}
	jobOpts, err := merged.BuildToJobOptions()
	if err != nil {
		return options.JobOptions{}, fmt.Errorf("failed to build JobConfig: %w", err)
	}
	return jobOpts, nil
}

// InitializeWithJobConfigAndNetwork initializes Ray with JobConfig and network settings.
// This is a convenience method for setting both job and network configuration.
//
// Parameters:
//   - builder: JobConfigBuilder to configure job settings
//   - network: NetworkOptions for network configuration
//
// Returns:
//   - error: any error encountered during initialization
//
// Example:
//
//	if err := api.Instance().InitializeWithJobConfigAndNetwork(
//	    options.NewJobConfigBuilder().
//	        WithCodeSearchPath("./userfuncs.so"),
//	    options.NetworkOptions{
//	        GcsAddress: "127.0.0.1:6379",
//	    },
//	); err != nil {
//	    log.Fatal(err)
//	}
func (r *Ray) InitializeWithJobConfigAndNetwork(
	builder *options.JobConfigBuilder,
	network options.NetworkOptions,
) error {
	jobOpts, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		return err
	}

	return r.InitWithOptions(&options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Job:        jobOpts,
		Network:    network,
	})
}

// Internal returns the underlying runtime handle held by the api service
// locator. It is the public escape hatch to the runtime singleton, mirroring
// Java's Ray.internal() (which returns the RayRuntime directly).
//
// It returns the contract.RuntimeHandle (the dependency inversion boundary
// abstraction) rather than the contract.Runtime itself; callers use
// handle.Runtime() to obtain the runtime. Unlike tryGetHandle(), which reports
// an uninitialized runtime through its ok result, Internal() panics.
//
// Panics:
//   - if the runtime is not initialized - this is a public escape hatch and
//     callers must ensure Init() has been called
func Internal() contract.RuntimeHandle {
	return getHandle()
}
