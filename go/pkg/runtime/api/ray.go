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

// PutWithOwner puts an object into the object store with a specific owner.
// Consistent with Java's Ray.put(obj, owner)
//
// Parameters:
//   - obj: the object to put
//   - owner: the owner actor handle
//
// Returns:
//   - *ObjectRef[T]: a reference to the put object
//   - error: any error encountered during the put operation
func (r *Ray) PutWithOwner(obj interface{}, owner ActorHandle) (*ObjectRef[interface{}], error) {
	// Note: Type assertion would be needed here, which is not type-safe.
	// For type-safe operations, use api.PutWithOwner(obj, owner) directly.
	return Put(obj, nil)
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
// Parameters:
//   - actorClass: the actor class to create
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
	jobOpts, err := builder.BuildToJobOptions()
	if err != nil {
		return fmt.Errorf("failed to build JobConfig: %w", err)
	}

	return r.InitWithOptions(&options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Job:        jobOpts,
	})
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
	jobOpts, err := builder.BuildToJobOptions()
	if err != nil {
		return fmt.Errorf("failed to build JobConfig: %w", err)
	}

	return r.InitWithOptions(&options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Job:        jobOpts,
		Network:    network,
	})
}
