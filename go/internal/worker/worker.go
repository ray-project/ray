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

// Package worker provides the Worker process implementation for Ray.
//
// This implementation initializes the runtime through api.InitWithOptions,
// which loads go_runtime.so via the plugin API and runs the plugin's exported
// Initialize (base.Initialize plus the driver-mode GCS handshake inside the
// plugin), identical to the proven pure-Go driver layout (go/test_driver.go).
//
// Design notes:
//  1. A single Worker process creates exactly one runtime instance (singleton pattern).
//  2. The runtime is created via api.InitWithOptions, which dynamically loads
//     go_runtime.so and calls its Initialize (plugin.Initialize -> base.Initialize).
//  3. The plugin's exported Initialize performs the driver-mode GCS handshake
//     (node info + JobID allocation), so the worker no longer talks to GCS directly.
//  4. This design is consistent with Java's RayNativeRuntime initialization.
package worker

import (
	"fmt"
	"os"
	goplugin "plugin"
	"strings"
	"sync"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/internal/runtime/actor"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
)

// Options contains configuration parameters for Worker startup.
//
// Comparison with Java: corresponds to RayNativeRuntime constructor parameters
//
// Usage:
//
//	opts := worker.NewOptions(
//	    options.WorkerTypeWorker,
//	    options.NetworkOptions{...},
//	    options.JobOptions{...},
//	    options.RuntimeOptions{...},
//	)
type Options struct {
	// Worker type (required)
	WorkerType options.WorkerType

	// Network configuration (required)
	Network options.NetworkOptions

	// Job configuration (required)
	Job options.JobOptions

	// Runtime configuration (required)
	Runtime options.RuntimeOptions

	// CodeSearchPath is the search path for user code plugins.
	// This is used to load user-defined functions at worker startup.
	CodeSearchPath []string
}

// Option is a function that configures an Options instance.
// Use With* functions to create options.
type Option func(*Options)

// initRuntimeWithOptionsFn wraps api.InitWithOptions so tests can inject a
// fake without loading go_runtime.so.
var initRuntimeWithOptionsFn = func(opts *options.InitializeOptions) error {
	return api.InitWithOptions(opts)
}

// validatePluginLoadableFn wraps plugin.CheckPluginLoadable (the startup
// plugin loadability pre-check) so tests can inject a fake.
var validatePluginLoadableFn = plugin.CheckPluginLoadable

// NewOptions creates a new Options instance.
//
// Comparison with Java: corresponds to new RayNativeRuntime(rayConfig)
//
// Parameters:
//   - workerType: Worker type (Worker or Driver)
//   - network: Network configuration (node IP, ports, etc.)
//   - job: Job configuration (JobID, JobConfig, etc.)
//   - runtime: Runtime configuration (sockets, log directory, etc.)
//   - opts...: Optional parameters
//
// Returns:
//   - *Options: configuration instance
func NewOptions(
	workerType options.WorkerType,
	network options.NetworkOptions,
	job options.JobOptions,
	runtime options.RuntimeOptions,
	opts ...Option,
) *Options {
	o := &Options{
		WorkerType:     workerType,
		Network:        network,
		Job:            job,
		Runtime:        runtime,
		CodeSearchPath: nil, // Will be set via WithCodeSearchPath option
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithCodeSearchPath sets the code search path for user plugins.
func WithCodeSearchPath(paths []string) Option {
	return func(o *Options) {
		o.CodeSearchPath = paths
	}
}

// Worker represents a Worker process instance.
//
// Comparison with Java: corresponds to RayNativeRuntime instance
//
// Lifecycle:
//  1. Creation: worker.New(opts)
//  2. Startup: worker.Run() - blocking call
//  3. Shutdown: worker.Shutdown() - optional, automatically called when process exits
type Worker struct {
	opts         *Options
	handle       contract.RuntimeHandle
	doneCh       chan struct{}
	shutdownOnce sync.Once
	mu           sync.RWMutex
	running      bool
}

// New creates a new Worker instance.
//
// Comparison with Java: corresponds to new RayNativeRuntime(rayConfig)
//
// Parameters:
//   - opts: Worker configuration options
//
// Returns:
//   - *Worker: Worker instance
func New(opts *Options) *Worker {
	return &Worker{
		opts:   opts,
		doneCh: make(chan struct{}),
	}
}

// registerUserFunctions reads user-registered functions from the global registry
// and registers them with the FunctionManager.
// This is called during worker startup, after base.Initialize() succeeds.
//
// The functions are wrapped from interface{} to function.Function type,
// which handles argument deserialization and result serialization.
func registerUserFunctions(rt contract.Runtime, codeSearchPath []string) error {
	// Get function manager from runtime interface (no type assertion needed)
	funcMgr := rt.GetFunctionManager()
	if funcMgr == nil {
		return rayerrors.NewInitializationError("function_manager", "FunctionManager is nil")
	}

	// Load user plugins from codeSearchPath and register functions
	if len(codeSearchPath) > 0 {
		log.Log.Info("loading user plugins from codeSearchPath", "paths", codeSearchPath)

		for i, pluginPath := range codeSearchPath {
			pluginPath = strings.TrimSpace(pluginPath)
			if pluginPath == "" {
				log.Log.V(1).Info("skipping empty codeSearchPath entry", "index", i)
				continue
			}

			info, err := os.Stat(pluginPath)
			if os.IsNotExist(err) {
				log.Log.V(1).Info("plugin file does not exist, skipping", "path", pluginPath)
				continue
			} else if err != nil {
				log.Log.Error(err, "failed to stat plugin file, skipping", "path", pluginPath)
				continue
			}
			// Skip directories: code_search_path may contain directories (e.g.
			// a Python source dir for cross-language calls) that are valid for
			// other languages but must not be plugin.Open'ed as a Go .so.
			if info.IsDir() {
				log.Log.V(1).Info("code search path entry is a directory, skipping", "path", pluginPath)
				continue
			}

			// Load the plugin
			p, err := goplugin.Open(pluginPath)
			if err != nil {
				log.Log.Error(err, "failed to load plugin", "path", pluginPath)
				return fmt.Errorf("failed to load plugin %s: %w", pluginPath, err)
			}

			// Look for RegisterFunctions symbol (optional)
			// If the plugin exports a RegisterFunctions function, call it
			sym, lookupErr := p.Lookup("RegisterFunctions")
			if lookupErr != nil {
				// The plugin's init() function may still have registered functions.
				log.Log.V(1).Info("plugin does not export RegisterFunctions", "path", pluginPath)
				continue
			}

			registerFunc, ok := sym.(func() error)
			if !ok {
				log.Log.V(1).Info("plugin RegisterFunctions has unexpected type, skipping",
					"path", pluginPath, "expected", "func() error", "got", fmt.Sprintf("%T", sym))
				continue
			}

			if regErr := registerFunc(); regErr != nil {
				log.Log.Error(regErr, "RegisterFunctions failed", "path", pluginPath)
				return fmt.Errorf("RegisterFunctions in plugin %s failed: %w", pluginPath, regErr)
			}
			log.Log.Info("RegisterFunctions called successfully", "path", pluginPath)
			// Note: The plugin's init() function may have already registered functions
		}
	}

	// Read registered functions from global registry
	// This includes functions registered by plugin's init() or RegisterFunctions()
	registeredFuncs, hasFuncs := api.GetRegisteredFunctions()
	if !hasFuncs {
		log.Log.V(1).Info("no user functions registered, skipping registration")
		return nil
	}

	log.Log.Info("registering user functions with FunctionManager", "function_count", len(registeredFuncs))

	// Actor constructors are registered with the runtime's ActorConstructorRegistrar
	// (if available) instead of the FunctionManager, because an actor constructor
	// returns a live instance rather than a serialized value.
	var actorRegistrar contract.ActorConstructorRegistrar
	if r, ok := rt.(contract.ActorConstructorRegistrar); ok {
		actorRegistrar = r
	}

	// Register each function
	for _, regFn := range registeredFuncs {
		// Actor "<init>" constructors are registered with the runtime so that
		// ACTOR_CREATION_TASK can build a live instance on the worker.
		if regFn.Descriptor().MethodName() == function.ConstructorName {
			if actorRegistrar != nil {
				ctor := actor.WrapActorConstructor(regFn.Function())
				actorRegistrar.RegisterActorConstructor(regFn.Descriptor().String(), ctor)
			} else {
				log.Log.Error(nil, "runtime does not support actor construction, skipping actor constructor",
					"descriptor", regFn.Descriptor().String())
			}
			continue
		}

		// Wrap Go function to function.Function type
		wrappedFn := function.WrapGoFunction(regFn.Function())

		// Register with FunctionManager
		if err := funcMgr.RegisterFunction(regFn.Descriptor(), wrappedFn); err != nil {
			log.Log.Error(err, "failed to register function",
				"descriptor", regFn.Descriptor().String())
			return fmt.Errorf("failed to register function %s: %w", regFn.Descriptor().String(), err)
		}
	}

	log.Log.Info("all user functions registered successfully", "count", len(registeredFuncs))

	// Mark registry as read-only to prevent further registration after startup
	api.MarkRegistryReadonly()

	return nil
}

// Run starts the Worker execution loop.
//
// Comparison with Java: corresponds to RayNativeRuntime.run() -> nativeRunTaskExecutor(taskExecutor)
//
// This is a blocking call until the Worker receives an exit signal or an error occurs.
//
// Workflow:
//  1. Startup pre-check: validate that go_runtime.so is loadable (ValidatePluginLoadable)
//  2. Validate the plasma store and raylet sockets
//  3. Call api.InitWithOptions to load go_runtime.so and start the runtime
//     (the plugin's Initialize performs the driver-mode GCS handshake)
//  4. Call handle.Runtime().Run() to execute task loop
//
// Returns:
//   - error: error on startup failure or task execution failure
//
// Note:
//   - This method is blocking, with task execution driven by C++ core_worker
//   - A single Worker process creates exactly one runtime instance (singleton pattern)
//   - The runtime is created through the plugin path (api.InitWithOptions -> plugin.Initialize -> base.Initialize)
func (w *Worker) Run() error {
	logger := log.WithName("worker")

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return rayerrors.ErrAlreadyRunning
	}
	w.running = true
	w.mu.Unlock()

	logger.Info("worker run started",
		"worker_type", w.opts.WorkerType,
		"node_ip", w.opts.Network.NodeIPAddress,
		"gcs_address", w.opts.Network.GcsAddress,
	)

	// validateSocket checks if a socket file exists and returns a standardized error.
	validateSocket := func(socketType, socketPath string) error {
		if _, err := os.Stat(socketPath); err != nil {
			category := rayerrors.CategoryInitialization
			message := fmt.Sprintf("%s socket does not exist: %s", socketType, socketPath)
			if !os.IsNotExist(err) {
				message = fmt.Sprintf("failed to stat %s socket: %s", socketType, socketPath)
			}
			return rayerrors.WrapRayError(err, rayerrors.CodeInitializationError,
				rayerrors.WithCategory(category),
				rayerrors.WithMessage(message))
		}
		return nil
	}

	// Fail fast on deployment errors (missing/invalid go_runtime.so)
	// before any business logic.
	if err := validatePluginLoadableFn(); err != nil {
		return err
	}

	if err := validateSocket("plasma store", w.opts.Runtime.StoreSocket); err != nil {
		return err
	}
	if err := validateSocket("raylet", w.opts.Runtime.RayletSocket); err != nil {
		return err
	}

	// Prepare initialization options.
	// The grouped Options structure simplifies passing configuration to api.InitWithOptions.
	initOpts := options.InitializeOptions{
		WorkerType: w.opts.WorkerType,
		Network:    w.opts.Network,
		Job:        w.opts.Job,
		Runtime:    w.opts.Runtime,
	}

	// Worker-mode JobID handling: the configured JobID (from --job-id /
	// RAY_JOB_ID) is passed through as-is. base.InitializeOptionsFromAPI
	// (inside go_runtime.so) forces a nil JobID for Worker mode, because C++
	// core_worker requires options.job_id.IsNil() and reads the real JobID
	// from RAY_JOB_ID (core_worker.cc:154).
	//
	// Driver mode: the plugin's exported Initialize performs the GCS handshake
	// (node info + JobID allocation, plugin_exports.go).

	// Initialize the runtime through the api plugin path:
	// api.InitWithOptions -> plugin.Initialize -> base.Initialize, identical
	// to the proven pure-Go driver layout (go/test_driver.go).
	if err := initRuntimeWithOptionsFn(&initOpts); err != nil {
		logger.Error(err, "api.InitWithOptions() returned error")
		return err
	}

	// The handle is attached to the api package by InitWithOptions (setHandle);
	// obtain it via api.Internal().
	w.handle = api.Internal()
	if w.handle == nil {
		return rayerrors.NewInitializationError("runtime", "api.Internal() returned nil handle")
	}

	logger.Info("Ray runtime initialized successfully", "handle", w.handle)

	// The idle worker process title ("ray::IDLE") and the sensitive-argv scrub
	// are handled inside the plugin Initialize (go_runtime.so): base.Initialize
	// has consumed the command line by then, and the cgo proctitle package must
	// stay on the plugin side so the thin raygo binary keeps no cgo dependency
	// (otherwise the plugin version check fails on the shared proctitle
	// package).

	// Get runtime and check if it's initialized
	rt := w.handle.Runtime()
	if rt == nil {
		logger.Error(nil, "handle.Runtime() returned nil - handle may be corrupted")
		return rayerrors.NewInitializationError("runtime", "handle.Runtime() returned nil")
	}

	// Register user functions from the global registry.
	// This reads functions registered via api.RegisterFunction() and registers
	// them with the FunctionManager so they can be looked up during task execution.
	if err := registerUserFunctions(rt, w.opts.CodeSearchPath); err != nil {
		logger.Error(err, "failed to register user functions")
		return err
	}

	// Run the task execution loop.
	// This calls NativeRuntime.Run() which delegates to C++ core_worker's RunTaskExecutionLoop().
	logger.Info("calling handle.Runtime().Run() - this will block until shutdown")
	if err := w.handle.Runtime().Run(); err != nil {
		logger.Error(err, "handle.Runtime().Run() returned error")
		return err
	}
	// This line should never be reached in Worker mode, as Run() is blocking
	logger.Info("handle.Runtime().Run() returned - this should only happen in Driver mode or on shutdown")

	return nil
}

// Shutdown shuts down the Worker.
//
// Comparison with Java: corresponds to RayNativeRuntime.shutdown()
//
// Note:
//   - This method uses sync.Once to ensure shutdown happens only once
//   - If handle is not initialized, Shutdown does nothing
//   - Typically called automatically when Worker process exits
//   - Calls api.Shutdown() which performs full plugin teardown
//     (ClearAllCachedClients + base.Shutdown)
func (w *Worker) Shutdown() {
	w.shutdownOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.handle != nil {
			// Full plugin teardown: ClearAllCachedClients + base.Shutdown via
			// the plugin's exported Shutdown.
			api.Shutdown()
			w.handle = nil
		}

		close(w.doneCh)
	})
}

// IsRunning returns whether the Worker is currently running.
//
// Returns:
//   - bool: true if Worker is running
func (w *Worker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.running
}

// GetHandle returns the runtime handle for testing purposes.
// Returns nil if the runtime is not initialized.
func (w *Worker) GetHandle() contract.RuntimeHandle {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.handle
}
