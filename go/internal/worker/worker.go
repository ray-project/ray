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
// This implementation directly calls base.Initialize() to create the runtime,
// rather than using Go's plugin API to dynamically load go_runtime.so.
//
// Design notes:
//  1. A single Worker process creates exactly one runtime instance (singleton pattern).
//  2. The runtime is created via base.Initialize(), which uses the factory registered
//     by go/internal/runtime/native's init() function.
//  3. For Driver mode, node connection info and JobID are fetched from GCS before initialization.
//  4. This design is consistent with Java's RayNativeRuntime initialization.
package worker

import (
	"context"
	"fmt"
	"os"
	"plugin"
	"reflect"
	"strings"
	"sync"
	"time"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/runtime/base"
	_ "github.com/ray-project/ray/go/internal/runtime/native" // Register Runtime factory (init function)
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/proto"
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
	handle       base.RuntimeHandle
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

// ============================================================================
// GCS Client Factory Implementation (for Dependency Inversion)
// ============================================================================

// gcsClientFactory implements api.GCSClientFactory by delegating to
// go/internal/gcs/native.ConnectClient().
//
// This allows worker to provide the concrete GCS client implementation
// to the api package without the api package directly importing go/internal/gcs/native.
type gcsClientAdapter struct {
	client gcs.Client
}

// GetNodeToConnect implements api.GCSClient.GetNodeToConnect.
func (a *gcsClientAdapter) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return a.client.GetNodeToConnect(ctx, nodeIpAddress)
}

// NextJobID implements api.GCSClient.NextJobID.
// Converts ids.JobID to hex string.
func (a *gcsClientAdapter) NextJobID(ctx context.Context) (string, error) {
	jobID, err := a.client.NextJobID(ctx)
	if err != nil {
		return "", err
	}
	return jobID.Hex(), nil
}

// Close implements api.GCSClient.Close.
func (a *gcsClientAdapter) Close() error {
	return a.client.Close()
}

// IsClosed implements api.GCSClient.IsClosed by delegating to the underlying client.
func (a *gcsClientAdapter) IsClosed() bool {
	if checker, ok := a.client.(interface{ IsClosed() bool }); ok {
		return checker.IsClosed()
	}
	return false
}

type gcsClientFactory struct{}

// CreateClient implements api.GCSClientFactory.CreateClient().
func (f *gcsClientFactory) CreateClient(opts gcs.ClientOptions) (api.GCSClient, error) {
	client, err := native.ConnectClient(opts)
	if err != nil {
		return nil, err
	}
	return &gcsClientAdapter{client: client}, nil
}

// registerGCSClientFactory registers the GCS client factory.
// This must be called before using api.FetchNodeInfoFromGCS() or api.NextJobID().
func registerGCSClientFactory() {
	api.RegisterGCSClientFactory(&gcsClientFactory{})
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

		for _, pluginPath := range codeSearchPath {
			pluginPath = strings.TrimSpace(pluginPath)
			if pluginPath == "" {
				continue
			}

			// Check if file exists - V(2) for detailed debugging
			if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
				log.Log.V(1).Info("plugin file does not exist, skipping", "path", pluginPath)
				continue
			}

			// Load the plugin
			p, err := plugin.Open(pluginPath)
			if err != nil {
				log.Log.Error(err, "failed to load plugin", "path", pluginPath)
				return fmt.Errorf("failed to load plugin %s: %w", pluginPath, err)
			}

			// Look for RegisterFunctions symbol (optional)
			// If the plugin exports a RegisterFunctions function, call it
			sym, err := p.Lookup("RegisterFunctions")
			if err == nil {
				// Found RegisterFunctions, call it
				if registerFunc, ok := sym.(func() error); ok {
					if err := registerFunc(); err != nil {
						log.Log.Error(err, "RegisterFunctions failed", "path", pluginPath)
						return fmt.Errorf("RegisterFunctions in plugin %s failed: %w", pluginPath, err)
					}
					log.Log.Info("RegisterFunctions called successfully", "path", pluginPath)
				}
			}
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

	// Register each function
	for _, regFn := range registeredFuncs {
		// Wrap Go function to function.Function type
		wrappedFn := wrapGoFunction(regFn.Function())

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

// wrapGoFunction wraps a Go function (interface{}) to function.Function type.
// The wrapper handles argument deserialization and result serialization.
//
// Parameters:
//   - fn: the Go function to wrap (must be a regular function)
//
// Returns:
//   - function.Function: wrapped function that can be called with FunctionArg slice
func wrapGoFunction(fn interface{}) function.Function {
	// Get the reflect.Value of the function
	funcValue := reflect.ValueOf(fn)
	funcType := funcValue.Type()

	return func(args []function.FunctionArg) ([]function.SerializedObject, error) {
		// Prepare arguments for calling the Go function
		in := make([]reflect.Value, len(args))

		// Use object.Serializer interface for deserialization
		// This decouples from specific msgpack implementation and follows
		// the Dependency Inversion Principle
		ser := object.GetSerializer()

		for i, arg := range args {
			if arg.IsPassByValue() && arg.Data != nil {
				// Deserialize pass-by-value argument
				// The expected type is determined by the function signature
				expectedType := funcType.In(i)

				// Create NativeRayObject from serialized data
				nativeObj := &object.NativeRayObject{
					Data:     arg.Data.Data,
					Metadata: arg.Data.Metadata,
				}

				// Deserialize directly to target type using Serializer interface
				// This avoids the issue of msgpack decoding small integers as int8/uint8
				deserialized := reflect.New(expectedType).Interface()
				if err := ser.DeserializeTo(nativeObj, deserialized); err != nil {
					return nil, fmt.Errorf("failed to deserialize argument %d: %w", i, err)
				}

				// Get the deserialized value
				in[i] = reflect.ValueOf(deserialized).Elem()

			} else if arg.IsPassByRef() {
				// For pass-by-reference, we need to fetch from object store
				// This requires access to the object store via the runtime
				// For now, return an error - this needs to be handled by the runtime
				return nil, fmt.Errorf("pass-by-reference arguments not yet supported")
			} else {
				// Handle nil or unsupported argument types
				in[i] = reflect.Zero(funcType.In(i))
			}
		}

		// Call the Go function
		out := funcValue.Call(in)

		// Serialize return values using object.Serializer interface
		// This automatically handles metadata type determination and provides
		// a consistent serialization approach across the codebase
		results := make([]function.SerializedObject, len(out))
		for i, val := range out {
			// Use Serializer.Serialize() to get NativeRayObject, then extract Data
			// The underlying implementation handles the 9-byte length header
			nativeObj, err := ser.Serialize(val.Interface())
			if err != nil {
				return nil, fmt.Errorf("failed to serialize return value %d: %w", i, err)
			}

			results[i] = function.SerializedObject{
				Data:     nativeObj.Data,
				Metadata: nativeObj.Metadata, // Use metadata from serializer (automatically determined)
			}
		}

		return results, nil
	}
}

// Run starts the Worker execution loop.
//
// Comparison with Java: corresponds to RayNativeRuntime.run() -> nativeRunTaskExecutor(taskExecutor)
//
// This is a blocking call until the Worker receives an exit signal or an error occurs.
//
// Workflow:
//  1. For Driver mode: fetch node info and JobID from GCS
//  2. Register GCS client factory for dependency inversion
//  3. Call base.Initialize() to create and start the runtime
//  4. Call handle.Runtime().Run() to execute task loop
//
// Returns:
//   - error: error on startup failure or task execution failure
//
// Note:
//   - This method is blocking, with task execution driven by C++ core_worker
//   - A single Worker process creates exactly one runtime instance (singleton pattern)
//   - The runtime factory is registered by go/internal/runtime/native's init() function
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

	// Validate socket paths before calling C++ initialization.
	// This provides clear error messages if the socket files don't exist.
	logger.Info("validating socket paths",
		"plasma_store_socket", w.opts.Runtime.StoreSocket,
		"raylet_socket", w.opts.Runtime.RayletSocket,
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
		logger.Info(fmt.Sprintf("%s socket exists", socketType), "socket", socketPath)
		return nil
	}

	if err := validateSocket("plasma store", w.opts.Runtime.StoreSocket); err != nil {
		return err
	}
	if err := validateSocket("raylet", w.opts.Runtime.RayletSocket); err != nil {
		return err
	}

	// Prepare initialization options.
	// The grouped Options structure simplifies passing configuration to base.Initialize().
	initOpts := options.InitializeOptions{
		WorkerType: w.opts.WorkerType,
		Network:    w.opts.Network,
		Job:        w.opts.Job,
		Runtime:    w.opts.Runtime,
	}

	// For Worker mode, do NOT pass JobID to C++.
	// The JobID is passed via RAY_JOB_ID environment variable (set by Raylet),
	// and C++ CoreWorkerProcess::GetProcessJobID() will read it from the environment.
	// We must pass an empty JobID to C++, which expects options.job_id.IsNil() for Worker mode.
	// See: src/ray/core_worker/core_worker.cc:154 - RAY_CHECK(options.job_id.IsNil())
	// If JobID is provided via command line or environment, clear it before passing to C++.
	if w.opts.WorkerType == options.WorkerTypeWorker && initOpts.Job.JobID != "" {
		logger.Info("clearing JobID for Worker mode (C++ expects Nil, actual JobID from RAY_JOB_ID env var)", "job_id", initOpts.Job.JobID)
		// Clear JobID before passing to C++ - the actual JobID will be read from RAY_JOB_ID environment variable
		initOpts.Job.JobID = ""
	}

	// For Driver mode, fetch node connection info and JobID from GCS.
	// This mimics Java's RayNativeRuntime.start() which calls getGcsClient().getNodeToConnectForDriver()
	// and getGcsClient().nextJobId().
	if w.opts.WorkerType == options.WorkerTypeDriver {
		logger.Info("fetching node info and JobID from GCS for Driver mode",
			"gcs_address", initOpts.Network.GcsAddress,
			"job_id", initOpts.Job.JobID,
		)

		// Register GCS client factory first, so api.WithCachedClient can use it.
		registerGCSClientFactory()

		gcsOpts := gcs.ClientOptions{
			Address:   initOpts.Network.GcsAddress,
			ClusterID: ids.NilClusterID(),
			TimeoutMs: 10000,
		}

		// Use cached client to fetch both node info and JobID in a single client lifecycle.
		err := api.WithCachedClient(initOpts.Network.GcsAddress, gcsOpts, func(client api.GCSClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Fetch node info from GCS for missing socket/port fields.
			// This mimics Java's GcsClient.getNodeToConnectForDriver().
			if initOpts.Runtime.StoreSocket == "" || initOpts.Runtime.RayletSocket == "" ||
				initOpts.Network.NodeManagerPort == 0 || initOpts.Network.NodeIPAddress == "" {
				logger.Info("fetching node info from GCS",
					"missing_store_socket", initOpts.Runtime.StoreSocket == "",
					"missing_raylet_socket", initOpts.Runtime.RayletSocket == "",
					"missing_node_manager_port", initOpts.Network.NodeManagerPort == 0,
					"missing_node_ip", initOpts.Network.NodeIPAddress == "",
				)

				nodeInfo, err := client.GetNodeToConnect(ctx, initOpts.Network.NodeIPAddress)
				if err != nil {
					logger.Error(err, "failed to fetch node info from GCS")
					// Log warning but continue - let C++ core worker handle missing fields
				} else if nodeInfo != nil {
					// Populate missing fields from GCS node info
					if initOpts.Runtime.StoreSocket == "" && nodeInfo.ObjectStoreSocketName != "" {
						initOpts.Runtime.StoreSocket = nodeInfo.ObjectStoreSocketName
					}
					if initOpts.Runtime.RayletSocket == "" && nodeInfo.RayletSocketName != "" {
						initOpts.Runtime.RayletSocket = nodeInfo.RayletSocketName
					}
					if initOpts.Network.NodeManagerPort == 0 && nodeInfo.NodeManagerPort != 0 {
						initOpts.Network.NodeManagerPort = int32(nodeInfo.NodeManagerPort)
					}
					// Use NodeManagerAddress from GCS if available
					if nodeInfo.NodeManagerAddress != "" {
						initOpts.Network.NodeIPAddress = nodeInfo.NodeManagerAddress
					}
					logger.Info("fetched node info from GCS",
						"store_socket", initOpts.Runtime.StoreSocket,
						"raylet_socket", initOpts.Runtime.RayletSocket,
						"node_manager_port", initOpts.Network.NodeManagerPort,
					)
				}
			}

			// Fetch next JobID from GCS if not provided.
			// This mimics Java's GcsClient.nextJobId() which allocates a new JobID for the driver.
			if initOpts.Job.JobID == "" {
				logger.Info("fetching next JobID from GCS", "gcs_address", initOpts.Network.GcsAddress)
				jobIDHex, err := client.NextJobID(ctx)
				if err != nil {
					logger.Error(err, "failed to fetch next JobID from GCS")
					return rayerrors.WrapRayError(err, rayerrors.CodeNetworkError,
						rayerrors.WithCategory(rayerrors.CategoryNetwork),
						rayerrors.WithMessage("failed to get next JobID from GCS"))
				}
				logger.Info("fetched next JobID from GCS", "job_id", jobIDHex)
				initOpts.Job.JobID = jobIDHex
			} else {
				logger.Info("JobID already set", "job_id", initOpts.Job.JobID)
			}

			return nil
		})
		if err != nil {
			logger.Error(err, "failed to fetch node info and JobID from GCS")
			return err
		}
	}

	logger.Info("initializing Ray runtime",
		"worker_type", initOpts.WorkerType,
		"gcs_address", initOpts.Network.GcsAddress,
		"node_ip", initOpts.Network.NodeIPAddress,
		"job_id", initOpts.Job.JobID,
		"store_socket", initOpts.Runtime.StoreSocket,
		"raylet_socket", initOpts.Runtime.RayletSocket,
	)

	// Initialize the runtime using base.Initialize().
	// This calls the factory registered by go/internal/runtime/native's init() function.
	// The factory creates and starts a NativeRuntime instance.
	var err error
	w.handle, err = base.Initialize(initOpts)
	if err != nil {
		logger.Error(err, "base.Initialize() returned error")
		return err
	}

	if w.handle == nil {
		return rayerrors.NewInitializationError("runtime", "base.Initialize returned nil handle")
	}

	logger.Info("Ray runtime initialized successfully", "handle", w.handle)

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
	err = w.handle.Runtime().Run()
	if err != nil {
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
//   - Calls base.Shutdown() which cleans up global state (handle registry, cached handle, etc.)
func (w *Worker) Shutdown() {
	w.shutdownOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.handle != nil {
			// Call base.Shutdown() to clean up global state.
			// This removes the handle from the registry, clears the global runtime,
			// and calls the underlying runtime's Shutdown() method.
			if err := base.Shutdown(w.handle); err != nil {
				log.Log.Error(err, "failed to shutdown Ray runtime")
			}
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
func (w *Worker) GetHandle() base.RuntimeHandle {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.handle
}
