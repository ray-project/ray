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

// Package native provides native implementation of the Runtime interface.
//
// Design notes:
// 1. Implements the contract.Runtime interface.
// 2. Indirectly calls CGO through the coreworker package.
// 3. Manages runtime state and thread safety.
// 4. GCS client uses lazy loading to reduce initialization overhead.
package native

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/runtime/base"
	cgoboundary "github.com/ray-project/ray/go/internal/runtime/cgo"
	cgocallback "github.com/ray-project/ray/go/internal/runtime/cgo"
	cgointerfaces "github.com/ray-project/ray/go/internal/runtime/cgo"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/ray-project/ray/go/internal/runtime/resource"
	"github.com/ray-project/ray/go/internal/runtime/serializer"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

var logger = log.WithName("native")

// Compile-time type checks to ensure interface implementations.
// These variables are never used at runtime; they exist solely to trigger
// compile-time errors if the types don't implement the interfaces correctly.
var _ contract.Runtime = (*NativeRuntime)(nil)
var _ contract.RuntimeHandle = (*NativeHandleImpl)(nil)

// globalWorkerContext is the package-level singleton instance of NativeWorkerContext.
// NativeWorkerContext is stateless (all methods call CGO functions), so a single
// instance can be shared across all NativeRuntime instances.
var globalWorkerContext = &cgointerfaces.NativeWorkerContext{}

var (
	ErrRuntimeAlreadyInitialized = errors.New("runtime already initialized")
	ErrRuntimeNotInitialized     = errors.New("runtime not initialized")
	ErrGCSClientNotAvailable     = errors.New("GCS client not available")
)

// NativeRuntime is the native implementation of the Runtime interface.
type NativeRuntime struct {
	opts            base.InitializeOptions
	handle          *cgoboundary.Handle
	mu              sync.RWMutex
	gcsClient       atomic.Pointer[gcs.Client]
	gcsInitError    atomic.Pointer[error]
	initialized     atomic.Bool
	workerContext   *cgointerfaces.NativeWorkerContext
	objectStore     object.ObjectStore
	executor        *cgointerfaces.NativeTaskExecutor
	functionManager *function.FunctionManager
	resourceManager resource.ResourceManager
}

// NewNativeRuntime creates a new NativeRuntime instance.
func NewNativeRuntime(opts base.InitializeOptions) (*NativeRuntime, error) {
	return &NativeRuntime{
		opts:            opts,
		handle:          nil,
		workerContext:   globalWorkerContext, // Use package-level singleton
		resourceManager: resource.NewResourceManager(),
	}, nil
}

// Start starts the native runtime.
//
// Thread safety: Uses atomic.Bool.CompareAndSwap to prevent duplicate initialization.
// GCS client is not initialized here, but lazily loaded on first access.
func (nr *NativeRuntime) Start() error {
	if !nr.initialized.CompareAndSwap(false, true) {
		logger.Info("NativeRuntime.Start() rejected - already initialized")
		return ErrRuntimeAlreadyInitialized
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()

	// Set up task executor callback before initializing core worker.
	// This ensures the callback is ready when tasks start arriving.
	// Initialize function manager first, then create executor with it
	logger.Info("creating FunctionManager and TaskExecutor")
	nr.functionManager = function.NewFunctionManager(nil)
	nr.executor = cgointerfaces.NewNativeTaskExecutor(nr.functionManager)
	cgocallback.SetTaskExecutor(nr.executeTask)

	// Call coreworker CGO initialization
	logger.Info("calling cgoboundary.Initialize()",
		"gcsAddress", nr.opts.Network.GcsAddress,
		"workerType", nr.opts.WorkerType)
	handle, err := cgoboundary.Initialize(nr.opts)
	if err != nil {
		logger.Error(err, "cgoboundary.Initialize() returned error",
			"gcsAddress", nr.opts.Network.GcsAddress,
			"workerType", nr.opts.WorkerType,
		)
		nr.initialized.Store(false)
		return fmt.Errorf("failed to initialize core worker: %w", err)
	}

	if handle == nil {
		logger.Error(nil, "cgoboundary.Initialize() returned nil handle - this should not happen")
		nr.initialized.Store(false)
		return fmt.Errorf("cgoboundary.Initialize() returned nil handle")
	}

	nr.handle = handle

	// Register the serializer's buffer pool with the object package.
	// This is done explicitly in Start() rather than in init() to:
	// 1. Follow dependency inversion principle (composition root pattern)
	// 2. Make the dependency explicit and testable
	// 3. Avoid implicit package initialization order dependencies
	// The serializer package provides the concrete implementation (BufferPoolWrapper)
	// which is injected into the object package at runtime startup.
	object.SetDefaultBufferPool(&serializer.BufferPoolWrapper{})

	// Create ObjectStore during Start() for zero-overhead access in the hot path.
	nativeStore := &objectstore.NativeObjectStore{}
	nr.objectStore = nativeStore

	// Register the Go task executor callback with C++.
	// This must be called after SetTaskExecutor() and before RunTaskExecutionLoop().
	cgocallback.RegisterTaskExecutorCallback()

	logger.Info("Native runtime started successfully")
	return nil
}

// executeTask is the task execution callback registered with C++.
// It is called by C++ when a task is received during task execution loop.
//
// This method delegates to the NativeTaskExecutor based on task type:
// - Normal tasks (actorID.IsNil()): Execute via executor.Execute()
// - Actor tasks: Execute via executor.ExecuteActorTask()
//
// Thread safety: NativeTaskExecutor is stateless and thread-safe.
// This method can be called concurrently by multiple task execution threads.
//
// Parameters:
//   - taskType: Type of task (matches ray::rpc::TaskType enum values)
//   - functionDescriptor: Function descriptor identifying the function to execute
//   - args: Function arguments (may contain ObjectRef or serialized values)
//   - numReturns: Number of expected return values
//   - actorID: Actor ID (nil for normal tasks, non-nil for actor tasks)
//
// Returns:
//   - []function.SerializedObject: Task execution results
//   - error: Execution error (if any)
func (nr *NativeRuntime) executeTask(
	taskType int,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	actorID ids.ActorID,
) ([]function.SerializedObject, error) {
	// A panic from a user function propagates to the caller GoExecuteTask (see
	// go/internal/runtime/cgo/task_executor.go), which recovers it and serializes a task
	// execution exception error object; no recovery is needed at this layer.

	// Delegate to NativeTaskExecutor based on task type
	if actorID.IsNil() {
		return nr.executor.Execute(functionDescriptor, args, numReturns)
	}
	return nr.executor.ExecuteActorTask(actorID, functionDescriptor, args, numReturns)
}

// Shutdown shuts down the native runtime.
//
// Thread safety: Uses atomic.Bool.CompareAndSwap to ensure shutdown happens only once.
// ObjectStore is closed along with the runtime to ensure proper resource cleanup.
//
// Shutdown order is critical:
// 1. Close GCS client FIRST (before C++ shutdown, so C++ objects are still valid)
// 2. Clear worker resources (before C++ shutdown)
// 3. Clear ObjectStore reference
// 4. Shutdown C++ LAST (handle.Shutdown() calls C++ shutdown which releases all C++ objects)
func (nr *NativeRuntime) Shutdown() error {
	logger.Info("Native runtime shutdown started")

	if !nr.initialized.CompareAndSwap(true, false) {
		logger.Info("Native runtime shutdown skipped: not initialized")
		return ErrRuntimeNotInitialized
	}

	nr.mu.Lock()
	if nr.handle == nil {
		nr.mu.Unlock()
		logger.Info("Native runtime shutdown skipped: handle is nil")
		return ErrRuntimeNotInitialized
	}

	// Clear handle while holding lock
	handle := nr.handle
	nr.handle = nil
	nr.mu.Unlock()

	// IMPORTANT: Close Go-layer resources BEFORE C++ shutdown
	// This ensures C++ objects are still valid when Go calls into C++

	// Close GCS client first (uses C++ CGcsClient internally)
	client := nr.gcsClient.Swap(nil)
	if client != nil && *client != nil {
		if err := (*client).Close(); err != nil {
			logger.Error(err, "Failed to close GCS client during shutdown. This is non-fatal as the process will exit.",
				"gcsAddress", (*client).Address(),
			)
		} else {
			logger.Info("GCS client closed")
		}
	}

	// Note: ClearWorkerResources is intentionally NOT called during shutdown.
	// The worker resource cache is only used to avoid repeated Raylet queries,
	// and it will be automatically cleaned up when the process exits.
	// Calling WorkerContext() during shutdown would return nil because initialized
	// is set to false at the start of Shutdown(), before C++ CoreWorker is actually
	// shutdown. This would cause a nil pointer dereference.

	// Clear ObjectStore reference
	nr.objectStore = nil
	logger.Info("ObjectStore cleared during shutdown")

	// Shutdown C++ LAST - after all Go-layer cleanup is done
	handle.Shutdown()

	logger.Info("Native runtime shutdown complete")
	return nil
}

// GetObjectStore returns the internal ObjectStore instance.
// This method is part of the Runtime interface and is called by RuntimeHandle.ObjectStore()
// to access the ObjectStore. The ObjectStore is created during Start() and cleared
// during Shutdown() to maintain proper lifecycle management.
//
// Thread safety: ObjectStore is created during Start() before any concurrent access,
// so no synchronization is needed in this getter.
//
// Note: After Shutdown() is called, this method returns nil.
func (nr *NativeRuntime) GetObjectStore() object.ObjectStore {
	return nr.objectStore
}

// GetTaskSubmitter returns the task submitter for submitting tasks.
// This method is part of the Runtime interface and is used by the API layer
// to obtain a TaskSubmitter for task submission operations.
//
// Thread safety: Creates a new NativeTaskSubmitter instance on each call.
// The FunctionManager is stateless, so multiple instances can coexist safely.
//
// Note: Returns nil if the runtime is not initialized.
func (nr *NativeRuntime) GetTaskSubmitter() submitter.TaskSubmitter {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	if nr.handle == nil {
		return nil
	}

	// Reuse the same FunctionManager instance
	return cgointerfaces.NewNativeTaskSubmitter(nr.functionManager)
}

// Run runs the task execution loop.
//
// Note: This method is only called in Worker mode.
func (nr *NativeRuntime) Run() error {
	// Only hold lock while checking handle to avoid blocking other operations
	// during long-running task execution loop.
	nr.mu.RLock()
	handle := nr.handle
	nr.mu.RUnlock()

	if handle == nil {
		return ErrRuntimeNotInitialized
	}

	// Run without holding lock to avoid blocking other operations
	return handle.RunTaskExecutionLoop()
}

// IsInitialized checks whether the runtime has been initialized.
func (nr *NativeRuntime) IsInitialized() bool {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	return nr.handle != nil
}

// GetNodeIPAddress returns the node IP address from runtime options.
func (nr *NativeRuntime) GetNodeIPAddress() string {
	return nr.opts.Network.NodeIPAddress
}

// GetFunctionManager returns the function manager for internal use.
// This is used by the worker package to register functions from the global registry.
//
// Note: This method is only intended for use during worker startup.
// Returns nil if the runtime is not initialized.
func (nr *NativeRuntime) GetFunctionManager() function.Manager {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	return nr.functionManager
}

// GetGcsClient returns the GCS client, initializing it lazily if needed.
//
// Thread safety: Uses atomic operations for lock-free concurrent access.
// The client is initialized lazily on first access. If initialization fails,
// subsequent calls will retry (unlike sync.Once which only executes once).
func (nr *NativeRuntime) GetGcsClient() (gcs.Client, error) {
	// Fast path: check if client already exists (lock-free)
	if client := nr.gcsClient.Load(); client != nil {
		return *client, nil
	}

	// Check if runtime is initialized
	nr.mu.RLock()
	handle := nr.handle
	nr.mu.RUnlock()

	if handle == nil {
		return nil, ErrRuntimeNotInitialized
	}

	// Need to initialize - use mutex to prevent concurrent initialization
	nr.mu.Lock()
	defer nr.mu.Unlock()

	// Double-check after acquiring lock
	if client := nr.gcsClient.Load(); client != nil {
		return *client, nil
	}

	// Create GCS client options from runtime options
	// Convert []byte to ids.ClusterID
	var clusterID ids.ClusterID
	if len(nr.opts.Job.ClusterID) > 0 {
		var err error
		clusterID, err = ids.ClusterIDFromBinary(nr.opts.Job.ClusterID)
		if err != nil {
			logger.Error(err, "Failed to create ClusterID, using nil ClusterID")
			clusterID = ids.NilClusterID()
		}
	} else {
		clusterID = ids.NilClusterID()
	}

	opts := gcs.ClientOptions{
		Address:   nr.opts.Network.GcsAddress,
		ClusterID: clusterID,
		TimeoutMs: 5000, // Default timeout
	}

	logger.Info("Initializing GCS client lazily",
		"address", opts.Address,
		"timeoutMs", opts.TimeoutMs,
	)

	// Initialize GCS client using CGO implementation
	client, err := native.ConnectClient(opts)
	if err != nil {
		logger.Error(err, "Failed to connect GCS client",
			"address", opts.Address,
		)
		initErr := fmt.Errorf("failed to connect GCS client: %w", err)
		nr.gcsInitError.Store(&initErr)
		return nil, initErr
	}

	// Store client atomically
	nr.gcsClient.Store(&client)
	// Clear any previous error
	nr.gcsInitError.Store(nil)
	logger.Info("GCS client initialized successfully")

	return client, nil
}

// CloseGcsClient closes the GCS client if it was initialized.
//
// Thread safety: Uses atomic operations for safe concurrent access.
// This is typically called during shutdown, but can be called independently.
func (nr *NativeRuntime) CloseGcsClient() error {
	client := nr.gcsClient.Swap(nil) // Atomically set to nil and get old value
	if client != nil && *client != nil {
		if err := (*client).Close(); err != nil {
			logger.Error(err, "Failed to close GCS client")
			return err
		}
		logger.Info("GCS client closed")
	}
	return nil
}

// WorkerContext returns the worker context accessor.
//
// Implementation: Returns the package-level singleton NativeWorkerContext instance.
// NativeWorkerContext is stateless (all methods call CGO functions) and thread-safe,
// so a single global instance (globalWorkerContext) is shared across all NativeRuntime
// instances to avoid unnecessary allocations on the hot path.
// Phase: Phase 1 (Basic context retrieval)
//
// Thread safety: NativeWorkerContext methods are thread-safe as they call C++ functions.
// The singleton instance is safe to share across goroutines.
func (r *NativeRuntime) WorkerContext() base.WorkerContext {
	// Return nil if runtime is not initialized to avoid accessing uninitialized CoreWorker
	if !r.initialized.Load() {
		return nil
	}
	// Return package-level singleton instance
	return r.workerContext
}

// GetRunMode returns the current run mode (cluster or local).
//
// Currently always returns RunModeCluster since the native implementation
// is designed for cluster mode operation.
//
// Returns:
//   - RunMode: The current run mode
func (nr *NativeRuntime) GetRunMode() base.RunMode {
	return base.RunModeCluster
}

// IsLocalMode returns true if running in local mode.
//
// The native implementation always runs in cluster mode, so this method
// always returns false.
//
// Note: This method is not part of WorkerContext interface, so it remains
// in the Runtime interface.
func (nr *NativeRuntime) IsLocalMode() bool {
	return false
}

// WasCurrentActorRestarted returns true if the current actor was restarted
// due to failure or other reasons.
//
// This method queries the GCS client to retrieve the actor's NumRestarts
// field and returns true if it is greater than zero.
//
// Returns:
//   - bool: True if the actor was restarted, false otherwise
func (nr *NativeRuntime) WasCurrentActorRestarted() bool {
	// Get current actor ID from WorkerContext
	ctx := nr.WorkerContext()
	if ctx == nil {
		return false
	}
	actorID := ctx.GetCurrentActorID()
	if actorID.IsNil() {
		// Not in actor context
		return false
	}

	// Get GCS client
	gcsClient, err := nr.GetGcsClient()
	if err != nil || gcsClient == nil {
		logger.Error(err, "Failed to get GCS client for actor restart check")
		return false
	}

	// Get actor info from GCS
	bgCtx := context.Background()
	actorInfo, err := gcsClient.GetActorInfo(bgCtx, actorID)
	if err != nil {
		logger.Error(err, "Failed to get actor info", "actorID", actorID)
		return false
	}

	// Check if NumRestarts > 0
	return int(actorInfo.NumRestarts) > 0
}

// GetAllNodeInfo returns information about all nodes in the cluster.
//
// Phase 2: This method will query the GCS client to retrieve comprehensive
// information about all nodes currently in the Ray cluster.
//
// Returns:
//   - []NodeInfo: Slice of NodeInfo for all nodes, or empty slice on error
func (nr *NativeRuntime) GetAllNodeInfo() []base.NodeInfo {
	// Get GCS client
	gcsClient, err := nr.GetGcsClient()
	if err != nil || gcsClient == nil {
		return []base.NodeInfo{}
	}

	// Get all nodes from GCS
	ctx := context.Background()
	nodesMap, err := gcsClient.GetAll(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to get all nodes from GCS")
		return []base.NodeInfo{}
	}

	// Convert protobuf to NodeInfo slice
	result := make([]base.NodeInfo, 0, len(nodesMap))
	for _, protoNode := range nodesMap {
		result = append(result, convertNodeInfo(protoNode))
	}

	return result
}

// GetAllActorInfo returns information about all actors in the cluster.
//
// Phase 2: This method will query the GCS client to retrieve comprehensive
// information about all actors currently registered in the Ray cluster.
//
// Returns:
//   - []ActorInfo: Slice of ActorInfo for all actors, or empty slice on error
func (nr *NativeRuntime) GetAllActorInfo() []base.ActorInfo {
	// Get GCS client
	gcsClient, err := nr.GetGcsClient()
	if err != nil || gcsClient == nil {
		return []base.ActorInfo{}
	}

	// Get all actors from GCS
	ctx := context.Background()
	actors, err := gcsClient.ListActors(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to get all actors from GCS")
		return []base.ActorInfo{}
	}

	// Convert protobuf to ActorInfo slice
	result := make([]base.ActorInfo, 0, len(actors))
	for _, protoActor := range actors {
		result = append(result, convertActorInfo(protoActor))
	}

	return result
}

// GetGpuIds returns the IDs of GPUs allocated to the current worker.
//
// This method retrieves GPU resource IDs from the resource manager and maps
// them to actual CUDA device IDs using the CUDA_VISIBLE_DEVICES environment
// variable. The mapping is necessary because CUDA_VISIBLE_DEVICES may reorder
// or restrict which physical GPU devices are visible to the process.
//
// For example, if CUDA_VISIBLE_DEVICES="0,2,4" and the resource manager
// assigns GPU resource IDs ["0", "1"], the returned CUDA device IDs will be
// ["0", "2"] (the first and second entries in CUDA_VISIBLE_DEVICES).
//
// Returns:
//   - []string: List of CUDA device IDs (e.g., ["0", "2", "4"]), or empty
//     slice if no GPUs are allocated or CUDA_VISIBLE_DEVICES is not set
func (nr *NativeRuntime) GetGpuIds() []string {
	// 1. Get current worker ID
	workerID := nr.WorkerContext().GetCurrentWorkerId()
	if workerID.IsNil() {
		logger.Info("Cannot get GPU IDs: worker ID is nil")
		return []string{}
	}

	// 2. Query resource manager for GPU resource IDs
	resourceIds := nr.resourceManager.GetWorkerResourceIds(workerID)

	// 3. Extract GPU resource IDs from resource map
	gpuResourceIds := object.ParseGPUResourceIds(resourceIds)
	if len(gpuResourceIds) == 0 {
		// No GPU resources allocated
		return []string{}
	}

	// 4. Read CUDA_VISIBLE_DEVICES environment variable
	cudaVisibleDevices := object.GetCudaVisibleDevices()

	// 5. Map GPU resource IDs to actual CUDA device IDs
	if cudaVisibleDevices == nil {
		// CUDA_VISIBLE_DEVICES not set, return resource IDs as-is
		return gpuResourceIds
	}

	// Map resource IDs to CUDA device IDs
	var gpuIds []string
	for _, resId := range gpuResourceIds {
		idx, err := strconv.Atoi(resId)
		if err != nil || idx < 0 || idx >= len(cudaVisibleDevices) {
			// Invalid index, skip this resource ID
			logger.Info("Invalid GPU resource ID", "resourceId", resId, "error", err)
			continue
		}
		gpuIds = append(gpuIds, cudaVisibleDevices[idx])
	}

	return gpuIds
}

// GetCurrentActorHandle returns the handle of the current actor.
//
// This method creates a NativeActorHandle with the current actor ID.
//
// Returns:
//   - submitter.ActorHandle: Actor handle if in actor context, nil otherwise
func (nr *NativeRuntime) GetCurrentActorHandle() submitter.ActorHandle {
	// Get current actor ID from WorkerContext
	ctx := nr.WorkerContext()
	if ctx == nil {
		return nil
	}
	actorID := ctx.GetCurrentActorID()
	if actorID.IsNil() {
		// Not in actor context
		return nil
	}

	// Create NativeActorHandle with the current actor ID
	return &object.NativeActorHandle{
		ActorID:  actorID,
		Language: object.LanguageGo,
	}
}

// NativeHandleImpl implements contract.RuntimeHandle interface for native runtime.
type NativeHandleImpl struct {
	runtime *NativeRuntime
}

// IsRuntimeHandle implements the marker method (exported to allow implementation in other packages).
func (h *NativeHandleImpl) IsRuntimeHandle() {}

// Runtime returns the underlying runtime implementation.
func (h *NativeHandleImpl) Runtime() contract.Runtime {
	return h.runtime
}

// ObjectStore returns the object store for storing and retrieving objects.
func (h *NativeHandleImpl) ObjectStore() object.ObjectStore {
	return h.runtime.GetObjectStore()
}

// NativeRuntimeFactory is the factory implementation for native runtime.
type NativeRuntimeFactory struct{}

// Initialize initializes the native runtime and returns a handle.
func (f *NativeRuntimeFactory) Initialize(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
	// Driver bootstrap: resolve the local node's connection info from GCS when
	// the caller did not supply it (e.g. an in-process driver launched outside
	// of `ray job submit`). The C++ CoreWorker reads the raylet port from these
	// options before it can talk to the raylet, so they cannot be left at zero.
	if opts != nil && opts.WorkerType == options.WorkerTypeDriver {
		if err := bootstrapDriverOptions(opts); err != nil {
			return nil, err
		}
	}

	// Convert API options to internal options
	baseOpts, err := convertToBaseOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to convert options: %w", err)
	}

	// Create NativeRuntime instance
	runtime, err := NewNativeRuntime(baseOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create native runtime: %w", err)
	}

	// Start the runtime (this initializes CGO core worker)
	if err := runtime.Start(); err != nil {
		return nil, fmt.Errorf("failed to start native runtime: %w", err)
	}

	// Create and return handle
	handle := &NativeHandleImpl{runtime: runtime}
	return handle, nil
}

// CreateRuntime creates a new NativeRuntime instance with the given options.
// This method implements the base.RuntimeFactory interface.
// Note: This function only creates the runtime instance, it does not start it.
// The caller is responsible for calling Start() to initialize the CGO core worker.
func (f *NativeRuntimeFactory) CreateRuntime(opts base.InitializeOptions) (contract.Runtime, error) {
	runtime, err := NewNativeRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create native runtime: %w", err)
	}

	return runtime, nil
}

// helper function: convert API options to base options
func convertToBaseOptions(apiOpts *options.InitializeOptions) (base.InitializeOptions, error) {
	if apiOpts == nil {
		return base.InitializeOptions{}, nil
	}

	// Perform conversion - map all fields from API options to base options
	// Note: Need to convert between different types (string <-> []byte, int32 <-> int)
	jobIDBytes, err := ids.JobIDFromHex(apiOpts.Job.JobID)
	if err != nil {
		return base.InitializeOptions{}, fmt.Errorf("invalid JobID: %w", err)
	}

	var clusterIDBytes []byte
	if apiOpts.Job.ClusterID != "" {
		clusterIDBytes = []byte(apiOpts.Job.ClusterID)
	}

	return base.InitializeOptions{
		WorkerType: apiOpts.WorkerType,
		Network: base.NetworkOptions{
			NodeIPAddress:   apiOpts.Network.NodeIPAddress,
			NodeManagerPort: int(apiOpts.Network.NodeManagerPort),
			GcsAddress:      apiOpts.Network.GcsAddress,
		},
		Job: base.JobOptions{
			JobID:     jobIDBytes.Binary(),
			ClusterID: clusterIDBytes,
			JobConfig: []byte(apiOpts.Job.JobConfig),
		},
		Runtime: base.RuntimeOptions{
			StoreSocket:    apiOpts.Runtime.StoreSocket,
			RayletSocket:   apiOpts.Runtime.RayletSocket,
			LogDir:         apiOpts.Runtime.LogDir,
			StartupToken:   int(apiOpts.Runtime.StartupToken),
			RuntimeEnvHash: int(apiOpts.Runtime.RuntimeEnvHash),
			WorkerIDHex:    apiOpts.Runtime.WorkerIDHex,
		},
	}, nil
}

// bootstrapDriverOptions fills in the local node's connection info from GCS
// when an in-process (cluster-mode) driver did not supply it explicitly.
// Ray Python resolves this in `node.py` by querying the GCS node table; the
// C++ CoreWorker needs the node_manager_port to connect to the raylet, so the
// values cannot remain zero.
//
// Only the fields that are still zero are overwritten; values the caller set
// are preserved.
func bootstrapDriverOptions(opts *options.InitializeOptions) error {
	// Short-circuit if the caller already provided the connection info.
	if opts.Network.NodeManagerPort != 0 && opts.Runtime.RayletSocket != "" &&
		opts.Runtime.StoreSocket != "" {
		return nil
	}

	nodeIP := opts.Network.NodeIPAddress
	if nodeIP == "" {
		nodeIP = detectLocalIP()
	}

	// Connect to GCS and ask for the node this driver should connect to.
	client, err := native.ConnectClient(gcs.ClientOptions{
		Address:   opts.Network.GcsAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000,
	})
	if err != nil {
		return fmt.Errorf("bootstrapDriverOptions: connect to GCS failed: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	node, err := client.GetNodeToConnect(ctx, nodeIP)
	if err != nil {
		return fmt.Errorf("bootstrapDriverOptions: get node info failed: %w", err)
	}
	if node == nil {
		return fmt.Errorf("bootstrapDriverOptions: GCS returned no node info for ip %s", nodeIP)
	}

	if opts.Network.NodeManagerPort == 0 {
		opts.Network.NodeManagerPort = node.GetNodeManagerPort()
	}
	if opts.Runtime.RayletSocket == "" {
		opts.Runtime.RayletSocket = node.GetRayletSocketName()
	}
	if opts.Runtime.StoreSocket == "" {
		opts.Runtime.StoreSocket = node.GetObjectStoreSocketName()
	}
	if opts.Network.NodeIPAddress == "" {
		// Prefer the address the node manager advertises for connecting back.
		if addr := node.GetNodeManagerAddress(); addr != "" {
			opts.Network.NodeIPAddress = addr
		} else {
			opts.Network.NodeIPAddress = nodeIP
		}
	}

	log.Log.Info("driver bootstrap resolved node info from GCS",
		"node_id", node.GetNodeId(), "ip", opts.Network.NodeIPAddress, "port", opts.Network.NodeManagerPort)
	return nil
}

// detectLocalIP returns the first non-loopback IPv4 address on this host,
// falling back to IPv4 loopback if none is found.
func detectLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipv4 := ipnet.IP.To4(); ipv4 != nil {
				return ipv4.String()
			}
		}
	}
	return "127.0.0.1"
}

// runtimeFactoryRegistered tracks whether the runtime factory has been registered.
var runtimeFactoryRegistered atomic.Bool

// init automatically registers the native runtime initializer when the package is loaded.
// This is the key mechanism that allows api.InitWithOptions() to work without importing internal packages.
func init() {
	// Register the native runtime initializer for cluster modes (Driver/Worker).
	api.RegisterInitializer(options.WorkerTypeDriver, func(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
		factory := &NativeRuntimeFactory{}
		return factory.Initialize(opts)
	})
	api.RegisterInitializer(options.WorkerTypeWorker, func(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
		factory := &NativeRuntimeFactory{}
		return factory.Initialize(opts)
	})

	// Also register the old-style runtime factory for backward compatibility
	if runtimeFactoryRegistered.CompareAndSwap(false, true) {
		base.SetRuntimeFactory(&NativeRuntimeFactory{})
	}
}
