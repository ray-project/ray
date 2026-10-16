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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/logconfig"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/pathutil"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// ============================================================================
// Runtime Lifecycle Functions
// ============================================================================

// currentRuntimePlugin retains the loaded runtime plugin across Init/Shutdown
// cycles so that api.Shutdown can drive the plugin's full teardown: the plugin's
// exported Shutdown (go/cmd/plugin) clears the api-level GCS client cache and
// calls base.Shutdown, which resets the base runtime singleton. Calling only
// handle.Runtime().Shutdown() would leave the base singleton in place, making a
// subsequent Init fail with ErrRuntimeAlreadyInitialized.
//
// It is an atomic.Pointer so the Shutdown-side read/clear has a happens-before
// edge with the Init-side store without sharing finalizerMu: Shutdown writes
// other state under finalizerMu, and an unsynchronized bare pointer write could
// race with the Shutdown goroutine's read.
var currentRuntimePlugin atomic.Pointer[plugin.RuntimePlugin]

// setupDriverLoggerFn configures the driver's component logger
// (ray_go_{PID}.log). It is a package-level variable so tests can observe and
// override it; the default is the logconfig-based setup.
var setupDriverLoggerFn = func(logDir string) error {
	return logconfig.NewLogOption(
		logconfig.WithLogsDir(logDir),
		logconfig.WithLoggingFilename(logconfig.RayGoLogFilename()),
	).SetupComponentLogger()
}

// Init initializes the Ray runtime with default options.
// This function must be called before using any other Ray API functions.
//
// Returns:
//   - error: any error encountered during initialization
//
// Example:
//
//	if err := api.Init(); err != nil {
//	    log.Fatalf("Failed to initialize Ray: %v", err)
//	}
func Init() error {
	return InitWithOptions(nil)
}

// InitLocal initializes the Ray runtime in local mode (pure in-process, no
// cluster). It routes through the local-mode short-circuit in InitWithOptions,
// which dispatches to the registered local-mode initializer instead of loading
// the go_runtime.so plugin. This keeps the executable free of the C++ core
// worker / gRPC that the plugin path statically links in.
//
// The local-mode initializer is registered by importing
// go/pkg/runtime/local (a public adapter package); without it, InitLocal
// falls through to the plugin path, which then fails if no runtime plugin is
// available.
//
// Example:
//
//	if err := api.InitLocal(); err != nil {
//	    log.Fatalf("Failed to initialize Ray in local mode: %v", err)
//	}
func InitLocal() error {
	return InitWithOptions(&options.InitializeOptions{WorkerType: options.WorkerTypeLocal})
}

// InitWithOptions initializes the Ray runtime with custom options.
// This function must be called before using any other Ray API functions.
//
// Parameters:
//   - opts: optional configuration options (can be nil for defaults)
//
// Returns:
//   - error: any error encountered during initialization
//
// Example:
//
//	opts := &options.InitializeOptions{
//	    WorkerType: options.WorkerTypeDriver,
//	}
//	if err := api.InitWithOptions(opts); err != nil {
//	    log.Fatalf("Failed to initialize Ray: %v", err)
//	}
func InitWithOptions(opts *options.InitializeOptions) error {
	// Handle nil opts by using zero value
	var initOpts options.InitializeOptions
	if opts != nil {
		initOpts = *opts
	}

	// Read configuration from environment variables if not provided in opts.
	// This follows the same pattern as Java's RayConfig.create() which reads
	// system properties, and Python's ray.init() which reads RAY_ADDRESS env var.
	//
	// Environment variables are set by:
	// 1. User explicitly: export RAY_ADDRESS=127.0.0.1:6379
	// 2. Inherited from parent process (e.g., raylet inherits from ray start)
	// 3. For Go worker started by setup_worker: passed via command line args
	//
	// Note: We support both RAY_ADDRESS (Python style) and RAY_address (Go style)
	// for compatibility. RAY_ADDRESS takes precedence.

	// GCS address (required for cluster mode)
	// Support both RAY_ADDRESS (Python/Java style) and RAY_address (Go style)
	if initOpts.Network.GcsAddress == "" {
		initOpts.Network.GcsAddress = os.Getenv("RAY_ADDRESS")
	}
	if initOpts.Network.GcsAddress == "" {
		initOpts.Network.GcsAddress = os.Getenv("RAY_address")
	}

	// Node IP address - multi-tier resolution (mimics Java and Python)
	// Priority 1: Environment variable RAY_NODE_IP_ADDRESS
	if initOpts.Network.NodeIPAddress == "" {
		initOpts.Network.NodeIPAddress = os.Getenv("RAY_NODE_IP_ADDRESS")
	}
	// Priority 2: Read from Ray session file (node_ip_address.json)
	if initOpts.Network.NodeIPAddress == "" {
		nodeIP := readNodeIPAddressFromFile()
		if nodeIP != "" {
			initOpts.Network.NodeIPAddress = nodeIP
		}
	}
	// Priority 3: Auto-detect local IP address (like Java's NetworkUtil.getIpAddress())
	if initOpts.Network.NodeIPAddress == "" {
		initOpts.Network.NodeIPAddress = getLocalIPAddress()
	}

	// Job ID (hex-encoded string)
	if initOpts.Job.JobID == "" {
		initOpts.Job.JobID = os.Getenv("RAY_JOB_ID")
	}

	// Plasma store socket name
	if initOpts.Runtime.StoreSocket == "" {
		initOpts.Runtime.StoreSocket = os.Getenv("RAY_OBJECT_STORE_SOCKET_NAME")
	}

	// Raylet socket name
	if initOpts.Runtime.RayletSocket == "" {
		initOpts.Runtime.RayletSocket = os.Getenv("RAY_RAYLET_SOCKET_NAME")
	}

	// Log directory - multi-tier resolution (mimics Python)
	// Priority 1: Environment variable RAY_LOGS_DIR
	if initOpts.Runtime.LogDir == "" {
		if logDir := os.Getenv("RAY_LOGS_DIR"); logDir != "" {
			initOpts.Runtime.LogDir = logDir
		}
	}
	// Priority 2: Derive from RAY_SESSION_DIR
	if initOpts.Runtime.LogDir == "" {
		if sessionDir := os.Getenv("RAY_SESSION_DIR"); sessionDir != "" {
			initOpts.Runtime.LogDir = filepath.Join(sessionDir, "logs")
		}
	}
	// Priority 3: Derive from session_latest symlink (like Python)
	if initOpts.Runtime.LogDir == "" {
		rayTempDir := "/tmp/ray"
		if tempDir := os.Getenv("RAY_TEMP_DIR"); tempDir != "" {
			rayTempDir = tempDir
		}
		sessionLatest := filepath.Join(rayTempDir, "session_latest")
		if info, err := os.Lstat(sessionLatest); err == nil && info.Mode()&os.ModeSymlink != 0 {
			realPath, err := os.Readlink(sessionLatest)
			if err == nil {
				initOpts.Runtime.LogDir = filepath.Join(realPath, "logs")
			}
		}
	}

	// Set default WorkerType to Driver only when no options were provided
	// (api.Init()). An explicit WorkerTypeWorker (zero value, iota 0) must be
	// preserved: promoting it here would initialize a worker process as a
	// driver, so its registration is rejected by the raylet ("Job ... already
	// started in worker pool") and tasks never execute.
	if opts == nil {
		initOpts.WorkerType = options.WorkerTypeDriver
	}

	// Route driver logs to ray_go_{PID}.log unless the caller already
	// configured its own logger (SkipDriverLogSetup).
	if initOpts.WorkerType == options.WorkerTypeDriver && initOpts.Runtime.LogDir != "" && !initOpts.SkipDriverLogSetup {
		if err := setupDriverLoggerFn(initOpts.Runtime.LogDir); err != nil {
			log.Log.Error(err, "failed to setup driver logger, continuing with stdout", "log_dir", initOpts.Runtime.LogDir)
		}
	}

	// Node manager port
	if initOpts.Network.NodeManagerPort == 0 {
		if portStr := os.Getenv("RAY_NODE_MANAGER_PORT"); portStr != "" {
			if port, err := strconv.Atoi(portStr); err == nil {
				initOpts.Network.NodeManagerPort = int32(port)
			}
		}
	}

	// Startup token
	if initOpts.Runtime.StartupToken == 0 {
		if tokenStr := os.Getenv("RAY_STARTUP_TOKEN"); tokenStr != "" {
			if token, err := strconv.Atoi(tokenStr); err == nil {
				initOpts.Runtime.StartupToken = int32(token)
			}
		}
	}

	// Runtime env hash
	if initOpts.Runtime.RuntimeEnvHash == 0 {
		if hashStr := os.Getenv("RAY_RUNTIME_ENV_HASH"); hashStr != "" {
			if hash, err := strconv.Atoi(hashStr); err == nil {
				initOpts.Runtime.RuntimeEnvHash = int32(hash)
			}
		}
	}

	// Validate required configuration for cluster mode
	if initOpts.WorkerType == options.WorkerTypeDriver && initOpts.Network.GcsAddress == "" {
		// Try to read from Ray's cluster file (same as Python's find_bootstrap_address)
		gcsAddress := readRayAddressFromFile()
		if gcsAddress != "" {
			initOpts.Network.GcsAddress = gcsAddress
		}
	}

	// Final validation - GCS address is required for cluster mode
	if initOpts.Network.GcsAddress == "" && initOpts.WorkerType != options.WorkerTypeLocal {
		return fmt.Errorf("GCS address not provided: address not provided: set RAY_address environment variable, " +
			"or run 'ray start --head' first, or pass options with api.InitWithOptions()")
	}

	// Validate JobConfig if provided
	if initOpts.Job.JobConfig != "" {
		// JobConfig is base64-encoded protobuf (matches Java/CPP serialization)
		// Decode and parse to validate
		decoded, err := base64.StdEncoding.DecodeString(initOpts.Job.JobConfig)
		if err != nil {
			return fmt.Errorf("invalid JobConfig base64: %w", err)
		}

		var config proto.JobConfig
		if err := protolib.Unmarshal(decoded, &config); err != nil {
			return fmt.Errorf("invalid JobConfig protobuf: %w", err)
		}

		// Log JobConfig for debugging - just print the key fields directly
		log.Log.Info("initializing with JobConfig",
			"codeSearchPath", config.CodeSearchPath,
			"rayNamespace", config.RayNamespace,
			"defaultActorLifetime", config.DefaultActorLifetime.String())
	} else if initOpts.WorkerType == options.WorkerTypeDriver {
		// No JobConfig was provided by the caller. Driver mode requires a
		// non-empty ray_namespace when creating actors (GCS enforces
		// !ray_namespace.empty() in GcsActorManager), so build a default
		// JobConfig whose namespace is auto-generated. This matches the
		// Java/C++ behavior where driver initialization always produces a
		// JobConfig with a random default namespace (Java: RayConfig.java
		// UUID.randomUUID(); C++: ConfigInternal.cc UniqueID::FromRandom()),
		// and reuses the same JobConfigBuilder serialization path as
		// InitializeWithJobConfig so the payload is byte-identical.
		//
		// The code search path is sourced from the RAY_CODE_SEARCH_PATH
		// environment variable (':' separated), mirroring the worker side:
		// the raylet sets RAY_CODE_SEARCH_PATH for Go workers from the job
		// config's code_search_path (worker_pool.cc), so the driver and
		// worker resolve the same user-function plugin. The job-injected
		// runtime_env/metadata (if any) are also merged in.
		builder := options.NewJobConfigBuilder()
		if searchPath := os.Getenv("RAY_CODE_SEARCH_PATH"); searchPath != "" {
			paths := strings.Split(searchPath, ":")
			nonEmpty := make([]string, 0, len(paths))
			for _, p := range paths {
				if p != "" {
					nonEmpty = append(nonEmpty, p)
				}
			}
			builder.WithCodeSearchPath(nonEmpty...)
		}
		if err := builder.MergeFromEnv(); err != nil {
			return fmt.Errorf("failed to build default JobConfig: %w", err)
		}
		jobOpts, err := builder.BuildToJobOptions()
		if err != nil {
			return fmt.Errorf("failed to build default JobConfig: %w", err)
		}
		initOpts.Job.JobConfig = jobOpts.JobConfig
		// Log the generated namespace for traceability.
		if decoded, err := base64.StdEncoding.DecodeString(initOpts.Job.JobConfig); err == nil {
			var config proto.JobConfig
			if err := protolib.Unmarshal(decoded, &config); err == nil {
				log.Log.Info("initialized with default JobConfig",
					"rayNamespace", config.RayNamespace,
					"codeSearchPath", config.CodeSearchPath)
			}
		}
	}

	// Local mode is a pure in-process runtime and must not load the
	// go_runtime.so plugin: the plugin's Initialize registers a GCS client
	// factory and performs driver-mode GCS handshakes that local mode does not
	// use, and plugin.Open requires the plugin to be built with exactly the same
	// package versions as this executable. Route through the registered
	// initializer instead (dependency inversion: api does not import internal
	// packages; the local_mode package registers the initializer from its
	// init(), reached by importing go/pkg/runtime/local). The native package
	// does NOT register an initializer: it only registers its runtime factory
	// for the driver/worker worker types.
	if initOpts.WorkerType == options.WorkerTypeLocal {
		if fn := getInitFunc(initOpts.WorkerType); fn != nil {
			handle, err := fn(&initOpts)
			if err != nil {
				return err
			}
			setHandle(handle)
			return nil
		}
		// No initializer registered: fall through to the plugin path for
		// backward compatibility (e.g. tests that load the plugin directly).
	}

	// Note: For Driver mode, fetching node info from GCS is now done in plugin.Initialize()
	// after the GCS client factory is registered. This ensures the factory is available
	// when FetchNodeInfoFromGCS is called.

	// Resolve the plugin path lazily, only for the plugin path. Local mode
	// returns above and never needs the go_runtime.so plugin (it may not even
	// be present in a pure local-mode environment).
	pluginPath := os.Getenv("RAY_GO_RUNTIME_PATH")
	if pluginPath == "" {
		var err error
		pluginPath, err = plugin.FindPluginPath()
		if err != nil {
			return err
		}
	}

	// Dynamically load the plugin
	// plugin.Open() will execute init() functions in all packages of go_runtime.so,
	// including internal/runtime/native's init() which registers the factory with base package.
	//
	// Note: LoadRuntimePlugin internally calls the plugin's Initialize function,
	// which initializes the base runtime. We reuse the handle from the loaded plugin
	// instead of calling Initialize again to avoid double-initialization.
	// This matches Java's behavior where Ray.init() calls nativeInitialize() exactly once.
	runtimePlugin, err := plugin.LoadRuntimePlugin(pluginPath, initOpts)
	if err != nil {
		return err
	}

	// The plugin loader caches the loaded plugin, so a subsequent InitWithOptions
	// after a previous Shutdown() returns the same plugin object. api.Shutdown
	// drives the plugin's full teardown (which resets the base runtime singleton
	// and clears the plugin's handle), so the cached plugin reports
	// IsInitialized()==false on the second call. In that case re-initialize the
	// plugin to obtain a fresh handle instead of reusing the stale one, which
	// would leave every submitter/object-store call failing with
	// "submitter_not_available". This keeps api.Init/Shutdown reentrant within a
	// single process (e.g. Go tests that Init and Shutdown per test).
	var handle contract.RuntimeHandle
	if !runtimePlugin.IsInitialized() {
		log.Log.Info("plugin is shut down, re-initializing")
		handle, err = runtimePlugin.Initialize(initOpts)
		if err != nil {
			return fmt.Errorf("failed to re-initialize plugin after shutdown: %w", err)
		}
	} else {
		// Reuse the handle obtained from LoadRuntimePlugin.
		// The handle was already created when loadPluginInternal called the plugin's
		// Initialize function. Calling Initialize again would cause double-initialization
		// error since base.Initialize is a singleton.
		handle = runtimePlugin.GetHandle()
		if handle == nil {
			return fmt.Errorf("LoadRuntimePlugin returned plugin with nil handle - Initialize may have failed")
		}
	}

	// Retain the plugin so api.Shutdown can perform full teardown
	// (ClearAllCachedClients + base.Shutdown), resetting the base runtime
	// singleton for a later re-init.
	currentRuntimePlugin.Store(runtimePlugin)

	// Store handle in api's service locator
	setHandle(handle)
	return nil
}

// Shutdown shuts down the Ray runtime.
// This function should be called when the application exits to clean up resources.
//
// Example:
//
//	defer api.Shutdown()
//
// Implementation notes:
//   - shutdownComplete is set BEFORE tearing down the runtime to prevent
//     finalizers from accessing C++ objects that are being shutdown.
//   - This ensures that if a finalizer runs concurrently with shutdown,
//     it will see shutdownComplete=true and skip RemoveLocalReference().
//   - When the runtime was initialized through the plugin path, shutdown goes
//     through the plugin's full teardown (go/cmd/plugin.Shutdown), which clears
//     the api-level GCS client cache and calls base.Shutdown. base.Shutdown
//     resets the base runtime singleton, making a later Init/InitWithOptions
//     build a fresh runtime in the same process. A bare
//     handle.Runtime().Shutdown() would only tear down the native runtime and
//     leave the base singleton behind, failing the next Init with
//     ErrRuntimeAlreadyInitialized.
func Shutdown() {
	handle, ok := tryGetHandle()
	// Always set shutdownComplete to prevent any stray finalizers from
	// accessing C++ objects during or after shutdown.
	shutdownComplete.Store(true)

	// Hold the write lock while tearing down the C++ runtime so that no
	// finalizer can be in the middle of a RemoveLocalReference call (which takes
	// the read lock) while the object store is being released. Finalizers either
	// finish before this lock is taken or block until shutdown completes and then
	// skip via the shutdownComplete flag.
	finalizerMu.Lock()
	defer finalizerMu.Unlock()

	if runtimePlugin := currentRuntimePlugin.Load(); runtimePlugin != nil {
		// Full plugin teardown: ClearAllCachedClients + base.Shutdown. This also
		// resets the base runtime singleton and clears the plugin's handle, so a
		// subsequent InitWithOptions re-initializes cleanly.
		if err := runtimePlugin.Shutdown(); err != nil {
			log.Log.Error(err, "failed to shutdown runtime plugin")
		}
		currentRuntimePlugin.Store(nil)
	} else if ok && handle != nil && handle.Runtime() != nil {
		// Local mode or worker-side runtime: no plugin loaded, tear down the
		// native runtime directly.
		_ = handle.Runtime().Shutdown()
	}
	clearHandle()
}

// IsInitialized checks if the Ray runtime is initialized.
//
// Returns:
//   - bool: true if the runtime is initialized, false otherwise
//
// Example:
//
//	if !api.IsInitialized() {
//	    if err := api.Init(); err != nil {
//	        log.Fatalf("Failed to initialize Ray: %v", err)
//	    }
//	}
func IsInitialized() bool {
	_, ok := tryGetHandle()
	return ok
}

// readRayAddressFromFile reads the Ray cluster address from the file system.
// This mimics Python's find_bootstrap_address() which reads from
// $RAY_SESSION_DIR/ray_current_cluster or /tmp/ray/ray_current_cluster.
//
// Returns empty string if the file doesn't exist or cannot be read.
//
// It is a package-level variable so tests can inject a fake resolver; the
// default keeps the original behavior.
var readRayAddressFromFile = func() string {
	addr, err := pathutil.ReadRayClusterFile("ray_current_cluster")
	if err == nil {
		return strings.TrimSpace(string(addr))
	}
	return ""
}

// readNodeIPAddressFromFile reads the node IP address from the Ray session file.
// This mimics Python's get_cached_node_ip_address() which reads from
// $RAY_SESSION_DIR/node_ip_address.json.
//
// The file format is JSON: {"node_ip_address": "192.168.1.100"}
// Returns empty string if the file doesn't exist or cannot be read.
func readNodeIPAddressFromFile() string {
	addr, err := pathutil.ReadRayClusterFile("node_ip_address.json")
	if err == nil {
		// Use standard JSON parsing for robustness
		var data struct {
			NodeIPAddress string `json:"node_ip_address"`
		}
		if err := json.Unmarshal(addr, &data); err == nil && data.NodeIPAddress != "" {
			return data.NodeIPAddress
		}
	}
	return ""
}

// getLocalIPAddress automatically gets the local IP address.
// This mimics Java's NetworkUtil.getIpAddress() which iterates
// over network interfaces to find a non-loopback IPv4 address.
//
// Returns "127.0.0.1" as fallback if no suitable address is found.
func getLocalIPAddress() string {
	// Get all network interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	// First pass: look for non-loopback, non-virtual interfaces with IPv4 addresses
	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// Skip virtual/tunnel interfaces
		if iface.Flags&net.FlagPointToPoint != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok {
				// Skip loopback and link-local addresses
				if ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() || ipNet.IP.IsLinkLocalMulticast() {
					continue
				}

				// Prefer IPv4 addresses
				if ipNet.IP.To4() != nil {
					return ipNet.IP.String()
				}
			}
		}
	}

	// Fallback to localhost
	return "127.0.0.1"
}

// FetchNodeInfoFromGCS fetches node connection info from GCS for Driver mode.
// This mimics Java's GcsClient.getNodeToConnectForDriver() which returns
// GcsNodeInfo containing objectStoreSocketName, rayletSocketName, and nodeManagerPort.
//
// Parameters:
//   - gcsAddress: GCS server address (host:port)
//   - nodeIpAddress: The node IP address to look up
//
// Returns:
//   - *proto.GcsNodeInfo: Node info with socket names and ports, or nil if not found
//   - error: Any error encountered during the fetch
//
// Note: This function requires the GCS client factory to be registered first
// via RegisterGCSClientFactory(). Callers should ensure this is done before invoking.
func FetchNodeInfoFromGCS(gcsAddress, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	if gcsAddress == "" || nodeIpAddress == "" {
		return nil, fmt.Errorf("GCS address and node IP address are required")
	}

	// Create GCS client options (mimics Java's GcsClientOptions)
	// Use NilClusterID since we're just fetching node info, not submitting jobs
	opts := gcs.ClientOptions{
		Address:   gcsAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000, // 10 seconds timeout
	}

	// Create GCS client using the registered factory
	// This follows the Dependency Inversion Principle:
	// - api package defines the GCSClientFactory interface
	// - go_runtime.so (via go/internal/gcs/native) implements the interface
	// - api package depends only on the abstraction, not the concrete implementation
	client, err := createGCSClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	// Get node info from GCS
	// This calls the C++ GlobalStateAccessor::GetNodeToConnectForDriver
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeInfo, err := client.GetNodeToConnect(ctx, nodeIpAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info from GCS: %w", err)
	}

	return nodeInfo, nil
}

// NextJobID fetches the next available JobID from GCS.
// This mimics Java's GcsClient.nextJobId() which allocates a new JobID for the driver.
//
// Parameters:
//   - gcsAddress: GCS server address (host:port)
//
// Returns:
//   - string: Hex-encoded JobID string (4 bytes = 8 hex characters)
//   - error: Any error encountered during the fetch
//
// Note: This function requires the GCS client factory to be registered first
// via RegisterGCSClientFactory(). Callers should ensure this is done before invoking.
func NextJobID(gcsAddress string) (string, error) {
	if gcsAddress == "" {
		return "", fmt.Errorf("GCS address is required")
	}

	// Create GCS client options
	opts := gcs.ClientOptions{
		Address:   gcsAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000, // 10 seconds timeout
	}

	// Create GCS client using the registered factory
	client, err := createGCSClient(opts)
	if err != nil {
		return "", fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	// Get next JobID from GCS
	// This calls the C++ GlobalStateAccessor::GetNextJobID
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID, err := client.NextJobID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get next JobID from GCS: %w", err)
	}

	return jobID.Hex(), nil
}
