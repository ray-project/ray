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
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/pathutil"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// ============================================================================
// Runtime Lifecycle Functions
// ============================================================================

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

	// If an initializer has been registered in-process (for example the pure-Go
	// local-mode runtime), use it directly instead of locating and loading a
	// runtime plugin .so. This keeps the minimal runtime usable without building
	// go_runtime.so.
	if initFunc := getInitFunc(initOpts.WorkerType); initFunc != nil {
		handle, err := getFactory().Initialize(&initOpts)
		if err != nil {
			return err
		}
		setHandle(handle)
		return nil
	}

	// Use plugin.Open() to dynamically load go_runtime.so at runtime
	// This follows the Dependency Inversion Principle: api depends only on
	// the abstract contract.RuntimeHandle interface, not on concrete implementations.
	pluginPath := os.Getenv("RAY_GO_RUNTIME_PATH")
	if pluginPath == "" {
		var err error
		pluginPath, err = plugin.FindPluginPath()
		if err != nil {
			return err
		}
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

	// Set default WorkerType to Driver if zero value (user application)
	if initOpts.WorkerType == 0 {
		initOpts.WorkerType = options.WorkerTypeDriver
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
	}

	// Note: For Driver mode, fetching node info from GCS is now done in plugin.Initialize()
	// after the GCS client factory is registered. This ensures the factory is available
	// when FetchNodeInfoFromGCS is called.

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

	// Reuse the handle obtained from LoadRuntimePlugin.
	// The handle was already created when loadPluginInternal called the plugin's
	// Initialize function. Calling Initialize again would cause double-initialization
	// error since base.Initialize is a singleton.
	handle := runtimePlugin.GetHandle()
	if handle == nil {
		return fmt.Errorf("LoadRuntimePlugin returned plugin with nil handle - Initialize may have failed")
	}

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
//   - shutdownComplete is set BEFORE calling handle.Runtime().Shutdown() to prevent
//     finalizers from accessing C++ objects that are being shutdown.
//   - This ensures that if a finalizer runs concurrently with shutdown,
//     it will see shutdownComplete=true and skip RemoveLocalReference().
//   - handle.Runtime().Shutdown() calls ShutdownAllocator() internally to stop
//     the Go object allocator finalizers.
func Shutdown() {
	handle, ok := tryGetHandle()
	// Always set shutdownComplete to prevent any stray finalizers from
	// accessing C++ objects during or after shutdown.
	shutdownComplete.Store(true)

	if ok && handle != nil && handle.Runtime() != nil {
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
func readRayAddressFromFile() string {
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

	jobIDHex, err := client.NextJobID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get next JobID from GCS: %w", err)
	}

	return jobIDHex, nil
}
