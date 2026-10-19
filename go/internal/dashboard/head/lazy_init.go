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

package head

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
)

// initOnce serializes concurrent lazy initializations. Without it, two
// goroutines that both observe IsInitialized() == false would initialize
// concurrently and the loser would panic in setHandle ("runtime already
// initialized").
var initOnce sync.Mutex

// attemptCount counts every lazy runtime init attempt since process start
// (successes and failures), feeding the /api/(ray/)version runtime object. It
// is incremented on the same code path as the middleware's Prometheus counter,
// so the two observations stay consistent without sharing state.
var attemptCount int32

// errNoNodeInfo is the sentinel wrapped (via %w) when the driver handshake
// could not fill node connection info (GetNodeToConnect failed or returned no
// node), so callers/tests can distinguish "node table empty" from other init
// failures.
var errNoNodeInfo = errors.New("no node info returned from GCS")

// initWithConfigFn is the actual runtime initializer. It is a package-level
// variable so tests can inject a fake without linking go_runtime.so.
var initWithConfigFn = initWithConfig

// loadGCSClientFn wraps plugin.LoadGCSClient so tests can inject a fake
// without loading go_runtime.so.
var loadGCSClientFn = plugin.LoadGCSClient

// initRuntimeWithOptionsFn wraps api.InitWithOptions so tests can inject a
// fake without loading go_runtime.so.
var initRuntimeWithOptionsFn = func(opts *options.InitializeOptions) error {
	return api.InitWithOptions(opts)
}

// preflightConnectFn opens a TCP connection to a node_manager_port. It is a
// package-level variable so tests can inject a fake. The preflight must happen
// BEFORE base.Initialize: base.Initialize dials the raylet via
// GenerateRayletAddress and any dial failure (UNAVAILABLE) reaches
// CoreWorkerProcess::QuickExit, which _Exit(1)s the whole process. Go cannot
// intercept QuickExit (it happens inside C++ at the process level), so the
// dashboard would die mid-request instead of degrading. TCP-level reachability
// is a deterministic proxy for "the raylet's grpc server is up": the raylet
// starts its node_manager grpc server (NodeManagerService) before it registers
// with GCS, so a GCS ALIVE node whose port is not yet listening is a stale
// recovered node (see the GcsNodeManager::Initialize restore path).
var preflightConnectFn = func(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	return conn.Close()
}

// preflightNodeManager checks that the node_manager_port returned by
// GetNodeToConnectForDriver actually accepts TCP connections before
// base.Initialize dials it. Returns nil (no-op) when the port or address is
// unset, mirroring the caller's "skip preflight" intent. See preflightConnectFn
// for why this must fail closed rather than letting base.Initialize proceed.
func preflightNodeManager(nodeIP string, nodeManagerPort int32) error {
	if nodeManagerPort == 0 || nodeIP == "" {
		return nil
	}
	addr := net.JoinHostPort(nodeIP, strconv.Itoa(int(nodeManagerPort)))
	if err := preflightConnectFn(addr, 500*time.Millisecond); err != nil {
		log.Log.Error(err, "node_manager_port unreachable; treating GCS node as stale and degrading",
			"node_manager_address", nodeIP,
			"node_manager_port", nodeManagerPort,
		)
		return fmt.Errorf("node_manager_port unreachable: %s: %w", addr, err)
	}
	log.Log.Info("node_manager_port reachable", "node_manager_address", addr)
	return nil
}

// initRetryBackoff is the cool-down after a failed lazy init. Each attempt
// constructs the C++ CoreWorker, which exits the whole process when the local
// raylet is unreachable, so retries are rate-limited instead of running per
// request. A package-level variable so tests can shorten it.
var initRetryBackoff = time.Minute

// lastInitErr/lastInitFailAt record the most recent lazy-init failure. Guarded
// by initOnce.
var (
	lastInitErr    error
	lastInitFailAt time.Time
)

// EnsureInitialized lazily initializes the Go runtime as a driver, aligned
// with the Python dashboard's require_initialized (optional_utils.py), which
// calls ray.init() on first use when ray.is_initialized() is false. The
// dashboard head calls this from each cross-language module (serve/flow/train/
// data) so a runtime that failed to initialize at startup (e.g. because the
// GCS was not ready during `ray start`) recovers on the next request instead
// of requiring a head restart.
//
// It is a no-op when the runtime is already initialized. Double-checked
// locking (initOnce + IsInitialized) makes concurrent calls safe.
//
// A failed attempt enters a cool-down: until initRetryBackoff elapses,
// subsequent calls return the cached error without re-entering the
// initializer. This matters because every attempt constructs the C++
// CoreWorker, and a CoreWorker that cannot reach the local raylet exits the
// whole process from the C++ side — an unrecoverable per-request crash. After
// the cool-down the retry is allowed again, preserving the recover-on-next-
// request design for a GCS or raylet that comes back.
func EnsureInitialized(cfg *HeadConfig) error {
	if api.IsInitialized() {
		return nil
	}
	initOnce.Lock()
	defer initOnce.Unlock()
	if api.IsInitialized() {
		return nil
	}
	if lastInitErr != nil && time.Since(lastInitFailAt) < initRetryBackoff {
		return lastInitErr
	}
	// Count the attempt on the same code path as the middleware's Prometheus
	// counter so /api/(ray/)version and /metrics stay consistent. A backoff-
	// cached failure above returns without incrementing, so the middleware's
	// initAttempts() > before snapshot records no outcome for it.
	atomic.AddInt32(&attemptCount, 1)
	if err := initWithConfigFn(cfg); err != nil {
		lastInitErr = err
		lastInitFailAt = time.Now()
		return err
	}
	lastInitErr = nil
	log.Log.Info("go runtime initialized lazily for dashboard head", "gcs", cfg.GCSAddress, "node", cfg.NodeIPAddress)
	return nil
}

// initAttempts returns the number of lazy init attempts since process start,
// feeding the /api/(ray/)version runtime object. Zero means the head's runtime
// was already initialized (or never needed lazy init), so no attempt was made.
func initAttempts() int {
	return int(atomic.LoadInt32(&attemptCount))
}

// RuntimeStatus is the additive /api/(ray/)version field exposing the
// dashboard head's own runtime init state for oncall. Python has no such
// field; consumers reading the original version keys are unaffected.
type RuntimeStatus struct {
	// Initialized reports whether the Go runtime is up right now.
	Initialized bool `json:"initialized"`
	// Attempts counts lazy init attempts tried since process start.
	Attempts int `json:"attempts"`
}

// RuntimeStatusInfo returns the dashboard head's current runtime init state.
func RuntimeStatusInfo() RuntimeStatus {
	return RuntimeStatus{Initialized: api.IsInitialized(), Attempts: initAttempts()}
}

// initWithConfig initializes the Go runtime as a driver through the api plugin
// path (api.InitWithOptions), so the dashboard head can call Python actors
// across languages (e.g. GetPythonActorWithNamespace in the flow/serve/train/
// data modules). Mirrors the Python dashboard, which connects as a driver in
// the internal dashboard namespace.
//
// The dashboard head runs inside the monolithic raygo binary, which links the
// go_runtime.so plugin (loaded by go/cmd/raygo/raygo.go). The handshake client
// is obtained through the plugin's export surface (plugin.LoadGCSClient) so the
// head stays aligned with the worker/agent plugin path instead of statically
// linking the C++ gcs_client_bridge.
//
// The code_search_path makes cross-language Python workers load actor classes
// from local code instead of GCS. Go cannot export a pickled Python actor class
// to the GCS function table (only a Python driver can export_actor_class), so
// without this the worker's _load_actor_class_from_gcs fails with
// "class_name None" when Go creates a Python wrapper actor. Point it at the ray
// Python source tree (or a wheel install) so the wrapper modules import
// locally. The paths are passed through verbatim, matching the Python
// dashboard's RAY_DASHBOARD_CODE_SEARCH_PATH handling (no absolutization).
func initWithConfig(cfg *HeadConfig) error {
	initOpts := options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network: options.NetworkOptions{
			GcsAddress:    cfg.GCSAddress,
			NodeIPAddress: cfg.NodeIPAddress,
		},
		Job: options.JobOptions{
			ClusterID: cfg.ClusterIDHex,
		},
	}

	// Driver-mode handshake: fetch node connection info (store/raylet sockets,
	// node manager port) and a fresh JobID from GCS, mirroring the Python
	// dashboard's ray.init(). The handshake client is obtained from go_runtime.so
	// via the plugin export surface (plugin.LoadGCSClient), which reflects into
	// the plugin's exported ConnectClient and needs no api-level factory
	// registration (that only happens inside the plugin's Initialize). It is
	// used only for the handshake and closed afterwards: the dashboard's
	// GCS-based modules query GCS through the pure-Go gRPC stub
	// (head.NewGCSClient), never api.GetGCSClient, so nothing here needs to
	// register the client in api's cache.
	gcsOpts := gcs.ClientOptions{
		Address:   cfg.GCSAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000,
	}
	handshakeClient, err := loadGCSClientFn(gcsOpts)
	if err != nil {
		return err
	}
	// The one-shot handshake client must not outlive the handshake: the plugin's
	// Initialize creates the long-lived client during InitWithOptions, and a
	// deferred close that runs at function end would clear the pkg/gcs global
	// singletons that client seeded. Close it explicitly right after the
	// handshake on the success path, and keep the close on every error path
	// (DriverHandshake failure, fail-closed, preflight failure) via the
	// handshakeOK flag. Nothing between the handshake return and function end
	// reads handshakeClient, so closing it here is safe.
	handshakeOK := false
	defer func() {
		if !handshakeOK {
			handshakeClient.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shared driver handshake: fill node connection info and allocate a fresh
	// JobID with the same rules as the plugin's Initialize
	// (plugin.DriverHandshake), so the later api.InitWithOptions handshake is a
	// no-op.
	if err := plugin.DriverHandshake(ctx, handshakeClient, &initOpts); err != nil {
		return err
	}
	handshakeClient.Close()
	handshakeOK = true

	// Fail closed: DriverHandshake tolerates a node-info fetch failure (it
	// logs and continues so a plain driver can surface the C++ connectivity
	// error), but the dashboard must not hand empty sockets / port 0 to
	// base.Initialize -- an empty raylet socket is RAY_LOG(FATAL) and a port-0
	// address hits CoreWorkerProcess::QuickExit, killing the whole dashboard.
	// Re-check the fields the handshake was supposed to fill and degrade
	// (return an error so the module handler surfaces a 500) when they are
	// still missing, so the next request retries.
	if initOpts.Runtime.StoreSocket == "" || initOpts.Runtime.RayletSocket == "" ||
		initOpts.Network.NodeManagerPort == 0 {
		return fmt.Errorf("%w for node %s (gcs %s)", errNoNodeInfo, cfg.NodeIPAddress, cfg.GCSAddress)
	}

	// Preflight the raylet's node_manager_port before base.Initialize.
	// GetNodeToConnectForDriver returns the first ALIVE node from GCS without
	// validating its port. After a GCS restart, GcsNodeManager::Initialize
	// restores the previous head's ALIVE node with a stale node_manager_port;
	// base.Initialize would then dial the dead port and hit
	// CoreWorkerProcess::QuickExit, killing the whole dashboard. A TCP connect
	// is a deterministic proxy: the raylet runs its node_manager grpc server
	// before RegisterGcs(), so an ALIVE node whose port refuses connections is
	// stale. On staleness we degrade (return an error so the module handler
	// surfaces a 500 and the process stays alive) instead of dialing a dead
	// port and dying.
	if err := preflightNodeManager(initOpts.Network.NodeIPAddress, initOpts.Network.NodeManagerPort); err != nil {
		return err
	}

	// Reuse the node's cached metrics agent port (ports_by_node.json) so the
	// driver's metrics exporter can reach the dashboard agent (mirroring the
	// Python dashboard head, node.py _get_cached_port). Without it the exporter
	// retries against a disabled port and logs "Failed to establish connection
	// to the metrics exporter agent" (rpc 14). The plugin's Initialize handshake
	// does NOT fill this field.
	if initOpts.Runtime.StoreSocket != "" {
		port, portErr := readMetricsAgentPort(cfg.SessionDir, initOpts.Network.NodeIPAddress, initOpts.Runtime.StoreSocket)
		if portErr != nil {
			log.Log.Error(portErr, "failed to read metrics agent port from ports_by_node.json")
		} else if port > 0 {
			initOpts.Runtime.MetricsAgentPort = int32(port)
			log.Log.Info("reusing node metrics agent port for dashboard head", "metrics_agent_port", port)
		}
	}

	// Build the JobConfig with the internal dashboard namespace and code search
	// path (unchanged). api.InitWithOptions never overwrites a non-empty
	// JobConfig (spec Key Fact 9).
	jobOpts, err := options.NewJobConfigBuilder().
		WithNamespace("_ray_internal_dashboard").
		WithCodeSearchPath(cfg.CodeSearchPath...).
		BuildToJobOptions()
	if err != nil {
		return err
	}
	initOpts.Job.JobConfig = jobOpts.JobConfig

	// The dashboard already configured its own logger (dashboard.go setupLogging
	// runs before initGoRuntime); skip the inline driver-logger setup inside
	// api.InitWithOptions so it is not overwritten mid-startup.
	initOpts.SkipDriverLogSetup = true

	// Initialize through the api plugin path. Because the handshake above filled
	// every field, the plugin's Initialize handshake is a no-op (its fill
	// conditions are == "" checks) and it allocates/reuses its own cached GCS
	// client through api.WithCachedClient for the same GCS address.
	return initRuntimeWithOptionsFn(&initOpts)
}

// ShutdownRuntime tears down the dashboard head's Go runtime. The head now
// initializes through api.InitWithOptions, so api.Shutdown drives the plugin's
// full teardown. It is a no-op when the runtime was never initialized
// (tryGetHandle/currentRuntimePlugin are nil and api.Shutdown is idempotent).
func ShutdownRuntime() {
	api.Shutdown()
}
