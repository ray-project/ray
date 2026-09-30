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
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// initOnce serializes concurrent lazy initializations. Without it, two
// goroutines that both observe IsInitialized() == false would initialize
// concurrently and the loser would panic in setHandle ("runtime already
// initialized").
var initOnce sync.Mutex

// initWithConfigFn is the actual runtime initializer. It is a package-level
// variable so tests can inject a fake without linking the C++ gcs_client_bridge
// (base.Initialize would attempt a real driver-mode GCS handshake).
var initWithConfigFn = initWithConfig

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
	if err := initWithConfigFn(cfg); err != nil {
		lastInitErr = err
		lastInitFailAt = time.Now()
		return err
	}
	lastInitErr = nil
	log.Log.Info("go runtime initialized lazily for dashboard head", "gcs", cfg.GCSAddress, "node", cfg.NodeIPAddress)
	return nil
}

// initWithConfig initializes the Go runtime as a driver through the static
// base.Initialize path, so the dashboard head can call Python actors across
// languages (e.g. GetPythonActorWithNamespace in the flow/serve/train/data
// modules). Mirrors the Python dashboard, which connects as a driver in the
// internal dashboard namespace.
//
// The dashboard head runs inside the monolithic raygo binary, which already
// statically links the C++ GCS client bridge (via default_worker). Loading the
// go_runtime.so plugin on top would trigger the "grpc_experiments linked both
// statically and dynamically" conflict, so the head initializes exactly like a
// worker process (internal/worker): register the GCS client factory, fetch
// driver node info and JobID from GCS, then base.Initialize + SetRuntime
// HandleForWorker. The static NativeRuntime's task submitter supports
// cross-language calls (CreateActor passes LanguagePython), so the
// serve/flow/train/data modules keep working.
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
	native.RegisterGCSClientFactory()

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

	// Driver mode: fetch node connection info (store/raylet sockets, node
	// manager port) and a fresh JobID from GCS, mirroring internal/worker.Run
	// and the Python dashboard's ray.init(). Only GcsAddress and NodeIPAddress
	// are required; the rest is auto-filled from GCS.
	gcsOpts := gcs.ClientOptions{
		Address:   cfg.GCSAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000,
	}
	if err := api.WithCachedClient(cfg.GCSAddress, gcsOpts, func(client api.GCSClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		nodeInfo, err := client.GetNodeToConnect(ctx, initOpts.Network.NodeIPAddress)
		if err != nil {
			log.Log.Error(err, "failed to fetch node info from GCS")
		} else if nodeInfo != nil {
			if nodeInfo.ObjectStoreSocketName != "" {
				initOpts.Runtime.StoreSocket = nodeInfo.ObjectStoreSocketName
			}
			if nodeInfo.RayletSocketName != "" {
				initOpts.Runtime.RayletSocket = nodeInfo.RayletSocketName
			}
			if nodeInfo.NodeManagerPort != 0 {
				initOpts.Network.NodeManagerPort = int32(nodeInfo.NodeManagerPort)
			}
			if nodeInfo.NodeManagerAddress != "" {
				initOpts.Network.NodeIPAddress = nodeInfo.NodeManagerAddress
			}
			log.Log.Info("fetched node info from GCS for dashboard head",
				"store_socket", initOpts.Runtime.StoreSocket,
				"raylet_socket", initOpts.Runtime.RayletSocket,
				"node_manager_port", initOpts.Network.NodeManagerPort,
			)
		}

		jobIDHex, err := client.NextJobID(ctx)
		if err != nil {
			return err
		}
		initOpts.Job.JobID = jobIDHex
		log.Log.Info("fetched next JobID from GCS for dashboard head", "job_id", jobIDHex)
		return nil
	}); err != nil {
		return err
	}

	// Build the JobConfig with the internal dashboard namespace and code search
	// path (mirroring the previous plugin path's JobConfigBuilder usage).
	jobOpts, err := options.NewJobConfigBuilder().
		WithNamespace("_ray_internal_dashboard").
		WithCodeSearchPath(cfg.CodeSearchPath...).
		BuildToJobOptions()
	if err != nil {
		return err
	}
	initOpts.Job.JobConfig = jobOpts.JobConfig

	// Initialize the runtime and attach it to the api package so the
	// cross-language modules' submitter calls work (mirrors worker.Run).
	handle, err := base.Initialize(initOpts)
	if err != nil {
		return err
	}
	api.SetRuntimeHandleForWorker(handle)
	return nil
}

// ShutdownRuntime tears down the dashboard head's statically initialized Go
// runtime. It mirrors worker.Worker.Shutdown and must be used instead of
// api.Shutdown (which would attempt a plugin teardown the static path never
// performed). No-op when the runtime was never initialized.
func ShutdownRuntime() {
	if h := base.GetHandle(); h != nil {
		_ = base.Shutdown(h)
	}
}
