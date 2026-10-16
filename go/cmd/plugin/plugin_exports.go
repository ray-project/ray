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
package main

import (
	"context"
	"time"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/proctitle"
	"github.com/ray-project/ray/go/internal/runtime/base"
	_ "github.com/ray-project/ray/go/internal/runtime/native" // Register Runtime factory (init function)
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
)

// Initialize is exported for plugin users to call.
// Initializes CoreWorker and returns a handle.
// This function delegates to base.Initialize().
func Initialize(opts options.InitializeOptions) (base.RuntimeHandle, error) {
	// Register GCS client factory for dependency inversion.
	// The concrete implementation lives in go/internal/gcs/native (shared with
	// the static worker / dashboard-head initialization paths), so the api
	// package never imports it directly.
	// This allows api.FetchNodeInfoFromGCS() to use GCS without directly
	// importing go/internal/gcs/native.
	native.RegisterGCSClientFactory()

	// Capture the original argv area before base.Initialize consumes the
	// command line (its C++ side reads argv). This keeps the cgo proctitle
	// package on the plugin side so the thin raygo binary keeps no cgo
	// dependency: a shared cgo package between raygo and go_runtime.so would
	// fail the plugin version check (the two are built under different
	// linkmodes). Fail-soft: on capture failure the process title still
	// updates /proc/self/comm via prctl.
	proctitle.Init()

	// For Driver mode, fetch node connection info from GCS if needed.
	// This mimics Java's GcsClient.getNodeToConnectForDriver().
	// Note: This must be called AFTER native.RegisterGCSClientFactory() and BEFORE base.Initialize().
	log.Log.Info("initializing worker",
		"workerType", opts.WorkerType,
		"gcsAddress", opts.Network.GcsAddress,
		"nodeIPAddress", opts.Network.NodeIPAddress,
		"jobID", opts.Job.JobID)

	if opts.WorkerType == options.WorkerTypeDriver {
		// Use cached GCS client to fetch both node info and JobID in a single client lifecycle.
		gcsOpts := gcs.ClientOptions{
			Address:   opts.Network.GcsAddress,
			ClusterID: ids.NilClusterID(),
			TimeoutMs: 10000,
		}

		err := api.WithCachedClient(opts.Network.GcsAddress, gcsOpts, func(client api.GCSClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Fill missing node connection info (store/raylet sockets, node
			// manager port, node IP) and allocate a fresh JobID through the
			// shared driver handshake, so the fill rules stay identical to the
			// dashboard head's pre-handshake (plugin.DriverHandshake).
			return plugin.DriverHandshake(ctx, client, &opts)
		})
		if err != nil {
			return nil, err
		}
	}

	log.Log.Info("calling base.Initialize", "gcsAddress", opts.Network.GcsAddress, "jobID", opts.Job.JobID)
	handle, err := base.Initialize(opts)
	if err != nil {
		log.Log.Error(err, "base.Initialize failed")
		return nil, err
	}
	if handle == nil {
		log.Log.Info("base.Initialize returned nil handle")
	} else {
		log.Log.Info("base.Initialize succeeded", "handle", handle)
	}

	// Now that base.Initialize() has consumed the command line, set the worker
	// process title to "ray::IDLE" (matching Python setproctitle in connect()
	// and C++ SetProcTitle after ray::Init). Doing it here also overwrites the
	// captured argv area, scrubbing sensitive args (e.g. --redis-password).
	// Only worker-mode processes get the IDLE title; drivers keep their own.
	if opts.WorkerType == options.WorkerTypeWorker {
		proctitle.SetTitle(proctitle.IdleTitle())
	}
	return handle, err
}

// Shutdown is exported for plugin users to call.
// Shuts down CoreWorker and cleans up resources.
// This function delegates to base.Shutdown().
func Shutdown(handle base.RuntimeHandle) error {
	api.ClearAllCachedClients()
	return base.Shutdown(handle)
}

// RunTaskExecutionLoop is exported for plugin users to call.
// Runs the task execution loop (blocking).
//
// Thread safety: Validates handle is registered before execution.
func RunTaskExecutionLoop(handle base.RuntimeHandle) error {
	// Validate handle using encapsulated function
	if err := base.ValidateHandle(handle); err != nil {
		return err
	}

	// Get runtime from handle and call Run.
	rt := handle.Runtime()
	if rt == nil {
		return rayerrors.ErrInvalidHandle
	}

	// Secondary check: verify runtime is still initialized
	// This is defense-in-depth to prevent race conditions where Shutdown is called concurrently
	if !rt.IsInitialized() {
		return rayerrors.ErrInvalidHandle
	}

	return rt.Run()
}

// ConnectClient forwards to the cgo GCS client constructor. The thin layer
// obtains gcs.Client values across the plugin boundary via plugin.Lookup.
func ConnectClient(opts gcs.ClientOptions) (gcs.Client, error) {
	return native.ConnectClient(opts)
}

// NewLogBatchPublisher forwards to the cgo log batch publisher constructor,
// returning the pkg-level interface so the thin layer never imports internal.
func NewLogBatchPublisher(client gcs.Client) (gcs.LogBatchPublisher, error) {
	return native.NewLogBatchPublisher(client)
}

// main function is required (plugin mode requirement) but will not be called.
func main() {}
