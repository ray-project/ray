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
	"fmt"
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
	"github.com/ray-project/ray/go/proto"
)

// ============================================================================
// GCS Client Factory Implementation (for Dependency Inversion)
// ============================================================================

// gcsClientFactory implements api.GCSClientFactory by delegating to
// go/internal/gcs/native.ConnectClient().
//
// This allows go_runtime.so to provide the concrete GCS client implementation
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
// This allows the cache to check if a cached client is still usable.
func (a *gcsClientAdapter) IsClosed() bool {
	// Delegate to the underlying client if it supports IsClosed()
	if checker, ok := a.client.(interface{ IsClosed() bool }); ok {
		return checker.IsClosed()
	}
	// Fallback: assume not closed if the underlying client doesn't support IsClosed()
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

// registerGCSClientFactory registers the GCS client factory during initialization.
// This is called by Initialize() to set up the dependency injection.
func registerGCSClientFactory() {
	api.RegisterGCSClientFactory(&gcsClientFactory{})
}

// Initialize is exported for plugin users to call.
// Initializes CoreWorker and returns a handle.
// This function delegates to base.Initialize().
func Initialize(opts options.InitializeOptions) (base.RuntimeHandle, error) {
	// Register GCS client factory for dependency inversion.
	// This allows api.FetchNodeInfoFromGCS() to use GCS without directly
	// importing go/internal/gcs/native.
	registerGCSClientFactory()

	// For Driver mode, fetch node connection info from GCS if needed.
	// This mimics Java's GcsClient.getNodeToConnectForDriver().
	// Note: This must be called AFTER registerGCSClientFactory() and BEFORE base.Initialize().
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

			// Fetch node info from GCS for missing socket/port fields
			// Always fetch from GCS to ensure we use the same IP that the Raylet registered with
			if opts.Runtime.StoreSocket == "" || opts.Runtime.RayletSocket == "" || opts.Network.NodeManagerPort == 0 || opts.Network.NodeIPAddress == "" {
				nodeInfo, err := client.GetNodeToConnect(ctx, opts.Network.NodeIPAddress)
				if err != nil {
					log.Log.Error(err, "failed to fetch node info from GCS")
					// Log warning but continue - let C++ core worker handle missing fields
					// The validation in base.InitializeOptionsFromAPI has been relaxed for Driver mode
				} else if nodeInfo != nil {
					// Populate missing fields from GCS node info
					if opts.Runtime.StoreSocket == "" && nodeInfo.ObjectStoreSocketName != "" {
						opts.Runtime.StoreSocket = nodeInfo.ObjectStoreSocketName
					}
					if opts.Runtime.RayletSocket == "" && nodeInfo.RayletSocketName != "" {
						opts.Runtime.RayletSocket = nodeInfo.RayletSocketName
					}
					if opts.Network.NodeManagerPort == 0 && nodeInfo.NodeManagerPort != 0 {
						opts.Network.NodeManagerPort = int32(nodeInfo.NodeManagerPort)
					}
					// Use NodeManagerAddress from GCS if available
					// This ensures we use the same IP that the Raylet actually registered with
					// Java does NOT do this - it always uses its own detected nodeIp
					// But we need this because our auto-detection may differ from Python's
					if nodeInfo.NodeManagerAddress != "" {
						opts.Network.NodeIPAddress = nodeInfo.NodeManagerAddress
					}
				}
			}

			// Fetch next JobID from GCS if not provided.
			// This mimics Java's GcsClient.nextJobId() which allocates a new JobID for the driver.
			// Note: JobID must be set before calling base.Initialize(), which passes it to C++.
			if opts.Job.JobID == "" {
				log.Log.Info("fetching next JobID from GCS", "gcsAddress", opts.Network.GcsAddress)
				jobIDHex, err := client.NextJobID(ctx)
				if err != nil {
					log.Log.Error(err, "failed to fetch next JobID from GCS")
					return fmt.Errorf("failed to get next JobID from GCS: %w", err)
				}
				log.Log.Info("fetched next JobID from GCS", "jobID", jobIDHex)
				opts.Job.JobID = jobIDHex
			} else {
				log.Log.Info("JobID already set", "jobID", opts.Job.JobID)
			}

			return nil
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

// main function is required (plugin mode requirement) but will not be called.
func main() {}
