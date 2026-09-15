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

// Package base provides base type definitions and interfaces for Ray Go Runtime.
package base

import (
	"encoding/hex"
	"fmt"

	"github.com/ray-project/ray/go/internal/common"
	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/pkg/options"
)

// NetworkOptions contains network-related configuration options (internal use).
type NetworkOptions struct {
	NodeIPAddress   string // Node IP address
	NodeManagerPort int    // Node manager port
	GcsAddress      string // GCS service address
}

// JobOptions contains job-related configuration options (internal use).
type JobOptions struct {
	JobID     []byte // Job ID byte array
	ClusterID []byte // Cluster ID byte array
	JobConfig []byte // Serialized Job configuration
}

// fromAPI converts options.JobOptions to internal JobOptions.
// Returns an error if any hex decoding fails.
func (j *JobOptions) fromAPI(apiJob options.JobOptions) error {
	// Convert JobID from hex string to byte array
	if apiJob.JobID != "" {
		jobIDBytes, err := hex.DecodeString(apiJob.JobID)
		if err != nil {
			return fmt.Errorf("invalid JobID hex string: %w", err)
		}
		j.JobID = jobIDBytes
	}

	// Convert ClusterID from hex string to byte array
	if apiJob.ClusterID != "" {
		clusterIDBytes, err := hex.DecodeString(apiJob.ClusterID)
		if err != nil {
			return fmt.Errorf("invalid ClusterID hex string: %w", err)
		}
		j.ClusterID = clusterIDBytes
	}

	// Convert JobConfig from string to byte array
	if apiJob.JobConfig != "" {
		j.JobConfig = []byte(apiJob.JobConfig)
	}

	return nil
}

// toAPI converts internal JobOptions to options.JobOptions.
func (j JobOptions) toAPI() options.JobOptions {
	return options.JobOptions{
		JobID:     hex.EncodeToString(j.JobID),
		ClusterID: hex.EncodeToString(j.ClusterID),
		JobConfig: string(j.JobConfig),
	}
}

// RuntimeOptions contains runtime-related configuration options (internal use).
type RuntimeOptions struct {
	StoreSocket    string // Object store socket path
	RayletSocket   string // Raylet socket path
	LogDir         string // Log directory
	StartupToken   int    // Worker startup token
	RuntimeEnvHash int    // Runtime environment hash
	WorkerIDHex    string // Worker ID (hex) assigned by the raylet; empty for drivers
	EnableLogging  bool   // Initialize C++ logging if true (default: true)
}

// InitializeOptions represents initialization options (for internal use, strongly typed).
//
// Design notes:
// 1. This struct is only used internally within the base package; external interaction is through types.InitializeOptions.
// 2. Uses type-safe WorkerType from the types package.
// 3. Go-specific types (such as []byte) are used to provide better type safety.
// 4. Options are grouped by category for better organization.
type InitializeOptions struct {
	// WorkerType is the type of worker.
	WorkerType options.WorkerType
	// RunMode is the runtime mode (cluster or local).
	RunMode RunMode
	// Network contains network configuration.
	Network NetworkOptions
	// Job contains job configuration.
	Job JobOptions
	// Runtime contains runtime configuration.
	Runtime RuntimeOptions
}

// InitializeOptionsFromAPI converts options.InitializeOptions to internal InitializeOptions.
// Returns an error if any hex decoding fails or if validation fails.
func InitializeOptionsFromAPI(opts options.InitializeOptions) (InitializeOptions, error) {
	result := InitializeOptions{
		WorkerType: opts.WorkerType,
		// Default to cluster mode, can be overridden by WorkerTypeLocal
		RunMode: RunModeCluster,
		Network: NetworkOptions{
			NodeIPAddress:   opts.Network.NodeIPAddress,
			NodeManagerPort: int(opts.Network.NodeManagerPort),
			GcsAddress:      opts.Network.GcsAddress,
		},
		Runtime: RuntimeOptions{
			StoreSocket:    opts.Runtime.StoreSocket,
			RayletSocket:   opts.Runtime.RayletSocket,
			LogDir:         opts.Runtime.LogDir,
			StartupToken:   int(opts.Runtime.StartupToken),
			RuntimeEnvHash: int(opts.Runtime.RuntimeEnvHash),
			WorkerIDHex:    opts.Runtime.WorkerIDHex,
			EnableLogging:  true, // Default to true to enable C++ logging
		},
	}

	// If WorkerType is Local, use local mode
	if opts.WorkerType == options.WorkerTypeLocal {
		result.RunMode = RunModeLocal
	}

	// Convert JobOptions using the encapsulated method
	if err := result.Job.fromAPI(opts.Job); err != nil {
		return result, err
	}

	// Validate initialization parameters
	if err := validateInitializeOptions(result); err != nil {
		return result, err
	}

	return result, nil
}

// validateInitializeOptions validates the initialization options.
func validateInitializeOptions(opts InitializeOptions) error {
	// Validate WorkerType
	if opts.WorkerType != options.WorkerTypeDriver &&
		opts.WorkerType != options.WorkerTypeWorker &&
		opts.WorkerType != options.WorkerTypeLocal {
		return rayerrors.NewInvalidArgumentError("WorkerType", fmt.Sprintf("invalid value: %d", opts.WorkerType))
	}

	// Validate NodeIPAddress
	if opts.Network.NodeIPAddress == "" {
		return rayerrors.NewInvalidArgumentError("NodeIPAddress", "cannot be empty")
	}
	if !common.ValidateIPAddress(opts.Network.NodeIPAddress) {
		return rayerrors.NewInvalidArgumentError("NodeIPAddress", fmt.Sprintf("invalid format: %s", opts.Network.NodeIPAddress))
	}

	// Validate NodeManagerPort (0 means random port)
	// For Driver mode, port can be fetched from GCS, so allow 0
	if opts.WorkerType == options.WorkerTypeWorker && !common.ValidatePortAllowZero(opts.Network.NodeManagerPort) {
		return rayerrors.NewInvalidArgumentError("NodeManagerPort", fmt.Sprintf("invalid value: %d", opts.Network.NodeManagerPort))
	}

	// Validate StoreSocket and RayletSocket ONLY for Worker mode
	// In Driver mode, these can be fetched from GCS (mimics Java Ray runtime).
	// If GCS fetch fails or returns empty, C++ core worker can handle the values.
	if opts.WorkerType == options.WorkerTypeWorker {
		if !common.ValidateSocketPath(opts.Runtime.StoreSocket) {
			return rayerrors.NewInvalidArgumentError("StoreSocket", "cannot be empty")
		}
		if !common.ValidateSocketPath(opts.Runtime.RayletSocket) {
			return rayerrors.NewInvalidArgumentError("RayletSocket", "cannot be empty")
		}
	}

	// Validate GcsAddress - required for cluster mode (Driver and Worker)
	if opts.Network.GcsAddress == "" && opts.WorkerType != options.WorkerTypeLocal {
		return rayerrors.NewInvalidArgumentError("GcsAddress", "cannot be empty")
	}

	// Validate LogDir - allow empty for Driver mode (will be auto-populated)
	if opts.Runtime.LogDir == "" && opts.WorkerType == options.WorkerTypeWorker {
		return rayerrors.NewInvalidArgumentError("LogDir", "cannot be empty")
	}

	return nil
}

// ToAPIOptions converts internal InitializeOptions to options.InitializeOptions.
func (o InitializeOptions) ToAPIOptions() options.InitializeOptions {
	return options.InitializeOptions{
		WorkerType: o.WorkerType,
		Network: options.NetworkOptions{
			NodeIPAddress:   o.Network.NodeIPAddress,
			NodeManagerPort: int32(o.Network.NodeManagerPort),
			GcsAddress:      o.Network.GcsAddress,
		},
		Job: o.Job.toAPI(),
		Runtime: options.RuntimeOptions{
			StoreSocket:    o.Runtime.StoreSocket,
			RayletSocket:   o.Runtime.RayletSocket,
			LogDir:         o.Runtime.LogDir,
			StartupToken:   int32(o.Runtime.StartupToken),
			RuntimeEnvHash: int32(o.Runtime.RuntimeEnvHash),
			WorkerIDHex:    o.Runtime.WorkerIDHex,
		},
	}
}
