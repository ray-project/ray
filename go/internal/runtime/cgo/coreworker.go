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

// Package cgo provides CGO bindings for Ray runtime.
package cgo

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "src/ray/core_worker/lib/go/native_runtime.h"
#include "src/ray/core_worker/lib/go/native_object_store.h"
*/
import "C"

import (
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/ray-project/ray/go/internal/runtime/base"
)

// Handle represents the C++ NativeRuntime handle.
//
// Thread safety design:
// 1. The ptr field is protected by mu.
// 2. Shutdown and RunTaskExecutionLoop use different lock modes.
type Handle struct {
	ptr *C.CNativeRuntime
	mu  sync.RWMutex
}

// toCNativeRuntimeInitializeOptions converts Go InitializeOptions to C struct.
// The caller is responsible for calling the returned cleanup function to free C strings.
func toCNativeRuntimeInitializeOptions(opts base.InitializeOptions) (*C.CNativeRuntimeInitializeOptions, func()) {
	var frees []func()

	// Convert strings to C strings using helper function.
	nodeIP, freeNodeIP := ToCString(opts.Network.NodeIPAddress)
	frees = append(frees, freeNodeIP)

	driverName, freeDriverName := ToCString("go_driver")
	frees = append(frees, freeDriverName)

	storeSocket, freeStoreSocket := ToCString(opts.Runtime.StoreSocket)
	frees = append(frees, freeStoreSocket)

	rayletSocket, freeRayletSocket := ToCString(opts.Runtime.RayletSocket)
	frees = append(frees, freeRayletSocket)

	// Convert JobID to hex string with length validation.
	// For Worker mode, JobID is empty and will be read from RAY_JOB_ID environment variable by C++.
	// We pass "ffffffff" (Nil JobID) to satisfy C++ FromHex() which expects 8 hex characters.
	var jobIDHex string
	if len(opts.Job.JobID) > 0 {
		// Validate JobID length (should be 4 bytes).
		if len(opts.Job.JobID) != 4 {
			// Return early with cleanup
			return nil, func() {
				for _, f := range frees {
					f()
				}
			}
		}
		jobIDHex = hex.EncodeToString(opts.Job.JobID)
	} else {
		// For Worker mode, pass Nil JobID (ffffffff) to C++.
		// C++ will read the actual JobID from RAY_JOB_ID environment variable.
		jobIDHex = "ffffffff"
	}
	cJobIDHex, freeJobIDHex := ToCString(jobIDHex)
	frees = append(frees, freeJobIDHex)

	gcsAddress, freeGcsAddress := ToCString(opts.Network.GcsAddress)
	frees = append(frees, freeGcsAddress)

	logDir, freeLogDir := ToCString(opts.Runtime.LogDir)
	frees = append(frees, freeLogDir)

	// Convert JobConfig to string.
	var jobConfigStr string
	if len(opts.Job.JobConfig) > 0 {
		jobConfigStr = string(opts.Job.JobConfig)
	}
	cJobConfig, freeJobConfig := ToCString(jobConfigStr)
	frees = append(frees, freeJobConfig)

	// Convert ClusterID to hex string.
	var clusterIDHex string
	if len(opts.Job.ClusterID) > 0 {
		clusterIDHex = hex.EncodeToString(opts.Job.ClusterID)
	}
	cClusterID, freeClusterID := ToCString(clusterIDHex)
	frees = append(frees, freeClusterID)

	// Worker ID (hex) assigned by the raylet (worker mode only).
	cWorkerID, freeWorkerID := ToCString(opts.Runtime.WorkerIDHex)
	frees = append(frees, freeWorkerID)

	// Build C struct.
	cOpts := &C.CNativeRuntimeInitializeOptions{
		worker_mode:           C.int(opts.WorkerType),
		node_ip_address:       nodeIP,
		node_manager_port:     C.int(opts.Network.NodeManagerPort),
		driver_name:           driverName,
		store_socket:          storeSocket,
		raylet_socket:         rayletSocket,
		job_id_hex:            cJobIDHex,
		gcs_address:           gcsAddress,
		cluster_id_hex:        cClusterID,
		log_dir:               logDir,
		job_config_serialized: cJobConfig,
		worker_id_hex:         cWorkerID,
		startup_token:         C.int(opts.Runtime.StartupToken),
		runtime_env_hash:      C.int(opts.Runtime.RuntimeEnvHash),
		enable_logging:        C.bool(true),
	}

	// Return cleanup function that frees all C strings.
	return cOpts, func() {
		for _, f := range frees {
			f()
		}
	}
}

// Initialize initializes the C++ NativeRuntime and returns the handle.
//
// Parameter conversion notes:
// 1. String parameters are converted to C strings and freed after call.
// 2. JobID is converted to hex string (consistent with C++ interface).
// 3. JobConfig is converted to string.
// 4. ClusterID is converted to hex string and passed to C++.
// Initialize initializes the C++ NativeRuntime and returns the handle.
//
// Parameter conversion notes:
// 1. String parameters are converted to C strings and freed after call.
// 2. JobID is converted to hex string (consistent with C++ interface).
// 3. JobConfig is converted to string.
// 4. ClusterID is converted to hex string and passed to C++.
func Initialize(opts base.InitializeOptions) (*Handle, error) {
	logger.Info("cgoboundary.Initialize() called", "workerType", opts.WorkerType, "nodeIP", opts.Network.NodeIPAddress, "gcsAddress", opts.Network.GcsAddress, "jobID", hex.EncodeToString(opts.Job.JobID), "clusterID", hex.EncodeToString(opts.Job.ClusterID), "jobConfig", string(opts.Job.JobConfig))

	// Debug: Print RAY_JOB_ID environment variable value before calling C++ initialization

	// Convert options to C struct using helper method.
	cOpts, free := toCNativeRuntimeInitializeOptions(opts)
	defer free()

	// Validate JobID length (already done in toCNativeRuntimeInitializeOptions, but double-check).
	if len(opts.Job.JobID) > 0 && len(opts.Job.JobID) != 4 {
		return nil, fmt.Errorf("invalid JobID length: expected 4 bytes, got %d", len(opts.Job.JobID))
	}

	// Call C++ initialization function using the options struct.
	// This reduces the number of parameters from 13 to 1.
	logger.Info("calling C.CNativeRuntime_Initialize()")
	rayJobID := os.Getenv("RAY_JOB_ID")
	logger.Info("RAY_JOB_ID environment variable before C.CNativeRuntime_Initialize", "ray_job_id", rayJobID, "jobConfig", string(opts.Job.JobConfig))
	handle := C.CNativeRuntime_Initialize(cOpts)

	if handle == nil {
		return nil, fmt.Errorf("failed to initialize C++ NativeRuntime: nodeIP=%s, gcsAddress=%s, jobID=%x, clusterID=%x",
			opts.Network.NodeIPAddress, opts.Network.GcsAddress, opts.Job.JobID, opts.Job.ClusterID)
	}

	logger.Info("C.CNativeRuntime_Initialize() succeeded, creating Handle")
	return &Handle{ptr: handle}, nil
}

// Shutdown shuts down the C++ NativeRuntime.
//
// Thread safety: Uses exclusive lock to ensure atomic close operation.
// The C++ CNativeRuntime_Shutdown() function is idempotent and can be called multiple times safely.
//
// Note: The h.ptr field is used as an initialization flag to prevent duplicate Shutdown calls.
// Even though the C++ function is global, we check h.ptr to ensure this Handle instance
// is properly tracked and Shutdown is executed at most once per Handle.
// Setting h.ptr to nil marks the Handle as shutdown for proper state tracking.
func (h *Handle) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ptr != nil {
		// Shutdown the allocator before C++ shutdown to prevent finalizers
		// from accessing C++ objects during shutdown.
		// This sets the r.shutdown flag, causing requestCleanup() to return
		// early without accessing C++ objects.
		ShutdownAllocator()
		C.CNativeRuntime_Shutdown()
		h.ptr = nil
	}
}

// RunTaskExecutionLoop runs the task execution loop.
//
// Note: This method is a blocking call until the Worker is shutdown.
//
// Thread safety: Uses shared lock to check if the runtime is initialized before running.
// The C++ CNativeRuntime_RunTaskExecutionLoop() and CNativeRuntime_Shutdown() functions
// coordinate internally via global state in C++. This method releases the lock before calling
// C++ to allow Shutdown() to be called from another goroutine, which signals the
// RunTaskExecutionLoop to exit.
//
// Expected usage pattern:
//
//	go handle.RunTaskExecutionLoop()  // Start in background
//	... do work ...
//	handle.Shutdown()                 // Signal exit
//
// Note: We copy h.ptr to a local variable and release the lock before calling the C++ function
// to avoid blocking other goroutines that may need to acquire the write lock (e.g., Shutdown).
func (h *Handle) RunTaskExecutionLoop() error {
	h.mu.RLock()
	ptr := h.ptr
	h.mu.RUnlock()

	if ptr == nil {
		return fmt.Errorf("NativeRuntime not initialized: call Initialize() first")
	}

	C.CNativeRuntime_RunTaskExecutionLoop()
	return nil
}

// IsInitialized returns whether the NativeRuntime has been initialized.
//
// Thread safety: Uses shared lock.
func (h *Handle) IsInitialized() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ptr != nil
}
