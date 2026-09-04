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

// Package object provides object serialization and deserialization functionality.
// This package includes threshold control for object passing by value vs by reference.
package object

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
)

var serializerLogger = log.WithName("serializer")

// DefaultLargestSizePassedByValue is the default threshold for passing objects by value.
// Objects smaller than this threshold (100KB) are passed by value (serialized directly).
// Objects larger than this threshold are passed by reference (stored in object store).
// This aligns with Java's implementation in SystemConfig.java.
const DefaultLargestSizePassedByValue int64 = 100 * 1024 // 100KB

// largestSizePassedByValue is the current threshold for passing objects by value.
// In local mode, this is hard-coded to 100KB.
// In cluster mode, this is read from core worker config via CGO.
var largestSizePassedByValue int64 = DefaultLargestSizePassedByValue

// isLocalMode indicates whether the runtime is in local mode.
// Local mode uses hard-coded threshold, cluster mode reads from config.
var isLocalMode bool

// thresholdMu protects access to threshold configuration
var thresholdMu sync.RWMutex

// init initializes the serializer package configuration.
func init() {
	// Default to cluster mode
	isLocalMode = false
}

// SetLocalMode sets the local mode flag and initializes the threshold accordingly.
// This should be called during runtime initialization.
func SetLocalMode(local bool) {
	thresholdMu.Lock()
	defer thresholdMu.Unlock()
	isLocalMode = local
	if local {
		// Local mode: hard-code 100KB threshold (aligns with Java)
		largestSizePassedByValue = DefaultLargestSizePassedByValue
		serializerLogger.Info("Local mode: object passing threshold set to 100KB")
	}
}

// SetLargestSizePassedByValue sets the threshold for passing objects by value.
// This is used in cluster mode to read the threshold from core worker config.
func SetLargestSizePassedByValue(threshold int64) {
	thresholdMu.Lock()
	defer thresholdMu.Unlock()
	largestSizePassedByValue = threshold
	serializerLogger.Info("Cluster mode: object passing threshold set from config", "threshold", threshold)
}

// ShouldPassByValue determines whether an object should be passed by value based on its size.
//
// Parameters:
//   - dataSize: The size of the serialized object data in bytes
//   - isLocalMode: Whether the runtime is in local mode
//
// Returns:
//   - bool: true if the object should be passed by value (size < threshold), false otherwise
//
// Implementation:
//   - Local mode: hard-coded 100KB threshold (aligns with Java's SystemConfig.java)
//   - Cluster mode: reads threshold from core worker config via CGO
func ShouldPassByValue(dataSize int, isLocalMode bool) bool {
	thresholdMu.RLock()
	defer thresholdMu.RUnlock()

	// Use the configured threshold
	threshold := largestSizePassedByValue

	// Compare size with threshold
	return int64(dataSize) < threshold
}

// GetCudaVisibleDevices reads the CUDA_VISIBLE_DEVICES environment variable.
// Returns nil if not set, empty slice if set to "NoDevFiles", or the list of GPU IDs.
//
// This function is used to map GPU resource IDs to actual CUDA device IDs.
func GetCudaVisibleDevices() []string {
	gpuIdsStr := os.Getenv("CUDA_VISIBLE_DEVICES")
	if gpuIdsStr == "" {
		return nil // Not set
	}
	if gpuIdsStr == "NoDevFiles" {
		return []string{} // No GPU devices
	}
	return strings.Split(gpuIdsStr, ",")
}

// ParseGPUResourceIds extracts GPU resource IDs from a resource map.
// The resource map contains resource names (e.g., "GPU", "GPU_group_XXX") mapped to resource IDs.
//
// Parameters:
//   - resourceIds: Map of resource name to list of resource IDs
//
// Returns:
//   - []string: List of GPU resource IDs
func ParseGPUResourceIds(resourceIds map[string][]string) []string {
	var gpuResourceIds []string
	for resName, resIds := range resourceIds {
		// Match GPU resources (including GPU_group_XXX pattern)
		if resName == "GPU" || strings.HasPrefix(resName, "GPU_group_") {
			gpuResourceIds = append(gpuResourceIds, resIds...)
		}
	}
	return gpuResourceIds
}

// mapToCudaDeviceIds maps GPU resource IDs to actual CUDA device IDs using CUDA_VISIBLE_DEVICES.
//
// Parameters:
//   - gpuResourceIds: List of GPU resource IDs (indices into CUDA_VISIBLE_DEVICES)
//   - cudaVisibleDevices: List of actual CUDA device IDs from environment variable
//
// Returns:
//   - []string: List of actual CUDA device IDs, or the original resource IDs if CUDA_VISIBLE_DEVICES not set
//
// Example:
//   If CUDA_VISIBLE_DEVICES="0,2,4" and gpuResourceIds=["0", "1"],
//   then the result is ["0", "2"] (CUDA_VISIBLE_DEVICES[0]="0", CUDA_VISIBLE_DEVICES[1]="2")
func mapToCudaDeviceIds(gpuResourceIds []string, cudaVisibleDevices []string) []string {
	if cudaVisibleDevices == nil {
		// CUDA_VISIBLE_DEVICES not set, return resource IDs as-is
		return gpuResourceIds
	}

	var gpuIds []string
	for _, resId := range gpuResourceIds {
		idx, err := strconv.Atoi(resId)
		if err != nil || idx < 0 || idx >= len(cudaVisibleDevices) {
			// Invalid index, skip this resource ID
			serializerLogger.Info("Invalid GPU resource ID", "resourceId", resId, "error", err)
			continue
		}
		gpuIds = append(gpuIds, cudaVisibleDevices[idx])
	}
	return gpuIds
}

// GetLargestSizePassedByValue returns the current threshold for passing objects by value.
// This is useful for debugging and testing.
func GetLargestSizePassedByValue() int64 {
	thresholdMu.RLock()
	defer thresholdMu.RUnlock()
	return largestSizePassedByValue
}

// GetIsLocalMode returns whether the runtime is in local mode.
func GetIsLocalMode() bool {
	thresholdMu.RLock()
	defer thresholdMu.RUnlock()
	return isLocalMode
}
