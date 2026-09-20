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

// Package resource provides resource management functionality for Ray runtime.
package resource

import (
	"sync"

	"github.com/ray-project/ray/go/pkg/ids"
)

// ResourceManager provides methods to query and manage worker resource allocations.
// Corresponds to Java's resource management functionality.
type ResourceManager interface {
	// GetWorkerResourceIds returns the resource IDs allocated to a worker.
	// The returned map has resource names as keys (e.g., "GPU", "CPU") and
	// resource IDs as values (e.g., ["0", "1"] for GPUs).
	GetWorkerResourceIds(workerID ids.UniqueID) map[string][]string
	// ClearWorkerResources clears the resource allocation for a worker.
	// This is called when a worker exits to prevent memory leak from the cache.
	ClearWorkerResources(workerID ids.UniqueID)
}

// ResourceManagerImpl implements ResourceManager with caching support.
type ResourceManagerImpl struct {
	// workerResources caches worker resource mappings
	workerResources map[ids.UniqueID]map[string][]string
	mu              sync.RWMutex
}

// NewResourceManager creates a new ResourceManager instance.
func NewResourceManager() *ResourceManagerImpl {
	return &ResourceManagerImpl{
		workerResources: make(map[ids.UniqueID]map[string][]string),
	}
}

// GetWorkerResourceIds returns the resource IDs allocated to a worker.
// This implementation first checks the cache, then queries the Raylet if needed.
func (r *ResourceManagerImpl) GetWorkerResourceIds(workerID ids.UniqueID) map[string][]string {
	// 1. Check cache first
	r.mu.RLock()
	resourceMap, ok := r.workerResources[workerID]
	r.mu.RUnlock()

	if ok {
		return resourceMap
	}

	// 2. Query Raylet resource scheduler (placeholder for now)
	// TODO: Implement actual Raylet query via GCS client
	resourceMap = r.queryRayletResources(workerID)

	// 3. Cache the result
	r.mu.Lock()
	r.workerResources[workerID] = resourceMap
	r.mu.Unlock()

	return resourceMap
}

// queryRayletResources queries the Raylet for worker resource allocations.
// This is a placeholder implementation that returns empty resource map.
// In the full implementation, this would query the Raylet via RPC.
func (r *ResourceManagerImpl) queryRayletResources(workerID ids.UniqueID) map[string][]string {
	// Placeholder: Return empty resource map
	// In production, this would:
	// 1. Create RPC request to Raylet
	// 2. Query resource allocations for workerID
	// 3. Parse response and return resource map

	// For now, return empty map
	return make(map[string][]string)
}

// SetWorkerResources sets the resource allocation for a worker (for testing).
// This method allows tests to simulate resource allocations.
func (r *ResourceManagerImpl) SetWorkerResources(workerID ids.UniqueID, resources map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workerResources[workerID] = resources
}

// ClearWorkerResources clears the resource allocation for a worker (for testing).
func (r *ResourceManagerImpl) ClearWorkerResources(workerID ids.UniqueID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.workerResources, workerID)
}

// ClearAllResources clears all cached resource allocations (for testing).
func (r *ResourceManagerImpl) ClearAllResources() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workerResources = make(map[ids.UniqueID]map[string][]string)
}
