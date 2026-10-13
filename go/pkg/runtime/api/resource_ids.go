// Copyright 2026 The Ray Authors.
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

package api

import (
	"github.com/ray-project/ray/go/pkg/errors"
)

// ResourceValue represents a resource ID and its capacity.
// Consistent with Java io.ray.api.runtimecontext.ResourceValue.
type ResourceValue struct {
	// ResourceID is the index of the resource (e.g., CPU #3, GPU #0).
	ResourceID int64
	// Capacity is the quantity of the resource assigned (e.g., 0.5 CPU).
	Capacity float64
}

// GetAvailableResourceIds returns the resource name -> resource ID/capacity
// mapping for the current worker.
// Consistent with Java RayRuntime.getAvailableResourceIds().
//
// Returns:
//   - map[string][]ResourceValue: resource name to resource values
//   - error: ErrRuntimeNotInitialized if the runtime is not initialized
func GetAvailableResourceIds() (map[string][]ResourceValue, error) {
	rt, err := internal()
	if err != nil {
		return nil, errors.ErrRuntimeNotInitialized
	}
	resourceIds := rt.GetResourceIds()
	result := make(map[string][]ResourceValue, len(resourceIds))
	for name, values := range resourceIds {
		converted := make([]ResourceValue, 0, len(values))
		for _, v := range values {
			converted = append(converted, ResourceValue{
				ResourceID: v.ResourceID,
				Capacity:   v.Capacity,
			})
		}
		result[name] = converted
	}
	return result, nil
}
