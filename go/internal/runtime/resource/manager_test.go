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

package resource

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/stretchr/testify/assert"
)

func TestResourceManager_GetWorkerResourceIds(t *testing.T) {
	rm := NewResourceManager()
	workerID := ids.NewUniqueID()

	t.Run("empty cache returns empty map", func(t *testing.T) {
		resources := rm.GetWorkerResourceIds(workerID)
		assert.Empty(t, resources)
	})

	t.Run("cached resources are returned", func(t *testing.T) {
		expectedResources := map[string][]string{
			"GPU": {"0", "1"},
			"CPU": {"0", "1", "2", "3"},
		}
		rm.SetWorkerResources(workerID, expectedResources)

		resources := rm.GetWorkerResourceIds(workerID)
		assert.Equal(t, expectedResources, resources)
	})

	t.Run("different workers have separate caches", func(t *testing.T) {
		workerID2 := ids.NewUniqueID()
		resources1 := map[string][]string{
			"GPU": {"0"},
		}
		resources2 := map[string][]string{
			"GPU": {"1"},
		}

		rm.SetWorkerResources(workerID, resources1)
		rm.SetWorkerResources(workerID2, resources2)

		assert.Equal(t, resources1, rm.GetWorkerResourceIds(workerID))
		assert.Equal(t, resources2, rm.GetWorkerResourceIds(workerID2))
	})

	t.Run("clear worker resources removes cache", func(t *testing.T) {
		rm.SetWorkerResources(workerID, map[string][]string{
			"GPU": {"0"},
		})

		rm.ClearWorkerResources(workerID)
		resources := rm.GetWorkerResourceIds(workerID)
		assert.Empty(t, resources)
	})

	t.Run("clear all resources removes all caches", func(t *testing.T) {
		workerID2 := ids.NewUniqueID()
		rm.SetWorkerResources(workerID, map[string][]string{"GPU": {"0"}})
		rm.SetWorkerResources(workerID2, map[string][]string{"GPU": {"1"}})

		rm.ClearAllResources()

		assert.Empty(t, rm.GetWorkerResourceIds(workerID))
		assert.Empty(t, rm.GetWorkerResourceIds(workerID2))
	})
}

func TestResourceManager_Caching(t *testing.T) {
	rm := NewResourceManager()
	workerID := ids.NewUniqueID()

	t.Run("first query caches result", func(t *testing.T) {
		// Set resources
		expectedResources := map[string][]string{
			"GPU": {"0", "1"},
		}
		rm.SetWorkerResources(workerID, expectedResources)

		// First query should cache the result
		resources1 := rm.GetWorkerResourceIds(workerID)
		assert.Equal(t, expectedResources, resources1)

		// Second query should return cached result (not affected by modifying expectedResources)
		resources2 := rm.GetWorkerResourceIds(workerID)
		assert.Equal(t, map[string][]string{"GPU": {"0", "1"}}, resources2)

		// Now modify the cached resources
		rm.SetWorkerResources(workerID, map[string][]string{
			"GPU": {"2", "3"},
		})

		// Query should return new cached result
		resources3 := rm.GetWorkerResourceIds(workerID)
		assert.Equal(t, map[string][]string{"GPU": {"2", "3"}}, resources3)
	})
}
