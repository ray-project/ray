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

package object

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCudaVisibleDevices(t *testing.T) {
	t.Run("not set returns nil", func(t *testing.T) {
		os.Unsetenv("CUDA_VISIBLE_DEVICES")
		result := GetCudaVisibleDevices()
		assert.Nil(t, result)
	})

	t.Run("empty string returns nil", func(t *testing.T) {
		os.Setenv("CUDA_VISIBLE_DEVICES", "")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")
		result := GetCudaVisibleDevices()
		assert.Nil(t, result)
	})

	t.Run("NoDevFiles returns empty slice", func(t *testing.T) {
		os.Setenv("CUDA_VISIBLE_DEVICES", "NoDevFiles")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")
		result := GetCudaVisibleDevices()
		assert.Equal(t, []string{}, result)
	})

	t.Run("single GPU returns slice with one element", func(t *testing.T) {
		os.Setenv("CUDA_VISIBLE_DEVICES", "0")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")
		result := GetCudaVisibleDevices()
		assert.Equal(t, []string{"0"}, result)
	})

	t.Run("multiple GPUs returns comma-separated slice", func(t *testing.T) {
		os.Setenv("CUDA_VISIBLE_DEVICES", "0,2,4")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")
		result := GetCudaVisibleDevices()
		assert.Equal(t, []string{"0", "2", "4"}, result)
	})
}

func TestParseGPUResourceIds(t *testing.T) {
	t.Run("empty map returns empty slice", func(t *testing.T) {
		resourceIds := make(map[string][]string)
		result := ParseGPUResourceIds(resourceIds)
		assert.Empty(t, result)
	})

	t.Run("GPU resource is extracted", func(t *testing.T) {
		resourceIds := map[string][]string{
			"GPU": {"0", "1"},
			"CPU": {"0", "1", "2", "3"},
		}
		result := ParseGPUResourceIds(resourceIds)
		assert.Equal(t, []string{"0", "1"}, result)
	})

	t.Run("GPU_group_XXX resources are extracted", func(t *testing.T) {
		resourceIds := map[string][]string{
			"GPU_group_0": {"0"},
			"GPU_group_1": {"1"},
			"CPU":         {"0", "1"},
		}
		result := ParseGPUResourceIds(resourceIds)
		assert.Len(t, result, 2)
		assert.Contains(t, result, "0")
		assert.Contains(t, result, "1")
	})

	t.Run("multiple GPU resources are combined", func(t *testing.T) {
		resourceIds := map[string][]string{
			"GPU":         {"0", "1"},
			"GPU_group_0": {"2"},
		}
		result := ParseGPUResourceIds(resourceIds)
		assert.Len(t, result, 3)
		assert.Contains(t, result, "0")
		assert.Contains(t, result, "1")
		assert.Contains(t, result, "2")
	})
}

func TestMapToCudaDeviceIds(t *testing.T) {
	t.Run("CUDA_VISIBLE_DEVICES not set returns resource IDs", func(t *testing.T) {
		gpuResourceIds := []string{"0", "1"}
		cudaVisibleDevices := []string(nil)
		result := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)
		assert.Equal(t, []string{"0", "1"}, result)
	})

	t.Run("valid mapping with CUDA_VISIBLE_DEVICES", func(t *testing.T) {
		gpuResourceIds := []string{"0", "1"}
		cudaVisibleDevices := []string{"0", "2", "4"}
		result := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)
		assert.Equal(t, []string{"0", "2"}, result)
	})

	t.Run("invalid index is skipped", func(t *testing.T) {
		gpuResourceIds := []string{"0", "5", "1"}
		cudaVisibleDevices := []string{"0", "2", "4"}
		result := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)
		// Index 0 -> cudaVisibleDevices[0]="0", Index 5 is skipped (out of range), Index 1 -> cudaVisibleDevices[1]="2"
		assert.Equal(t, []string{"0", "2"}, result) // "5" is skipped
	})

	t.Run("non-numeric resource ID is skipped", func(t *testing.T) {
		gpuResourceIds := []string{"0", "invalid", "1"}
		cudaVisibleDevices := []string{"0", "2", "4"}
		result := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)
		// Index 0 -> cudaVisibleDevices[0]="0", "invalid" is skipped (not a number), Index 1 -> cudaVisibleDevices[1]="2"
		assert.Equal(t, []string{"0", "2"}, result) // "invalid" is skipped
	})

	t.Run("negative index is skipped", func(t *testing.T) {
		gpuResourceIds := []string{"-1", "0", "1"}
		cudaVisibleDevices := []string{"0", "2", "4"}
		result := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)
		// Index -1 is skipped (negative), Index 0 -> cudaVisibleDevices[0]="0", Index 1 -> cudaVisibleDevices[1]="2"
		assert.Equal(t, []string{"0", "2"}, result) // "-1" is skipped
	})
}

func TestGPUResourceMappingIntegration(t *testing.T) {
	t.Run("full GPU mapping flow", func(t *testing.T) {
		// Simulate CUDA_VISIBLE_DEVICES="0,2,4"
		os.Setenv("CUDA_VISIBLE_DEVICES", "0,2,4")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")

		// Simulate resource allocation: GPU resources ["0", "1"]
		resourceIds := map[string][]string{
			"GPU": {"0", "1"},
		}

		// Parse GPU resource IDs
		gpuResourceIds := ParseGPUResourceIds(resourceIds)
		assert.Equal(t, []string{"0", "1"}, gpuResourceIds)

		// Map to CUDA device IDs
		cudaVisibleDevices := GetCudaVisibleDevices()
		gpuIds := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)

		// Should map to CUDA_VISIBLE_DEVICES[0]="0" and CUDA_VISIBLE_DEVICES[1]="2"
		assert.Equal(t, []string{"0", "2"}, gpuIds)
	})

	t.Run("GPU_group_XXX with CUDA_VISIBLE_DEVICES", func(t *testing.T) {
		os.Setenv("CUDA_VISIBLE_DEVICES", "1,3,5,7")
		defer os.Unsetenv("CUDA_VISIBLE_DEVICES")

		resourceIds := map[string][]string{
			"GPU_group_0": {"0"},
			"GPU_group_1": {"1"},
		}

		gpuResourceIds := ParseGPUResourceIds(resourceIds)
		assert.Len(t, gpuResourceIds, 2)

		cudaVisibleDevices := GetCudaVisibleDevices()
		gpuIds := mapToCudaDeviceIds(gpuResourceIds, cudaVisibleDevices)

		// Should map to CUDA_VISIBLE_DEVICES[0]="1" and CUDA_VISIBLE_DEVICES[1]="3"
		// Note: map iteration order is non-deterministic, so we check for set equality
		assert.ElementsMatch(t, []string{"1", "3"}, gpuIds)
	})
}
