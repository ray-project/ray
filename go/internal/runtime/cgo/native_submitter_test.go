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

//go:build cgo
// +build cgo

package cgo

/*
#include <stdlib.h>
*/
import "C"
import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/stretchr/testify/assert"
)

// TestNewNativeTaskSubmitter tests the constructor.
func TestNewNativeTaskSubmitter(t *testing.T) {
	funcManager := function.NewFunctionManager(nil)
	submitter := NewNativeTaskSubmitter(funcManager)

	if submitter == nil {
		t.Fatal("NewNativeTaskSubmitter() returned nil")
	}

	if submitter.functionManager != funcManager {
		t.Error("Expected functionManager to be set")
	}
}

// A cluster-dependent GetActor test is intentionally absent from this unit
// target. GetActor resolves a named actor through the C++ CoreWorker GCS
// client, which aborts the process when no core worker is running; because Go
// runs tests in declaration order in a single binary, that abort would also
// kill every test declared after it (the task_executor and converter tests).
// Cluster-level coverage for named-actor lookup belongs in an integration test
// target, not here.

func TestNativeActorHandle_ID(t *testing.T) {
	t.Run("ReturnsCorrectActorID", func(t *testing.T) {
		expectedActorID, _ := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f1011121314")

		handle := &object.NativeActorHandle{
			ActorID:  expectedActorID,
			Language: object.LanguageGo,
		}

		// Test that ID() method returns the correct actor ID
		actualActorID := handle.ID()
		assert.Equal(t, expectedActorID, actualActorID)
	})

	t.Run("ImplementsActorHandleInterface", func(t *testing.T) {
		// Verify that NativeActorHandle implements submitter.ActorHandle interface
		var _ interface{ ID() ids.ActorID } = &object.NativeActorHandle{}
	})
}

func TestConvertTaskOptionsToC(t *testing.T) {
	t.Run("NilOptions", func(t *testing.T) {
		result := convertTaskOptionsToC(nil)
		assert.Nil(t, result, "Nil options should return nil C struct")
	})

	t.Run("EmptyOptions", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result, "Empty options should return valid C struct")
		freeTaskOptions(result)
	})

	t.Run("ResourcesMap", func(t *testing.T) {
		opts := &submitter.TaskOptions{
			Resources: map[string]float64{
				"CPU": 2.0,
				"GPU": 1.0,
				"TPU": 4.0,
			},
		}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result)
		assert.NotNil(t, result.resources, "Resources should be set")
		resourcesStr := C.GoString(result.resources)
		assert.Contains(t, resourcesStr, "CPU:2.0", "CPU resource should be present")
		assert.Contains(t, resourcesStr, "GPU:1.0", "GPU resource should be present")
		assert.Contains(t, resourcesStr, "TPU:4.0", "TPU resource should be present")
		freeTaskOptions(result)
	})

	t.Run("RuntimeEnv", func(t *testing.T) {
		opts := &submitter.TaskOptions{
			RuntimeEnv: `{"pip": ["requests", "numpy"]}`,
		}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result)
		assert.NotNil(t, result.runtime_env, "RuntimeEnv should be set")
		runtimeEnvStr := C.GoString(result.runtime_env)
		assert.Equal(t, `{"pip": ["requests", "numpy"]}`, runtimeEnvStr)
		freeTaskOptions(result)
	})

	t.Run("PlacementGroup", func(t *testing.T) {
		pgID, _ := ids.PlacementGroupIDFromHex("0102030405060708090a0b0c0d0e0f10")
		opts := &submitter.TaskOptions{
			PlacementGroup: &submitter.PlacementGroupOptions{
				ID:          pgID,
				BundleIndex: 2,
			},
		}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result)
		assert.NotNil(t, result.placement_group_id, "PlacementGroup ID should be set")
		assert.Equal(t, C.int(2), result.bundle_index, "Bundle index should be set")
		freeTaskOptions(result)
	})

	t.Run("RetryPolicy", func(t *testing.T) {
		opts := &submitter.TaskOptions{
			RetryPolicy: &submitter.RetryPolicy{
				MaxRetries: 3,
			},
		}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result)
		assert.Equal(t, C.int(3), result.max_retries, "Max retries should be set")
		freeTaskOptions(result)
	})

	t.Run("AllOptionsCombined", func(t *testing.T) {
		pgID, _ := ids.PlacementGroupIDFromHex("0102030405060708090a0b0c0d0e0f10")
		opts := &submitter.TaskOptions{
			NumGPUs: 1.0,
			GPUIDs:  []string{"0", "2"},
			Resources: map[string]float64{
				"CPU": 4.0,
				"GPU": 1.0,
			},
			RuntimeEnv: `{"pip": ["numpy"]}`,
			PlacementGroup: &submitter.PlacementGroupOptions{
				ID:          pgID,
				BundleIndex: 1,
			},
			RetryPolicy: &submitter.RetryPolicy{
				MaxRetries: 5,
			},
			Name: "test_task",
		}
		result := convertTaskOptionsToC(opts)
		assert.NotNil(t, result)

		assert.NotNil(t, result.resources)
		resourcesStr := C.GoString(result.resources)
		assert.Contains(t, resourcesStr, "CPU:4.0")
		assert.Contains(t, resourcesStr, "GPU:1.0")

		assert.NotNil(t, result.runtime_env)
		assert.Equal(t, `{"pip": ["numpy"]}`, C.GoString(result.runtime_env))

		assert.NotNil(t, result.placement_group_id)
		assert.Equal(t, C.int(1), result.bundle_index)

		freeTaskOptions(result)
	})
}

func TestWithGPUs(t *testing.T) {
	t.Run("FractionalGPU", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithGPUs(0.5)
		optionFunc(opts)

		assert.Equal(t, 0.5, opts.NumGPUs)
		assert.Equal(t, 0.5, opts.Resources["GPU"])
		assert.Empty(t, opts.GPUIDs)
	})

	t.Run("IntegerGPU", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithGPUs(2.0)
		optionFunc(opts)

		assert.Equal(t, 2.0, opts.NumGPUs)
		assert.Equal(t, 2.0, opts.Resources["GPU"])
		assert.Empty(t, opts.GPUIDs)
	})

	t.Run("GPUWithAffinity", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithGPUs(1.0, "0", "2")
		optionFunc(opts)

		assert.Equal(t, 1.0, opts.NumGPUs)
		assert.Equal(t, 1.0, opts.Resources["GPU"])
		assert.Equal(t, []string{"0", "2"}, opts.GPUIDs)
	})

	t.Run("ZeroGPU", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithGPUs(0.0)
		optionFunc(opts)

		assert.Equal(t, 0.0, opts.NumGPUs)
		_, exists := opts.Resources["GPU"]
		assert.False(t, exists)
		assert.Empty(t, opts.GPUIDs)
	})
}

func TestWithResources(t *testing.T) {
	t.Run("CustomResources", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		customResources := map[string]float64{
			"TPU":        2.0,
			"memory":     1024.0,
			"custom_gpu": 1.0,
		}
		optionFunc := submitter.WithResources(customResources)
		optionFunc(opts)

		assert.Equal(t, customResources, opts.Resources)
	})

	t.Run("EmptyResources", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithResources(map[string]float64{})
		optionFunc(opts)

		assert.Empty(t, opts.Resources)
	})

	t.Run("NilResources", func(t *testing.T) {
		opts := &submitter.TaskOptions{}
		optionFunc := submitter.WithResources(nil)
		optionFunc(opts)

		assert.Nil(t, opts.Resources)
	})
}
