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

package submitter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskOptions_GPUFields(t *testing.T) {
	t.Run("default TaskOptions has no GPU configuration", func(t *testing.T) {
		opts := &TaskOptions{}
		assert.Equal(t, float64(0), opts.NumGPUs)
		assert.Empty(t, opts.GPUIDs)
	})

	t.Run("TaskOptions with GPU configuration", func(t *testing.T) {
		opts := &TaskOptions{
			NumGPUs: 1.0,
			GPUIDs:  []string{"0", "2"},
		}
		assert.Equal(t, float64(1.0), opts.NumGPUs)
		assert.Equal(t, []string{"0", "2"}, opts.GPUIDs)
	})

	t.Run("fractional GPU support", func(t *testing.T) {
		opts := &TaskOptions{
			NumGPUs: 0.5,
		}
		assert.Equal(t, float64(0.5), opts.NumGPUs)
	})
}

func TestWithGPUs(t *testing.T) {
	t.Run("sets NumGPUs field", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(1.0)(opts)
		assert.Equal(t, float64(1.0), opts.NumGPUs)
	})

	t.Run("sets GPUIDs field when provided", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(1.0, "0", "2")(opts)
		assert.Equal(t, []string{"0", "2"}, opts.GPUIDs)
	})

	t.Run("does not set GPUIDs when not provided", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(1.0)(opts)
		assert.Empty(t, opts.GPUIDs)
	})

	t.Run("sets Resources[\"GPU\"] for backward compatibility", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(1.5)(opts)
		assert.Equal(t, float64(1.5), opts.Resources["GPU"])
	})

	t.Run("creates Resources map if nil", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(1.0)(opts)
		assert.NotNil(t, opts.Resources)
		assert.Equal(t, float64(1.0), opts.Resources["GPU"])
	})

	t.Run("fractional GPU support", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(0.5)(opts)
		assert.Equal(t, float64(0.5), opts.NumGPUs)
		assert.Equal(t, float64(0.5), opts.Resources["GPU"])
	})

	t.Run("zero GPUs does not set Resources", func(t *testing.T) {
		opts := &TaskOptions{}
		WithGPUs(0)(opts)
		assert.Equal(t, float64(0), opts.NumGPUs)
		// Resources should not be set for 0 GPUs
		_, exists := opts.Resources["GPU"]
		assert.False(t, exists)
	})
}

func TestWithResources(t *testing.T) {
	t.Run("sets Resources field", func(t *testing.T) {
		opts := &TaskOptions{}
		resources := map[string]float64{
			"TPU":    2.0,
			"memory": 1024.0,
		}
		WithResources(resources)(opts)
		assert.Equal(t, resources, opts.Resources)
	})

	t.Run("overwrites existing Resources", func(t *testing.T) {
		opts := &TaskOptions{
			Resources: map[string]float64{
				"CPU": 4.0,
			},
		}
		newResources := map[string]float64{
			"TPU": 2.0,
		}
		WithResources(newResources)(opts)
		assert.Equal(t, newResources, opts.Resources)
	})

	t.Run("nil resources sets nil", func(t *testing.T) {
		opts := &TaskOptions{}
		WithResources(nil)(opts)
		assert.Nil(t, opts.Resources)
	})
}

func TestTaskOptions_WithGPUsAndResources(t *testing.T) {
	t.Run("combines GPU and custom resources", func(t *testing.T) {
		opts := &TaskOptions{}

		// Apply WithGPUs first
		WithGPUs(1.0, "0")(opts)

		// Then add custom resources
		WithResources(map[string]float64{
			"TPU": 2.0,
		})(opts)

		assert.Equal(t, float64(1.0), opts.NumGPUs)
		assert.Equal(t, []string{"0"}, opts.GPUIDs)
		assert.Equal(t, float64(2.0), opts.Resources["TPU"])
		// Note: WithResources overwrites Resources, so GPU is lost
		_, exists := opts.Resources["GPU"]
		assert.False(t, exists)
	})

	t.Run("WithGPUs after WithResources preserves both", func(t *testing.T) {
		opts := &TaskOptions{}

		// Apply WithResources first
		WithResources(map[string]float64{
			"TPU": 2.0,
		})(opts)

		// Then apply WithGPUs
		WithGPUs(1.0, "0")(opts)

		assert.Equal(t, float64(1.0), opts.NumGPUs)
		assert.Equal(t, []string{"0"}, opts.GPUIDs)
		assert.Equal(t, float64(2.0), opts.Resources["TPU"])
		assert.Equal(t, float64(1.0), opts.Resources["GPU"])
	})
}
