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

package runtime_env

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeRuntimeEnv(t *testing.T) {
	t.Run("merge with nil parent", func(t *testing.T) {
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		result := MergeRuntimeEnv(nil, child, false)
		assert.NotNil(t, result)
		assert.Equal(t, []string{"gcs://path/to/module.zip"}, result.PyModules())
	})

	t.Run("merge with nil child", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		result := MergeRuntimeEnv(parent, nil, false)
		assert.NotNil(t, result)
		assert.Equal(t, []string{"gcs://path/to/module.zip"}, result.PyModules())
	})

	t.Run("merge both nil returns empty", func(t *testing.T) {
		result := MergeRuntimeEnv(nil, nil, false)
		assert.NotNil(t, result)
		assert.Empty(t, *result)
	})

	t.Run("merge without override - no conflict", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		tmpDir := t.TempDir()
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.WorkingDir = tmpDir
			return nil
		})
		result := MergeRuntimeEnv(parent, child, false)
		assert.NotNil(t, result)
		assert.Equal(t, []string{"gcs://path/to/module.zip"}, result.PyModules())
		assert.Equal(t, tmpDir, result.WorkingDir())
	})

	t.Run("merge without override - with conflict returns nil", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/parent.zip"}
			return nil
		})
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/child.zip"}
			return nil
		})
		result := MergeRuntimeEnv(parent, child, false)
		assert.Nil(t, result)
	})

	t.Run("merge with override - child wins", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/parent.zip"}
			return nil
		})
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/child.zip"}
			return nil
		})
		result := MergeRuntimeEnv(parent, child, true)
		assert.NotNil(t, result)
		assert.Equal(t, []string{"gcs://path/to/child.zip"}, result.PyModules())
	})

	t.Run("merge env_vars without conflict", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY1": "val1"}
			return nil
		})
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY2": "val2"}
			return nil
		})
		result := MergeRuntimeEnv(parent, child, false)
		assert.NotNil(t, result)
		envVars := result.EnvVars()
		assert.Equal(t, "val1", envVars["KEY1"])
		assert.Equal(t, "val2", envVars["KEY2"])
	})

	t.Run("merge env_vars with conflict without override returns nil", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY1": "val1"}
			return nil
		})
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY1": "val2"}
			return nil
		})
		result := MergeRuntimeEnv(parent, child, false)
		assert.Nil(t, result)
	})

	t.Run("merge env_vars with override - child wins", func(t *testing.T) {
		parent, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY1": "val1", "KEY2": "val2"}
			return nil
		})
		child, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"KEY1": "overridden"}
			return nil
		})
		result := MergeRuntimeEnv(parent, child, true)
		assert.NotNil(t, result)
		envVars := result.EnvVars()
		assert.Equal(t, "overridden", envVars["KEY1"])
		assert.Equal(t, "val2", envVars["KEY2"])
	})
}
