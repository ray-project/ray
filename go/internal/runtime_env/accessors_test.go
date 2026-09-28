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

func TestRuntimeEnv_GetAndHas(t *testing.T) {
	t.Run("Get returns value for existing key", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		val := env.Get(FieldPyModules, nil)
		assert.NotNil(t, val)
		assert.Equal(t, []string{"gcs://path/to/module.zip"}, val)
	})

	t.Run("Get returns default for missing key", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		val := env.Get("nonexistent", "default_value")
		assert.Equal(t, "default_value", val)
	})

	t.Run("Has returns true for existing non-empty field", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		assert.True(t, env.Has(FieldPyModules))
	})

	t.Run("Has returns false for missing field", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		assert.False(t, env.Has(FieldWorkingDir))
	})

	t.Run("Has returns false for empty string field", func(t *testing.T) {
		// Empty string is not set to the map, so Has should return false.
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyExecutable = ""
			return nil
		})
		assert.Nil(t, env)
	})

	t.Run("Has returns false for empty slice field", func(t *testing.T) {
		// Empty slice is not set to the map (hasValue returns false), so env
		// will be nil. We need to set some other field to make env non-nil.
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{}
			opts.WorkingDir = "gcs://path/to/working_dir.zip" // Set another field to make env non-nil.
			return nil
		}, WithValidate(false))
		// PyModules should not be in the map because it's empty.
		_, exists := (*env)[FieldPyModules]
		assert.False(t, exists)
		assert.False(t, env.Has(FieldPyModules))
	})
}

func TestRuntimeEnv_ToDict(t *testing.T) {
	t.Run("ToDict returns map representation", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip", "https://s3.example.com/bucket/package.whl"}
			opts.EnvVars = map[string]string{"KEY": "value"}
			return nil
		}, WithValidate(false))
		dict := env.ToDict()
		assert.Equal(t, []string{"gcs://path/to/module.zip", "https://s3.example.com/bucket/package.whl"}, dict["py_modules"])
		assert.Equal(t, map[string]string{"KEY": "value"}, dict["env_vars"])
	})

	t.Run("ToDict on nil returns nil", func(t *testing.T) {
		var env RuntimeEnv
		dict := env.ToDict()
		assert.Nil(t, dict)
	})
}

func TestRuntimeEnv_Plugins(t *testing.T) {
	t.Run("Plugins returns unknown fields", func(t *testing.T) {
		env := &RuntimeEnv{"py_modules": []string{"mod1"}, "custom_plugin": "custom_value"}
		plugins := env.Plugins()
		assert.Len(t, plugins, 1)
		assert.Equal(t, "custom_plugin", plugins[0].Key)
		assert.Equal(t, "custom_value", plugins[0].Value)
	})

	t.Run("Plugins returns empty for known fields only", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		plugins := env.Plugins()
		assert.Len(t, plugins, 0)
	})
}

func TestRuntimeEnv_GetExtension(t *testing.T) {
	t.Run("GetExtension returns value for extension field", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.RayRelease = "2.7.0"
			return nil
		})
		val, err := env.GetExtension(FieldRayRelease)
		assert.NoError(t, err)
		assert.Equal(t, "2.7.0", val)
	})

	t.Run("GetExtension returns error for non-extension field", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		val, err := env.GetExtension(FieldPyModules)
		assert.Error(t, err)
		assert.Nil(t, val)
		assert.Contains(t, err.Error(), "extension key must be one of")
	})
}

func TestRuntimeEnv_ContainerMethods(t *testing.T) {
	t.Run("PyContainerImage when no container", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		image, err := env.PyContainerImage()
		assert.Error(t, err)
		assert.Equal(t, "", image)
	})

	t.Run("PyContainerWorkerPath when no container", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		path, err := env.PyContainerWorkerPath()
		assert.Error(t, err)
		assert.Equal(t, "", path)
	})

	t.Run("PyContainerRunOptions when no container", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		opts, err := env.PyContainerRunOptions()
		assert.Error(t, err)
		assert.Equal(t, []string{}, opts)
	})
}

func TestRuntimeEnv_OtherMethods(t *testing.T) {
	t.Run("PyModulesURIs returns modules", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip", "https://s3.example.com/bucket/package.whl"}
			return nil
		})
		uris := env.PyModulesURIs()
		assert.Equal(t, []string{"gcs://path/to/module.zip", "https://s3.example.com/bucket/package.whl"}, uris)
	})

	t.Run("PluginURIs always returns empty", func(t *testing.T) {
		env, _ := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		uris := env.PluginURIs()
		assert.Empty(t, uris)
	})
}
