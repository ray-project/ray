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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRuntimeEnvParamNilOrEmpty(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(nil))
	})

	t.Run("empty string", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(""))
	})

	t.Run("non-empty string", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty("hello"))
	})

	t.Run("empty slice", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty([]string{}))
	})

	t.Run("non-empty slice", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty([]string{"a"}))
	})

	t.Run("empty interface slice", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty([]interface{}{}))
	})

	t.Run("non-empty interface slice", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty([]interface{}{"a"}))
	})

	t.Run("empty map", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(map[string]interface{}{}))
	})

	t.Run("non-empty map", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(map[string]interface{}{"a": "b"}))
	})

	t.Run("empty string map", func(t *testing.T) {
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(map[string]string{}))
	})

	t.Run("non-empty string map", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(map[string]string{"a": "b"}))
	})

	t.Run("false bool", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(false))
	})

	t.Run("true bool", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(true))
	})

	t.Run("int value defaults to false", func(t *testing.T) {
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(42))
	})

	t.Run("nil ContainerConfig pointer", func(t *testing.T) {
		var config *ContainerConfig = nil
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(config))
	})

	t.Run("ContainerConfig with zero fields", func(t *testing.T) {
		config := &ContainerConfig{}
		assert.True(t, IsRuntimeEnvParamNilOrEmpty(config))
	})

	t.Run("ContainerConfig with non-zero Image", func(t *testing.T) {
		config := &ContainerConfig{Image: "my-image"}
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(config))
	})

	t.Run("ContainerConfig with non-zero WorkerPath", func(t *testing.T) {
		config := &ContainerConfig{WorkerPath: "/path"}
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(config))
	})

	t.Run("ContainerConfig with non-zero RunOptions", func(t *testing.T) {
		config := &ContainerConfig{RunOptions: []string{"--rm"}}
		assert.False(t, IsRuntimeEnvParamNilOrEmpty(config))
	})
}

func TestContainerConfig(t *testing.T) {
	config := &ContainerConfig{
		Image:      "my-image:latest",
		WorkerPath: "/path/to/worker",
		RunOptions: []string{"--rm", "-v", "/data:/data"},
	}
	assert.Equal(t, "my-image:latest", config.Image)
	assert.Equal(t, "/path/to/worker", config.WorkerPath)
	assert.Equal(t, []string{"--rm", "-v", "/data:/data"}, config.RunOptions)
}

func TestFromMap(t *testing.T) {
	t.Run("create from map with py_modules", func(t *testing.T) {
		data := map[string]interface{}{"py_modules": []interface{}{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}, env.PyModules())
	})

	t.Run("create from map with py_modules as string slice", func(t *testing.T) {
		data := map[string]interface{}{"py_modules": []string{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}, env.PyModules())
	})

	t.Run("create from map with java_jars", func(t *testing.T) {
		data := map[string]interface{}{"java_jars": []interface{}{"jar1.jar", "jar2.jar"}}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"jar1.jar", "jar2.jar"}, env.JavaJars())
	})

	t.Run("create from map with container as map", func(t *testing.T) {
		data := map[string]interface{}{
			"container": map[string]interface{}{
				"image": "my-image", "worker_path": "/worker",
				"run_options": []interface{}{"--rm", "-v", "/data:/data"},
			},
		}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasContainer())
	})

	t.Run("create from map with container as struct", func(t *testing.T) {
		data := map[string]interface{}{
			"container": &ContainerConfig{Image: "my-image", WorkerPath: "/worker", RunOptions: []string{"--rm"}},
		}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasContainer())
	})

	t.Run("create from map with env_vars as interface map", func(t *testing.T) {
		data := map[string]interface{}{"env_vars": map[string]interface{}{"KEY1": "value1", "KEY2": "value2"}}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		envVars := env.EnvVars()
		assert.Equal(t, "value1", envVars["KEY1"])
		assert.Equal(t, "value2", envVars["KEY2"])
	})

	t.Run("create from map with env_vars as string map", func(t *testing.T) {
		data := map[string]interface{}{"env_vars": map[string]string{"KEY1": "value1", "KEY2": "value2"}}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		envVars := env.EnvVars()
		assert.Equal(t, "value1", envVars["KEY1"])
		assert.Equal(t, "value2", envVars["KEY2"])
	})

	t.Run("create from map with config as map", func(t *testing.T) {
		data := map[string]interface{}{
			"config": map[string]interface{}{"setup_timeout_seconds": 400, "eager_install": false},
		}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		config := env.Get(FieldConfig, nil)
		assert.NotNil(t, config)
	})

	t.Run("create from map with config as RuntimeEnvConfig", func(t *testing.T) {
		data := map[string]interface{}{
			"config": RuntimeEnvConfig{"setup_timeout_seconds": 500, "eager_install": true},
		}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		config := env.Get(FieldConfig, nil)
		assert.NotNil(t, config)
	})

	t.Run("create from map with conda and pip returns error", func(t *testing.T) {
		data := map[string]interface{}{
			"conda": "env1",
			"pip":   map[string]interface{}{"packages": []string{"requests"}},
		}
		env, err := FromMap(data)
		assert.Error(t, err)
		assert.Nil(t, env)
	})
}

func TestDeserialize(t *testing.T) {
	t.Run("deserialize valid json using FromMap", func(t *testing.T) {
		data := map[string]interface{}{"py_modules": []interface{}{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}, "working_dir": "gcs://path/to/workspace.zip"}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}, env.PyModules())
		assert.Equal(t, "gcs://path/to/workspace.zip", env.WorkingDir())
	})

	t.Run("deserialize invalid json returns error", func(t *testing.T) {
		jsonStr := `{invalid json}`
		env, err := Deserialize(jsonStr)
		assert.Error(t, err)
		assert.Nil(t, env)
	})

	t.Run("deserialize empty json object using FromMap returns nil", func(t *testing.T) {
		data := map[string]interface{}{}
		env, err := FromMap(data)
		assert.NoError(t, err)
		assert.Nil(t, env)
	})
}

func TestSerialize(t *testing.T) {
	t.Run("serialize env to json", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/mod1.zip", "https://s3.example.com/bucket/mod2.whl"}
			opts.WorkingDir = "gcs://path/to/workspace.zip"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		jsonStr, err := Serialize(env)
		assert.NoError(t, err)
		assert.Contains(t, jsonStr, "py_modules")
		assert.Contains(t, jsonStr, "working_dir")
	})
}

func TestMapToContainerConfig(t *testing.T) {
	t.Run("convert map to ContainerConfig", func(t *testing.T) {
		m := map[string]interface{}{
			"image":       "my-image",
			"worker_path": "/worker",
			"run_options": []interface{}{"--rm", "-v", "/data:/data"},
		}
		config := mapToContainerConfig(m)
		assert.NotNil(t, config)
		assert.Equal(t, "my-image", config.Image)
		assert.Equal(t, "/worker", config.WorkerPath)
		assert.Equal(t, []string{"--rm", "-v", "/data:/data"}, config.RunOptions)
	})

	t.Run("convert map with string slice run_options", func(t *testing.T) {
		m := map[string]interface{}{
			"image":       "my-image",
			"run_options": []string{"--rm"},
		}
		config := mapToContainerConfig(m)
		assert.NotNil(t, config)
		assert.Equal(t, "my-image", config.Image)
		assert.Equal(t, []string{"--rm"}, config.RunOptions)
	})
}

func TestWithOptions(t *testing.T) {
	opts := &RuntimeEnvOptions{PyModules: []string{"mod1"}}
	fn := withOptions(opts)
	newOpts := &RuntimeEnvOptions{}
	err := fn(newOpts)
	assert.NoError(t, err)
	assert.Equal(t, []string{"mod1"}, newOpts.PyModules)
}

func TestFilterEnvVarsWithPrefix(t *testing.T) {
	testKey := "RAY_TEST_FILTER_VAR"
	testValue := "test_value_123"
	originalValue := os.Getenv(testKey)
	os.Setenv(testKey, testValue)
	defer func() {
		if originalValue == "" {
			os.Unsetenv(testKey)
		} else {
			os.Setenv(testKey, originalValue)
		}
	}()

	t.Run("filter RAY_ prefix variables", func(t *testing.T) {
		rayVars := FilterEnvVarsWithPrefix("RAY_")
		assert.Contains(t, rayVars, testKey)
		assert.Equal(t, testValue, rayVars[testKey])
	})

	t.Run("filter non-existent prefix", func(t *testing.T) {
		nonExistentVars := FilterEnvVarsWithPrefix("NONEXISTENT_PREFIX_")
		_, exists := nonExistentVars[testKey]
		assert.False(t, exists)
	})

	t.Run("empty prefix matches all", func(t *testing.T) {
		allVars := FilterEnvVarsWithPrefix("")
		assert.NotEmpty(t, allVars)
	})
}

func TestEnvVarsToStringSlice(t *testing.T) {
	input := map[string]string{
		"KEY1": "value1",
		"KEY2": "value2",
		"KEY3": "",
	}

	result := EnvVarsToStringSlice(input)

	assert.Len(t, result, 3)

	expectedSet := map[string]bool{
		"KEY1=value1": true,
		"KEY2=value2": true,
		"KEY3=":       true,
	}

	for _, envVar := range result {
		assert.True(t, expectedSet[envVar], "Unexpected env var: %s", envVar)
	}
}

func TestCollectRayEnvVars_BackwardCompatibility(t *testing.T) {
	testVars := map[string]string{
		"RAY_TEST_VAR1": "value1",
		"RAY_TEST_VAR2": "value2",
		"OTHER_VAR":     "other_value",
	}

	originalValues := make(map[string]string)
	for k, v := range testVars {
		originalValues[k] = os.Getenv(k)
		os.Setenv(k, v)
	}
	defer func() {
		for k, originalValue := range originalValues {
			if originalValue == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, originalValue)
			}
		}
	}()

	rayVars := collectRayEnvVars()

	assert.Contains(t, rayVars, "RAY_TEST_VAR1=value1")
	assert.Contains(t, rayVars, "RAY_TEST_VAR2=value2")

	for _, envVar := range rayVars {
		assert.NotContains(t, envVar, "OTHER_VAR", "Should not include non-RAY_ variables")
	}
}
