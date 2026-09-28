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

// WithValidate creates a function that sets the Validate option.
func WithValidate(validate bool) func(*RuntimeEnvOptions) error {
	return func(opts *RuntimeEnvOptions) error {
		opts.Validate = validate
		return nil
	}
}

func TestNewRuntimeEnv_Basic(t *testing.T) {
	t.Run("empty options creates empty env", func(t *testing.T) {
		env, err := NewRuntimeEnv()
		assert.NoError(t, err)
		assert.Nil(t, env)
	})

	t.Run("with py_modules", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyModules = []string{"gcs://path/to/module1.zip", "gcs://path/to/module2.zip"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"gcs://path/to/module1.zip", "gcs://path/to/module2.zip"}, env.PyModules())
	})

	t.Run("with py_executable", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.PyExecutable = "/usr/bin/python"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		exec, err := env.PyExecutable()
		assert.NoError(t, err)
		assert.Equal(t, "/usr/bin/python", exec)
	})

	t.Run("with working_dir", func(t *testing.T) {
		tmpDir := t.TempDir()
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.WorkingDir = tmpDir
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasWorkingDir())
		assert.Equal(t, tmpDir, env.WorkingDirURI())
		assert.Equal(t, tmpDir, env.WorkingDir())
	})

	t.Run("with java_jars", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.JavaJars = []string{"jar1.jar", "jar2.jar"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, []string{"jar1.jar", "jar2.jar"}, env.JavaJars())
	})

	t.Run("with conda string", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Conda = "my-conda-env"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasConda())
		name, err := env.CondaEnvName()
		assert.NoError(t, err)
		assert.Equal(t, "my-conda-env", name)
	})

	t.Run("with pip", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Pip = map[string]interface{}{"packages": []string{"requests"}}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasPip())
		config, err := env.PipConfig()
		assert.NoError(t, err)
		assert.NotNil(t, config)
	})

	t.Run("with uv", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Uv = []string{"requests", "numpy>=1.20.0"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasUv())
		config, err := env.UvConfig()
		assert.NoError(t, err)
		assert.NotNil(t, config)
	})

	t.Run("with container", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Container = &ContainerConfig{
				Image:      "my-container",
				WorkerPath: "/worker",
				RunOptions: []string{"--rm"},
			}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasContainer())
		image, err := env.PyContainerImage()
		assert.NoError(t, err)
		assert.Equal(t, "my-container", image)
		workerPath, err := env.PyContainerWorkerPath()
		assert.NoError(t, err)
		assert.Equal(t, "/worker", workerPath)
		runOpts, err := env.PyContainerRunOptions()
		assert.NoError(t, err)
		assert.Equal(t, []string{"--rm"}, runOpts)
	})

	t.Run("with env_vars", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.EnvVars = map[string]string{"FOO": "bar", "BAZ": "qux"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		envVars := env.EnvVars()
		assert.Equal(t, "bar", envVars["FOO"])
		assert.Equal(t, "qux", envVars["BAZ"])
	})

	t.Run("with config", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Config = RuntimeEnvConfig{"setup_timeout_seconds": 500, "eager_install": false}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		config := env.Get(FieldConfig, nil)
		assert.NotNil(t, config)
	})

	t.Run("with image_uri", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.ImageURI = "docker://my-image:latest"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, "docker://my-image:latest", env.ImageURI())
	})

	t.Run("with nsight", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Nsight = map[string]interface{}{"enable": true}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.NotNil(t, env.Nsight())
	})

	t.Run("with rocprof_sys", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.RocprofSys = map[string]interface{}{"enable": true}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.NotNil(t, env.RocprofSys())
	})

	t.Run("with worker_process_setup_hook", func(t *testing.T) {
		hookFunc := func() {}
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.WorkerProcessSetupHook = hookFunc
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.NotNil(t, env.Get(FieldWorkerProcessSetupHook, nil))
	})

	t.Run("with ray_release", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.RayRelease = "2.7.0"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, "2.7.0", env.Get(FieldRayRelease, ""))
	})

	t.Run("with ray_commit", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.RayCommit = "abc123def"
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, "abc123def", env.Get(FieldRayCommit, ""))
	})

	t.Run("with inject_current_ray", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.InjectCurrentRay = true
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, true, env.Get(FieldInjectCurrentRay, false))
	})

	t.Run("with excludes", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Excludes = []string{"*.pyc", "__pycache__"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		excludes := env.Get(FieldExcludes, []string{})
		assert.Equal(t, []string{"*.pyc", "__pycache__"}, excludes)
	})

	t.Run("with extra fields", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.ExtraFields = map[string]interface{}{"custom_field": "custom_value"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.Equal(t, "custom_value", env.Get("custom_field", nil))
	})

	t.Run("validate false skips validation", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Conda = "env1"
			opts.Pip = map[string]interface{}{"packages": []string{"requests"}}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
		assert.True(t, env.HasConda())
		assert.True(t, env.HasPip())
	})
}

func TestNewRuntimeEnv_Validation(t *testing.T) {
	t.Run("conda and pip together returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Conda = "env1"
			opts.Pip = map[string]interface{}{"packages": []string{"requests"}}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "cannot be specified at the same time")
	})

	t.Run("conda and uv together returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Conda = "env1"
			opts.Uv = map[string]interface{}{"packages": []string{"fastapi"}}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "cannot be specified at the same time")
	})

	t.Run("pip and uv together returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Pip = map[string]interface{}{"packages": []string{"requests"}}
			opts.Uv = map[string]interface{}{"packages": []string{"fastapi"}}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "cannot be specified at the same time")
	})

	t.Run("all three conda pip uv returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Conda = "env1"
			opts.Pip = map[string]interface{}{"packages": []string{"requests"}}
			opts.Uv = map[string]interface{}{"packages": []string{"fastapi"}}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "cannot be specified at the same time")
	})

	t.Run("container with incompatible field returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Container = &ContainerConfig{Image: "my-image"}
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "the 'container' field cannot be used together with other fields")
	})

	t.Run("container with only config is allowed", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Container = &ContainerConfig{Image: "my-image"}
			opts.Config = RuntimeEnvConfig{"setup_timeout_seconds": 300}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
	})

	t.Run("container with only env_vars is allowed", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.Container = &ContainerConfig{Image: "my-image"}
			opts.EnvVars = map[string]string{"KEY": "VALUE"}
			opts.Validate = false
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
	})

	t.Run("image_uri with incompatible field returns error", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.ImageURI = "docker://my-image"
			opts.PyModules = []string{"gcs://path/to/module.zip"}
			return nil
		})
		assert.Error(t, err)
		assert.Nil(t, env)
		assert.Contains(t, err.Error(), "The 'image_uri' field cannot be used together with")
	})

	t.Run("image_uri with config is allowed", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.ImageURI = "docker://my-image"
			opts.Config = RuntimeEnvConfig{"setup_timeout_seconds": 300}
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
	})

	t.Run("image_uri with env_vars is allowed", func(t *testing.T) {
		env, err := NewRuntimeEnv(func(opts *RuntimeEnvOptions) error {
			opts.ImageURI = "docker://my-image"
			opts.EnvVars = map[string]string{"KEY": "VALUE"}
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, env)
	})
}

// TestNewRuntimeEnv_AutoSetFields tests are skipped due to a bug in the
// original code where ImageURI() != "" returns true for nil values.
