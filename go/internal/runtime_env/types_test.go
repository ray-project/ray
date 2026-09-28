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

// TestFieldConstants tests the field constant values
func TestFieldConstants(t *testing.T) {
	// Verify that field constants are defined and non-empty
	assert.NotEmpty(t, FieldPyModules)
	assert.NotEmpty(t, FieldPyExecutable)
	assert.NotEmpty(t, FieldWorkingDir)
	assert.NotEmpty(t, FieldJavaJars)
	assert.NotEmpty(t, FieldConda)
	assert.NotEmpty(t, FieldPip)
	assert.NotEmpty(t, FieldUv)
	assert.NotEmpty(t, FieldContainer)
	assert.NotEmpty(t, FieldEnvVars)
	assert.NotEmpty(t, FieldConfig)
	assert.NotEmpty(t, FieldImageURI)
	assert.NotEmpty(t, FieldNsight)
	assert.NotEmpty(t, FieldRocprofSys)
	assert.NotEmpty(t, FieldWorkerProcessSetupHook)
	assert.NotEmpty(t, FieldRayRelease)
	assert.NotEmpty(t, FieldRayCommit)
	assert.NotEmpty(t, FieldInjectCurrentRay)
	assert.NotEmpty(t, FieldExcludes)
}

// TestKnownFieldsSlice tests the KnownFields slice
func TestKnownFieldsSlice(t *testing.T) {
	// Verify that KnownFields is defined and contains expected fields
	assert.NotNil(t, KnownFields)

	// Check some expected fields exist in KnownFields
	expectedFields := []string{
		"py_modules",
		"py_executable",
		"working_dir",
		"java_jars",
		"conda",
		"pip",
		"uv",
		"container",
		"env_vars",
		"config",
		"image_uri",
	}

	for _, field := range expectedFields {
		assert.Contains(t, KnownFields, field, "KnownFields should contain %s", field)
	}
}

// TestExtensionFieldsSlice tests the ExtensionFields slice
func TestExtensionFieldsSlice(t *testing.T) {
	// Verify that ExtensionFields contains expected keys
	assert.NotNil(t, ExtensionFields)

	// Check expected extension fields exist
	expectedExtensionFields := []string{
		"_ray_release",
		"_ray_commit",
		"_inject_current_ray",
	}

	for _, field := range expectedExtensionFields {
		assert.Contains(t, ExtensionFields, field, "ExtensionFields should contain %s", field)
	}
}

// TestImageURICompatibleKeysSlice tests the ImageURICompatibleKeys slice
func TestImageURICompatibleKeysSlice(t *testing.T) {
	// Verify that ImageURICompatibleKeys contains expected keys
	assert.NotNil(t, ImageURICompatibleKeys)

	// Check expected compatible keys exist
	expectedCompatibleKeys := []string{
		"image_uri",
		"config",
		"env_vars",
	}

	for _, key := range expectedCompatibleKeys {
		assert.Contains(t, ImageURICompatibleKeys, key, "ImageURICompatibleKeys should contain %s", key)
	}
}

// TestContainerConfigStruct tests the ContainerConfig struct
func TestContainerConfigStruct(t *testing.T) {
	config := &ContainerConfig{
		Image:      "my-image:latest",
		WorkerPath: "/path/to/worker",
		RunOptions: []string{"--rm", "-v", "/data:/data"},
	}

	assert.Equal(t, "my-image:latest", config.Image)
	assert.Equal(t, "/path/to/worker", config.WorkerPath)
	assert.Equal(t, []string{"--rm", "-v", "/data:/data"}, config.RunOptions)
}

// TestRuntimeEnvOptionsStruct tests the RuntimeEnvOptions struct
func TestRuntimeEnvOptionsStruct(t *testing.T) {
	opts := &RuntimeEnvOptions{
		PyModules:    []string{"mod1", "mod2"},
		PyExecutable: "/usr/bin/python",
		WorkingDir:   "/workspace",
		Validate:     true,
	}

	assert.Equal(t, []string{"mod1", "mod2"}, opts.PyModules)
	assert.Equal(t, "/usr/bin/python", opts.PyExecutable)
	assert.Equal(t, "/workspace", opts.WorkingDir)
	assert.True(t, opts.Validate)
}
