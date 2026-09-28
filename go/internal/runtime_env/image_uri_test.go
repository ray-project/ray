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
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestImageURIPlugin_Name verifies the plugin name.
func TestImageURIPlugin_Name(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	if plugin.Name() != "image_uri" {
		t.Errorf("Expected name 'image_uri', got '%s'", plugin.Name())
	}
}

// TestImageURIPlugin_Priority verifies the plugin priority.
func TestImageURIPlugin_Priority(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	if priority := plugin.Priority(); priority != 10 {
		t.Errorf("Expected priority 10, got %d", priority)
	}
}

// TestImageURIPlugin_Validate_Empty checks validation of an empty config.
func TestImageURIPlugin_Validate_Empty(t *testing.T) {
	plugin := &ImageURIPlugin{}
	runtimeEnv := &RuntimeEnv{}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for empty config, got %v", err)
	}
}

// TestImageURIPlugin_Validate_Valid checks validation of a valid image_uri.
func TestImageURIPlugin_Validate_Valid(t *testing.T) {
	plugin := &ImageURIPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldImageURI: "rayproject/ray:nightly-py39",
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid image_uri, got %v", err)
	}
}

// TestImageURIPlugin_Validate_InvalidType checks validation of an invalid type.
func TestImageURIPlugin_Validate_InvalidType(t *testing.T) {
	plugin := &ImageURIPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldImageURI: 123, // Not a string.
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for invalid type, got nil")
	}
}

// TestImageURIPlugin_Validate_EmptyString checks validation of an empty string.
func TestImageURIPlugin_Validate_EmptyString(t *testing.T) {
	plugin := &ImageURIPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldImageURI: "",
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for empty string, got nil")
	}
}

// TestImageURIPlugin_GetURIs_Empty checks GetURIs with an empty config.
func TestImageURIPlugin_GetURIs_Empty(t *testing.T) {
	plugin := &ImageURIPlugin{}
	runtimeEnv := &RuntimeEnv{}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("Expected 0 URIs, got %d", len(uris))
	}
}

// TestImageURIPlugin_GetURIs_WithURI checks GetURIs.
func TestImageURIPlugin_GetURIs_WithURI(t *testing.T) {
	plugin := &ImageURIPlugin{}
	expectedURI := "rayproject/ray:nightly-py39"
	runtimeEnv := &RuntimeEnv{
		FieldImageURI: expectedURI,
	}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 1 {
		t.Errorf("Expected 1 URI, got %d", len(uris))
	}
	if uris[0] != expectedURI {
		t.Errorf("Expected URI = %s, got %s", expectedURI, uris[0])
	}
}

// TestImageURIPlugin_ModifyContext_Empty checks ModifyContext with empty URIs.
func TestImageURIPlugin_ModifyContext_Empty(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	err = plugin.ModifyContext([]string{}, nil, context)
	if err != nil {
		t.Errorf("Expected no error for empty URIs, got %v", err)
	}
}

// TestContainerPlugin_Name verifies the container plugin name.
func TestContainerPlugin_Name(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	if plugin.Name() != "container" {
		t.Errorf("Expected name 'container', got '%s'", plugin.Name())
	}
}

// TestContainerPlugin_Priority verifies the container plugin priority.
func TestContainerPlugin_Priority(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	if priority := plugin.Priority(); priority != RayRuntimeEnvPluginDefaultPriority {
		t.Errorf("Expected priority %d, got %d", RayRuntimeEnvPluginDefaultPriority, priority)
	}
}

// TestContainerPlugin_Validate_Empty checks validation of an empty config.
func TestContainerPlugin_Validate_Empty(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for empty config, got %v", err)
	}
}

// TestContainerPlugin_Validate_Valid checks validation of a valid container config.
func TestContainerPlugin_Validate_Valid(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image": "rayproject/ray:nightly-py39",
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid container config, got %v", err)
	}
}

// TestContainerPlugin_Validate_WithWorkerPath checks a config with worker_path.
func TestContainerPlugin_Validate_WithWorkerPath(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:nightly-py39",
			"worker_path": "/app/worker.py",
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid config with worker_path, got %v", err)
	}
}

// TestContainerPlugin_Validate_WithRunOptions checks a config with run_options.
func TestContainerPlugin_Validate_WithRunOptions(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:nightly-py39",
			"run_options": []string{"--memory=4g", "--cpus=2"},
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid config with run_options, got %v", err)
	}
}

// TestContainerPlugin_Validate_MissingImage checks a config missing the image field.
func TestContainerPlugin_Validate_MissingImage(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"worker_path": "/app/worker.py",
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for missing image, got nil")
	}
}

// TestContainerPlugin_Validate_InvalidImageType checks an invalid image field type.
func TestContainerPlugin_Validate_InvalidImageType(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image": 123, // Not a string.
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for invalid image type, got nil")
	}
}

// TestContainerPlugin_Validate_InvalidWorkerPathType checks an invalid worker_path field type.
func TestContainerPlugin_Validate_InvalidWorkerPathType(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:nightly-py39",
			"worker_path": 123, // Not a string.
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for invalid worker_path type, got nil")
	}
}

// TestContainerPlugin_Validate_InvalidRunOptionsType checks an invalid run_options field type.
func TestContainerPlugin_Validate_InvalidRunOptionsType(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:nightly-py39",
			"run_options": "not_an_array", // Not an array.
		},
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for invalid run_options type, got nil")
	}
}

// TestContainerPlugin_GetURIs_Empty checks GetURIs with an empty config.
func TestContainerPlugin_GetURIs_Empty(t *testing.T) {
	plugin := &ContainerPlugin{}
	runtimeEnv := &RuntimeEnv{}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("Expected 0 URIs, got %d", len(uris))
	}
}

// TestContainerPlugin_GetURIs_WithImage checks GetURIs.
func TestContainerPlugin_GetURIs_WithImage(t *testing.T) {
	plugin := &ContainerPlugin{}
	expectedURI := "rayproject/ray:nightly-py39"
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image": expectedURI,
		},
	}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 1 {
		t.Errorf("Expected 1 URI, got %d", len(uris))
	}
	if uris[0] != expectedURI {
		t.Errorf("Expected URI = %s, got %s", expectedURI, uris[0])
	}
}

// TestContainerPlugin_ModifyContext_Empty checks ModifyContext with empty URIs.
func TestContainerPlugin_ModifyContext_Empty(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	err = plugin.ModifyContext([]string{}, nil, context)
	if err != nil {
		t.Errorf("Expected no error for empty URIs, got %v", err)
	}
}

// TestNewImageURIPlugin checks the image URI plugin constructor.
func TestNewImageURIPlugin(t *testing.T) {
	rayTmpDir := "/tmp/ray_test"
	plugin, err := NewImageURIPlugin(rayTmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if plugin == nil {
		t.Fatal("Expected non-nil plugin")
	}
	if plugin.Name() != "image_uri" {
		t.Errorf("Expected name 'image_uri', got '%s'", plugin.Name())
	}
	if plugin.Priority() != 10 {
		t.Errorf("Expected priority 10, got %d", plugin.Priority())
	}
}

// TestNewContainerPlugin checks the container plugin constructor.
func TestNewContainerPlugin(t *testing.T) {
	rayTmpDir := "/tmp/ray_test"
	plugin, err := NewContainerPlugin(rayTmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if plugin == nil {
		t.Fatal("Expected non-nil plugin")
	}
	if plugin.Name() != "container" {
		t.Errorf("Expected name 'container', got '%s'", plugin.Name())
	}
	if plugin.Priority() != 10 {
		t.Errorf("Expected priority 10, got %d", plugin.Priority())
	}
}

// TestImageURIPlugin_DeleteURI checks URI deletion (should return 0).
func TestImageURIPlugin_DeleteURI(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}

	size := plugin.DeleteURI("rayproject/ray:nightly")
	if size != 0 {
		t.Errorf("Expected deleted size 0 for container image, got %d", size)
	}
}

// TestContainerPlugin_DeleteURI checks URI deletion (should return 0).
func TestContainerPlugin_DeleteURI(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}

	size := plugin.DeleteURI("rayproject/ray:nightly")
	if size != 0 {
		t.Errorf("Expected deleted size 0 for container image, got %d", size)
	}
}

// TestImageURIPlugin_ModifyContext_SingleURI checks ModifyContext with a single URI.
func TestImageURIPlugin_ModifyContext_SingleURI(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	plugin.workerPath = "/opt/conda/lib/python3.9/site-packages/ray/_private/workers/default_worker.py"
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	uris := []string{"rayproject/ray:nightly-py39"}
	err = plugin.ModifyContext(uris, nil, context)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that py_executable is set to the container command.
	if context.PyExecutable == "" {
		t.Error("Expected PyExecutable to be set to container command")
	}
	if context.OverrideWorkerEntrypoint == "" {
		t.Error("Expected OverrideWorkerEntrypoint to be set")
	}
}

// TestContainerPlugin_ModifyContext_WithConfig checks ModifyContext with a container config.
func TestContainerPlugin_ModifyContext_WithConfig(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray")
	if err != nil {
		t.Fatal(err)
	}
	plugin.workerPath = "/opt/conda/lib/python3.9/site-packages/ray/_private/workers/default_worker.py"
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}
	runtimeEnv := &RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:nightly-py39",
			"worker_path": "/custom/worker.py",
			"run_options": []string{"--memory=4g"},
		},
	}

	uris := []string{"rayproject/ray:nightly-py39"}
	err = plugin.ModifyContext(uris, runtimeEnv, context)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the custom worker_path is used.
	if context.OverrideWorkerEntrypoint != "/custom/worker.py" {
		t.Errorf("Expected OverrideWorkerEntrypoint = /custom/worker.py, got %s", context.OverrideWorkerEntrypoint)
	}
}

func TestTempDirManager_ConcurrentCreate(t *testing.T) {
	manager := newTempDirManager()

	const numGoroutines = 10
	const opsPerGoroutine = 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make(chan string, numGoroutines*opsPerGoroutine)
	errors := make(chan error, numGoroutines*opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				pattern := "test_concurrent_"
				tmpDir, err := manager.createTempDir(pattern)
				if err != nil {
					errors <- err
					return
				}
				results <- tmpDir

				if err := manager.removeTempDir(tmpDir); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation failed: %v", err)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.tempDirs) != 0 {
		t.Errorf("Expected all temp dirs to be cleaned up, but %d remain", len(manager.tempDirs))
	}
}

func TestTempDirManager_CleanupAll(t *testing.T) {
	manager := newTempDirManager()

	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		tmpDir, err := manager.createTempDir("test_cleanup_")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		dirs[i] = tmpDir

		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			t.Errorf("Temp dir should exist after creation: %s", tmpDir)
		}
	}

	manager.cleanupAll()

	for _, dir := range dirs {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("Temp dir should be deleted after cleanupAll: %s", dir)
		}
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.tempDirs) != 0 {
		t.Error("tempDirs map should be empty after cleanupAll")
	}
}

func TestModifyContextShared_WithRayEnvVars(t *testing.T) {
	originalVars := make(map[string]string)
	testVars := map[string]string{
		"RAY_JOB_ID":   "test-job-123",
		"RAY_ADDRESS":  "localhost:6379",
		"RAY_TEMP_DIR": "/tmp/ray-test",
		"OTHER_VAR":    "should-not-be-included",
	}

	for k, v := range testVars {
		if orig, exists := os.LookupEnv(k); exists {
			originalVars[k] = orig
		}
		os.Setenv(k, v)
	}

	defer func() {
		for k := range testVars {
			if orig, exists := originalVars[k]; exists {
				os.Setenv(k, orig)
			} else {
				os.Unsetenv(k)
			}
		}
	}()

	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	config := ModifyContextConfig{
		RayTmpDir:  "/tmp/ray-test",
		ImageURI:   "rayproject/ray:test",
		WorkerPath: "/opt/worker.py",
		RunOptions: []string{"--memory=2g"},
		Context:    context,
	}

	err := modifyContextShared(config)
	if err != nil {
		t.Fatalf("modifyContextShared failed: %v", err)
	}

	if context.PyExecutable == "" {
		t.Error("PyExecutable should be set")
	}

	if context.OverrideWorkerEntrypoint != "/opt/worker.py" {
		t.Errorf("OverrideWorkerEntrypoint should be /opt/worker.py, got %s", context.OverrideWorkerEntrypoint)
	}

	if !strings.Contains(context.PyExecutable, "RAY_JOB_ID=test-job-123") {
		t.Error("Container command should include RAY_JOB_ID")
	}

	if !strings.Contains(context.PyExecutable, "RAY_ADDRESS=localhost:6379") {
		t.Error("Container command should include RAY_ADDRESS")
	}

	if strings.Contains(context.PyExecutable, "OTHER_VAR") {
		t.Error("Container command should not automatically include non-RAY_ env vars")
	}
}

func TestModifyContextShared_EmptyRunOptions(t *testing.T) {
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	config := ModifyContextConfig{
		RayTmpDir:  "/tmp/ray-test",
		ImageURI:   "rayproject/ray:test",
		WorkerPath: "/opt/worker.py",
		RunOptions: []string{},
		Context:    context,
	}

	err := modifyContextShared(config)
	if err != nil {
		t.Fatalf("modifyContextShared failed with empty run options: %v", err)
	}

	if context.PyExecutable == "" {
		t.Error("PyExecutable should be set even with empty run options")
	}
}

func TestModifyContextSpecialCharacters(t *testing.T) {
	context := &RuntimeEnvContext{
		EnvVars: map[string]string{
			"TEST_VAR": "value with spaces and $pecial chars",
		},
	}

	config := ModifyContextConfig{
		RayTmpDir:  "/tmp/ray-test",
		ImageURI:   "rayproject/ray:test",
		WorkerPath: "/opt/worker.py",
		RunOptions: []string{},
		Context:    context,
	}

	err := modifyContextShared(config)
	if err != nil {
		t.Fatalf("modifyContextShared failed with special characters: %v", err)
	}

	// Verify special characters are handled correctly (env var values should be quoted).
	if !strings.Contains(context.PyExecutable, "TEST_VAR=") {
		t.Error("Container command should include TEST_VAR")
	}

	// Check for some form of quoting (single or double quotes).
	// The actual format is: --env TEST_VAR='value...'
	hasSingleQuotes := strings.Contains(context.PyExecutable, "TEST_VAR='")
	hasDoubleQuotes := strings.Contains(context.PyExecutable, "TEST_VAR=\"")
	if !hasSingleQuotes && !hasDoubleQuotes {
		t.Logf("PyExecutable: %s", context.PyExecutable)
		t.Error("Special characters in env vars should be quoted (either single or double quotes)")
	}
}

func TestBaseContainerPlugin_GetURIs_DifferentTypes(t *testing.T) {
	plugin := &baseContainerPlugin{
		name:     "test",
		priority: 10,
	}

	tests := []struct {
		name         string
		runtimeEnv   RuntimeEnv
		fieldName    string
		expectedURIs []string
	}{
		{
			name:         "empty_runtime_env",
			runtimeEnv:   RuntimeEnv{},
			fieldName:    FieldContainer,
			expectedURIs: []string{},
		},
		{
			name: "nil_container_value",
			runtimeEnv: RuntimeEnv{
				FieldContainer: nil,
			},
			fieldName:    FieldContainer,
			expectedURIs: []string{},
		},
		{
			name: "container_as_map_with_image",
			runtimeEnv: RuntimeEnv{
				FieldContainer: map[string]interface{}{
					"image": "rayproject/ray:test",
				},
			},
			fieldName:    FieldContainer,
			expectedURIs: []string{"rayproject/ray:test"},
		},
		{
			name: "container_as_map_without_image",
			runtimeEnv: RuntimeEnv{
				FieldContainer: map[string]interface{}{
					"worker_path": "/app/worker.py",
				},
			},
			fieldName:    FieldContainer,
			expectedURIs: []string{},
		},
		{
			name: "container_as_string",
			runtimeEnv: RuntimeEnv{
				FieldContainer: "rayproject/ray:test",
			},
			fieldName:    FieldContainer,
			expectedURIs: []string{"rayproject/ray:test"},
		},
		{
			name: "container_as_empty_string",
			runtimeEnv: RuntimeEnv{
				FieldContainer: "",
			},
			fieldName:    FieldContainer,
			expectedURIs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uris := plugin.GetURIs(&tt.runtimeEnv, tt.fieldName)

			if len(uris) != len(tt.expectedURIs) {
				t.Errorf("Expected %d URIs, got %d", len(tt.expectedURIs), len(uris))
			}

			for i, expected := range tt.expectedURIs {
				if i >= len(uris) {
					t.Errorf("Missing URI at index %d", i)
					continue
				}
				if uris[i] != expected {
					t.Errorf("Expected URI %s, got %s", expected, uris[i])
				}
			}
		})
	}
}

func TestImageURIPlugin_Create_ContextCancellation(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray-test")
	if err != nil {
		t.Fatal(err)
	}

	runtimeEnvContext := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = plugin.Create(ctx, "rayproject/ray:test", &RuntimeEnv{}, runtimeEnvContext)
	if err == nil {
		t.Error("Expected error when context is cancelled, got nil")
	}

	if !strings.Contains(err.Error(), "context canceled") &&
		!strings.Contains(err.Error(), "context deadline exceeded") {
		t.Logf("Got error (may be context or podman related): %v", err)
	}
}

func TestContainerPlugin_ModifyContext_InvalidConfigType(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray-test")
	if err != nil {
		t.Fatal(err)
	}

	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	runtimeEnv := RuntimeEnv{
		FieldContainer: "not_a_map",
	}

	uris := []string{"rayproject/ray:test"}
	err = plugin.ModifyContext(uris, &runtimeEnv, context)
	if err == nil {
		t.Error("Expected error for invalid container config type, got nil")
	}

	if !strings.Contains(err.Error(), "invalid container config type") {
		t.Errorf("Expected 'invalid container config type' error, got: %v", err)
	}
}

func TestDeleteURI_WithNonExistentDir(t *testing.T) {
	plugin, err := NewImageURIPlugin("/tmp/ray-test-nonexistent")
	if err != nil {
		t.Fatal(err)
	}

	size := plugin.DeleteURI("rayproject/ray:test")
	if size != 0 {
		t.Errorf("Expected deleted size 0, got %d", size)
	}
}

func TestGlobalTempDirManager_Isolation(t *testing.T) {
	dir1, err := globalTempDirManager.createTempDir("test_isolation_1_")
	if err != nil {
		t.Fatalf("Failed to create first temp dir: %v", err)
	}

	dir2, err := globalTempDirManager.createTempDir("test_isolation_2_")
	if err != nil {
		t.Fatalf("Failed to create second temp dir: %v", err)
	}

	if dir1 == dir2 {
		t.Error("Two temp dirs should have different paths")
	}

	if _, err := os.Stat(dir1); os.IsNotExist(err) {
		t.Errorf("First temp dir should exist: %s", dir1)
	}
	if _, err := os.Stat(dir2); os.IsNotExist(err) {
		t.Errorf("Second temp dir should exist: %s", dir2)
	}

	if err := globalTempDirManager.removeTempDir(dir1); err != nil {
		t.Errorf("Failed to remove first temp dir: %v", err)
	}

	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Error("First temp dir should be deleted")
	}
	if _, err := os.Stat(dir2); os.IsNotExist(err) {
		t.Error("Second temp dir should still exist")
	}

	globalTempDirManager.removeTempDir(dir2)
}

func TestModifyContextShared_CommandStructure(t *testing.T) {
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	config := ModifyContextConfig{
		RayTmpDir:  "/tmp/ray-custom",
		ImageURI:   "custom/repo:image-tag",
		WorkerPath: "/custom/path/worker.py",
		RunOptions: []string{"--memory=4g", "--cpus=2"},
		Context:    context,
	}

	err := modifyContextShared(config)
	if err != nil {
		t.Fatalf("modifyContextShared failed: %v", err)
	}

	cmdStr := context.PyExecutable

	expectedComponents := []string{
		PodmanCommand,
		PodmanRunFlag,
		PodmanVolumeFlag,
		"/tmp/ray-custom:/tmp/ray-custom",
		PodmanCgroupFlag,
		PodmanNetworkFlag,
		PodmanPidFlag,
		PodmanIpcFlag,
		PodmanUsernsFlag,
		PodmanEntrypointFlag,
		"python",
		"custom/repo:image-tag",
		"--memory=4g",
		"--cpus=2",
	}

	for _, component := range expectedComponents {
		if !strings.Contains(cmdStr, component) {
			t.Errorf("Container command missing expected component: %s", component)
		}
	}
}

func TestContainerConfig_WorkerPathOverride(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray-test")
	if err != nil {
		t.Fatal(err)
	}

	plugin.workerPath = "/default/worker.py"

	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	runtimeEnv := RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image":       "rayproject/ray:test",
			"worker_path": "/custom/worker.py",
		},
	}

	uris := []string{"rayproject/ray:test"}
	err = plugin.ModifyContext(uris, &runtimeEnv, context)
	if err != nil {
		t.Fatalf("ModifyContext failed: %v", err)
	}

	if context.OverrideWorkerEntrypoint != "/custom/worker.py" {
		t.Errorf("Expected custom worker_path /custom/worker.py, got %s", context.OverrideWorkerEntrypoint)
	}
}

func TestContainerConfig_RunOptionsParsing(t *testing.T) {
	plugin, err := NewContainerPlugin("/tmp/ray-test")
	if err != nil {
		t.Fatal(err)
	}

	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	runtimeEnv := RuntimeEnv{
		FieldContainer: map[string]interface{}{
			"image": "rayproject/ray:test",
			"run_options": []interface{}{
				"--memory=4g",
				"--cpus=2",
				"--shm-size=1g",
			},
		},
	}

	uris := []string{"rayproject/ray:test"}
	err = plugin.ModifyContext(uris, &runtimeEnv, context)
	if err != nil {
		t.Fatalf("ModifyContext failed: %v", err)
	}

	cmdStr := context.PyExecutable

	expectedOptions := []string{"--memory=4g", "--cpus=2", "--shm-size=1g"}
	for _, opt := range expectedOptions {
		if !strings.Contains(cmdStr, opt) {
			t.Errorf("Container command missing run option: %s", opt)
		}
	}
}

func TestPullImageAndGetWorkerPathShared_InvalidImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pullImageAndGetWorkerPathShared(ctx, "invalid::image::uri")
	if err == nil {
		t.Fatal("Expected error with invalid image URI, got nil")
	}

	if !strings.Contains(err.Error(), "podman command failed") &&
		!strings.Contains(err.Error(), "failed to pull") {
		t.Logf("Got error (format may vary): %v", err)
	}
}

func BenchmarkModifyContextShared(b *testing.B) {
	context := &RuntimeEnvContext{
		EnvVars: map[string]string{
			"VAR1": "value1",
			"VAR2": "value2",
			"VAR3": "value3",
		},
	}

	config := ModifyContextConfig{
		RayTmpDir:  "/tmp/ray-bench",
		ImageURI:   "rayproject/ray:bench",
		WorkerPath: "/opt/worker.py",
		RunOptions: []string{"--memory=2g"},
		Context:    context,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = modifyContextShared(config)
	}
}

func BenchmarkTempDirManager_CreateRemove(b *testing.B) {
	manager := newTempDirManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmpDir, err := manager.createTempDir("bench_")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}

		if err := manager.removeTempDir(tmpDir); err != nil {
			b.Fatalf("Failed to remove temp dir: %v", err)
		}
	}
}
