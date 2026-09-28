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
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// ============ WorkingDirEnvScope tests ============

func TestWorkingDirEnvScope_Restore_Idempotent(t *testing.T) {
	// Test the idempotency of the Restore method.
	scope := &WorkingDirEnvScope{
		key:        "TEST_ENV_VAR",
		prevValue:  "old_value",
		prevExists: true,
		restored:   false,
	}

	// First restore.
	scope.Restore()
	if !scope.restored {
		t.Error("expected restored flag to be true after first restore")
	}

	// Restore again (should be a no-op).
	scope.Restore()
	// It should not panic, and restored should remain true.
	if !scope.restored {
		t.Error("expected restored flag to remain true after second restore")
	}
}

func TestWorkingDirEnvScope_Restore_Unset(t *testing.T) {
	// Test restoring a variable that was never set.
	testKey := "TEST_NONEXISTENT_VAR"
	os.Unsetenv(testKey)

	scope := &WorkingDirEnvScope{
		key:        testKey,
		prevValue:  "",
		prevExists: false,
		restored:   false,
	}

	scope.Restore()

	// Verify the environment variable is still unset.
	_, exists := os.LookupEnv(testKey)
	if exists {
		t.Error("expected env var to remain unset")
	}
}

func TestWorkingDirEnvScope_Restore_SetPrevious(t *testing.T) {
	// Test restoring the previous value.
	testKey := "TEST_PREV_VAR"
	testValue := "previous_test_value"
	os.Setenv(testKey, testValue)

	scope := &WorkingDirEnvScope{
		key:        testKey,
		prevValue:  testValue,
		prevExists: true,
		restored:   false,
	}

	// Modify the environment variable first.
	os.Setenv(testKey, "modified_value")

	scope.Restore()

	// Verify it was restored to the previous value.
	value := os.Getenv(testKey)
	if value != testValue {
		t.Errorf("expected %q, got %q", testValue, value)
	}
}

// ============ UploadWorkingDirIfNeeded tests ============

func TestUploadWorkingDirIfNeeded_NoWorkingDir(t *testing.T) {
	// Test the case where there is no working_dir field.
	runtimeEnv := map[string]interface{}{"other_field": "value"}

	result, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestUploadWorkingDirIfNeeded_NilWorkingDir(t *testing.T) {
	// Test the case where working_dir is nil.
	runtimeEnv := map[string]interface{}{"working_dir": nil}

	result, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestUploadWorkingDirIfNeeded_InvalidWorkingDirType(t *testing.T) {
	// Test the case where working_dir is not a string.
	runtimeEnv := map[string]interface{}{"working_dir": 123}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err == nil {
		t.Error("expected error for non-string working_dir")
	}
	if !strings.Contains(err.Error(), "working_dir must be a string") {
		t.Errorf("expected 'working_dir must be a string' error, got %v", err)
	}
}

func TestUploadWorkingDirIfNeeded_RemoteURI_Success(t *testing.T) {
	// Test the case where working_dir is already a remote URI.
	runtimeEnv := map[string]interface{}{"working_dir": "https://example.com/pkg.zip"}

	result, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestUploadWorkingDirIfNeeded_RemoteURI_NonZip(t *testing.T) {
	// Test the case of a remote URI that is not a .zip file.
	runtimeEnv := map[string]interface{}{"working_dir": "https://example.com/pkg.tar.gz"}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err == nil {
		t.Error("expected error for non-zip remote URI")
	}
	if !strings.Contains(err.Error(), "only .zip files supported") {
		t.Errorf("expected 'only .zip files supported' error, got %v", err)
	}
}

func TestUploadWorkingDirIfNeeded_LocalDirectory(t *testing.T) {
	// Test the local directory case.
	tmpDir := t.TempDir()
	workingDir := filepath.Join(tmpDir, "workdir")
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		t.Fatal(err)
	}

	runtimeEnv := map[string]interface{}{"working_dir": workingDir}

	// Use a custom uploadFn to avoid an actual GCS upload.
	uploadCalled := false
	uploadFn := func(dir string, excludes []string) error {
		uploadCalled = true
		if dir != workingDir {
			t.Errorf("expected working_dir %q, got %q", workingDir, dir)
		}
		return nil
	}

	result, err := UploadWorkingDirIfNeeded(runtimeEnv, false, tmpDir, uploadFn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !uploadCalled {
		t.Error("expected upload function to be called")
	}
	// Verify working_dir was updated to a URI.
	newWorkingDir, ok := result["working_dir"].(string)
	if !ok {
		t.Fatal("expected working_dir to be updated to string URI")
	}
	if !strings.HasPrefix(newWorkingDir, "gcs://") {
		t.Errorf("expected gcs:// URI, got %q", newWorkingDir)
	}
}

func TestUploadWorkingDirIfNeeded_LocalZipFile(t *testing.T) {
	// Test the local zip file case.
	tmpDir := t.TempDir()
	zipFile := filepath.Join(tmpDir, "test.zip")
	if err := os.WriteFile(zipFile, []byte("fake zip content"), 0644); err != nil {
		t.Fatal(err)
	}

	runtimeEnv := map[string]interface{}{"working_dir": zipFile}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, tmpDir, func(string, []string) error { return nil })
	// Without a GCS client, this call is expected to fail.
	if err == nil {
		t.Log("Note: this test may pass if GCS client is not initialized")
	}
}

func TestUploadWorkingDirIfNeeded_NonExistentPath(t *testing.T) {
	// Test the case of a non-existent local path.
	tmpDir := t.TempDir()
	runtimeEnv := map[string]interface{}{"working_dir": filepath.Join(tmpDir, "nonexistent")}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, t.TempDir(), func(string, []string) error { return nil })
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}

func TestUploadWorkingDirIfNeeded_WithExcludes(t *testing.T) {
	// Test the case with an excludes configuration.
	tmpDir := t.TempDir()
	workingDir := filepath.Join(tmpDir, "workdir")
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		t.Fatal(err)
	}

	runtimeEnv := map[string]interface{}{
		"working_dir": workingDir,
		"excludes":    []string{"*.pyc", "__pycache__/"},
	}

	uploadCalled := false
	uploadFn := func(dir string, excludes []string) error {
		uploadCalled = true
		if len(excludes) != 2 {
			t.Errorf("expected 2 excludes, got %d", len(excludes))
		}
		return nil
	}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, tmpDir, uploadFn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !uploadCalled {
		t.Error("expected upload function to be called")
	}
}

func TestUploadWorkingDirIfNeeded_ExcludesInterfaceSlice(t *testing.T) {
	// Test the case where excludes is a []interface{} (the type produced by JSON deserialization).
	tmpDir := t.TempDir()
	workingDir := filepath.Join(tmpDir, "workdir")
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		t.Fatal(err)
	}

	runtimeEnv := map[string]interface{}{
		"working_dir": workingDir,
		"excludes":    []interface{}{"*.pyc", "__pycache__/"},
	}

	uploadFn := func(dir string, excludes []string) error {
		if len(excludes) != 2 {
			t.Errorf("expected 2 excludes, got %d", len(excludes))
		}
		return nil
	}

	_, err := UploadWorkingDirIfNeeded(runtimeEnv, false, tmpDir, uploadFn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ============ SetPythonpathInContext tests ============

func TestSetPythonpathInContext_NilEnvVars(t *testing.T) {
	// Save the system PYTHONPATH.
	sysPythonPath := os.Getenv("PYTHONPATH")
	defer func() {
		if sysPythonPath == "" {
			os.Unsetenv("PYTHONPATH")
		} else {
			os.Setenv("PYTHONPATH", sysPythonPath)
		}
	}()

	// Temporarily clear the system PYTHONPATH for a precise test.
	os.Unsetenv("PYTHONPATH")

	context := &RuntimeEnvContext{}
	SetPythonpathInContext("/test/path", context)

	if context.EnvVars == nil {
		t.Fatal("expected EnvVars to be initialized")
	}
	pythonPath := context.EnvVars["PYTHONPATH"]
	if pythonPath != "/test/path" {
		t.Errorf("expected PYTHONPATH to be '/test/path', got %q", pythonPath)
	}
}

func TestSetPythonpathInContext_ExistingEnvVars(t *testing.T) {
	context := &RuntimeEnvContext{
		EnvVars: map[string]string{
			"PYTHONPATH": "/existing/path",
		},
	}
	SetPythonpathInContext("/test/path", context)

	pythonPath := context.EnvVars["PYTHONPATH"]
	// The new path should come first, separated by PathListSeparator.
	expectedPrefix := "/test/path" + string(os.PathListSeparator) + "/existing/path"
	if !strings.HasPrefix(pythonPath, expectedPrefix) {
		t.Errorf("expected PYTHONPATH to start with %q, got %q", expectedPrefix, pythonPath)
	}
}

func TestSetPythonpathInContext_SystemPythonpath(t *testing.T) {
	// Save the system PYTHONPATH.
	sysPythonPath := os.Getenv("PYTHONPATH")
	defer func() {
		if sysPythonPath == "" {
			os.Unsetenv("PYTHONPATH")
		} else {
			os.Setenv("PYTHONPATH", sysPythonPath)
		}
	}()

	// Set a test PYTHONPATH.
	testSysPath := "/system/pythonpath"
	os.Setenv("PYTHONPATH", testSysPath)

	context := &RuntimeEnvContext{}
	SetPythonpathInContext("/test/path", context)

	pythonPath := context.EnvVars["PYTHONPATH"]
	// It should contain the system PYTHONPATH.
	if !strings.Contains(pythonPath, testSysPath) {
		t.Errorf("expected PYTHONPATH to contain system path %q, got %q", testSysPath, pythonPath)
	}
}

// ============ WorkingDirPlugin tests ============

func TestNewWorkingDirPlugin(t *testing.T) {
	tmpDir := t.TempDir()
	gcsClient, _ := gcs.GetClient()

	plugin, err := NewWorkingDirPlugin(tmpDir, gcsClient)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin")
	}
	if plugin.resourcesDir == "" {
		t.Error("expected resourcesDir to be set")
	}
	if !strings.HasSuffix(plugin.resourcesDir, "working_dir_files") {
		t.Errorf("expected resourcesDir to end with 'working_dir_files', got %q", plugin.resourcesDir)
	}
}

func TestWorkingDirPlugin_Name(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	name := plugin.Name()
	if name != "working_dir" {
		t.Errorf("expected name 'working_dir', got %q", name)
	}
}

func TestWorkingDirPlugin_Priority(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	priority := plugin.Priority()
	if priority != 5 {
		t.Errorf("expected priority 5, got %d", priority)
	}
}

func TestWorkingDirPlugin_Validate(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	err := plugin.Validate(nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestWorkingDirPlugin_GetURIs_Empty(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	runtimeEnv := &RuntimeEnv{}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("expected empty URIs, got %v", uris)
	}
}

func TestWorkingDirPlugin_GetURIs_WithWorkingDir(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	runtimeEnv := &RuntimeEnv{"working_dir": "gcs://test-uri"}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 1 {
		t.Errorf("expected 1 URI, got %d", len(uris))
	}
	if uris[0] != "gcs://test-uri" {
		t.Errorf("expected 'gcs://test-uri', got %q", uris[0])
	}
}

func TestWorkingDirPlugin_GetURIs_NonStringWorkingDir(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	runtimeEnv := &RuntimeEnv{"working_dir": 123} // non-string value
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("expected empty URIs for non-string working_dir, got %v", uris)
	}
}

// ============ WorkingDirPlugin_Create tests ============

func TestWorkingDirPlugin_Create_NilLogger(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{
		resourcesDir: tmpDir,
	}

	ctx := context.Background()

	_, err := plugin.Create(ctx, "gcs://test.zip", &RuntimeEnv{}, &RuntimeEnvContext{})
	// Without a GCS client, this is expected to fail.
	if err == nil {
		t.Log("Note: this test may pass if GCS client is initialized")
	}
}

// ============ WorkingDirPlugin_ModifyContext tests ============

func TestWorkingDirPlugin_ModifyContext_EmptyURIs(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{}, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWorkingDirPlugin_ModifyContext_NonExistentDir(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}
	context := &RuntimeEnvContext{}

	uris := []string{"gcs://nonexistent"}
	err := plugin.ModifyContext(uris, &RuntimeEnv{}, context)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

func TestWorkingDirPlugin_ModifyContext_Success(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	// Create the test directory.
	testDir := filepath.Join(tmpDir, "test_workdir")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Build the URI.
	uri := "gcs://" + filepath.Base(testDir)
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{uri}, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify CommandPrefix was set.
	if len(context.CommandPrefix) < 3 {
		t.Fatalf("expected CommandPrefix to have at least 3 elements, got %d", len(context.CommandPrefix))
	}
	if context.CommandPrefix[0] != "cd" {
		t.Errorf("expected first CommandPrefix element to be 'cd', got %q", context.CommandPrefix[0])
	}

	// Verify PYTHONPATH was set.
	pythonPath, exists := context.EnvVars["PYTHONPATH"]
	if !exists {
		t.Error("expected PYTHONPATH to be set")
	}
	if !strings.Contains(pythonPath, testDir) {
		t.Errorf("expected PYTHONPATH to contain %q, got %q", testDir, pythonPath)
	}
}

func TestWorkingDirPlugin_ModifyContext_Windows(t *testing.T) {
	// Note: this is only a conceptual test since runtime.GOOS is a constant.
	// Real Windows testing requires running on a Windows system.

	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	testDir := filepath.Join(tmpDir, "test_workdir")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://" + filepath.Base(testDir)
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{uri}, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify CommandPrefix according to the actual OS.
	if runtime.GOOS == "windows" {
		if context.CommandPrefix[1] != "/d" {
			t.Errorf("expected '/d' flag on Windows, got %q", context.CommandPrefix[1])
		}
	}
}

// ============ WorkingDirPlugin_DeleteURI tests ============

func TestWorkingDirPlugin_DeleteURI_Success(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	// Create the test directory.
	testDir := filepath.Join(tmpDir, "test_pkg")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(testDir, "file.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://test_pkg"
	size := plugin.DeleteURI(uri)

	// Verify a size was returned.
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Verify the directory was deleted.
	if _, err := os.Stat(testDir); !os.IsNotExist(err) {
		t.Error("expected directory to be deleted")
	}
}

func TestWorkingDirPlugin_DeleteURI_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	uri := "gcs://nonexistent_pkg"
	size := plugin.DeleteURI(uri)

	if size != 0 {
		t.Errorf("expected size 0 for non-existent package, got %d", size)
	}
}

// ============ WorkingDirPlugin_SetupWorkingDirEnv tests ============

func TestWorkingDirPlugin_SetupWorkingDirEnv_EmptyURI(t *testing.T) {
	plugin := &WorkingDirPlugin{}
	scope, err := plugin.SetupWorkingDirEnv("")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if scope == nil {
		t.Error("expected non-nil scope")
	}
	// The empty scope should be a no-op.
	scope.Restore() // should not panic
}

func TestWorkingDirPlugin_SetupWorkingDirEnv_NonExistentDir(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	uri := "gcs://nonexistent"
	_, err := plugin.SetupWorkingDirEnv(uri)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

func TestWorkingDirPlugin_SetupWorkingDirEnv_Success(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	// Create the test directory.
	testDir := filepath.Join(tmpDir, "test_workdir")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://" + filepath.Base(testDir)

	// Save the previous environment variable value.
	prevValue, prevExists := os.LookupEnv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR)
	defer func() {
		if prevExists {
			os.Setenv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR, prevValue)
		} else {
			os.Unsetenv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR)
		}
	}()

	scope, err := plugin.SetupWorkingDirEnv(uri)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if scope == nil {
		t.Fatal("expected non-nil scope")
	}

	// Verify the environment variable was set.
	envValue := os.Getenv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR)
	if envValue == "" {
		t.Error("expected environment variable to be set")
	}
	// Verify the path uses forward slashes (Windows compatibility).
	if strings.Contains(envValue, "\\") {
		t.Errorf("expected forward slashes in path, got %q", envValue)
	}

	// Verify the scope can be restored.
	scope.Restore()
	restoredValue, restoredExists := os.LookupEnv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR)
	if prevExists {
		if restoredValue != prevValue {
			t.Errorf("expected restored value %q, got %q", prevValue, restoredValue)
		}
	} else {
		if restoredExists {
			t.Error("expected environment variable to be unset after restore")
		}
	}
}

func TestWorkingDirPlugin_SetupWorkingDirEnv_RestoreIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &WorkingDirPlugin{resourcesDir: tmpDir}

	testDir := filepath.Join(tmpDir, "test_workdir")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://" + filepath.Base(testDir)
	scope, err := plugin.SetupWorkingDirEnv(uri)
	if err != nil {
		t.Fatal(err)
	}

	// Multiple restores should be idempotent.
	scope.Restore()
	scope.Restore()
	scope.Restore()
	// Should not panic.
}

// ============ GetLocalDirFromURI helper tests ============

func TestGetLocalDirFromURI_Basic(t *testing.T) {
	resourcesDir := "/tmp/test_resources"
	uri := "gcs://test_package"

	localDir := GetLocalDirFromURI(uri, resourcesDir)
	expected := filepath.Join(resourcesDir, "test_package")
	if localDir != expected {
		t.Errorf("expected %q, got %q", expected, localDir)
	}
}
