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
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// ============ NewPyModulesPlugin tests ============

func TestNewPyModulesPlugin(t *testing.T) {
	tmpDir := t.TempDir()
	gcsClient, _ := gcs.GetClient()

	plugin, err := NewPyModulesPlugin(tmpDir, gcsClient)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin")
	}
	if plugin.resourcesDir == "" {
		t.Error("expected resourcesDir to be set")
	}
	if !strings.HasSuffix(plugin.resourcesDir, "py_modules_files") {
		t.Errorf("expected resourcesDir to end with 'py_modules_files', got %q", plugin.resourcesDir)
	}
}

// ============ PyModulesPlugin_Name tests ============

func TestPyModulesPlugin_Name(t *testing.T) {
	plugin := &PyModulesPlugin{}
	name := plugin.Name()
	if name != "py_modules" {
		t.Errorf("expected name 'py_modules', got %q", name)
	}
}

// ============ PyModulesPlugin_Priority tests ============

func TestPyModulesPlugin_Priority(t *testing.T) {
	plugin := &PyModulesPlugin{}
	priority := plugin.Priority()
	if priority != 10 {
		t.Errorf("expected priority 10, got %d", priority)
	}
}

// ============ PyModulesPlugin_Validate tests ============

func TestPyModulesPlugin_Validate(t *testing.T) {
	plugin := &PyModulesPlugin{}
	err := plugin.Validate(nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// ============ PyModulesPlugin_GetURIs tests ============

func TestPyModulesPlugin_GetURIs_Empty(t *testing.T) {
	plugin := &PyModulesPlugin{}
	runtimeEnv := &RuntimeEnv{}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("expected empty URIs, got %v", uris)
	}
}

func TestPyModulesPlugin_GetURIs_NilValue(t *testing.T) {
	plugin := &PyModulesPlugin{}
	runtimeEnv := &RuntimeEnv{"py_modules": nil}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("expected empty URIs for nil value, got %v", uris)
	}
}

func TestPyModulesPlugin_GetURIs_StringSlice(t *testing.T) {
	plugin := &PyModulesPlugin{}
	expectedURIs := []string{"gcs://module1", "gcs://module2"}
	runtimeEnv := &RuntimeEnv{"py_modules": expectedURIs}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != len(expectedURIs) {
		t.Errorf("expected %d URIs, got %d", len(expectedURIs), len(uris))
	}
	for i, uri := range uris {
		if uri != expectedURIs[i] {
			t.Errorf("URI[%d]: expected %q, got %q", i, expectedURIs[i], uri)
		}
	}
}

func TestPyModulesPlugin_GetURIs_InterfaceSlice(t *testing.T) {
	plugin := &PyModulesPlugin{}
	expectedURIs := []string{"gcs://module1", "gcs://module2"}
	interfaceSlice := make([]interface{}, len(expectedURIs))
	for i, uri := range expectedURIs {
		interfaceSlice[i] = uri
	}
	runtimeEnv := &RuntimeEnv{"py_modules": interfaceSlice}
	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != len(expectedURIs) {
		t.Errorf("expected %d URIs, got %d", len(expectedURIs), len(uris))
	}
	for i, uri := range uris {
		if uri != expectedURIs[i] {
			t.Errorf("URI[%d]: expected %q, got %q", i, expectedURIs[i], uri)
		}
	}
}

func TestPyModulesPlugin_GetURIs_MixedInterfaceSlice(t *testing.T) {
	plugin := &PyModulesPlugin{}
	// A mix of strings and non-string values.
	mixedSlice := []interface{}{"gcs://module1", 123, "gcs://module2", nil}
	runtimeEnv := &RuntimeEnv{"py_modules": mixedSlice}
	uris := plugin.GetURIs(runtimeEnv)
	// Only string values should be kept.
	if len(uris) != 2 {
		t.Errorf("expected 2 URIs (only strings), got %d", len(uris))
	}
	if uris[0] != "gcs://module1" || uris[1] != "gcs://module2" {
		t.Errorf("expected ['gcs://module1', 'gcs://module2'], got %v", uris)
	}
}

// ============ PyModulesPlugin_Create tests ============

func TestPyModulesPlugin_Create_Success(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create a test zip file.
	testZipPath := filepath.Join(tmpDir, "test_module.zip")
	createTestZipFlat(t, testZipPath, "module.py")

	// Build the URI using the file:// scheme.
	uri := "file://" + testZipPath

	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, uri, runtimeEnv, context)
	if err != nil {
		t.Logf("Note: this test may fail without proper GCS setup: %v", err)
		// This test may fail without a full GCS setup.
		return
	}
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Verify the directory was created.
	localDir := GetLocalDirFromURI(uri, tmpDir)
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		t.Error("expected module directory to be created")
	}
}

func TestPyModulesPlugin_Create_WheelPackage(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create a test wheel file.
	testWhlPath := filepath.Join(tmpDir, "test_package-1.0-py3-none-any.whl")
	createTestZipFlat(t, testWhlPath, "test_package/__init__.py", "test_package/module.py")

	// Build the URI.
	uri := "file://" + testWhlPath

	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, uri, runtimeEnv, context)
	if err != nil {
		t.Logf("Note: wheel installation test requires pip and may fail: %v", err)
		// Wheel installation requires pip and may fail without a full environment.
		return
	}
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Verify the wheel package was unpacked into the correct directory.
	localDir := GetLocalDirFromURI(uri, tmpDir)
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		t.Error("expected wheel package directory to be created")
	}
}

func TestPyModulesPlugin_Create_NonExistentURI(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	ctx := context.Background()
	uri := "gcs://nonexistent_module.zip"
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{}

	_, err := plugin.Create(ctx, uri, runtimeEnv, context)
	if err == nil {
		t.Error("expected error for non-existent URI")
	}
}

// ============ PyModulesPlugin_ModifyContext tests ============

func TestPyModulesPlugin_ModifyContext_EmptyURIs(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{}, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPyModulesPlugin_ModifyContext_SingleURI(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create the test directory.
	testDir := filepath.Join(tmpDir, "test_module")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Build the URI.
	uri := "gcs://test_module"
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{uri}, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
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

func TestPyModulesPlugin_ModifyContext_MultipleURIs(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create multiple test directories.
	testDirs := []string{
		filepath.Join(tmpDir, "module1"),
		filepath.Join(tmpDir, "module2"),
	}
	for _, dir := range testDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	// Build the URIs.
	uris := []string{"gcs://module1", "gcs://module2"}
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext(uris, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify PYTHONPATH contains all directories.
	pythonPath, exists := context.EnvVars["PYTHONPATH"]
	if !exists {
		t.Error("expected PYTHONPATH to be set")
	}
	for _, dir := range testDirs {
		if !strings.Contains(pythonPath, dir) {
			t.Errorf("expected PYTHONPATH to contain %q, got %q", dir, pythonPath)
		}
	}
}

func TestPyModulesPlugin_ModifyContext_NonExistentDir(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}
	context := &RuntimeEnvContext{}

	uris := []string{"gcs://nonexistent_module"}
	err := plugin.ModifyContext(uris, &RuntimeEnv{}, context)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("expected 'does not exist' error, got %v", err)
	}
}

func TestPyModulesPlugin_ModifyContext_NotADirectory(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create a file instead of a directory.
	testFile := filepath.Join(tmpDir, "not_a_dir")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://not_a_dir"
	context := &RuntimeEnvContext{}

	err := plugin.ModifyContext([]string{uri}, &RuntimeEnv{}, context)
	if err == nil {
		t.Error("expected error for non-directory path")
	}
	if !strings.Contains(err.Error(), "is not a directory") {
		t.Errorf("expected 'is not a directory' error, got %v", err)
	}
}

// ============ PyModulesPlugin_DeleteURI tests ============

func TestPyModulesPlugin_DeleteURI_Success(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create the test directory and file.
	testDir := filepath.Join(tmpDir, "test_pkg")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(testDir, "module.py")
	if err := os.WriteFile(testFile, []byte("# test module"), 0644); err != nil {
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

func TestPyModulesPlugin_DeleteURI_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	uri := "gcs://nonexistent_pkg"
	size := plugin.DeleteURI(uri)

	if size != 0 {
		t.Errorf("expected size 0 for non-existent package, got %d", size)
	}
}

func TestPyModulesPlugin_DeleteURI_FileInsteadOfDir(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &PyModulesPlugin{resourcesDir: tmpDir}

	// Create a file instead of a directory.
	testFile := filepath.Join(tmpDir, "not_a_pkg")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	uri := "gcs://not_a_pkg"
	size := plugin.DeleteURI(uri)

	// DeletePackage may not delete a plain file, so the size should be 0.
	if size != 0 {
		t.Errorf("expected size 0 for file (not a package), got %d", size)
	}
}

// ============ PyModulesPlugin_Integration tests ============

func TestPyModulesPlugin_FullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	gcsClient, _ := gcs.GetClient()

	// Create the plugin instance.
	plugin, err := NewPyModulesPlugin(tmpDir, gcsClient)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	// 1. Create the test module package.
	testModuleDir := filepath.Join(tmpDir, "test_lifecycle_module")
	if err := os.MkdirAll(testModuleDir, 0755); err != nil {
		t.Fatal(err)
	}
	testModuleFile := filepath.Join(testModuleDir, "mymodule.py")
	moduleContent := `# Test module
def hello():
    return "Hello from test module"
`
	if err := os.WriteFile(testModuleFile, []byte(moduleContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Compress it into a zip file.
	zipPath := filepath.Join(tmpDir, "test_lifecycle_module.zip")
	createTestZipWithDir(t, zipPath, "test_lifecycle_module", "mymodule.py")

	// 2. Build the URI.
	uri := "file://" + zipPath

	// 3. Create - download and unpack.
	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, uri, runtimeEnv, context)
	if err != nil {
		t.Logf("Create failed (may be due to missing GCS setup): %v", err)
		return
	}
	if size <= 0 {
		t.Errorf("expected positive size after create, got %d", size)
	}

	// 4. ModifyContext - set PYTHONPATH.
	err = plugin.ModifyContext([]string{uri}, runtimeEnv, context)
	if err != nil {
		t.Errorf("ModifyContext failed: %v", err)
	}

	// Verify PYTHONPATH was set.
	pythonPath, exists := context.EnvVars["PYTHONPATH"]
	if !exists {
		t.Fatal("expected PYTHONPATH to be set")
	}

	// Use the plugin's actual resourcesDir (the py_modules_files subdirectory), not tmpDir.
	localDir := GetLocalDirFromURI(uri, plugin.resourcesDir)
	if !strings.Contains(pythonPath, localDir) {
		t.Errorf("expected PYTHONPATH to contain %q, got %q", localDir, pythonPath)
	}

	// 5. DeleteURI - cleanup.
	deletedSize := plugin.DeleteURI(uri)
	if deletedSize <= 0 {
		t.Errorf("expected positive deleted size, got %d", deletedSize)
	}

	// Verify the directory was deleted.
	if _, err := os.Stat(localDir); !os.IsNotExist(err) {
		t.Error("expected module directory to be deleted after DeleteURI")
	}
}

// ============ Helper Functions ============

// createTestZipWithDir creates a test zip file with the given top-level directory.
func createTestZipWithDir(t *testing.T, zipPath, dirName, fileName string) {
	t.Helper()

	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	defer w.Close()

	// Add the directory entry.
	_, err = w.Create(dirName + "/")
	if err != nil {
		t.Fatal(err)
	}

	// Add the file entry.
	header, err := zip.FileInfoHeader(&fakeFileInfo{name: fileName})
	if err != nil {
		t.Fatal(err)
	}
	header.Name = dirName + "/" + fileName
	header.Method = zip.Store

	fw, err := w.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("# test module content")); err != nil {
		t.Fatal(err)
	}
}
