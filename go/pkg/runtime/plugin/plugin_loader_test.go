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

package plugin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// =============================================================================
// Plugin Path Resolution Tests
// =============================================================================

// TestGetLibraryName tests the platform-specific library name
func TestGetLibraryName(t *testing.T) {
	name := getLibraryName()
	if name == "" {
		t.Error("getLibraryName() should not return empty string")
	}

	// Verify expected value for current platform
	expected := ""
	switch runtime.GOOS {
	case "linux":
		expected = "go_runtime.so"
	case "darwin":
		expected = "libgo_runtime.dylib"
	case "windows":
		expected = "go_runtime.dll"
	}

	if expected != "" && name != expected {
		t.Errorf("getLibraryName() = %s, want %s (GOOS=%s)", name, expected, runtime.GOOS)
	}

	t.Logf("getLibraryName() = %s (GOOS=%s)", name, runtime.GOOS)
}

// TestFindPluginPath tests the plugin path finding function
func TestFindPluginPath(t *testing.T) {
	// Note: This test may fail in actual runtime if go_runtime.so doesn't exist
	// This is expected behavior, test is mainly for verifying path finding logic

	path, err := FindPluginPath()
	if err != nil {
		// Skip test instead of failing if plugin not found in development environment
		t.Skipf("FindPluginPath() skipped: plugin not found (%v)", err)
		return
	}

	if path == "" {
		t.Error("FindPluginPath() returned empty path")
	}

	// Verify returned path exists
	if _, err := os.Stat(path); err != nil {
		t.Errorf("FindPluginPath() returned non-existent path: %s", path)
	}

	t.Logf("FindPluginPath() = %s", path)
}

// TestFindPluginPath_WithMockFile tests plugin path finding with mock file
func TestFindPluginPath_WithMockFile(t *testing.T) {
	// Create temporary directory and mock plugin file
	tempDir := t.TempDir()
	libName := getLibraryName()
	mockPlugin := filepath.Join(tempDir, libName)

	// Create an empty mock plugin file
	if err := os.WriteFile(mockPlugin, []byte("mock plugin"), 0755); err != nil {
		t.Fatalf("failed to create mock plugin: %v", err)
	}

	// Save current working directory
	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)

	// Change to temporary directory
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	// Test current working directory finding
	path, err := FindPluginPath()
	if err != nil {
		// Note: May fail because executable path and source path may not match temp dir
		// Mainly for verifying test logic
		t.Logf("FindPluginPath() in temp dir: %v (expected if exe/source paths don't match)", err)
	} else {
		t.Logf("FindPluginPath() found: %s", path)
	}
}

// TestFindPluginPath_EnvVar tests environment variable priority
func TestFindPluginPath_EnvVar(t *testing.T) {
	// Create temporary mock plugin file
	tempDir := t.TempDir()
	libName := getLibraryName()
	mockPlugin := filepath.Join(tempDir, libName)

	if err := os.WriteFile(mockPlugin, []byte("mock plugin"), 0755); err != nil {
		t.Fatalf("failed to create mock plugin: %v", err)
	}

	// Set environment variable
	origEnv := os.Getenv("RAY_GO_RUNTIME_PATH")
	os.Setenv("RAY_GO_RUNTIME_PATH", mockPlugin)
	defer func() {
		if origEnv == "" {
			os.Unsetenv("RAY_GO_RUNTIME_PATH")
		} else {
			os.Setenv("RAY_GO_RUNTIME_PATH", origEnv)
		}
	}()

	// Test environment variable priority
	path, err := FindPluginPath()
	if err != nil {
		t.Fatalf("FindPluginPath() with env var failed: %v", err)
	}

	if path != mockPlugin {
		t.Errorf("FindPluginPath() = %s, want %s", path, mockPlugin)
	}

	t.Logf("FindPluginPath() with env var = %s", path)
}

// TestFindPluginPath_EnvVarNotFound tests when environment variable path doesn't exist
func TestFindPluginPath_EnvVarNotFound(t *testing.T) {
	// Set a non-existent path
	origEnv := os.Getenv("RAY_GO_RUNTIME_PATH")
	os.Setenv("RAY_GO_RUNTIME_PATH", "/nonexistent/path/go_runtime.so")
	defer func() {
		if origEnv == "" {
			os.Unsetenv("RAY_GO_RUNTIME_PATH")
		} else {
			os.Setenv("RAY_GO_RUNTIME_PATH", origEnv)
		}
	}()

	// Test should return error
	_, err := FindPluginPath()
	if err == nil {
		t.Error("FindPluginPath() should return error when env var path not found")
	}

	t.Logf("FindPluginPath() with invalid env var: %v (expected)", err)
}

// TestValidatePluginLoadable_StrictEnvPath verifies the startup plugin
// loadability pre-check treats an
// explicitly set RAY_GO_RUNTIME_PATH as authoritative: a non-existent path is
// a deployment error that must fail fast, instead of silently falling back to
// FindPluginPath's multi-tier search (which may find a valid plugin elsewhere,
// e.g. Python site-packages). This mirrors api.InitWithOptions' resolution.
func TestValidatePluginLoadable_StrictEnvPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.so")
	t.Setenv("RAY_GO_RUNTIME_PATH", missing)

	err := ValidatePluginLoadable()
	if err == nil {
		t.Fatal("ValidatePluginLoadable() = nil error, want failure for non-existent RAY_GO_RUNTIME_PATH")
	}
	if !strings.Contains(err.Error(), "plugin validation failed") {
		t.Fatalf("error %q does not mention plugin validation failed", err)
	}
	if !strings.Contains(err.Error(), missing) {
		t.Fatalf("error %q does not mention the configured plugin path %q", err, missing)
	}
}

// TestValidatePluginLoadable_UnsetEnvUsesFindPluginPath verifies that when
// RAY_GO_RUNTIME_PATH is unset, ValidatePluginLoadable resolves via
// FindPluginPath (accepting any plugin found by the multi-tier search).
func TestValidatePluginLoadable_UnsetEnvUsesFindPluginPath(t *testing.T) {
	origEnv, hadEnv := os.LookupEnv("RAY_GO_RUNTIME_PATH")
	os.Unsetenv("RAY_GO_RUNTIME_PATH")
	t.Cleanup(func() {
		if hadEnv {
			os.Setenv("RAY_GO_RUNTIME_PATH", origEnv)
		} else {
			os.Unsetenv("RAY_GO_RUNTIME_PATH")
		}
	})

	_, err := FindPluginPath()
	pluginFound := err == nil
	valErr := ValidatePluginLoadable()
	if pluginFound && valErr != nil {
		t.Fatalf("ValidatePluginLoadable() = %v, want nil when FindPluginPath finds a plugin", valErr)
	}
	if !pluginFound && valErr == nil {
		t.Fatal("ValidatePluginLoadable() = nil, want error when FindPluginPath finds no plugin")
	}
}

// TestGetLibraryName_AllPlatforms tests library names for all platforms (simulated)
func TestGetLibraryName_AllPlatforms(t *testing.T) {
	// Note: Can only test current platform at runtime
	// This test is for documenting expected behavior

	testCases := []struct {
		goos     string
		expected string
	}{
		{"linux", "go_runtime.so"},
		{"darwin", "libgo_runtime.dylib"},
		{"windows", "go_runtime.dll"},
		{"freebsd", "go_runtime.so"},
		{"unknown", "go_runtime.so"}, // default value
	}

	// Current platform test
	current := getLibraryName()
	t.Logf("Current platform (GOOS=%s): %s", runtime.GOOS, current)

	// Verify current platform return value is in expected list
	found := false
	for _, tc := range testCases {
		if tc.goos == runtime.GOOS {
			found = true
			if current != tc.expected {
				t.Errorf("getLibraryName() = %s, want %s for GOOS=%s", current, tc.expected, runtime.GOOS)
			}
			break
		}
	}
	if !found {
		// Unknown platform should return default value
		if current != "go_runtime.so" {
			t.Errorf("getLibraryName() = %s, want default 'go_runtime.so' for unknown platform", current)
		}
	}
}

// =============================================================================
// Stage 2: Reserved Tests (Not yet implemented)
// =============================================================================

// TODO(Stage 2): TestPluginLoader_LoadPlugin
// func TestPluginLoader_LoadPlugin(t *testing.T) {
//     loader := NewPluginLoader()
//     p, err := loader.LoadPlugin()
//     // ...
// }

// TestCallPluginFuncErrorBeforeValueValidator verifies callPluginFunc validates
// the trailing error result before the value-type validators: a function that
// returns (nil, someErr) must surface someErr (or a wrapping of it), never the
// value validator's "must return ..., got <nil>" message. It also guards the
// typed-nil path where a value validator would dereference nil (e.g. calling
// Runtime() on a nil handle) before the error was observed.
func TestCallPluginFuncErrorBeforeValueValidator(t *testing.T) {
	sentinel := errors.New("sentinel failure from plugin")
	// Signature: func() (any, error) returning (nil, sentinel). The nil first
	// value is a typed nil interface, which the value validator must never see.
	fn := func() (any, error) { return nil, sentinel }

	valueValidator := func(v reflect.Value) error {
		return fmt.Errorf("value validator ran on %v; want error validator first", v)
	}

	_, err := callPluginFunc(reflect.ValueOf(fn), nil, 2, valueValidator, validateError)
	if err == nil {
		t.Fatal("callPluginFunc() = nil error, want the sentinel error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("callPluginFunc() error = %v, want sentinel %v (value validator must not mask it)", err, sentinel)
	}
}

// TODO(Stage 2): TestPluginLoader_Concurrent
// func TestPluginLoader_Concurrent(t *testing.T) {
//     loader := NewPluginLoader()
//     // Concurrent test...
// }

// =============================================================================
// Stage 3: Reserved Tests (Not yet implemented)
// =============================================================================

// TODO(Stage 3): TestValidatePluginFile
// func TestValidatePluginFile(t *testing.T) {
//     // Security validation test...
// }

// TODO(Stage 3): TestExtractWithLock
// func TestExtractWithLock(t *testing.T) {
//     // Process-safe extraction test...
// }
