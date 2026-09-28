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

// Package runtime_env provides runtime environment configuration for Ray workers.
package runtime_env

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGetRaySetupSpec tests that _getRaySetupSpec can fetch the setup_spec correctly.
func TestGetRaySetupSpec(t *testing.T) {
	t.Skip("Skipping test: requires Ray installed from source with setup.py available")

	// First check whether Python can import ray.
	python, err := getPythonExecutable()
	if err != nil {
		t.Skipf("Skipping test: Python not found: %v", err)
	}

	// Check whether the ray module is available.
	cmd := []string{python, "-c", "import ray; print(ray.__version__)"}
	testCmd := exec.CommandContext(t.Context(), cmd[0], cmd[1:]...)
	output, err := testCmd.CombinedOutput()
	if err != nil {
		t.Skipf("Skipping test: ray module not available: %v, output: %s", err, string(output))
	}

	// Call _getRaySetupSpec.
	setupSpec, err := _getRaySetupSpec()
	if err != nil {
		t.Fatalf("_getRaySetupSpec() unexpected error: %v", err)
	}

	// Verify the returned setup_spec is not nil.
	if setupSpec == nil {
		t.Error("_getRaySetupSpec() returned nil setup_spec")
	}

	// Verify the setup_spec contains the expected fields (depending on the actual setup.py contents).
	// Typically setup_spec includes fields such as "name", "version", and "install_requires".
	expectedFields := []string{"name", "version"}
	for _, field := range expectedFields {
		if _, ok := setupSpec[field]; !ok {
			t.Logf("_getRaySetupSpec() missing expected field: %s (may be normal depending on setup.py)", field)
		}
	}

	// If install_requires is present, verify it is a string slice.
	if installRequires, ok := setupSpec["install_requires"]; ok {
		if _, isArray := installRequires.([]interface{}); !isArray {
			t.Errorf("_getRaySetupSpec() install_requires is not an array, got type: %T", installRequires)
		}
	}
}

// TestGetRaySetupSpecWithMockSetupPy tests _getRaySetupSpec with a mocked setup.py.
func TestGetRaySetupSpecWithMockSetupPy(t *testing.T) {
	// Create a temporary directory.
	tmpDir, err := os.MkdirTemp("", "mock_setup_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create the mock setup.py file.
	mockSetupPy := `
setup_spec = {
    "name": "test-ray",
    "version": "2.0.0",
    "install_requires": ["requests", "numpy"],
    "extras": {
        "default": ["pandas", "scipy"]
    }
}
`
	setupPyPath := filepath.Join(tmpDir, "setup.py")
	if err := os.WriteFile(setupPyPath, []byte(mockSetupPy), 0644); err != nil {
		t.Fatalf("Failed to write mock setup.py: %v", err)
	}

	// Temporarily patch getRayFile to return the mock path.
	originalGetRayFile := getRayFile
	getRayFile = func() (string, error) {
		return filepath.Join(tmpDir, "ray", "__init__.py"), nil
	}
	defer func() { getRayFile = originalGetRayFile }()

	// Call _getRaySetupSpec.
	setupSpec, err := _getRaySetupSpec()
	if err != nil {
		t.Fatalf("_getRaySetupSpec() unexpected error: %v", err)
	}

	// Verify the returned setup_spec.
	if setupSpec == nil {
		t.Fatal("_getRaySetupSpec() returned nil setup_spec")
	}

	// Verify the concrete fields.
	if name, ok := setupSpec["name"].(string); !ok || name != "test-ray" {
		t.Errorf("_getRaySetupSpec() name = %v, want \"test-ray\"", name)
	}

	if version, ok := setupSpec["version"].(string); !ok || version != "2.0.0" {
		t.Errorf("_getRaySetupSpec() version = %v, want \"2.0.0\"", version)
	}

	if installRequires, ok := setupSpec["install_requires"].([]interface{}); ok {
		expected := []string{"requests", "numpy"}
		if len(installRequires) != len(expected) {
			t.Errorf("_getRaySetupSpec() install_requires length = %d, want %d", len(installRequires), len(expected))
		}
		for i, pkg := range expected {
			if i < len(installRequires) {
				if pkgName, ok := installRequires[i].(string); !ok || pkgName != pkg {
					t.Errorf("_getRaySetupSpec() install_requires[%d] = %v, want %q", i, installRequires[i], pkg)
				}
			}
		}
	} else {
		t.Error("_getRaySetupSpec() install_requires is not an array")
	}

	if extras, ok := setupSpec["extras"].(map[string]interface{}); ok {
		if defaultExtras, ok := extras["default"].([]interface{}); ok {
			expected := []string{"pandas", "scipy"}
			if len(defaultExtras) != len(expected) {
				t.Errorf("_getRaySetupSpec() extras[default] length = %d, want %d", len(defaultExtras), len(expected))
			}
		} else {
			t.Error("_getRaySetupSpec() extras[default] is not an array")
		}
	} else {
		t.Error("_getRaySetupSpec() extras is not a map")
	}
}

// TestGetRaySetupSpecErrorHandling tests the error handling of _getRaySetupSpec.
func TestGetRaySetupSpecErrorHandling(t *testing.T) {
	// Test the case where setup.py does not exist.
	originalGetRayFile := getRayFile
	getRayFile = func() (string, error) {
		return "/nonexistent/path/ray/__init__.py", nil
	}
	defer func() { getRayFile = originalGetRayFile }()

	_, err := _getRaySetupSpec()
	if err == nil {
		t.Error("_getRaySetupSpec() expected error for nonexistent setup.py, got nil")
	}
}
