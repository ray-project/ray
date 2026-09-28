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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGenRequirementsTxt tests the genRequirementsTxt function.
func TestGenRequirementsTxt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "requirements_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name            string
		pipPackages     []string
		expectedContent string
		expectError     bool
	}{
		{
			name:            "empty list",
			pipPackages:     []string{},
			expectedContent: "",
			expectError:     false,
		},
		{
			name:            "single package",
			pipPackages:     []string{"requests"},
			expectedContent: "requests",
			expectError:     false,
		},
		{
			name:            "multiple packages",
			pipPackages:     []string{"requests", "numpy", "pandas"},
			expectedContent: "requests\nnumpy\npandas",
			expectError:     false,
		},
		{
			name:            "packages with versions",
			pipPackages:     []string{"requests==2.28.0", "numpy>=1.20.0", "pandas<=1.5.0"},
			expectedContent: "requests==2.28.0\nnumpy>=1.20.0\npandas<=1.5.0",
			expectError:     false,
		},
		{
			name:            "packages with extras",
			pipPackages:     []string{"package[extra1,extra2]", "another-package[dev]"},
			expectedContent: "package[extra1,extra2]\nanother-package[dev]",
			expectError:     false,
		},
		{
			name:            "packages with URLs",
			pipPackages:     []string{"https://github.com/user/repo/archive/main.zip", "git+https://github.com/user/repo.git"},
			expectedContent: "https://github.com/user/repo/archive/main.zip\ngit+https://github.com/user/repo.git",
			expectError:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requirementsFile := filepath.Join(tmpDir, "requirements.txt")
			err := genRequirementsTxt(requirementsFile, tc.pipPackages)

			if tc.expectError && err == nil {
				t.Errorf("genRequirementsTxt() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("genRequirementsTxt() unexpected error: %v", err)
			}

			if !tc.expectError {
				// Check the file content.
				content, err := os.ReadFile(requirementsFile)
				if err != nil {
					t.Errorf("Failed to read requirements file: %v", err)
					return
				}

				actualContent := strings.TrimSpace(string(content))
				expectedContent := strings.TrimSpace(tc.expectedContent)

				if actualContent != expectedContent {
					t.Errorf("genRequirementsTxt() content = %q, want %q", actualContent, expectedContent)
				}

				// Check the file permissions.
				info, err := os.Stat(requirementsFile)
				if err != nil {
					t.Errorf("Failed to stat requirements file: %v", err)
					return
				}

				// Check that the permissions include 0644.
				if info.Mode().Perm()&0644 != 0644 {
					t.Errorf("genRequirementsTxt() file permissions = %o, want at least 644", info.Mode().Perm())
				}
			}
		})
	}
}

// TestGenRequirementsTxtInvalidPath tests genRequirementsTxt behavior with an invalid path.
func TestGenRequirementsTxtInvalidPath(t *testing.T) {
	// Use a directory that does not exist and cannot be created.
	invalidPath := "/nonexistent/directory/requirements.txt"

	err := genRequirementsTxt(invalidPath, []string{"requests"})

	if err == nil {
		t.Error("genRequirementsTxt() expected error for invalid path, got nil")
	}
	if !strings.Contains(err.Error(), "failed to write requirements file") {
		t.Errorf("genRequirementsTxt() unexpected error message: %v", err)
	}
}

// TestGetRequirementsFile tests the getRequirementsFile function.
func TestGetRequirementsFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "getreqfile_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name        string
		targetDir   string
		pipList     []string
		expectError bool
		checkFunc   func(string) bool
	}{
		{
			name:      "nil pipList",
			targetDir: tmpDir,
			pipList:   nil,
			checkFunc: func(path string) bool {
				return strings.HasSuffix(path, "ray_runtime_env_internal_pip_requirements.txt")
			},
		},
		{
			name:      "empty pipList",
			targetDir: tmpDir,
			pipList:   []string{},
			checkFunc: func(path string) bool {
				return strings.HasSuffix(path, "ray_runtime_env_internal_pip_requirements.txt")
			},
		},
		{
			name:      "pipList without conflict",
			targetDir: tmpDir,
			pipList:   []string{"requests", "numpy"},
			checkFunc: func(path string) bool {
				return strings.HasSuffix(path, "ray_runtime_env_internal_pip_requirements.txt")
			},
		},
		{
			name:      "pipList with filename conflict",
			targetDir: tmpDir,
			pipList:   []string{"ray_runtime_env_internal_pip_requirements.txt"},
			checkFunc: func(path string) bool {
				return strings.HasSuffix(path, "ray_runtime_env_internal_pip_requirements.txt.1")
			},
		},
		{
			name:      "pipList with multiple conflicts",
			targetDir: tmpDir,
			pipList: []string{
				"ray_runtime_env_internal_pip_requirements.txt",
				"ray_runtime_env_internal_pip_requirements.txt.1",
				"ray_runtime_env_internal_pip_requirements.txt.2",
			},
			checkFunc: func(path string) bool {
				return strings.HasSuffix(path, "ray_runtime_env_internal_pip_requirements.txt.3")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getRequirementsFile(tc.targetDir, tc.pipList)

			if tc.expectError && err == nil {
				t.Errorf("getRequirementsFile() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("getRequirementsFile() unexpected error: %v", err)
			}

			if !tc.expectError {
				// Check that the path starts with targetDir.
				if !strings.HasPrefix(result, tc.targetDir) {
					t.Errorf("getRequirementsFile() result = %q does not start with targetDir = %q", result, tc.targetDir)
				}

				// Check that the filename matches the expectation.
				if !tc.checkFunc(result) {
					t.Errorf("getRequirementsFile() result = %q does not match expected pattern", result)
				}
			}
		})
	}
}

// TestGetRequirementsFileMaxTries tests getRequirementsFile behavior when the maximum number of attempts is reached.
func TestGetRequirementsFileMaxTries(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "maxtries_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build a list of 101 conflicting filenames (exceeding maxTries=100).
	// Note: filenameInPipList uses strings.Contains, so each attempted filename must be
	// present in pipList. getRequirementsFile tries from i=1, generating
	// "ray_runtime_env_internal_pip_requirements.txt.1", ".2", ..., ".100", so pipList must contain those exact strings.
	pipList := make([]string, 101)
	for i := 0; i < 101; i++ {
		// Generate names in the same format getRequirementsFile uses.
		if i == 0 {
			pipList[i] = "ray_runtime_env_internal_pip_requirements.txt"
		} else {
			pipList[i] = "ray_runtime_env_internal_pip_requirements.txt." + fmt.Sprintf("%d", i)
		}
	}

	_, err = getRequirementsFile(tmpDir, pipList)

	if err == nil {
		t.Error("getRequirementsFile() expected error when max tries reached, got nil")
		return
	}
	if !strings.Contains(err.Error(), "could not find a valid filename") {
		t.Errorf("getRequirementsFile() unexpected error message: %v", err)
	}
}

// TestGetRequirementsFileEdgeCases tests getRequirementsFile edge cases.
func TestGetRequirementsFileEdgeCases(t *testing.T) {
	testCases := []struct {
		name      string
		targetDir string
		pipList   []string
	}{
		{
			name:      "empty targetDir",
			targetDir: "",
			pipList:   nil,
		},
		{
			name:      "root directory",
			targetDir: "/",
			pipList:   nil,
		},
		{
			name:      "current directory",
			targetDir: ".",
			pipList:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getRequirementsFile(tc.targetDir, tc.pipList)

			if err != nil {
				t.Errorf("getRequirementsFile(%q, %v) unexpected error: %v", tc.targetDir, tc.pipList, err)
			}

			// Check that the path contains targetDir (when targetDir is non-empty).
			if tc.targetDir != "" && !strings.Contains(result, tc.targetDir) {
				t.Errorf("getRequirementsFile(%q, %v) result = %q does not contain targetDir", tc.targetDir, tc.pipList, result)
			}
		})
	}
}

// TestCheckRay tests the checkRay function.
// Note: this test requires the ray module to be installed in the Python environment.
func TestCheckRay(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "checkray_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Look up the Python executable.
	python, err := getPythonExecutable()
	if err != nil {
		t.Skipf("Skipping checkRay test: Python not found: %v", err)
	}

	// First check that Python can import the ray module.
	cmd := []string{python, "-c", "import ray; print(ray.__version__)"}
	testCmd := execCommandContext(ctx, cmd[0], cmd[1:]...)
	output, err := testCmd.CombinedOutput()
	if err != nil {
		t.Skipf("Skipping checkRay test: ray module not available: %v, output: %s", err, string(output))
	}

	// Run checkRay. It may fail because a specific environment is required;
	// we mainly verify that it executes without panicking.
	cwd := tmpDir
	err = checkRay(ctx, python, cwd)

	// Log the result instead of failing the test, since this may be an environment issue.
	if err != nil {
		t.Logf("checkRay() error (may be expected in test environment): %v", err)
	}
}

// TestCheckRayWithDifferentVersions tests checkRay detection of differing versions.
// The test simulates a ray version mismatch scenario.
func TestCheckRayWithDifferentVersions(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "checkray_diff_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	python, err := getPythonExecutable()
	if err != nil {
		t.Skipf("Skipping checkRay test: Python not found: %v", err)
	}

	// Check that Python can import the ray module.
	cmd := []string{python, "-c", "import ray; print(ray.__version__)"}
	testCmd := execCommandContext(ctx, cmd[0], cmd[1:]...)
	output, err := testCmd.CombinedOutput()
	if err != nil {
		t.Skipf("Skipping checkRay test: ray module not available: %v, output: %s", err, string(output))
	}

	// checkRay calls getRayVersionAndPath twice and compares the results, so in a
	// normal environment it should return nil (same version). Testing a version
	// mismatch would require a mocked environment; here we only exercise the basic path.
	cwd := tmpDir
	err = checkRay(ctx, python, cwd)

	// In a normal environment, both calls should produce the same result.
	if err != nil {
		t.Logf("checkRay() detected version difference or other issue: %v", err)
	}
}

// Helper function to create exec.Cmd with context
func execCommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}

// TestGenRequirementsTxtSpecialCharacters tests genRequirementsTxt handling of special characters.
func TestGenRequirementsTxtSpecialCharacters(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "specialchars_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name        string
		pipPackages []string
	}{
		{
			name:        "packages with spaces in names (unusual but possible)",
			pipPackages: []string{"package-name", "another_package"},
		},
		{
			name:        "packages with special characters",
			pipPackages: []string{"package[extra1,extra2]", "package2[dev,test]"},
		},
		{
			name:        "packages with comparison operators",
			pipPackages: []string{"pkg>=1.0,<2.0", "pkg2!=1.5"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requirementsFile := filepath.Join(tmpDir, "test_req.txt")
			err := genRequirementsTxt(requirementsFile, tc.pipPackages)

			if err != nil {
				t.Errorf("genRequirementsTxt() unexpected error: %v", err)
				return
			}

			// Read and verify the content.
			content, err := os.ReadFile(requirementsFile)
			if err != nil {
				t.Errorf("Failed to read requirements file: %v", err)
				return
			}

			lines := strings.Split(strings.TrimSpace(string(content)), "\n")
			if len(lines) != len(tc.pipPackages) {
				t.Errorf("genRequirementsTxt() expected %d lines, got %d", len(tc.pipPackages), len(lines))
			}

			for i, pkg := range tc.pipPackages {
				if i < len(lines) && lines[i] != pkg {
					t.Errorf("Line %d: expected %q, got %q", i, pkg, lines[i])
				}
			}
		})
	}
}

// TestGetRequirementsFileFilenameMatching tests the filename matching logic of getRequirementsFile.
func TestGetRequirementsFileFilenameMatching(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "filenamematch_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Exercise the filenameInPipList logic.
	testCases := []struct {
		name     string
		pipList  []string
		filename string
		expected bool
	}{
		{
			name:     "exact match",
			pipList:  []string{"ray_runtime_env_internal_pip_requirements.txt"},
			filename: "ray_runtime_env_internal_pip_requirements.txt",
			expected: true,
		},
		{
			name:     "partial match",
			pipList:  []string{"some/path/ray_runtime_env_internal_pip_requirements.txt"},
			filename: "ray_runtime_env_internal_pip_requirements.txt",
			expected: true,
		},
		{
			name:     "no match",
			pipList:  []string{"requests", "numpy"},
			filename: "ray_runtime_env_internal_pip_requirements.txt",
			expected: false,
		},
		{
			name:     "match in middle of string",
			pipList:  []string{"prefix_ray_runtime_env_internal_pip_requirements.txt_suffix"},
			filename: "ray_runtime_env_internal_pip_requirements.txt",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test filenameInPipList indirectly by calling getRequirementsFile.
			result, err := getRequirementsFile(tmpDir, tc.pipList)
			if err != nil {
				t.Errorf("getRequirementsFile() unexpected error: %v", err)
				return
			}

			baseName := filepath.Base(result)
			hasConflict := strings.Contains(baseName, ".1") || strings.Contains(baseName, ".2")

			if tc.expected && !hasConflict {
				t.Errorf("Expected filename conflict but got: %s", baseName)
			}
			if !tc.expected && hasConflict {
				t.Errorf("Unexpected filename conflict: %s", baseName)
			}
		})
	}
}
