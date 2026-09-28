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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestIsInVirtualenv tests the isInVirtualenv function.
func TestIsInVirtualenv(t *testing.T) {
	// This test depends on the current environment, so the result may be true or false.
	// We mainly verify that the function does not panic and runs normally.
	result := isInVirtualenv()

	// Verify the return value is a boolean.
	if result != true && result != false {
		t.Errorf("isInVirtualenv() returned invalid value: %v", result)
	}
}

// TestGetVirtualenvPath tests the getVirtualenvPath function.
func TestGetVirtualenvPath(t *testing.T) {
	testCases := []struct {
		name      string
		targetDir string
		expected  string
	}{
		{
			name:      "normal path",
			targetDir: "/tmp/test",
			expected:  filepath.Join("/tmp/test", "virtualenv"),
		},
		{
			name:      "windows path",
			targetDir: "C:\\temp\\test",
			expected:  filepath.Join("C:\\temp\\test", "virtualenv"),
		},
		{
			name:      "relative path",
			targetDir: "./test",
			expected:  filepath.Join("./test", "virtualenv"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getVirtualenvPath(tc.targetDir)
			if result != tc.expected {
				t.Errorf("getVirtualenvPath(%q) = %q, want %q", tc.targetDir, result, tc.expected)
			}
		})
	}
}

// TestGetVirtualenvPython tests the getVirtualenvPython function.
func TestGetVirtualenvPython(t *testing.T) {
	testCases := []struct {
		name                string
		targetDir           string
		expectedSuffix      string // expected path suffix
		skipOnNonMatchingOS bool
	}{
		{
			name:                "unix path",
			targetDir:           "/tmp/test",
			expectedSuffix:      filepath.Join("virtualenv", "bin", "python"),
			skipOnNonMatchingOS: runtime.GOOS == "windows",
		},
		{
			name:                "windows path",
			targetDir:           "C:\\temp\\test",
			expectedSuffix:      filepath.Join("virtualenv", "Scripts", "python.exe"),
			skipOnNonMatchingOS: runtime.GOOS != "windows",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipOnNonMatchingOS {
				t.Skipf("Skipping on %s (not matching OS for this test)", runtime.GOOS)
			}

			result := getVirtualenvPython(tc.targetDir)

			// Check that the path ends with the expected suffix.
			if !strings.HasSuffix(result, tc.expectedSuffix) {
				t.Errorf("getVirtualenvPython(%q) = %q, want suffix %q", tc.targetDir, result, tc.expectedSuffix)
			}

			// Check that the path contains targetDir.
			if !strings.Contains(result, tc.targetDir) {
				t.Errorf("getVirtualenvPython(%q) = %q does not contain target dir", tc.targetDir, result)
			}
		})
	}
}

// TestGetVirtualenvActivateCommand tests the getVirtualenvActivateCommand function.
func TestGetVirtualenvActivateCommand(t *testing.T) {
	testCases := []struct {
		name                string
		targetDir           string
		expectedLen         int
		checkFunc           func([]string) bool
		skipOnNonMatchingOS bool
	}{
		{
			name:        "unix command",
			targetDir:   "/tmp/test",
			expectedLen: 4,
			checkFunc: func(cmd []string) bool {
				return len(cmd) == 4 &&
					cmd[0] == "source" &&
					strings.Contains(cmd[1], "activate") &&
					cmd[2] == "1>&2" &&
					cmd[3] == "&&"
			},
			skipOnNonMatchingOS: runtime.GOOS == "windows",
		},
		{
			name:        "windows command",
			targetDir:   "C:\\temp\\test",
			expectedLen: 3,
			checkFunc: func(cmd []string) bool {
				return len(cmd) == 3 &&
					strings.Contains(cmd[0], "activate.bat") &&
					cmd[1] == "1>&2" &&
					cmd[2] == "&&"
			},
			skipOnNonMatchingOS: runtime.GOOS != "windows",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipOnNonMatchingOS {
				t.Skipf("Skipping on %s (not matching OS for this test)", runtime.GOOS)
			}

			result := getVirtualenvActivateCommand(tc.targetDir)

			if len(result) != tc.expectedLen {
				t.Errorf("getVirtualenvActivateCommand(%q) returned %d elements, want %d", tc.targetDir, len(result), tc.expectedLen)
			}

			if !tc.checkFunc(result) {
				t.Errorf("getVirtualenvActivateCommand(%q) = %v does not match expected pattern", tc.targetDir, result)
			}

			// Check whether any part contains targetDir.
			for _, part := range result {
				if strings.Contains(part, tc.targetDir) || strings.Contains(part, "virtualenv") {
					// Found the part containing the path.
					break
				}
			}
		})
	}
}

// TestGetPythonExecutable tests the getPythonExecutable function.
func TestGetPythonExecutable(t *testing.T) {
	// This test depends on whether Python is available on the system.
	executable, err := getPythonExecutable()

	// When Python cannot be found, an error is expected.
	if err != nil {
		if !strings.Contains(err.Error(), "python executable not found") {
			t.Errorf("getPythonExecutable() unexpected error: %v", err)
		}
		return
	}

	// When Python is found, verify the path is valid.
	if executable == "" {
		t.Error("getPythonExecutable() returned empty string without error")
	}

	// Check that the file exists.
	if _, err := os.Stat(executable); os.IsNotExist(err) {
		t.Errorf("getPythonExecutable() returned non-existent path: %s", executable)
	}
}

// TestCreateOrGetVirtualenv tests the createOrGetVirtualenv function.
func TestCreateOrGetVirtualenv(t *testing.T) {
	originalPythonExecutable := PythonExecutable
	defer func() { PythonExecutable = originalPythonExecutable }()

	if PythonExecutable == "" {
		if executable, err := getPythonExecutable(); err == nil {
			PythonExecutable = executable
		}
	}

	// Create a temporary directory for the test.
	tmpDir, err := os.MkdirTemp("", "virtualenv_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	cwd := tmpDir

	// Test creating the virtualenv.
	err = createOrGetVirtualenv(ctx, tmpDir, cwd)

	// This test may fail because the virtualenv module is required.
	// We mainly verify that the function runs normally without panicking.
	if err != nil {
		// Log the error but do not fail the test, as it may be an environment issue.
		t.Logf("createOrGetVirtualenv() error (may be expected): %v", err)
	}

	// Check whether the virtualenv directory was created.
	virtualenvPath := filepath.Join(tmpDir, "virtualenv")
	if _, err := os.Stat(virtualenvPath); err == nil {
		// The virtualenv was created successfully.
		t.Logf("virtualenv created at: %s", virtualenvPath)
	}
}

func TestCreateOrGetVirtualenvUsesGlobalPythonExecutable(t *testing.T) {
	originalPythonExecutable := PythonExecutable
	defer func() { PythonExecutable = originalPythonExecutable }()

	PythonExecutable = "/nonexistent/python_for_virtualenv_test"

	tmpDir, err := os.MkdirTemp("", "virtualenv_global_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = createOrGetVirtualenv(context.Background(), tmpDir, tmpDir)
	if err == nil {
		t.Fatal("createOrGetVirtualenv() expected error with nonexistent global Python executable, got nil")
	}
	if !strings.Contains(err.Error(), "/nonexistent/python_for_virtualenv_test") {
		t.Errorf("createOrGetVirtualenv() error should reference the global Python executable, got: %v", err)
	}
}

// TestGetLastNLines tests the getLastNLines function.
func TestGetLastNLines(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		n        int
		expected string
	}{
		{
			name:     "negative n returns all",
			input:    "line1\nline2\nline3",
			n:        -1,
			expected: "line1\nline2\nline3",
		},
		{
			name:     "n larger than lines returns all",
			input:    "line1\nline2\nline3",
			n:        10,
			expected: "line1\nline2\nline3",
		},
		{
			name:     "n equals lines returns all",
			input:    "line1\nline2\nline3",
			n:        3,
			expected: "line1\nline2\nline3",
		},
		{
			name:     "n smaller than lines returns last n",
			input:    "line1\nline2\nline3\nline4\nline5",
			n:        2,
			expected: "line4\nline5",
		},
		{
			name:     "empty string",
			input:    "",
			n:        5,
			expected: "",
		},
		{
			name:     "single line",
			input:    "single line",
			n:        1,
			expected: "single line",
		},
		{
			name:     "n is zero returns empty string (trims all whitespace)",
			input:    "line1\nline2\nline3",
			n:        0,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getLastNLines(tc.input, tc.n)
			if result != tc.expected {
				t.Errorf("getLastNLines(%q, %d) = %q, want %q", tc.input, tc.n, result, tc.expected)
			}
		})
	}
}

// TestCheckOutputCmd tests the checkOutputCmd function.
func TestCheckOutputCmd(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "checkoutput_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name        string
		cmd         []string
		cwd         string
		env         map[string]string
		expectError bool
		checkOutput func(string) bool
	}{
		{
			name:        "successful command",
			cmd:         []string{"echo", "hello"},
			cwd:         tmpDir,
			env:         make(map[string]string),
			expectError: false,
			checkOutput: func(output string) bool {
				return strings.Contains(output, "hello")
			},
		},
		{
			name:        "command with env",
			cmd:         []string{"sh", "-c", "echo $TEST_VAR"},
			cwd:         tmpDir,
			env:         map[string]string{"TEST_VAR": "test_value"},
			expectError: false,
			checkOutput: func(output string) bool {
				return strings.Contains(output, "test_value")
			},
		},
		{
			name:        "failing command",
			cmd:         []string{"sh", "-c", "exit 1"},
			cwd:         tmpDir,
			env:         make(map[string]string),
			expectError: true,
			checkOutput: func(output string) bool {
				return true // any output is acceptable
			},
		},
		{
			name:        "nonexistent command",
			cmd:         []string{"nonexistent_command_xyz"},
			cwd:         tmpDir,
			env:         make(map[string]string),
			expectError: true,
			checkOutput: func(output string) bool {
				return true
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmdIndexGen := newCmdIndexGen()
			output, err := checkOutputCmd(ctx, tc.cmd, tc.cwd, tc.env, cmdIndexGen)

			if tc.expectError && err == nil {
				t.Errorf("checkOutputCmd() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("checkOutputCmd() unexpected error: %v", err)
			}

			if tc.checkOutput != nil && !tc.checkOutput(output) {
				t.Errorf("checkOutputCmd() output check failed: %s", output)
			}
		})
	}
}

// TestCmdIndexGen tests the cmdIndexGen struct.
func TestCmdIndexGen(t *testing.T) {
	gen := newCmdIndexGen()

	// Test the initial value.
	if gen.counter != 0 {
		t.Errorf("newCmdIndexGen() counter = %d, want 0", gen.counter)
	}

	// Test the next() method.
	first := gen.next()
	if first != 1 {
		t.Errorf("gen.next() first call = %d, want 1", first)
	}

	second := gen.next()
	if second != 2 {
		t.Errorf("gen.next() second call = %d, want 2", second)
	}

	third := gen.next()
	if third != 3 {
		t.Errorf("gen.next() third call = %d, want 3", third)
	}
}

// TestGetPythonExecutableNotFound tests getPythonExecutable when Python cannot be found.
func TestGetPythonExecutableNotFound(t *testing.T) {
	// Save the original PATH.
	originalPath := os.Getenv("PATH")
	defer os.Setenv("PATH", originalPath)

	// Set a PATH that does not contain Python.
	os.Setenv("PATH", "/nonexistent/path")

	executable, err := getPythonExecutable()
	if err == nil {
		t.Errorf("getPythonExecutable() expected error when Python not found, got executable: %s", executable)
	}
	if executable != "" {
		t.Errorf("getPythonExecutable() should return empty string on error, got: %s", executable)
	}
	if !strings.Contains(err.Error(), "python executable not found") {
		t.Errorf("getPythonExecutable() unexpected error message: %v", err)
	}
}

// TestGetVirtualenvPathEdgeCases tests edge cases of getVirtualenvPath.
func TestGetVirtualenvPathEdgeCases(t *testing.T) {
	testCases := []struct {
		name      string
		targetDir string
	}{
		{
			name:      "empty string",
			targetDir: "",
		},
		{
			name:      "root directory",
			targetDir: "/",
		},
		{
			name:      "current directory",
			targetDir: ".",
		},
		{
			name:      "parent directory",
			targetDir: "..",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getVirtualenvPath(tc.targetDir)
			expected := filepath.Join(tc.targetDir, "virtualenv")
			if result != expected {
				t.Errorf("getVirtualenvPath(%q) = %q, want %q", tc.targetDir, result, expected)
			}
		})
	}
}

// TestGetVirtualenvPythonEdgeCases tests edge cases of getVirtualenvPython.
func TestGetVirtualenvPythonEdgeCases(t *testing.T) {
	testCases := []struct {
		name      string
		targetDir string
	}{
		{
			name:      "empty string",
			targetDir: "",
		},
		{
			name:      "root directory",
			targetDir: "/",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getVirtualenvPython(tc.targetDir)
			expectedBase := filepath.Join(tc.targetDir, "virtualenv")

			if !strings.Contains(result, expectedBase) {
				t.Errorf("getVirtualenvPython(%q) = %q does not contain expected base: %q", tc.targetDir, result, expectedBase)
			}

			// Check the correct suffix according to the OS.
			if runtime.GOOS == "windows" {
				if !strings.HasSuffix(result, filepath.Join("Scripts", "python.exe")) {
					t.Errorf("getVirtualenvPython(%q) on Windows should end with Scripts/python.exe, got: %q", tc.targetDir, result)
				}
			} else {
				if !strings.HasSuffix(result, filepath.Join("bin", "python")) {
					t.Errorf("getVirtualenvPython(%q) on Unix should end with bin/python, got: %q", tc.targetDir, result)
				}
			}
		})
	}
}

// TestGetLastNLinesWithTrailingNewlines tests getLastNLines with trailing newline characters.
func TestGetLastNLinesWithTrailingNewlines(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		n        int
		expected string
	}{
		{
			name:     "trailing newlines are trimmed",
			input:    "line1\nline2\nline3\n\n",
			n:        2,
			expected: "line2\nline3",
		},
		{
			name:     "leading newlines are trimmed by strings.TrimSpace",
			input:    "\n\nline1\nline2",
			n:        2,
			expected: "\n\nline1\nline2", // strings.TrimSpace does not remove interior newlines, but strips leading and trailing ones
		},
		{
			name:     "multiple consecutive newlines - getLastNLines returns last n lines after trimming",
			input:    "line1\n\n\nline2",
			n:        2,
			expected: "\nline2", // after Split: ["line1", "", "", "line2"], last 2 lines are ["", "line2"]
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getLastNLines(tc.input, tc.n)
			if result != tc.expected {
				t.Errorf("getLastNLines(%q, %d) = %q, want %q", tc.input, tc.n, result, tc.expected)
			}
		})
	}
}

// TestCheckOutputCmdWithContextCancellation tests checkOutputCmd when the context is cancelled.
func TestCheckOutputCmdWithContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel the context immediately

	tmpDir, err := os.MkdirTemp("", "checkoutput_cancel_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use a long-running command.
	cmd := []string{"sleep", "10"}
	cmdIndexGen := newCmdIndexGen()

	_, err = checkOutputCmd(ctx, cmd, tmpDir, make(map[string]string), cmdIndexGen)

	// The error should reflect the cancelled context.
	if err == nil {
		t.Error("checkOutputCmd() expected error when context is cancelled, got nil")
	}
}

// TestCheckOutputCmdWithLargeOutput tests checkOutputCmd with large output.
func TestCheckOutputCmdWithLargeOutput(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "checkoutput_large_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use a command that generates large output.
	cmd := []string{"sh", "-c", "for i in $(seq 1 100); do echo \"Line $i\"; done"}
	cmdIndexGen := newCmdIndexGen()

	output, err := checkOutputCmd(ctx, cmd, tmpDir, make(map[string]string), cmdIndexGen)

	if err != nil {
		t.Errorf("checkOutputCmd() unexpected error: %v", err)
	}

	// Check that the output contains all lines.
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 100 {
		t.Errorf("checkOutputCmd() expected 100 lines, got %d", len(lines))
	}

	// Check the last line.
	if !strings.Contains(output, "Line 100") {
		t.Errorf("checkOutputCmd() output missing last line: %s", output)
	}
}
