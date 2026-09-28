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
	"testing"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestGetCondaBinExecutable(t *testing.T) {
	tests := []struct {
		name           string
		executableName string
		setupEnv       map[string]string
		expectedPath   string
	}{
		{
			name:           "default executable on linux",
			executableName: "conda",
			setupEnv:       map[string]string{},
			expectedPath:   "conda",
		},
		{
			name:           "with RAY_CONDA_HOME on linux",
			executableName: "conda",
			setupEnv: map[string]string{
				RAY_CONDA_HOME: "/opt/conda",
			},
			expectedPath: "/opt/conda/bin/conda",
		},
		{
			name:           "with CONDA_EXE on linux",
			executableName: "conda",
			setupEnv: map[string]string{
				"CONDA_EXE": "/home/user/miniconda3/bin/conda",
			},
			expectedPath: "/home/user/miniconda3/bin/conda",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save the original environment variables.
			origRayCondaHome := os.Getenv(RAY_CONDA_HOME)
			origCondaExe := os.Getenv("CONDA_EXE")

			// Clear the environment variables to isolate the test.
			os.Unsetenv(RAY_CONDA_HOME)
			os.Unsetenv("CONDA_EXE")

			// Set the test environment variables.
			for k, v := range tt.setupEnv {
				os.Setenv(k, v)
			}

			defer func() {
				// Restore the original environment variables.
				if origRayCondaHome != "" {
					os.Setenv(RAY_CONDA_HOME, origRayCondaHome)
				} else {
					os.Unsetenv(RAY_CONDA_HOME)
				}
				if origCondaExe != "" {
					os.Setenv("CONDA_EXE", origCondaExe)
				} else {
					os.Unsetenv("CONDA_EXE")
				}
			}()

			result := getCondaBinExecutable(tt.executableName)
			assert.Equal(t, tt.expectedPath, result)
		})
	}
}

func TestGetCondaEnvName(t *testing.T) {
	// Create a temporary test file.
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_env.yml")

	content := `name: test_env
dependencies:
  - python=3.8
  - numpy
`
	err := os.WriteFile(testFile, []byte(content), 0644)
	assert.NoError(t, err)

	envName, err := getCondaEnvName(testFile)
	assert.NoError(t, err)
	assert.Contains(t, envName, "ray-")
	assert.Len(t, envName, 44) // "ray-" + 40 hex characters.
}

func TestExecCmd(t *testing.T) {
	tests := []struct {
		name           string
		cmd            []string
		throwOnError   bool
		expectError    bool
		expectExitCode int
	}{
		{
			name:           "successful command",
			cmd:            []string{"echo", "hello"},
			throwOnError:   true,
			expectError:    false,
			expectExitCode: 0,
		},
		{
			name:           "failing command with throw",
			cmd:            []string{"false"},
			throwOnError:   true,
			expectError:    true,
			expectExitCode: 1,
		},
		{
			name:           "failing command without throw",
			cmd:            []string{"false"},
			throwOnError:   false,
			expectError:    false,
			expectExitCode: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exitCode, stdout, stderr, err := execCmd(tt.cmd, tt.throwOnError)

			if tt.expectError {
				assert.Error(t, err)
				assert.IsType(t, &shellCommandException{}, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectExitCode, exitCode)
			// For a failing command, stdout may be empty while stderr carries content.
			if tt.expectExitCode != 0 {
				// For a failing command, stdout and stderr may each be empty or not depending on the command,
				// so here we only check that nothing panics or fails hard.
			} else {
				assert.NotEmpty(t, stdout)
				assert.Empty(t, stderr)
			}
		})
	}
}

func TestExecCmdStreamToLogger(t *testing.T) {
	ctx := context.Background()
	logger := log.Log

	tests := []struct {
		name        string
		cmd         []string
		expectLines int
	}{
		{
			name:        "simple echo command",
			cmd:         []string{"echo", "hello world"},
			expectLines: 1,
		},
		{
			name:        "multiple lines",
			cmd:         []string{"sh", "-c", "echo line1; echo line2; echo line3"},
			expectLines: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exitCode, output, err := execCmdStreamToLogger(ctx, tt.cmd, logger)

			assert.NoError(t, err)
			assert.Equal(t, 0, exitCode)
			assert.NotEmpty(t, output)

			lines := splitLines(output)
			assert.GreaterOrEqual(t, len(lines), tt.expectLines)
		})
	}
}

func TestGetCondaEnvs(t *testing.T) {
	tests := []struct {
		name     string
		info     map[string]interface{}
		expected [][2]string
	}{
		{
			name: "empty envs",
			info: map[string]interface{}{
				"conda_prefix": "/opt/conda",
				"envs":         []interface{}{},
			},
			expected: [][2]string{},
		},
		{
			name: "single base env",
			info: map[string]interface{}{
				"conda_prefix": "/opt/conda",
				"envs":         []interface{}{"/opt/conda"},
			},
			expected: [][2]string{{"base", "/opt/conda"}},
		},
		{
			name: "multiple envs",
			info: map[string]interface{}{
				"conda_prefix": "/opt/conda",
				"envs": []interface{}{
					"/opt/conda",
					"/opt/conda/envs/test",
					"/opt/conda/envs/prod",
				},
			},
			expected: [][2]string{
				{"base", "/opt/conda"},
				{"test", "/opt/conda/envs/test"},
				{"prod", "/opt/conda/envs/prod"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getCondaEnvs(tt.info)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// splitLines splits the output into lines.
func splitLines(output string) []string {
	if output == "" {
		return []string{}
	}

	lines := make([]string, 0)
	currentLine := ""

	for _, ch := range output {
		if ch == '\n' {
			if currentLine != "" {
				lines = append(lines, currentLine)
				currentLine = ""
			}
		} else {
			currentLine += string(ch)
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return lines
}
