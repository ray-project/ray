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
	"os/exec"
	"path/filepath"
	"strings"
)

// getRayFile returns the value of ray.__file__.
var getRayFile = func() (string, error) {
	// Obtain ray.__file__ by invoking the Python interpreter.
	cmd := exec.Command("python", "-c", "import ray; print(ray.__file__)")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute python command: %w", err)
	}
	rayFile := strings.TrimSpace(string(output))
	if rayFile == "" {
		return "", fmt.Errorf("python command returned empty output")
	}
	return rayFile, nil
}

// _resolveCurrentRayPath resolves the installation path of the current Ray.
// When Ray is built from source via pip install -e, it returns ".../python".
// When Ray is installed from prebuilt binaries, it returns ".../site-packages".
func _resolveCurrentRayPath() (string, error) {
	rayFile, err := getRayFile()
	if err != nil {
		return "", fmt.Errorf("failed to get Ray file path: %w", err)
	}
	if rayFile == "" {
		return "", fmt.Errorf("Ray file path is empty")
	}
	// Get the directory containing ray.__file__.
	rayDir := filepath.Dir(rayFile)
	// Then go one directory up.
	parentDir := filepath.Dir(rayDir)
	return parentDir, nil
}

// _resolveCloneVirtualenvPath resolves the path of _clonevirtualenv.py.
func _resolveCloneVirtualenvPath() (string, error) {
	rayFile, err := getRayFile()
	if err != nil {
		return "", fmt.Errorf("failed to get Ray file path: %w", err)
	}
	if rayFile == "" {
		return "", fmt.Errorf("Ray file path is empty")
	}
	// Get the directory containing ray.__file__, e.g. /path/to/site-packages/ray.
	rayDir := filepath.Dir(rayFile)

	// Build the full path of _clonevirtualenv.py:
	// ray/_private/runtime_env/_clonevirtualenv.py, joined directly onto rayDir.
	cloneVirtualenvPath := filepath.Join(rayDir, "_private", "runtime_env", "_clonevirtualenv.py")
	return cloneVirtualenvPath, nil
}

// runPythonScript runs a Python script and returns its output.
// It is a general-purpose helper for executing Python code from Go.
func runPythonScript(script string) (string, error) {
	pythonCmd, err := getPythonExecutable()
	if err != nil {
		return "", fmt.Errorf("failed to get Python executable: %w", err)
	}

	ctx := context.Background()
	cmd := exec.CommandContext(ctx, pythonCmd, "-c", script)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute python script: %w", err)
	}

	return strings.TrimSpace(string(output)), nil
}

// runPythonScriptWithBinary runs a script with the given Python binary and
// returns its output.
// It is a general-purpose helper for executing Python code from Go.
func runPythonScriptWithBinary(pythonBinary, script string) (string, error) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, pythonBinary, "-c", script)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute python script: %w", err)
	}

	return strings.TrimSpace(string(output)), nil
}
