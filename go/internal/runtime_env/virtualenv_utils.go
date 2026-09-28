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
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/ray-project/ray/go/pkg/log"
)

// isInVirtualenv reports whether the current process is running inside a virtualenv.
func isInVirtualenv() bool {
	python, err := getPythonExecutable()
	if err != nil {
		// Return false if no Python executable can be found.
		return false
	}

	// Run a Python command to check the virtualenv status.
	cmd := exec.Command(python, "-c",
		"import sys; print('yes' if (hasattr(sys, 'real_prefix') or "+
			"(hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)) else 'no')")
	output, err := cmd.Output()
	if err != nil {
		// Return false if the Python command fails.
		return false
	}

	result := strings.TrimSpace(string(output))
	return result == "yes"
}

// getVirtualenvPath returns the virtualenv directory under targetDir.
func getVirtualenvPath(targetDir string) string {
	return filepath.Join(targetDir, "virtualenv")
}

// getVirtualenvPython returns the path of the Python executable inside the virtualenv.
func getVirtualenvPython(targetDir string) string {
	virtualenvPath := getVirtualenvPath(targetDir)
	if runtime.GOOS == "windows" {
		return filepath.Join(virtualenvPath, "Scripts", "python.exe")
	}
	return filepath.Join(virtualenvPath, "bin", "python")
}

// getVirtualenvActivateCommand returns the command prefix that activates the virtualenv.
func getVirtualenvActivateCommand(targetDir string) []string {
	virtualenvPath := getVirtualenvPath(targetDir)
	if runtime.GOOS == "windows" {
		return []string{filepath.Join(virtualenvPath, "Scripts", "activate.bat"), "1>&2", "&&"}
	}
	return []string{"source", filepath.Join(virtualenvPath, "bin/activate"), "1>&2", "&&"}
}

// getPythonExecutable returns the path of the Python executable.
func getPythonExecutable() (string, error) {
	executable, err := exec.LookPath("python3")
	if err != nil {
		executable, err = exec.LookPath("python")
		if err != nil {
			return "", fmt.Errorf("python executable not found in PATH")
		}
	}
	return executable, nil
}

// createOrGetVirtualenv creates a virtualenv at the given path, cloning the
// current one when already inside a virtualenv.
func createOrGetVirtualenv(ctx context.Context, path string, cwd string) error {
	python := PythonExecutable
	if python == "" {
		var err error
		python, err = getPythonExecutable()
		if err != nil {
			return err
		}
	}
	virtualenvPath := filepath.Join(path, "virtualenv")
	virtualenvAppDataPath := filepath.Join(path, "virtualenv_app_data")

	var currentPythonDir string
	var env map[string]string

	// Get sys.prefix by invoking Python so it exactly matches the sys.prefix of
	// that Python version.
	cmd := exec.Command(python, "-c", "import sys; print(sys.prefix)")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get sys.prefix: %w", err)
	}
	currentPythonDir = strings.TrimSpace(string(output))

	// Set up env according to the OS (mirrors the _WIN32 check in the Python version).
	if runtime.GOOS == "windows" {
		// Copy the environment variables, mirroring Python's os.environ.copy().
		env = make(map[string]string)
		for _, e := range os.Environ() {
			if key, val, ok := strings.Cut(e, "="); ok {
				env[key] = val
			}
		}
	} else {
		env = make(map[string]string)
	}

	if isInVirtualenv() {
		// Already inside a virtualenv: clone it with virtualenv-clone.
		// Note: still use the currentPythonDir computed above, not getPackagesDir.

		// Resolve the path of the _clonevirtualenv.py script.
		scriptPath, err := _resolveCloneVirtualenvPath()
		if err != nil {
			return fmt.Errorf("failed to resolve clonevirtualenv script path: %w", err)
		}

		createVenvCmd := []string{python, scriptPath, currentPythonDir, virtualenvPath}
		log.Log.V(1).Info("Cloning virtualenv", "script_path", scriptPath, "current_python_dir", currentPythonDir, "virtualenv_path", virtualenvPath)

		cmdIndexGen := newCmdIndexGen()
		_, err = checkOutputCmd(ctx, createVenvCmd, cwd, env, cmdIndexGen)
		return err
	} else {
		// Not inside a virtualenv: create a fresh virtualenv.
		// currentPythonDir has already been set up above according to the OS.
		env = make(map[string]string)

		createVenvCmd := []string{
			python,
			"-m",
			"virtualenv",
			"--app-data",
			virtualenvAppDataPath,
			"--reset-app-data",
			"--no-periodic-update",
			"--system-site-packages",
			"--no-download",
			virtualenvPath,
		}
		log.Log.V(1).Info("Creating virtualenv at virtualenv_path, current python dir",
			"virtualenv_path", virtualenvPath, "current_python_dir", currentPythonDir)
		cmdIndexGen := newCmdIndexGen()
		_, err = checkOutputCmd(ctx, createVenvCmd, cwd, env, cmdIndexGen)
		return err
	}
}

// cmdIndexGen generates command indexes, mirroring Python's itertools.count(1).
type cmdIndexGen struct {
	counter int64
}

// newCmdIndexGen returns a new command index generator.
func newCmdIndexGen() *cmdIndexGen {
	return &cmdIndexGen{counter: 0}
}

// next returns the next command index.
func (g *cmdIndexGen) next() int64 {
	return atomic.AddInt64(&g.counter, 1)
}

// getLastNLines returns the last n lines of the string.
func getLastNLines(s string, n int) string {
	if n < 0 {
		return s
	}
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) <= n {
		return s
	}
	start := len(lines) - n
	return strings.Join(lines[start:], "\n")
}

// checkOutputCmd runs the command and checks its output.
// It returns the command's output string along with any error.
func checkOutputCmd(ctx context.Context, cmd []string, cwd string, env map[string]string, cmdIndexGen *cmdIndexGen) (string, error) {
	cmdIndex := int64(0)
	if cmdIndexGen != nil {
		cmdIndex = cmdIndexGen.next()
	}

	log.Log.V(1).Info("Run cmd[%d] %v", cmdIndex, cmd)

	command := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	command.Dir = cwd

	// Set the environment variables for the command.
	if len(env) > 0 {
		command.Env = make([]string, 0, len(env))
		for k, v := range env {
			command.Env = append(command.Env, k+"="+v)
		}
	} else {
		command.Env = os.Environ()
	}

	output, err := command.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		truncatedOutput := getLastNLines(outputStr, 50)
		if exitErr, ok := err.(*exec.ExitError); ok {
			return truncatedOutput, fmt.Errorf("Run cmd[%d] failed with exit code %d, output: %s", cmdIndex, exitErr.ExitCode(), truncatedOutput)
		}
		return truncatedOutput, fmt.Errorf("Run cmd[%d] got exception: %w, output: %s", cmdIndex, err, truncatedOutput)
	}

	if len(outputStr) > 0 {
		log.Log.V(1).Info("Output of cmd[%d]: %s", cmdIndex, outputStr)
	} else {
		log.Log.V(1).Info("No output for cmd[%d]", cmdIndex)
	}

	return outputStr, nil
}
