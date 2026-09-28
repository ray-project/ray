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
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/pkg/log"
)

const (
	// RAY_CONDA_HOME is the environment variable that specifies the conda installation path.
	RAY_CONDA_HOME = "RAY_CONDA_HOME"
)

var (
	// _WIN32 reports whether the current OS is Windows.
	_WIN32 = runtime.GOOS == "windows"
)

// getCondaActivateCommands returns the commands that activate the given conda env.
func getCondaActivateCommands(condaEnvName string) []string {
	var activateCondaEnv []string

	// Check for newer conda versions.
	if !_WIN32 && (os.Getenv("CONDA_EXE") != "" || os.Getenv(RAY_CONDA_HOME) != "") {
		condaPath := getCondaBinExecutable("conda")
		activateCondaEnv = []string{
			".",
			fmt.Sprintf("%s/../etc/profile.d/conda.sh", filepath.Dir(condaPath)),
			"&&",
		}
		activateCondaEnv = append(activateCondaEnv, "conda", "activate", condaEnvName)
	} else {
		activatePath := getCondaBinExecutable("activate")
		if !_WIN32 {
			// Use bash command syntax.
			activateCondaEnv = []string{"source", activatePath, condaEnvName}
		} else {
			condaPath := getCondaBinExecutable("conda")
			activateCondaEnv = []string{condaPath, "activate", condaEnvName}
		}
	}
	return append(activateCondaEnv, "1>&2", "&&")
}

// getCondaBinExecutable returns the path of the given executable within the conda installation.
// The conda home directory (expected to contain a 'bin' subdirectory on Linux) can be configured
// via the RAY_CONDA_HOME environment variable. If RAY_CONDA_HOME is not set, the CONDA_EXE
// environment variable set when conda is activated is used instead. If neither is set, this
// function returns executable_name.
func getCondaBinExecutable(executableName string) string {
	condaHome := os.Getenv(RAY_CONDA_HOME)
	if condaHome != "" {
		if _WIN32 {
			candidate := filepath.Join(condaHome, fmt.Sprintf("%s.exe", executableName))
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
			candidate = filepath.Join(condaHome, fmt.Sprintf("%s.bat", executableName))
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		} else {
			return filepath.Join(condaHome, "bin", executableName)
		}
	} else {
		condaHome = "."
	}

	// Use CONDA_EXE per https://github.com/conda/conda/issues/7126.
	if condaExe := os.Getenv("CONDA_EXE"); condaExe != "" {
		condaBinDir := filepath.Dir(condaExe)
		if _WIN32 {
			candidate := filepath.Join(condaHome, fmt.Sprintf("%s.exe", executableName))
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
			candidate = filepath.Join(condaHome, fmt.Sprintf("%s.bat", executableName))
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		} else {
			return filepath.Join(condaBinDir, executableName)
		}
	}

	if _WIN32 {
		return executableName + ".bat"
	}
	return executableName
}

// fileExists reports whether the file exists.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// getCondaEnvName derives the env name from the conda env file contents.
func getCondaEnvName(condaEnvPath string) (string, error) {
	content, err := os.ReadFile(condaEnvPath)
	if err != nil {
		return "", fmt.Errorf("failed to read conda env file: %w", err)
	}

	hash := sha1.Sum(content)
	return fmt.Sprintf("ray-%s", hex.EncodeToString(hash[:])), nil
}

// createCondaEnvIfNeeded creates the conda env if it does not exist.
// condaYamlFile: the path to the conda environment.yml file.
// prefix: the directory the env is installed into via the --prefix option; this also becomes the conda env name.
func createCondaEnvIfNeeded(ctx context.Context, condaYamlFile string, prefix string, logger logr.Logger) error {
	if !logger.GetSink().Enabled(0) {
		logger = log.Log
	}

	condaPath := getCondaBinExecutable("conda")

	// Test whether conda is available.
	cmd := exec.CommandContext(ctx, condaPath, "--help")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("could not find Conda executable at '%s'. "+
			"Ensure Conda is installed as per the instructions at "+
			"https://conda.io/projects/conda/en/latest/user-guide/install/index.html. "+
			"You can also configure Ray to look for a specific "+
			"Conda executable by setting the %s environment variable to the path of the Conda executable.",
			condaPath, RAY_CONDA_HOME)
	}

	// Get the list of existing envs.
	cmd = exec.CommandContext(ctx, condaPath, "env", "list", "--json")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list conda environments: %w", err)
	}

	// Parse the JSON output.
	var envData struct {
		Envs []string `json:"envs"`
	}

	// Find the position of the first '{' character to tolerate any extra output.
	jsonStart := strings.Index(string(output), "{")
	if jsonStart == -1 {
		return fmt.Errorf("invalid conda env list output: no JSON object found")
	}

	if err := json.Unmarshal([]byte(output[jsonStart:]), &envData); err != nil {
		return fmt.Errorf("failed to parse conda env list JSON: %w", err)
	}

	// Check whether the env already exists.
	envExists := false
	for _, env := range envData.Envs {
		if env == prefix {
			envExists = true
			break
		}
	}

	if envExists {
		logger.Info("Conda environment already exists", "prefix", prefix)
		return nil
	}

	// Create the new env.
	createCmd := []string{
		condaPath,
		"env",
		"create",
		"--file",
		condaYamlFile,
		"--prefix",
		prefix,
	}

	logger.Info("Creating conda environment", "prefix", prefix)
	exitCode, outputStr, err := execCmdStreamToLogger(ctx, createCmd, logger)
	if err != nil || exitCode != 0 {
		// Remove the directory directly to clean up a possibly partially created env.
		if removeErr := os.RemoveAll(prefix); removeErr != nil && !os.IsNotExist(removeErr) {
			logger.Error(removeErr, "Failed to remove partially created conda environment", "prefix", prefix)
		}
		return fmt.Errorf("failed to install conda environment %s:\nOutput:\n%s", prefix, outputStr)
	}

	return nil
}

// dirExists reports whether the directory exists.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// deleteCondaEnv deletes the given conda env.
func deleteCondaEnv(ctx context.Context, prefix string, logger logr.Logger) (bool, error) {
	if !logger.GetSink().Enabled(0) {
		logger = log.Log
	}

	logger.Info("Deleting conda environment", "prefix", prefix)

	condaPath := getCondaBinExecutable("conda")
	deleteCmd := []string{condaPath, "remove", "-p", prefix, "--all", "-y"}

	exitCode, outputStr, err := execCmdStreamToLogger(ctx, deleteCmd, logger)
	if err != nil || exitCode != 0 {
		logger.V(1).Info("Failed to delete conda environment", "prefix", prefix, "output", outputStr)
		return false, err
	}

	return true, nil
}

// getCondaEnvList returns the full paths of all conda envs.
func getCondaEnvList(ctx context.Context) ([]string, error) {
	condaPath := getCondaBinExecutable("conda")

	// Test whether conda is available.
	cmd := exec.CommandContext(ctx, condaPath, "--help")
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("could not find Conda executable at %s", condaPath)
	}

	// Get the env list.
	cmd = exec.CommandContext(ctx, condaPath, "env", "list", "--json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list conda environments: %w", err)
	}

	// Parse the JSON output.
	var envData struct {
		Envs []string `json:"envs"`
	}

	if err := json.Unmarshal(output, &envData); err != nil {
		return nil, fmt.Errorf("failed to parse conda env list JSON: %w", err)
	}

	return envData.Envs, nil
}

// getCondaInfoJSON returns the output of `conda info --json`.
// The returned map contains the following key information:
// - conda_prefix: the conda installation path.
// - envs: the list of absolute paths of the conda envs.
func getCondaInfoJSON(ctx context.Context) (map[string]interface{}, error) {
	condaPath := getCondaBinExecutable("conda")

	// Test whether conda is available.
	cmd := exec.CommandContext(ctx, condaPath, "--help")
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("could not find Conda executable at %s", condaPath)
	}

	// Get the conda info.
	cmd = exec.CommandContext(ctx, condaPath, "info", "--json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get conda info: %w", err)
	}

	// Parse the JSON output.
	var info map[string]interface{}
	if err := json.Unmarshal(output, &info); err != nil {
		return nil, fmt.Errorf("failed to parse conda info JSON: %w", err)
	}

	return info, nil
}

// getCondaEnvs returns the conda env list as a list of (name, path) tuples.
func getCondaEnvs(condaInfo map[string]interface{}) [][2]string {
	prefix, ok := condaInfo["conda_prefix"].(string)
	if !ok {
		return [][2]string{}
	}

	envsInterface, ok := condaInfo["envs"].([]interface{})
	if !ok {
		return [][2]string{}
	}

	ret := make([][2]string, 0, len(envsInterface))
	for _, envInterface := range envsInterface {
		env, ok := envInterface.(string)
		if !ok {
			continue
		}

		if env == prefix {
			ret = append(ret, [2]string{"base", env})
		} else {
			ret = append(ret, [2]string{filepath.Base(env), env})
		}
	}

	return ret
}

// shellCommandException represents a failed shell command execution.
type shellCommandException struct {
	exitCode int
	stdout   string
	stderr   string
}

func (e *shellCommandException) Error() string {
	return fmt.Sprintf("Non-zero exit code: %d\n\nSTDOUT:\n%s\n\nSTDERR:%s",
		e.exitCode, e.stdout, e.stderr)
}

// execCmd runs the command as a subprocess.
// It returns the (exitCode, stdout, stderr) triple.
// If throwOnError is true and the exit code is non-zero, an error is returned.
func execCmd(cmd []string, throwOnError bool) (int, string, string, error) {
	command := exec.Command(cmd[0], cmd[1:]...)

	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := command.StderrPipe()
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := command.Start(); err != nil {
		return -1, "", "", fmt.Errorf("failed to start command: %w", err)
	}

	// Read stdout and stderr.
	stdoutBytes, err := io.ReadAll(stdoutPipe)
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to read stdout: %w", err)
	}

	stderrBytes, err := io.ReadAll(stderrPipe)
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to read stderr: %w", err)
	}

	if err := command.Wait(); err != nil {
		// Wait may return an ExitError, but we can still obtain the exit code.
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode := exitErr.ExitCode()
			if throwOnError && exitCode != 0 {
				return exitCode, string(stdoutBytes), string(stderrBytes), &shellCommandException{
					exitCode: exitCode,
					stdout:   string(stdoutBytes),
					stderr:   string(stderrBytes),
				}
			}
			return exitCode, string(stdoutBytes), string(stderrBytes), nil
		}
		return -1, "", "", fmt.Errorf("command wait failed: %w", err)
	}

	exitCode := command.ProcessState.ExitCode()
	if throwOnError && exitCode != 0 {
		return exitCode, string(stdoutBytes), string(stderrBytes), &shellCommandException{
			exitCode: exitCode,
			stdout:   string(stdoutBytes),
			stderr:   string(stderrBytes),
		}
	}

	return exitCode, string(stdoutBytes), string(stderrBytes), nil
}

// execCmdStreamToLogger runs the command as a subprocess and streams its output to the logger.
// It returns the last n_lines lines of output (stdout and stderr combined).
func execCmdStreamToLogger(ctx context.Context, cmd []string, logger logr.Logger, nLines ...int) (int, string, error) {
	n := 50 // Keep the last 50 lines by default.
	if len(nLines) > 0 {
		n = nLines[0]
	}

	command := exec.CommandContext(ctx, cmd[0], cmd[1:]...)

	// Merge stdout and stderr into the same pipe.
	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		return -1, "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := command.Start(); err != nil {
		return -1, "", fmt.Errorf("failed to start command: %w", err)
	}

	lastNLines := make([]string, 0, n)
	scanner := bufio.NewScanner(stdoutPipe)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		lastNLines = append(lastNLines, line)
		if len(lastNLines) > n {
			lastNLines = lastNLines[1:]
		}

		logger.Info(line)
	}

	if err := scanner.Err(); err != nil {
		return -1, "", fmt.Errorf("error reading command output: %w", err)
	}

	if err := command.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), strings.Join(lastNLines, "\n"), nil
		}
		return -1, "", fmt.Errorf("command wait failed: %w", err)
	}

	return command.ProcessState.ExitCode(), strings.Join(lastNLines, "\n"), nil
}
