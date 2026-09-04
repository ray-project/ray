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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"

	"al.essio.dev/pkg/shellescape"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// envVarPattern matches ${VAR} reference patterns in environment variables.
var envVarPattern = regexp.MustCompile(`\$\{[A-Z0-9_]+\}`)

var runtimeGOOS = runtime.GOOS

var PythonExecutable string

type RuntimeEnvContext struct {
	CommandPrefix            []string          `json:"command_prefix,omitempty"`
	EnvVars                  map[string]string `json:"env_vars,omitempty"`
	PyExecutable             string            `json:"py_executable,omitempty"`
	OverrideWorkerEntrypoint string            `json:"override_worker_entrypoint,omitempty"`
	JavaJars                 []string          `json:"java_jars,omitempty"`
	CppExecutable            string            `json:"cpp_executable,omitempty"`
	GoExecutable             string            `json:"go_executable,omitempty"`
}

func NewRuntimeEnvContext(envVars map[string]string) *RuntimeEnvContext {
	return &RuntimeEnvContext{
		EnvVars:      envVars,
		PyExecutable: PythonExecutable,
	}
}

func (c *RuntimeEnvContext) SerializeContext() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to serialize runtime env context: %w", err)
	}
	return string(data), nil
}

func DeserializeContext(s string) (*RuntimeEnvContext, error) {
	if s == "" {
		s = "{}"
	}
	var runtimeEnvCtx RuntimeEnvContext
	if err := json.Unmarshal([]byte(s), &runtimeEnvCtx); err != nil {
		return nil, fmt.Errorf("failed to deserialize runtime env context: %w", err)
	}
	if runtimeEnvCtx.PyExecutable == "" {
		runtimeEnvCtx.PyExecutable = PythonExecutable
	}
	return &runtimeEnvCtx, nil
}

func (c *RuntimeEnvContext) IsEmpty() bool {
	return len(c.CommandPrefix) == 0 &&
		len(c.EnvVars) == 0 &&
		c.PyExecutable == "" &&
		c.OverrideWorkerEntrypoint == "" &&
		len(c.JavaJars) == 0 &&
		c.CppExecutable == "" &&
		c.GoExecutable == ""
}

// applyEnvVars applies the environment variable configuration.
func (c *RuntimeEnvContext) applyEnvVars() error {
	for k, v := range c.EnvVars {
		// Expand ${VAR} and $VAR forms with os.ExpandEnv, then remove any
		// remaining ${VAR} references using the precompiled envVarPattern.
		expanded := os.ExpandEnv(v)
		if err := os.Setenv(k, envVarPattern.ReplaceAllString(expanded, "")); err != nil {
			return fmt.Errorf("failed to set env %s: %w", k, err)
		}
	}
	return nil
}

// ExecWorker executes the Worker process.
// Corresponds to Python: RuntimeEnvContext.exec_worker()
//
// Args:
//   - passthroughArgs: args passed to the worker
//   - language: worker language type (PYTHON, JAVA, CPP, GO)
//
// Note: On Unix systems it execs to replace the current process; on Windows it uses exec.Command().Run().
func (c *RuntimeEnvContext) ExecWorker(passthroughArgs []string, language proto.Language) error {
	// 1. Apply environment variables.
	if err := c.applyEnvVars(); err != nil {
		return err
	}

	// 2. Build the executable command.
	cmd, err := c.buildWorkerCommand(passthroughArgs, language)
	if err != nil {
		return err
	}

	// 3. Execute (Unix: exec, Windows: exec.Command).
	if runtime.GOOS == "windows" {
		return c.execWorkerWindows(cmd)
	}
	return c.execWorkerUnix(cmd)
}

// buildWorkerCommand builds the worker startup command.
// It returns the command array and an error.
func (c *RuntimeEnvContext) buildWorkerCommand(passthroughArgs []string, language proto.Language) ([]string, error) {
	var executable []string

	switch language {
	case proto.Language_PYTHON:
		if runtimeGOOS == "windows" {
			executable = []string{c.PyExecutable}
		} else {
			executable = []string{"exec", c.PyExecutable}
		}

	case proto.Language_JAVA:
		executable = []string{"java"}
		var classPathBuilder strings.Builder

		rayJarsDir, _ := common.GetRayJarsDir()
		classPathBuilder.WriteString(filepath.Join(rayJarsDir, "*"))
		for _, jar := range c.JavaJars {
			classPathBuilder.WriteString(":")
			classPathBuilder.WriteString(jar)
			classPathBuilder.WriteString("/*")
			classPathBuilder.WriteString(":")
			classPathBuilder.WriteString(jar)
		}

		classPathArgs := []string{"-cp", classPathBuilder.String()}
		passthroughArgs = append(classPathArgs, passthroughArgs...)

	case proto.Language_CPP:
		executable = c.buildExecPrefix(c.CppExecutable)

	case proto.Language_GO:
		executable = c.buildExecPrefix(c.GoExecutable)

	default:
		// Unknown language, use the exec prefix.
		if runtimeGOOS == "windows" {
			executable = []string{}
		} else {
			executable = []string{"exec"}
		}
	}

	// Handle override_worker_entrypoint.
	if c.OverrideWorkerEntrypoint != "" && len(passthroughArgs) > 0 {
		passthroughArgs[0] = c.OverrideWorkerEntrypoint
	}

	cmd := append(c.CommandPrefix, executable...)
	if runtimeGOOS == "windows" {
		for index, arg := range passthroughArgs {
			passthroughArgs[index] = strings.ReplaceAll(arg, "&", "%26")
		}
	} else {
		for index, arg := range passthroughArgs {
			passthroughArgs[index] = shellescape.Quote(arg)
		}
	}

	cmd = append(cmd, passthroughArgs...)

	if len(cmd) == 0 {
		return nil, fmt.Errorf("no executable specified")
	}

	return cmd, nil
}

func (c *RuntimeEnvContext) buildExecPrefix(executable string) []string {
	if executable == "" {
		executable = os.Args[0]
	}
	if runtime.GOOS == "windows" {
		return []string{executable}
	}
	return []string{"exec", executable}
}

// execWorkerUnix executes the worker on Unix systems (using syscall.Exec to replace the current process).
func (c *RuntimeEnvContext) execWorkerUnix(cmd []string) error {
	if len(cmd) == 0 || cmd[0] == "" {
		return fmt.Errorf("no executable specified")
	}

	// macOS special handling: DYLD_LIBRARY_PATH.
	// The environment variable must be inlined into the bash -c command string,
	// e.g.: DYLD_LIBRARY_PATH=/path cmd args
	var envPrefix string
	if runtime.GOOS == "darwin" {
		if libPath := os.Getenv("DYLD_LIBRARY_PATH"); libPath != "" {
			envPrefix = fmt.Sprintf("DYLD_LIBRARY_PATH=%s ", shellescape.Quote(libPath))
		}
	}

	// Join the cmd array into a single string as the argument to bash -c.
	// bash -c expects a single string command, e.g.: bash -c "exec raygo setup_worker ..."
	cmdStr := strings.Join(cmd, " ")
	if envPrefix != "" {
		// macOS: prepend the environment variable to the command.
		cmdStr = envPrefix + cmdStr
	}

	// Build the bash -c command array.
	// argv[0] = "/bin/bash", argv[1] = "-c", argv[2] = command string.
	bashCmd := []string{"/bin/bash", "-c", cmdStr}

	// Print the debug log.
	log.Log.Info("execWorkerUnix: executing command",
		"cmd", cmd,
		"cmdStr", cmdStr,
		"bashCmd", bashCmd,
	)

	return syscall.Exec("/bin/bash", bashCmd, os.Environ())
}

// execWorkerWindows executes the worker on Windows systems.
func (c *RuntimeEnvContext) execWorkerWindows(cmd []string) error {
	if len(cmd) == 0 || cmd[0] == "" {
		return fmt.Errorf("no executable specified")
	}

	executable := cmd[0]
	args := []string{}
	if len(cmd) > 1 {
		args = cmd[1:]
	}

	execCmd := exec.Command(executable, args...)
	execCmd.Stdin = os.Stdin
	execCmd.Stdout = os.Stdout
	execCmd.Stderr = os.Stderr
	execCmd.Env = os.Environ()

	return execCmd.Run()
}
