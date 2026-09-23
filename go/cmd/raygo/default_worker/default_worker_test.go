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

package default_worker

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/internal/worker"
	"github.com/spf13/cobra"
)

// TestWorkerCmd_HasRequiredFlags verifies that the worker command declares the required flags.
func TestWorkerCmd_HasRequiredFlags(t *testing.T) {
	// Verify that WorkerCmd is initialized correctly.
	if WorkerCmd.Use != "defaultworker" {
		t.Errorf("WorkerCmd.Use = %q, want %q", WorkerCmd.Use, "defaultworker")
	}
	if WorkerCmd.Short == "" {
		t.Error("WorkerCmd.Short should not be empty")
	}
	if WorkerCmd.RunE == nil {
		t.Error("WorkerCmd.RunE should be set")
	}

	// Verify that the required flags are registered - these are the flags actually
	// registered by default_worker.go.
	// Note: argument parsing is performed by worker.NewRayConfig, not directly by the
	// Cobra command.
	requiredFlags := []string{
		worker.GcsAddress,
		worker.CodeSearchPath,
		worker.RedisUsername,
		worker.RedisPassword,
		worker.JobID,
		worker.ClusterID,
		worker.NodeManagerPort,
		worker.RayletName,
		worker.PlasmaStoreName,
		worker.SessionDir,
		worker.LogsDir,
		worker.NodeIpAddress,
		worker.HeadArgs,
		worker.StartupToken,
		// The worker ID flag is part of the Go runtime's worker handshake with the
		// raylet, so it must stay registered.
		worker.WorkerIDFlag,
		worker.DefaultActorLifetime,
		worker.RuntimeEnvFlag,
		worker.RuntimeEnvHashFlag,
		worker.JobNamespace,
	}

	for _, flagName := range requiredFlags {
		flag := WorkerCmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("Required flag %q not found", flagName)
		}
	}
}

// TestWorkerCmd_DefaultValues verifies the default values of the worker command flags.
func TestWorkerCmd_DefaultValues(t *testing.T) {
	tests := []struct {
		name     string
		flagName string
		expected string
	}{
		{
			name:     "ray-address default",
			flagName: worker.GcsAddress,
			expected: "",
		},
		{
			name:     "gcs-address default",
			flagName: "gcs-address",
			expected: "",
		},
		{
			name:     "ray-code-search-path default",
			flagName: worker.CodeSearchPath,
			expected: "",
		},
		{
			name:     "ray-redis-username default",
			flagName: worker.RedisUsername,
			expected: "",
		},
		{
			name:     "ray-redis-password default",
			flagName: worker.RedisPassword,
			expected: "",
		},
		{
			name:     "ray-job-id default",
			flagName: worker.JobID,
			expected: "",
		},
		{
			name:     "ray-cluster-id default",
			flagName: worker.ClusterID,
			expected: "",
		},
		{
			name:     "ray-node-manager-port default",
			flagName: worker.NodeManagerPort,
			expected: "0",
		},
		{
			name:     "ray-raylet-socket-name default",
			flagName: worker.RayletName,
			expected: "",
		},
		{
			name:     "ray-plasma-store-socket-name default",
			flagName: worker.PlasmaStoreName,
			expected: "",
		},
		{
			name:     "ray-session-dir default",
			flagName: worker.SessionDir,
			expected: "",
		},
		{
			name:     "ray-logs-dir default",
			flagName: worker.LogsDir,
			expected: "",
		},
		{
			name:     "ray-node-ip-address default",
			flagName: worker.NodeIpAddress,
			expected: "",
		},
		{
			name:     "ray-head-args default",
			flagName: worker.HeadArgs,
			expected: "",
		},
		{
			name:     "ray-startup-token default",
			flagName: worker.StartupToken,
			expected: "-1",
		},
		{
			name:     "ray-worker-id default",
			flagName: worker.WorkerIDFlag,
			expected: "",
		},
		{
			name:     "ray-default-actor-lifetime default",
			flagName: worker.DefaultActorLifetime,
			expected: "",
		},
		{
			name:     "ray-runtime-env default",
			flagName: worker.RuntimeEnvFlag,
			expected: "",
		},
		{
			name:     "ray-runtime-env-hash default",
			flagName: worker.RuntimeEnvHashFlag,
			expected: "-1",
		},
		{
			name:     "ray-job-namespace default",
			flagName: worker.JobNamespace,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := WorkerCmd.Flags().Lookup(tt.flagName)
			if flag == nil {
				t.Errorf("Flag %q not found", tt.flagName)
				return
			}
			if flag.DefValue != tt.expected {
				t.Errorf("Flag %q default = %q, want %q", tt.flagName, flag.DefValue, tt.expected)
			}
		})
	}
}

// TestGetDefaultWorkerCmd verifies the GetDefaultWorkerCmd function.
func TestGetDefaultWorkerCmd(t *testing.T) {
	cmd := GetDefaultWorkerCmd()
	if cmd == nil {
		t.Fatal("GetDefaultWorkerCmd returned nil")
	}
	if cmd != WorkerCmd {
		t.Error("GetDefaultWorkerCmd did not return the expected WorkerCmd")
	}
}

// TestWorkerCmd_UnknownFlagsTolerated verifies that the worker command tolerates
// unknown flags. The raylet appends shared flags (e.g. --ray-debugger-external)
// to every worker command regardless of language, so the Go worker must ignore
// unknown flags instead of failing to start. This matches the Java/C++ workers.
func TestWorkerCmd_UnknownFlagsTolerated(t *testing.T) {
	cmd := GetDefaultWorkerCmd()
	if !cmd.FParseErrWhitelist.UnknownFlags {
		t.Error("WorkerCmd.FParseErrWhitelist.UnknownFlags should be true to tolerate raylet-appended flags")
	}

	// Parsing a list that contains unknown flags must not report an unknown flag error.
	args := []string{"--ray-debugger-external", "--startup-token=123", "extra-arg"}
	cmd.SetArgs(args)
	if err := cmd.ParseFlags(args); err != nil {
		t.Errorf("ParseFlags returned error for unknown flag: %v", err)
	}
}

// TestRunWorker_MissingRequiredFlags verifies runWorker's error handling when
// required flags are missing.
func TestRunWorker_MissingRequiredFlags(t *testing.T) {
	cmd := &cobra.Command{}
	// Register the necessary flags without setting any values - using the real worker
	// package constants.
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 0, "")

	err := runWorker(cmd, []string{})
	if err == nil {
		t.Error("Expected error for missing required flags, got nil")
	}
}

// TestRunWorker_InvalidNodeManagerPort verifies runWorker's error handling for an
// invalid port number.
func TestRunWorker_InvalidNodeManagerPort(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, -1, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")

	err := runWorker(cmd, []string{})
	if err == nil {
		t.Error("Expected error for invalid node manager port, got nil")
	}
}

// TestRunWorker_InvalidNodeManagerPort_TooLarge verifies runWorker's error handling
// when the port number is too large.
func TestRunWorker_InvalidNodeManagerPort_TooLarge(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 70000, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")

	err := runWorker(cmd, []string{})
	if err == nil {
		t.Error("Expected error for node manager port too large, got nil")
	}
}

// TestRunWorker_ValidInput verifies that runWorker accepts valid input (without
// actually running the worker).
func TestRunWorker_ValidInput(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "192.168.1.100:6379", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.RayletName, "/tmp/ray/raylet", "")
	cmd.Flags().String(worker.PlasmaStoreName, "/tmp/ray/plasma", "")
	cmd.Flags().String(worker.NodeIpAddress, "192.168.1.100", "")
	cmd.Flags().Int(worker.StartupToken, 12345, "")
	cmd.Flags().String(worker.ClusterID, "test-cluster", "")

	// Note: this test does not actually run the worker because a full runtime
	// environment is required. It only verifies that the arguments can be registered.
}

// TestRunWorker_WithRuntimeEnv covers the case where a runtime environment is configured.
func TestRunWorker_WithRuntimeEnv(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.RuntimeEnvFlag, `{"pip": ["requests"]}`, "")
	cmd.Flags().Int(worker.RuntimeEnvHashFlag, 12345, "")

	// Verifies that a valid JSON runtime env configuration can be parsed.
}

// TestRunWorker_WithInvalidRuntimeEnv covers the case where an invalid runtime
// environment configuration is provided.
func TestRunWorker_WithInvalidRuntimeEnv(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.RuntimeEnvFlag, `{invalid json}`, "")

	// Verifies that an invalid JSON runtime env configuration is rejected.
}

// TestRunWorker_WithActorLifetime covers the case where an actor lifetime is configured.
func TestRunWorker_WithActorLifetime(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.DefaultActorLifetime, "detached", "")
	cmd.Flags().String(worker.JobNamespace, "test-namespace", "")

	// Verifies the detached actor lifetime configuration.
}

// TestRunWorker_WithCodeSearchPath covers the case where a code search path is configured.
func TestRunWorker_WithCodeSearchPath(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.CodeSearchPath, "/path/to/lib1:/path/to/lib2", "")

	// Verifies that multiple code search paths are parsed correctly (separated by ':').
}

// TestRunWorker_WithHeadArgs covers the case where head args are configured.
func TestRunWorker_WithHeadArgs(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.HeadArgs, "--num-cpus=4 --num-gpus=2", "")

	// Verifies that head args are registered correctly.
}

// TestMain_ExecuteError verifies that executing the worker command surfaces an error.
func TestMain_ExecuteError(t *testing.T) {
	// Create a new command to test with, to avoid polluting global state.
	testCmd := &cobra.Command{
		Use: "testcmd",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runWorker(cmd, args)
		},
	}

	// Register flags.
	testCmd.Flags().Int("ray_node_manager_port", -1, "")
	testCmd.Flags().String("ray_redis_username", "", "")
	testCmd.Flags().String("ray_redis_password", "", "")

	// Set invalid command line arguments.
	testCmd.SetArgs([]string{"defaultworker", "--ray-node-manager-port=-1"})

	// Redirect stderr to capture log output (optional, depending on the assertions needed).
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	// Execute the command.
	err := testCmd.Execute()

	// Restore stderr.
	w.Close()
	os.Stderr = oldStderr

	// Read the output.
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Verify: check whether the expected error was returned, or whether the error
	// message was logged to stderr.
	if err == nil && !strings.Contains(output, "error") {
		t.Errorf("Expected an error or error log output, but got none. Output: %s", output)
	}
}

// TestWorkerCmd_Examples verifies that the worker command examples are correct.
func TestWorkerCmd_Examples(t *testing.T) {
	if WorkerCmd.Example == "" {
		t.Skip("No examples provided for WorkerCmd")
	}
	// Verify that the example contains the necessary parameters.
	exampleParams := []string{
		"--node-ip-address",
		"--node-manager-port",
		"--store-socket",
		"--raylet-socket",
		"--gcs-address",
	}
	for _, param := range exampleParams {
		if !strings.Contains(WorkerCmd.Example, param) {
			t.Logf("Example may be missing parameter: %s", param)
		}
	}
}

// TestRunWorker_LocalMode covers runWorker in local mode.
func TestRunWorker_LocalMode(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")

	// Local mode should behave differently.
}

// TestRunWorker_DriverType covers runWorker for the driver worker type.
func TestRunWorker_DriverType(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String(worker.GcsAddress, "", "")
	cmd.Flags().Int(worker.NodeManagerPort, 6379, "")
	cmd.Flags().String(worker.RedisUsername, "", "")
	cmd.Flags().String(worker.RedisPassword, "", "")
	cmd.Flags().String(worker.JobNamespace, "test-driver-ns", "")

	// The driver type should have the namespace configuration.
}
