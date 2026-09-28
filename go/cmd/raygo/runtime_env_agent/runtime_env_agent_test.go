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

package runtime_env_agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/gcs/mockgcs"
)

// testClusterIDHex is the hex representation of the test cluster ID; 56 hex
// characters map to a 28-byte cluster ID.
const testClusterIDHex = "0123456789abcdef0123456789abcdef0123456789abcdef01234567"

// runAgentWithFakeGCS injects a fake GCS client and runs the run function with
// a cancellable context. server.Start blocks until ctx is cancelled, so the
// context is cancelled on a timer to let runRuntimeEnvAgent return. The fake
// client is injected via connectGCSClient and restored through t.Cleanup so
// other tests are not affected.
func runAgentWithFakeGCS(t *testing.T, run func(context.Context) error) error {
	t.Helper()
	origConnect := connectGCSClient
	connectGCSClient = func(opts gcs.ClientOptions) (gcs.Client, error) {
		return &mockgcs.Client{}, nil
	}
	t.Cleanup(func() { connectGCSClient = origConnect })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()

	select {
	case err := <-errCh:
		return err
	case <-time.After(500 * time.Millisecond):
		cancel()
	}
	select {
	case err := <-errCh:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("run function did not return after context cancel")
	}
	return nil
}

// makeTempDirs creates temporary log and runtime env directories under a fresh
// temporary base directory and returns their paths.
func makeTempDirs(t *testing.T) (baseDir, logDirPath, runtimeEnvDirPath string) {
	t.Helper()
	baseDir = t.TempDir()
	logDirPath = filepath.Join(baseDir, "logs")
	runtimeEnvDirPath = filepath.Join(baseDir, "runtime_env")
	if err := os.MkdirAll(logDirPath, 0755); err != nil {
		t.Fatalf("Failed to create log dir: %v", err)
	}
	if err := os.MkdirAll(runtimeEnvDirPath, 0755); err != nil {
		t.Fatalf("Failed to create runtime env dir: %v", err)
	}
	return baseDir, logDirPath, runtimeEnvDirPath
}

// TestIsValidLogLevel tests the isValidLogLevel function.
func TestIsValidLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		expected bool
	}{
		{"valid debug", "debug", true},
		{"valid info", "info", true},
		{"valid warning", "warning", true},
		{"valid error", "error", true},
		{"valid critical", "critical", true},
		{"invalid level", "invalid", false},
		{"empty level", "", false},
		{"uppercase debug", "DEBUG", false},
		{"uppercase INFO", "INFO", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidLogLevel(tt.level)
			if result != tt.expected {
				t.Errorf("isValidLogLevel(%q) = %v, want %v", tt.level, result, tt.expected)
			}
		})
	}
}

// TestRuntimeEnvAgentCmdInitialization tests RuntimeEnvAgentCmd initialization.
func TestRuntimeEnvAgentCmdInitialization(t *testing.T) {
	// init() runs automatically at package load, so no manual call is needed.

	// Verify the command is initialized correctly.
	if RuntimeEnvAgentCmd == nil {
		t.Fatal("RuntimeEnvAgentCmd should not be nil")
	}
	if RuntimeEnvAgentCmd.Use != "runtime-env-agent" {
		t.Errorf("RuntimeEnvAgentCmd.Use = %q, want %q", RuntimeEnvAgentCmd.Use, "runtime-env-agent")
	}
	if !strings.Contains(RuntimeEnvAgentCmd.Short, "Runtime Env Agent") {
		t.Errorf("RuntimeEnvAgentCmd.Short = %q, should contain %q", RuntimeEnvAgentCmd.Short, "Runtime Env Agent")
	}

	// Verify the required flags are registered.
	flags := RuntimeEnvAgentCmd.Flags()
	if flags.Lookup("node-ip-address") == nil {
		t.Error("flag node-ip-address should be registered")
	}
	if flags.Lookup("runtime-env-agent-port") == nil {
		t.Error("flag runtime-env-agent-port should be registered")
	}
	if flags.Lookup("gcs-address") == nil {
		t.Error("flag gcs-address should be registered")
	}
	if flags.Lookup("cluster-id-hex") == nil {
		t.Error("flag cluster-id-hex should be registered")
	}
	if flags.Lookup("runtime-env-dir") == nil {
		t.Error("flag runtime-env-dir should be registered")
	}
	if flags.Lookup("logging-rotate-bytes") == nil {
		t.Error("flag logging-rotate-bytes should be registered")
	}
	if flags.Lookup("logging-rotate-backup-count") == nil {
		t.Error("flag logging-rotate-backup-count should be registered")
	}
	if flags.Lookup("log-dir") == nil {
		t.Error("flag log-dir should be registered")
	}
	if flags.Lookup("temp-dir") == nil {
		t.Error("flag temp-dir should be registered")
	}

	// Verify the optional flags are registered.
	if flags.Lookup("logging-level") == nil {
		t.Error("flag logging-level should be registered")
	}
	if flags.Lookup("logging-format") == nil {
		t.Error("flag logging-format should be registered")
	}
	if flags.Lookup("logging-filename") == nil {
		t.Error("flag logging-filename should be registered")
	}
	if flags.Lookup("stdout-filepath") == nil {
		t.Error("flag stdout-filepath should be registered")
	}
	if flags.Lookup("stderr-filepath") == nil {
		t.Error("flag stderr-filepath should be registered")
	}
}

// TestGetRuntimeEnvAgentCmd tests the GetRuntimeEnvAgentCmd function.
func TestGetRuntimeEnvAgentCmd(t *testing.T) {
	// init() runs automatically at package load, so no manual call is needed.

	cmd := GetRuntimeEnvAgentCmd()
	if cmd == nil {
		t.Fatal("GetRuntimeEnvAgentCmd should not return nil")
	}
	if cmd != RuntimeEnvAgentCmd {
		t.Error("GetRuntimeEnvAgentCmd should return RuntimeEnvAgentCmd")
	}
}

// TestRunRuntimeEnvAgent tests the runRuntimeEnvAgent function.
func TestRunRuntimeEnvAgent(t *testing.T) {
	// Create temporary directories for the test.
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	// Set the test parameters. Port 0 makes the OS pick a free port so the
	// tests do not depend on fixed ports being available on the host.
	nodeIPAddress = "127.0.0.1"
	runtimeEnvAgentPort = 0
	gcsAddress = "127.0.0.1:6379"
	clusterIDHex = testClusterIDHex
	runtimeEnvDir = runtimeEnvDirPath
	logDir = logDirPath
	tempDir = baseDir
	loggingLevel = "info"
	loggingFilename = "test_runtime_env.log"
	loggingRotateBytes = 1024 * 1024 * 100 // 100MB
	loggingRotateBackupCount = 5

	err := runAgentWithFakeGCS(t, runRuntimeEnvAgent)
	if err != nil {
		t.Errorf("runRuntimeEnvAgent returned error: %v", err)
	}
}

// TestRunRuntimeEnvAgentWithDefaultLogSize tests runRuntimeEnvAgent with the
// default log size.
func TestRunRuntimeEnvAgentWithDefaultLogSize(t *testing.T) {
	// Create temporary directories for the test.
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	// Set the test parameters; a zero log size means the defaults are used.
	nodeIPAddress = "127.0.0.1"
	runtimeEnvAgentPort = 0
	gcsAddress = "127.0.0.1:6379"
	clusterIDHex = testClusterIDHex
	runtimeEnvDir = runtimeEnvDirPath
	logDir = logDirPath
	tempDir = baseDir
	loggingLevel = "info"
	loggingFilename = "test_runtime_env_default.log"
	loggingRotateBytes = 0       // Use the default value.
	loggingRotateBackupCount = 0 // Use the default value.

	err := runAgentWithFakeGCS(t, runRuntimeEnvAgent)
	if err != nil {
		t.Errorf("runRuntimeEnvAgent with default log size returned error: %v", err)
	}
}

// TestRunRuntimeEnvAgentZeroBackup tests runRuntimeEnvAgent with a zero log
// backup count.
func TestRunRuntimeEnvAgentZeroBackup(t *testing.T) {
	// Create temporary directories for the test.
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	// Set the test parameters with a zero log backup count.
	nodeIPAddress = "127.0.0.1"
	runtimeEnvAgentPort = 0
	gcsAddress = "127.0.0.1:6379"
	clusterIDHex = testClusterIDHex
	runtimeEnvDir = runtimeEnvDirPath
	logDir = logDirPath
	tempDir = baseDir
	loggingLevel = "debug"
	loggingFilename = "test_runtime_env_zero.log"
	loggingRotateBytes = 1024 * 1024 * 50
	loggingRotateBackupCount = 0 // The code falls back to the default of 5.

	err := runAgentWithFakeGCS(t, runRuntimeEnvAgent)
	if err != nil {
		t.Errorf("runRuntimeEnvAgent with zero backup returned error: %v", err)
	}
}

// TestRuntimeEnvAgentCmdPreRunE tests the PreRunE validation logic.
func TestRuntimeEnvAgentCmdPreRunE(t *testing.T) {
	// init() runs automatically at package load, so no manual call is needed.

	// Valid log levels.
	validLevels := []string{"debug", "info", "warning", "error", "critical"}
	for _, level := range validLevels {
		loggingLevel = level
		err := RuntimeEnvAgentCmd.PreRunE(RuntimeEnvAgentCmd, []string{})
		if err != nil {
			t.Errorf("log level %s should be valid but returned error: %v", level, err)
		}
	}

	// Invalid log level.
	loggingLevel = "invalid"
	err := RuntimeEnvAgentCmd.PreRunE(RuntimeEnvAgentCmd, []string{})
	if err == nil {
		t.Error("invalid log level should return an error")
	}
	if !strings.Contains(err.Error(), "invalid logging-level") {
		t.Errorf("error message should contain 'invalid logging-level', got: %v", err)
	}
}

// TestRuntimeEnvAgentCmdRunE tests the RunE execution logic.
func TestRuntimeEnvAgentCmdRunE(t *testing.T) {
	// init() runs automatically at package load, so no manual call is needed.

	// Create temporary directories.
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	// Set the test parameters.
	nodeIPAddress = "127.0.0.1"
	runtimeEnvAgentPort = 0
	gcsAddress = "127.0.0.1:6379"
	clusterIDHex = testClusterIDHex
	runtimeEnvDir = runtimeEnvDirPath
	logDir = logDirPath
	tempDir = baseDir
	loggingLevel = "info"
	loggingFilename = "test_run.log"
	loggingRotateBytes = 1024 * 1024 * 100
	loggingRotateBackupCount = 5

	// The cobra RunE signature is fixed, so the fake GCS client cannot be
	// passed directly; inject it via connectGCSClient and restore it through
	// t.Cleanup so other tests are not affected.
	origConnect := connectGCSClient
	connectGCSClient = func(opts gcs.ClientOptions) (gcs.Client, error) {
		return &mockgcs.Client{}, nil
	}
	t.Cleanup(func() { connectGCSClient = origConnect })

	err := runAgentWithFakeGCS(t, func(ctx context.Context) error {
		RuntimeEnvAgentCmd.SetContext(ctx)
		return RuntimeEnvAgentCmd.RunE(RuntimeEnvAgentCmd, []string{})
	})
	if err != nil {
		t.Errorf("RunE returned error: %v", err)
	}
}

// TestIsValidLogLevelEdgeCases tests isValidLogLevel edge cases.
func TestIsValidLogLevelEdgeCases(t *testing.T) {
	// All valid levels.
	if !isValidLogLevel("debug") {
		t.Error("isValidLogLevel('debug') should return true")
	}
	if !isValidLogLevel("info") {
		t.Error("isValidLogLevel('info') should return true")
	}
	if !isValidLogLevel("warning") {
		t.Error("isValidLogLevel('warning') should return true")
	}
	if !isValidLogLevel("error") {
		t.Error("isValidLogLevel('error') should return true")
	}
	if !isValidLogLevel("critical") {
		t.Error("isValidLogLevel('critical') should return true")
	}

	// Invalid levels.
	if isValidLogLevel("") {
		t.Error("isValidLogLevel('') should return false")
	}
	if isValidLogLevel("DEBUG") {
		t.Error("isValidLogLevel('DEBUG') should return false")
	}
	if isValidLogLevel("INFO") {
		t.Error("isValidLogLevel('INFO') should return false")
	}
	if isValidLogLevel("unknown") {
		t.Error("isValidLogLevel('unknown') should return false")
	}
	if isValidLogLevel("trace") {
		t.Error("isValidLogLevel('trace') should return false")
	}
}

// TestRunRuntimeEnvAgentDifferentLogLevels tests runRuntimeEnvAgent with
// different log levels.
func TestRunRuntimeEnvAgentDifferentLogLevels(t *testing.T) {
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	logLevels := []string{"debug", "info", "warning", "error"}
	for _, level := range logLevels {
		t.Run("level_"+level, func(t *testing.T) {
			nodeIPAddress = "127.0.0.1"
			runtimeEnvAgentPort = 0
			gcsAddress = "127.0.0.1:6379"
			clusterIDHex = testClusterIDHex
			runtimeEnvDir = runtimeEnvDirPath
			logDir = logDirPath
			tempDir = baseDir
			loggingLevel = level
			loggingFilename = "test_runtime_env_" + level + ".log"
			loggingRotateBytes = 1024 * 1024 * 100
			loggingRotateBackupCount = 5

			err := runAgentWithFakeGCS(t, runRuntimeEnvAgent)
			if err != nil {
				t.Errorf("runRuntimeEnvAgent with level %s returned error: %v", level, err)
			}
		})
	}
}

// TestRunRuntimeEnvAgentWindows tests runRuntimeEnvAgent as on Windows.
func TestRunRuntimeEnvAgentWindows(t *testing.T) {
	// Save the original GOOS value.
	originalGOOS := runtimeGOOS
	defer func() { runtimeGOOS = originalGOOS }()

	// Simulate the Windows platform.
	runtimeGOOS = "windows"

	// Create temporary directories for the test.
	baseDir, logDirPath, runtimeEnvDirPath := makeTempDirs(t)

	// Set the test parameters.
	nodeIPAddress = "127.0.0.1"
	runtimeEnvAgentPort = 0
	gcsAddress = "127.0.0.1:6379"
	clusterIDHex = testClusterIDHex
	runtimeEnvDir = runtimeEnvDirPath
	logDir = logDirPath
	tempDir = baseDir
	loggingLevel = "debug"
	loggingFilename = "test_runtime_env_windows.log"
	loggingRotateBytes = 1024 * 1024 * 100
	loggingRotateBackupCount = 3

	err := runAgentWithFakeGCS(t, runRuntimeEnvAgent)
	if err != nil {
		t.Errorf("runRuntimeEnvAgent on Windows returned error: %v", err)
	}
}
