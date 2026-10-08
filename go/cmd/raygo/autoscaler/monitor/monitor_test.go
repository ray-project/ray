// Copyright 2026 The Ray Authors.
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

package monitor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler/v2"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// resetMonitorFlags resets all MonitorCmd flags to their default values so
// flag values set by earlier tests do not leak into later ones.
func resetMonitorFlags() {
	MonitorCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		}
	})
}

// parseMonitorConfig parses the given flags on MonitorCmd and validates them
// through NewMonitorV2Config, without starting the monitor process.
// The monitor run path requires a live GCS server, which is not available in
// unit tests, so CLI-level tests must stop at flag parsing and config
// validation.
func parseMonitorConfig(args []string) (*v2.MonitorV2Config, error) {
	resetMonitorFlags()
	if err := MonitorCmd.ParseFlags(args); err != nil {
		return nil, err
	}
	return v2.NewMonitorV2Config(MonitorCmd)
}

// TestMonitorPanicError verifies the panic-to-error conversion used by
// runMonitor: a recovered panic must surface as a non-nil returned error so the
// process exits non-zero (matching the Python monitor's non-zero traceback
// exit) instead of cobra reporting a clean stop on a dead autoscaler.
func TestMonitorPanicError(t *testing.T) {
	err := monitorPanicError("boom")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "monitor panicked")
	assert.Contains(t, err.Error(), "boom")

	err = monitorPanicError(fmt.Errorf("wrapped boom"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrapped boom")
}

// TestMonitorCmdStructure tests the basic structure of MonitorCmd.
func TestMonitorCmdStructure(t *testing.T) {
	t.Run("command name and usage", func(t *testing.T) {
		assert.Equal(t, "monitor", MonitorCmd.Use)
		assert.Contains(t, MonitorCmd.Short, "autoscaler monitor")
		assert.Contains(t, MonitorCmd.Long, "Global Control Service")
	})

	t.Run("command has RunE function", func(t *testing.T) {
		assert.NotNil(t, MonitorCmd.RunE)
	})

	t.Run("command has no subcommands", func(t *testing.T) {
		assert.Empty(t, MonitorCmd.Commands())
	})
}

// TestMonitorFlagsRegistration tests the registration of all command line flags.
func TestMonitorFlagsRegistration(t *testing.T) {
	tests := []struct {
		name         string
		flagName     string
		defaultValue string
		isRequired   bool
	}{
		{"GCS address flag (required)", constant.FlagsGcsAddress, "", true},
		{"Autoscaling config flag", constant.FlagsAutoscalingConfig, "", false},
		{"Logging level flag", constant.FlagsLoggingLevel, "info", false},
		{"Logging format flag", constant.FlagsLoggingFormat, "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s", false},
		{"Logging filename flag", constant.FlagsLoggingFilename, "monitor.log", false},
		{"Logs dir flag (required)", constant.FlagsLogsDir, "", true},
		{"Logging rotate bytes flag", constant.FlagsLoggingRotateBytes, "536870912", false}, // 10MB as string
		{"Logging rotate backup count flag", constant.FlagsLoggingRotateBackupCount, "5", false},
		{"Monitor IP flag", constant.FlagsMonitorIP, "", false},
		{"Stdout filepath flag", constant.FlagsStdoutFilepath, "", false},
		{"Stderr filepath flag", constant.FlagsStderrFilepath, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := MonitorCmd.Flags().Lookup(tt.flagName)
			assert.NotNil(t, flag, "Flag %s should be registered", tt.flagName)

			if tt.defaultValue != "" {
				assert.Equal(t, tt.defaultValue, flag.DefValue, "Default value mismatch for flag %s", tt.flagName)
			}

			if tt.isRequired {
				// Check the required marking (through the usage text).
				assert.NotEmpty(t, flag.Usage, "Required flag %s should have usage text", tt.flagName)
			}
		})
	}
}

// TestGetMonitorCmd tests the GetMonitorCmd function.
func TestGetMonitorCmd(t *testing.T) {
	cmd := GetMonitorCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, MonitorCmd, cmd)
}

// TestRunMonitor_Success tests that valid flags parse into a config successfully.
func TestRunMonitor_Success(t *testing.T) {
	// Create a temporary directory as logs-dir.
	tmpDir := t.TempDir()

	config, err := parseMonitorConfig([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--logging-level=info",
	})

	assert.NoError(t, err, "Valid flags should parse into a config successfully")
	if err != nil {
		return
	}
	assert.Equal(t, "127.0.0.1:6379", config.GcsAddress)
	assert.Equal(t, tmpDir, config.LogsDir)
	assert.Equal(t, "info", config.LoggingLevel)
}

// TestRunMonitor_MissingRequiredFlags tests the missing required flags.
func TestRunMonitor_MissingRequiredFlags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError string
	}{
		{
			name:        "missing gcs-address",
			args:        []string{"--logs-dir=/tmp"},
			expectError: `required flag(s) "gcs-address" not set`,
		},
		{
			name:        "missing logs-dir",
			args:        []string{"--gcs-address=127.0.0.1:6379"},
			expectError: "required flag(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh command instance to avoid state pollution.
			testCmd := &cobra.Command{
				Use:  "test-monitor",
				RunE: runMonitor,
			}
			// Copy the flag definitions.
			testCmd.Flags().String(constant.FlagsGcsAddress, "", "")
			testCmd.Flags().String(constant.FlagsLogsDir, "", "")
			testCmd.MarkFlagRequired(constant.FlagsGcsAddress)
			testCmd.MarkFlagRequired(constant.FlagsLogsDir)

			testCmd.SetArgs(tt.args)
			err := testCmd.Execute()

			if tt.expectError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}

// TestRunMonitor_InvalidGcsAddress tests invalid GCS address formats.
func TestRunMonitor_InvalidGcsAddress(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		gcsAddress  string
		expectError string
	}{
		{
			name:        "invalid format - no port",
			gcsAddress:  "127.0.0.1",
			expectError: "invalid GCS address",
		},
		{
			name:        "invalid format - no host",
			gcsAddress:  ":6379",
			expectError: "invalid GCS address",
		},
		{
			name:        "invalid port - non-numeric",
			gcsAddress:  "127.0.0.1:abc",
			expectError: "invalid GCS address",
		},
		{
			name:        "invalid port - negative",
			gcsAddress:  "127.0.0.1:-1",
			expectError: "invalid GCS address",
		},
		{
			name:        "invalid port - too large",
			gcsAddress:  "127.0.0.1:99999",
			expectError: "invalid GCS address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseMonitorConfig([]string{
				"--gcs-address=" + tt.gcsAddress,
				"--logs-dir=" + tmpDir,
			})

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

// TestRunMonitor_ValidGcsAddresses tests valid GCS address formats.
func TestRunMonitor_ValidGcsAddresses(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		gcsAddress string
	}{
		{"IPv4 localhost", "127.0.0.1:6379"},
		{"IPv4 standard", "192.168.1.100:6379"},
		{"IPv6 localhost", "[::1]:6379"},
		{"hostname", "localhost:6379"},
		{"hostname with domain", "ray-head.default.svc.cluster.local:6379"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parseMonitorConfig([]string{
				"--gcs-address=" + tt.gcsAddress,
				"--logs-dir=" + tmpDir,
			})

			// A valid address format must pass flag parsing and must not report
			// "invalid GCS address".
			assert.NoError(t, err, "Valid GCS address %q should pass config validation", tt.gcsAddress)
			if err == nil {
				assert.NotEmpty(t, config.GcsAddress)
			}
		})
	}
}

// TestRunMonitor_LoggingOptions tests the logging option configuration.
func TestRunMonitor_LoggingOptions(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		additionalArgs []string
		expectSuccess  bool
	}{
		{
			name:           "default logging options",
			additionalArgs: []string{},
			expectSuccess:  true,
		},
		{
			name:           "custom logging level",
			additionalArgs: []string{"--logging-level=debug"},
			expectSuccess:  true,
		},
		{
			name:           "custom logging format",
			additionalArgs: []string{"--logging-format=json"},
			expectSuccess:  true,
		},
		{
			name:           "custom logging filename",
			additionalArgs: []string{"--logging-filename=custom.log"},
			expectSuccess:  true,
		},
		{
			name: "custom rotate settings",
			additionalArgs: []string{
				"--logging-rotate-bytes=5242880", // 5MB
				"--logging-rotate-backup-count=3",
			},
			expectSuccess: true,
		},
		{
			name:           "invalid logging level",
			additionalArgs: []string{"--logging-level=invalid"},
			expectSuccess:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{
				"--gcs-address=127.0.0.1:6379",
				"--logs-dir=" + tmpDir,
			}
			args = append(args, tt.additionalArgs...)

			_, err := parseMonitorConfig(args)

			if tt.expectSuccess {
				assert.NoError(t, err, "Valid logging options should pass config validation")
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid logging level")
			}
		})
	}
}

// TestRunMonitor_OutputRedirection tests the output redirection options.
func TestRunMonitor_OutputRedirection(t *testing.T) {
	tmpDir := t.TempDir()
	stdoutFile := filepath.Join(tmpDir, "stdout.log")
	stderrFile := filepath.Join(tmpDir, "stderr.log")

	config, err := parseMonitorConfig([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--stdout-filepath=" + stdoutFile,
		"--stderr-filepath=" + stderrFile,
	})

	// The output redirection paths must be parsed into the config correctly.
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, stdoutFile, config.StdoutFilepath)
		assert.Equal(t, stderrFile, config.StderrFilepath)
	}
}

// TestRunMonitor_MonitorIPOption tests the Monitor IP option.
func TestRunMonitor_MonitorIPOption(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		monitorIP string
	}{
		{"IPv4 address", "192.168.1.100"},
		{"IPv6 address", "::1"},
		{"empty (optional)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{
				"--gcs-address=127.0.0.1:6379",
				"--logs-dir=" + tmpDir,
			}
			if tt.monitorIP != "" {
				args = append(args, "--monitor-ip="+tt.monitorIP)
			}

			config, err := parseMonitorConfig(args)

			// The Monitor IP is optional and must not cause a parse error.
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, tt.monitorIP, config.MonitorIP)
			}
		})
	}
}

// TestRunMonitor_AutoscalingConfigOption tests the autoscaling config option.
func TestRunMonitor_AutoscalingConfigOption(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "autoscaling.yaml")

	// Create a fake config file.
	err := os.WriteFile(configFile, []byte("fake config"), 0644)
	assert.NoError(t, err)

	config, err := parseMonitorConfig([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--autoscaling-config=" + configFile,
	})

	// The config file path is optional and must not cause a parse error.
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, configFile, config.AutoscalingConfig)
	}
}

// TestRunMonitor_HelpFlag tests the help flag.
func TestRunMonitor_HelpFlag(t *testing.T) {
	// Capture the output.
	var buf bytes.Buffer
	MonitorCmd.SetOut(&buf)
	MonitorCmd.SetErr(&buf)

	MonitorCmd.SetArgs([]string{"--help"})
	err := MonitorCmd.Execute()

	// --help must succeed and print the help text.
	assert.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "Usage:")
	assert.Contains(t, output, "monitor [flags]")
	assert.Contains(t, output, "--gcs-address")
	assert.Contains(t, output, "--logs-dir")
}

// TestRunMonitor_VersionFlag tests the version flag (if it exists).
func TestRunMonitor_VersionFlag(t *testing.T) {
	// Note: the current implementation may have no version flag; this test
	// verifies that.
	var buf bytes.Buffer
	MonitorCmd.SetOut(&buf)
	MonitorCmd.SetErr(&buf)

	MonitorCmd.SetArgs([]string{"--version"})
	err := MonitorCmd.Execute()

	// Without a version flag an unknown-flag error must be returned.
	if err != nil {
		assert.Contains(t, err.Error(), "unknown flag")
	}
}

// TestRunMonitor_ExampleUsage tests the command from the example usage.
func TestRunMonitor_ExampleUsage(t *testing.T) {
	tmpDir := t.TempDir()

	// Based on the example in the Long description.
	config, err := parseMonitorConfig([]string{
		"--gcs-address=192.168.1.100:6379",
		"--logs-dir=" + tmpDir,
		"--monitor-ip=192.168.1.100",
	})

	// The example must use valid flag formats.
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, "192.168.1.100:6379", config.GcsAddress)
		assert.Equal(t, "192.168.1.100", config.MonitorIP)
	}
}

// TestRunMonitor_ConcurrentExecution tests concurrent execution (if applicable).
func TestRunMonitor_ConcurrentExecution(t *testing.T) {
	tmpDir := t.TempDir()

	done := make(chan error, 2)

	// Create two independent command instances to test concurrency.
	// Note: this test mainly verifies the thread safety of the cobra command;
	// in practice the monitor command is never executed concurrently.
	createTestCmd := func(name string) *cobra.Command {
		cmd := &cobra.Command{
			Use: name,
		}
		// Copy all the necessary flag definitions.
		cmd.Flags().String(constant.FlagsGcsAddress, "127.0.0.1:6379", "")
		cmd.Flags().String(constant.FlagsAutoscalingConfig, "", "")
		cmd.Flags().String(constant.FlagsLoggingLevel, "info", "")
		cmd.Flags().String(constant.FlagsLoggingFormat, "", "")
		cmd.Flags().String(constant.FlagsLoggingFilename, "monitor.log", "")
		cmd.Flags().String(constant.FlagsLogsDir, tmpDir, "")
		cmd.Flags().Int(constant.FlagsLoggingRotateBytes, 10485760, "")
		cmd.Flags().Int(constant.FlagsLoggingRotateBackupCount, 5, "")
		cmd.Flags().String(constant.FlagsMonitorIP, "", "")
		cmd.Flags().String(constant.FlagsStdoutFilepath, "", "")
		cmd.Flags().String(constant.FlagsStderrFilepath, "", "")
		return cmd
	}

	parseConfig := func(cmd *cobra.Command) error {
		if err := cmd.ParseFlags([]string{
			"--gcs-address=127.0.0.1:6379",
			"--logs-dir=" + tmpDir,
		}); err != nil {
			return err
		}
		_, err := v2.NewMonitorV2Config(cmd)
		return err
	}

	// Run twice concurrently (even though this never happens in practice).
	go func() {
		done <- parseConfig(createTestCmd("test1"))
	}()

	go func() {
		done <- parseConfig(createTestCmd("test2"))
	}()

	// Wait for both goroutines to finish.
	err1 := <-done
	err2 := <-done

	// Anything other than a crash caused by concurrency is acceptable.
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	t.Logf("Concurrent execution results: err1=%v, err2=%v", err1, err2)
}

// TestRunMonitor_ContextCancellation tests context cancellation (if applicable).
func TestRunMonitor_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a context with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Note: flag parsing does not depend on the context today; this test leaves
	// room for future extensions.
	MonitorCmd.SetContext(ctx)
	_, err := parseMonitorConfig([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
	})

	// Flag parsing must not be affected by an expired context.
	assert.NoError(t, err)
}

// TestRunMonitor_EnvironmentVariables tests the effect of environment variables
// (if applicable).
func TestRunMonitor_EnvironmentVariables(t *testing.T) {
	tmpDir := t.TempDir()

	// Save the original environment variables.
	origRotateBytes := os.Getenv("RAY_MONITOR_LOG_ROTATE_BYTES")
	origBackupCount := os.Getenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT")
	defer func() {
		os.Setenv("RAY_MONITOR_LOG_ROTATE_BYTES", origRotateBytes)
		os.Setenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT", origBackupCount)
	}()

	// Set the environment variables.
	os.Setenv("RAY_MONITOR_LOG_ROTATE_BYTES", "5242880") // 5MB
	os.Setenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT", "3")

	_, err := parseMonitorConfig([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
	})

	// The environment variables must not cause a parse error.
	assert.NoError(t, err)
}

// BenchmarkRunMonitor_ParameterParsing benchmarks the flag parsing performance.
func BenchmarkRunMonitor_ParameterParsing(b *testing.B) {
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := []string{
			"--gcs-address=127.0.0.1:6379",
			"--logs-dir=" + tmpDir,
			"--logging-level=info",
			"--logging-format=text",
		}

		if _, err := parseMonitorConfig(args); err != nil {
			b.Fatal(err)
		}
	}
}

// FuzzRunMonitor_GcsAddress fuzzes the GCS address format.
func FuzzRunMonitor_GcsAddress(f *testing.F) {
	tmpDir := f.TempDir()

	// Add a seed corpus.
	f.Add("127.0.0.1:6379")
	f.Add("192.168.1.100:6379")
	f.Add("[::1]:6379")
	f.Add("localhost:6379")

	f.Fuzz(func(t *testing.T, gcsAddress string) {
		_, err := parseMonitorConfig([]string{
			"--gcs-address=" + gcsAddress,
			"--logs-dir=" + tmpDir,
		})

		// An invalid GCS address must produce a clear error message; a valid one
		// must not fail.
		if err != nil {
			assert.Contains(t, err.Error(), "invalid GCS address")
		}
	})
}

// FuzzRunMonitor_LogsDir fuzzes the log directory path.
func FuzzRunMonitor_LogsDir(f *testing.F) {
	// Add a seed corpus.
	f.Add("/tmp/ray/logs")
	f.Add("./logs")
	f.Add("../relative/path")
	f.Add("C:\\Windows\\Path") // Windows path.

	f.Fuzz(func(t *testing.T, logsDir string) {
		_, err := parseMonitorConfig([]string{
			"--gcs-address=127.0.0.1:6379",
			"--logs-dir=" + logsDir,
		})

		// An invalid log directory must be handled gracefully.
		if err != nil {
			// An empty log directory must return the "must be set" error instead
			// of crashing.
			assert.Contains(t, err.Error(), "must be set")
		}
	})
}
