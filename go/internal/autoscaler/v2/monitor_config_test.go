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

package v2

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// TestMonitorV2Config_StructFields tests the field definitions of the MonitorV2Config struct.
func TestMonitorV2Config_StructFields(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress:               "127.0.0.1:6379",
		AutoscalingConfig:        "/path/to/config.yaml",
		LoggingLevel:             "debug",
		LoggingFormat:            "json",
		LoggingFilename:          "test.log",
		LogsDir:                  "/tmp/logs",
		LoggingRotateBytes:       10485760,
		LoggingRotateBackupCount: 5,
		MonitorIP:                "192.168.1.1",
		StdoutFilepath:           "/tmp/stdout.log",
		StderrFilepath:           "/tmp/stderr.log",
	}

	assert.Equal(t, "127.0.0.1:6379", config.GcsAddress)
	assert.Equal(t, "/path/to/config.yaml", config.AutoscalingConfig)
	assert.Equal(t, "debug", config.LoggingLevel)
	assert.Equal(t, "json", config.LoggingFormat)
	assert.Equal(t, "test.log", config.LoggingFilename)
	assert.Equal(t, "/tmp/logs", config.LogsDir)
	assert.Equal(t, 10485760, config.LoggingRotateBytes)
	assert.Equal(t, 5, config.LoggingRotateBackupCount)
	assert.Equal(t, "192.168.1.1", config.MonitorIP)
	assert.Equal(t, "/tmp/stdout.log", config.StdoutFilepath)
	assert.Equal(t, "/tmp/stderr.log", config.StderrFilepath)
}

// TestNewMonitorV2Config_NilCmd tests the nil cmd case.
func TestNewMonitorV2Config_NilCmd(t *testing.T) {
	config, err := NewMonitorV2Config(nil)
	assert.Nil(t, config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cmd is nil")
}

// TestNewMonitorV2Config_ValidConfig tests a valid config parse.
func TestNewMonitorV2Config_ValidConfig(t *testing.T) {
	tmpDir := t.TempDir()

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--logging-level=debug",
		"--logging-format=json",
		"--logging-filename=test.log",
		"--logging-rotate-bytes=5242880",
		"--logging-rotate-backup-count=3",
		"--monitor-ip=192.168.1.1",
		"--stdout-filepath=/tmp/stdout.log",
		"--stderr-filepath=/tmp/stderr.log",
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	assert.Equal(t, "127.0.0.1:6379", config.GcsAddress)
	assert.Equal(t, "debug", config.LoggingLevel)
	assert.Equal(t, "json", config.LoggingFormat)
	assert.Equal(t, "test.log", config.LoggingFilename)
	assert.Equal(t, tmpDir, config.LogsDir)
	assert.Equal(t, 5242880, config.LoggingRotateBytes)
	assert.Equal(t, 3, config.LoggingRotateBackupCount)
	assert.Equal(t, "192.168.1.1", config.MonitorIP)
	assert.Equal(t, "/tmp/stdout.log", config.StdoutFilepath)
	assert.Equal(t, "/tmp/stderr.log", config.StderrFilepath)
}

// TestNewMonitorV2Config_DefaultValues tests the default values.
func TestNewMonitorV2Config_DefaultValues(t *testing.T) {
	tmpDir := t.TempDir()

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	// Verify the defaults.
	assert.Equal(t, common.LoggerLevel, config.LoggingLevel)
	assert.Equal(t, common.LoggerFormat, config.LoggingFormat)
	assert.Equal(t, common.MONITOR_LOG_FILE_NAME, config.LoggingFilename)
	assert.Equal(t, common.MonitorLogRotateBytes(), config.LoggingRotateBytes)
	assert.Equal(t, common.MonitorLogRotateBackupCount(), config.LoggingRotateBackupCount)
	assert.Empty(t, config.AutoscalingConfig)
	assert.Empty(t, config.MonitorIP)
	assert.Empty(t, config.StdoutFilepath)
	assert.Empty(t, config.StderrFilepath)
}

// TestNewMonitorV2Config_EmptyGcsAddress tests an empty GCS address (optional parameter).
func TestNewMonitorV2Config_EmptyGcsAddress(t *testing.T) {
	tmpDir := t.TempDir()

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--logs-dir=" + tmpDir,
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Empty(t, config.GcsAddress)
}

// TestNewMonitorV2Config_InvalidGcsAddress tests invalid GCS address formats.
func TestNewMonitorV2Config_InvalidGcsAddress(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		gcsAddress string
		expectErr  string
	}{
		{
			name:       "no port",
			gcsAddress: "127.0.0.1",
			expectErr:  "invalid GCS address",
		},
		{
			name:       "no host",
			gcsAddress: ":6379",
			expectErr:  "invalid GCS address",
		},
		{
			name:       "non-numeric port",
			gcsAddress: "127.0.0.1:abc",
			expectErr:  "invalid GCS address",
		},
		{
			name:       "negative port",
			gcsAddress: "127.0.0.1:-1",
			expectErr:  "invalid GCS address",
		},
		{
			name:       "port too large",
			gcsAddress: "127.0.0.1:99999",
			expectErr:  "invalid GCS address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := createTestMonitorCmd()
			cmd.SetArgs([]string{
				"--gcs-address=" + tt.gcsAddress,
				"--logs-dir=" + tmpDir,
			})

			err := cmd.Execute()
			assert.NoError(t, err)

			config, err := NewMonitorV2Config(cmd)
			assert.Error(t, err)
			assert.Nil(t, config)
			assert.Contains(t, err.Error(), tt.expectErr)
		})
	}
}

// TestNewMonitorV2Config_MissingLogsDir tests the missing required logs-dir parameter.
func TestNewMonitorV2Config_MissingLogsDir(t *testing.T) {
	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "--logs-dir must be set")
}

// TestNewMonitorV2Config_InvalidLoggingLevel tests an invalid log level.
func TestNewMonitorV2Config_InvalidLoggingLevel(t *testing.T) {
	tmpDir := t.TempDir()

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--logging-level=invalid_level",
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid logging level")
	assert.Contains(t, err.Error(), "must be one of")
}

// TestNewMonitorV2Config_ValidLoggingLevels tests every valid log level.
func TestNewMonitorV2Config_ValidLoggingLevels(t *testing.T) {
	tmpDir := t.TempDir()

	validLevels := []string{"debug", "info", "warning", "error", "critical"}

	for _, level := range validLevels {
		t.Run(level, func(t *testing.T) {
			cmd := createTestMonitorCmd()
			cmd.SetArgs([]string{
				"--gcs-address=127.0.0.1:6379",
				"--logs-dir=" + tmpDir,
				"--logging-level=" + level,
			})

			err := cmd.Execute()
			assert.NoError(t, err)

			config, err := NewMonitorV2Config(cmd)
			assert.NoError(t, err)
			assert.NotNil(t, config)
			assert.Equal(t, level, config.LoggingLevel)
		})
	}
}

// TestNewMonitorV2Config_AutoscalingConfig tests the autoscaling config file path.
func TestNewMonitorV2Config_AutoscalingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "autoscaling.yaml")

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--autoscaling-config=" + configPath,
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, configPath, config.AutoscalingConfig)
}

// TestNewMonitorV2Config_MonitorIP tests the Monitor IP address.
func TestNewMonitorV2Config_MonitorIP(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		monitorIP string
	}{
		{"IPv4", "192.168.1.100"},
		{"IPv6", "::1"},
		{"empty", ""},
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

			cmd := createTestMonitorCmd()
			cmd.SetArgs(args)

			err := cmd.Execute()
			assert.NoError(t, err)

			config, err := NewMonitorV2Config(cmd)
			assert.NoError(t, err)
			assert.NotNil(t, config)
			assert.Equal(t, tt.monitorIP, config.MonitorIP)
		})
	}
}

// TestNewMonitorV2Config_OutputFilepaths tests the output file paths.
func TestNewMonitorV2Config_OutputFilepaths(t *testing.T) {
	tmpDir := t.TempDir()
	stdoutPath := filepath.Join(tmpDir, "stdout.log")
	stderrPath := filepath.Join(tmpDir, "stderr.log")

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--stdout-filepath=" + stdoutPath,
		"--stderr-filepath=" + stderrPath,
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, stdoutPath, config.StdoutFilepath)
	assert.Equal(t, stderrPath, config.StderrFilepath)
}

// TestNewMonitorV2Config_LoggingRotation tests the log rotation configuration.
func TestNewMonitorV2Config_LoggingRotation(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		rotateBytes    int
		backupCount    int
		expectedBytes  int
		expectedBackup int
	}{
		{
			name:           "custom values",
			rotateBytes:    5242880,
			backupCount:    3,
			expectedBytes:  5242880,
			expectedBackup: 3,
		},
		{
			name:           "zero rotate bytes (use default)",
			rotateBytes:    0,
			backupCount:    3,
			expectedBytes:  common.MonitorLogRotateBytes(),
			expectedBackup: 3,
		},
		{
			name:           "negative rotate bytes (use default)",
			rotateBytes:    -100,
			backupCount:    3,
			expectedBytes:  common.MonitorLogRotateBytes(),
			expectedBackup: 3,
		},
		{
			name:           "negative backup count (use default)",
			rotateBytes:    10485760,
			backupCount:    -1,
			expectedBytes:  10485760,
			expectedBackup: common.MonitorLogRotateBackupCount(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{
				"--gcs-address=127.0.0.1:6379",
				"--logs-dir=" + tmpDir,
			}

			// Only add the flag when the value is non-zero.
			if tt.rotateBytes != 0 {
				args = append(args, "--logging-rotate-bytes="+fmt.Sprintf("%d", tt.rotateBytes))
			}
			if tt.backupCount >= 0 {
				args = append(args, "--logging-rotate-backup-count="+fmt.Sprintf("%d", tt.backupCount))
			}

			cmd := createTestMonitorCmd()
			cmd.SetArgs(args)

			err := cmd.Execute()
			assert.NoError(t, err)

			config, err := NewMonitorV2Config(cmd)
			assert.NoError(t, err)
			assert.NotNil(t, config)
			assert.Equal(t, tt.expectedBytes, config.LoggingRotateBytes)
			assert.Equal(t, tt.expectedBackup, config.LoggingRotateBackupCount)
		})
	}
}

// TestIsValidLoggingLevel tests the isValidLoggingLevel function.
func TestIsValidLoggingLevel(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  bool
	}{
		{"valid debug", "debug", true},
		{"valid info", "info", true},
		{"valid warning", "warning", true},
		{"valid error", "error", true},
		{"valid critical", "critical", true},
		{"invalid empty", "", false},
		{"invalid random", "random", false},
		{"invalid DEBUG uppercase", "DEBUG", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidLoggingLevel(tt.level)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestMonitorV2Config_String tests the String method.
func TestMonitorV2Config_String(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: "/path/to/config.yaml",
		LogsDir:           "/tmp/logs",
		LoggingLevel:      "debug",
		MonitorIP:         "192.168.1.1",
	}

	str := config.String()
	assert.Contains(t, str, "MonitorV2Config")
	assert.Contains(t, str, "GcsAddress: 127.0.0.1:6379")
	assert.Contains(t, str, "AutoscalingConfig: /path/to/config.yaml")
	assert.Contains(t, str, "LogsDir: /tmp/logs")
	assert.Contains(t, str, "LoggingLevel: debug")
	assert.Contains(t, str, "MonitorIP: 192.168.1.1")
}

// TestNewMonitorV2Config_EnvironmentVariables tests the effect of environment
// variables on the defaults.
func TestNewMonitorV2Config_EnvironmentVariables(t *testing.T) {
	tmpDir := t.TempDir()

	// Save the original environment variables.
	origRotateBytes := os.Getenv("RAY_MONITOR_LOG_ROTATE_BYTES")
	origBackupCount := os.Getenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT")
	defer func() {
		os.Setenv("RAY_MONITOR_LOG_ROTATE_BYTES", origRotateBytes)
		os.Setenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT", origBackupCount)
	}()

	// Set custom environment variables.
	os.Setenv("RAY_MONITOR_LOG_ROTATE_BYTES", "20971520") // 20MB
	os.Setenv("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT", "10")

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
	})

	err := cmd.Execute()
	assert.NoError(t, err)

	config, err := NewMonitorV2Config(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	// Verify the environment variables overrode the defaults.
	assert.Equal(t, 20971520, config.LoggingRotateBytes)
	assert.Equal(t, 10, config.LoggingRotateBackupCount)
}

// TestNewMonitorV2Config_GcsAddressNormalization tests GCS address normalization.
func TestNewMonitorV2Config_GcsAddressNormalization(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name         string
		inputAddress string
		wantAddress  string
	}{
		{
			name:         "standard IPv4",
			inputAddress: "127.0.0.1:6379",
			wantAddress:  "127.0.0.1:6379",
		},
		{
			name:         "IPv4 with leading zeros in port",
			inputAddress: "192.168.1.1:06379",
			wantAddress:  "192.168.1.1:6379",
		},
		{
			name:         "hostname with port",
			inputAddress: "localhost:6379",
			wantAddress:  "localhost:6379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := createTestMonitorCmd()
			cmd.SetArgs([]string{
				"--gcs-address=" + tt.inputAddress,
				"--logs-dir=" + tmpDir,
			})

			err := cmd.Execute()
			assert.NoError(t, err)

			config, err := NewMonitorV2Config(cmd)
			assert.NoError(t, err)
			assert.NotNil(t, config)
			assert.Equal(t, tt.wantAddress, config.GcsAddress)
		})
	}
}

// Helper function to create a test monitor command
func createTestMonitorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "test-monitor",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	// Register all flags
	cmd.Flags().String(constant.FlagsGcsAddress, "", "GCS address")
	cmd.Flags().String(constant.FlagsAutoscalingConfig, "", "Autoscaling config path")
	cmd.Flags().String(constant.FlagsLoggingLevel, common.LoggerLevel, "Logging level")
	cmd.Flags().String(constant.FlagsLoggingFormat, common.LoggerFormat, "Logging format")
	cmd.Flags().String(constant.FlagsLoggingFilename, common.MONITOR_LOG_FILE_NAME, "Logging filename")
	cmd.Flags().String(constant.FlagsLogsDir, "", "Logs directory")
	cmd.Flags().Int(constant.FlagsLoggingRotateBytes, common.MonitorLogRotateBytes(), "Logging rotate bytes")
	cmd.Flags().Int(constant.FlagsLoggingRotateBackupCount, common.MonitorLogRotateBackupCount(), "Logging rotate backup count")
	cmd.Flags().String(constant.FlagsMonitorIP, "", "Monitor IP")
	cmd.Flags().String(constant.FlagsStdoutFilepath, "", "Stdout filepath")
	cmd.Flags().String(constant.FlagsStderrFilepath, "", "Stderr filepath")

	return cmd
}

// BenchmarkNewMonitorV2Config benchmarks the config parsing performance.
func BenchmarkNewMonitorV2Config(b *testing.B) {
	tmpDir := b.TempDir()

	cmd := createTestMonitorCmd()
	cmd.SetArgs([]string{
		"--gcs-address=127.0.0.1:6379",
		"--logs-dir=" + tmpDir,
		"--logging-level=info",
		"--logging-rotate-bytes=10485760",
		"--logging-rotate-backup-count=5",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cmd.Execute()
		_, _ = NewMonitorV2Config(cmd)
	}
}

// FuzzNewMonitorV2Config_GcsAddress fuzzes the GCS address input.
func FuzzNewMonitorV2Config_GcsAddress(f *testing.F) {
	tmpDir := f.TempDir()

	// Seed corpus with valid and invalid addresses
	f.Add("127.0.0.1:6379")
	f.Add("localhost:6379")
	f.Add("127.0.0.1")
	f.Add(":6379")
	f.Add("127.0.0.1:abc")

	f.Fuzz(func(t *testing.T, gcsAddress string) {
		cmd := createTestMonitorCmd()
		cmd.SetArgs([]string{
			"--gcs-address=" + gcsAddress,
			"--logs-dir=" + tmpDir,
		})

		_ = cmd.Execute()
		config, err := NewMonitorV2Config(cmd)

		// Either config is valid or we get an expected error
		if err == nil && config != nil {
			// Valid case: ensure GcsAddress is properly formatted
			assert.NotEmpty(t, config.GcsAddress)
		} else if err != nil {
			// Error case: ensure error message is descriptive
			assert.Contains(t, err.Error(), "invalid GCS address")
		}
	})
}
