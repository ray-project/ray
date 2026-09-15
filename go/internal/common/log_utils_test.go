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

package common_test

import (
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestLogOption(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		opt := common.NewLogOption()
		assert.Equal(t, "info", opt.LoggingLevel)
		assert.Equal(t, "", opt.LoggingFormat)
		assert.Equal(t, "", opt.LoggingFilename)
		assert.Equal(t, "", opt.LogsDir)
		assert.Equal(t, 512*1024*1024, opt.LoggingRotateBytes) // DefaultLoggingRotateBytes
		assert.Equal(t, 5, opt.LoggingRotateBackupCount)       // DefaultLoggingRotateBackupCount
		assert.Equal(t, "", opt.StdoutFilepath)
		assert.Equal(t, "", opt.StderrFilepath)
	})

	t.Run("WithLoggingLevel sets level", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingLevel("debug"))
		assert.Equal(t, "debug", opt.LoggingLevel)
	})

	t.Run("WithLoggingFormat sets format", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingFormat("%(asctime)s %(message)s"))
		assert.Equal(t, "%(asctime)s %(message)s", opt.LoggingFormat)
	})

	t.Run("WithLogsDir sets logs dir", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLogsDir("/tmp/logs"))
		assert.Equal(t, "/tmp/logs", opt.LogsDir)
	})

	t.Run("WithLogsDir ignores empty string", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLogsDir(""))
		assert.Equal(t, "", opt.LogsDir)
	})

	t.Run("WithLoggingFilename sets filename", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingFilename("test.log"))
		assert.Equal(t, "test.log", opt.LoggingFilename)
	})

	t.Run("WithLoggingRotateBytes sets positive value", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingRotateBytes(50 * 1024 * 1024))
		assert.Equal(t, 50*1024*1024, opt.LoggingRotateBytes)
	})

	t.Run("WithLoggingRotateBytes ignores non-positive value", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingRotateBytes(-100))
		assert.Equal(t, 512*1024*1024, opt.LoggingRotateBytes) // Should keep default
	})

	t.Run("WithLoggingRotateBackupCount sets positive value", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingRotateBackupCount(10))
		assert.Equal(t, 10, opt.LoggingRotateBackupCount)
	})

	t.Run("WithLoggingRotateBackupCount ignores non-positive value", func(t *testing.T) {
		opt := common.NewLogOption(common.WithLoggingRotateBackupCount(0))
		assert.Equal(t, 5, opt.LoggingRotateBackupCount) // Should keep default
	})

	t.Run("WithStdoutFilepath sets path", func(t *testing.T) {
		opt := common.NewLogOption(common.WithStdoutFilepath("/tmp/stdout.log"))
		assert.Equal(t, "/tmp/stdout.log", opt.StdoutFilepath)
	})

	t.Run("WithStderrFilepath sets path", func(t *testing.T) {
		opt := common.NewLogOption(common.WithStderrFilepath("/tmp/stderr.log"))
		assert.Equal(t, "/tmp/stderr.log", opt.StderrFilepath)
	})

	t.Run("multiple options can be combined", func(t *testing.T) {
		opt := common.NewLogOption(
			common.WithLoggingLevel("warning"),
			common.WithLogsDir("/var/log/ray"),
			common.WithLoggingFilename("monitor.log"),
			common.WithLoggingRotateBytes(20*1024*1024),
			common.WithLoggingRotateBackupCount(3),
		)
		assert.Equal(t, "warning", opt.LoggingLevel)
		assert.Equal(t, "/var/log/ray", opt.LogsDir)
		assert.Equal(t, "monitor.log", opt.LoggingFilename)
		assert.Equal(t, 20*1024*1024, opt.LoggingRotateBytes)
		assert.Equal(t, 3, opt.LoggingRotateBackupCount)
	})
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name        string
		level       string
		wantErr     bool
		description string
	}{
		{"debug level", "debug", false, "Debug level should be valid"},
		{"info level", "info", false, "Info level should be valid"},
		{"warning level", "warning", false, "Warning level should be valid"},
		{"error level", "error", false, "Error level should be valid"},
		{"critical level", "critical", false, "Critical level should be valid"},
		{"invalid level", "invalid", true, "Invalid level should return error"},
		{"empty level", "", true, "Empty level should return error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLevel, err := common.ParseLogLevel(tt.level)
			if tt.wantErr {
				assert.Error(t, err)
				// For invalid levels, it should still return a valid level (default to Info)
				assert.GreaterOrEqual(t, int(gotLevel), -1)
				assert.LessOrEqual(t, int(gotLevel), 4)
			} else {
				assert.NoError(t, err)
				// Verify the returned level is within valid range
				assert.GreaterOrEqual(t, int(gotLevel), -1)
				assert.LessOrEqual(t, int(gotLevel), 4)
			}
		})
	}
}

func TestSetupComponentLogger(t *testing.T) {
	t.Run("basic setup with logs dir and filename succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("test.log"),
			common.WithLoggingLevel("info"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
		// Note: SetupComponentLogger configures the logger but doesn't immediately create the file
		// The file will be created when logging occurs
	})

	t.Run("setup with custom rotate settings succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("rotate_test.log"),
			common.WithLoggingRotateBytes(1024*1024), // 1MB
			common.WithLoggingRotateBackupCount(2),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with stdout redirection succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()
		stdoutFile := filepath.Join(tmpDir, "stdout.log")

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStdoutFilepath(stdoutFile),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with stderr redirection succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()
		stderrFile := filepath.Join(tmpDir, "stderr.log")

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStderrFilepath(stderrFile),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with both stdout and stderr redirection succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()
		stdoutFile := filepath.Join(tmpDir, "stdout.log")
		stderrFile := filepath.Join(tmpDir, "stderr.log")

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStdoutFilepath(stdoutFile),
			common.WithStderrFilepath(stderrFile),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup without logs dir uses default output", func(t *testing.T) {
		opt := common.NewLogOption(
			common.WithLoggingLevel("info"),
			common.WithLoggingFilename("ignored.log"), // Should be ignored without LogsDir
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
		// Logger should use default output (console)
	})

	t.Run("setup with absolute stdout path succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()
		stdoutFile := filepath.Join(tmpDir, "absolute_stdout.log")

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStdoutFilepath(stdoutFile),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with absolute stderr path succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()
		stderrFile := filepath.Join(tmpDir, "absolute_stderr.log")

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStderrFilepath(stderrFile),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with debug level succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("debug.log"),
			common.WithLoggingLevel("debug"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with warning level succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("warning.log"),
			common.WithLoggingLevel("warning"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with error level succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("error.log"),
			common.WithLoggingLevel("error"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with critical level succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("critical.log"),
			common.WithLoggingLevel("critical"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup with invalid level returns error", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("invalid_level.log"),
			common.WithLoggingLevel("invalid"),
		)

		err := opt.SetupComponentLogger()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported log level")
	})

	t.Run("setup with custom format succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("custom_format.log"),
			common.WithLoggingFormat("%(asctime)s %(levelname)s %(message)s"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("setup multiple times succeeds", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt1 := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("first.log"),
		)

		opt2 := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("second.log"),
		)

		err := opt1.SetupComponentLogger()
		assert.NoError(t, err)

		err = opt2.SetupComponentLogger()
		assert.NoError(t, err)
	})
}

func TestLogOptionEdgeCases(t *testing.T) {
	t.Run("zero rotate bytes handled gracefully", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("zero_rotate.log"),
			common.WithLoggingRotateBytes(0),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("negative rotate backup count handled gracefully", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("negative_backup.log"),
			common.WithLoggingRotateBackupCount(-1),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("very large rotate bytes", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("large_rotate.log"),
			common.WithLoggingRotateBytes(1024*1024*1024), // 1GB
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
	})

	t.Run("relative stdout path resolved against logs dir", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStdoutFilepath("relative/stdout.log"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
		// The path resolution happens internally, we just verify no error
	})

	t.Run("relative stderr path resolved against logs dir", func(t *testing.T) {
		tmpDir := t.TempDir()

		opt := common.NewLogOption(
			common.WithLogsDir(tmpDir),
			common.WithLoggingFilename("main.log"),
			common.WithStderrFilepath("relative/stderr.log"),
		)

		err := opt.SetupComponentLogger()
		assert.NoError(t, err)
		// The path resolution happens internally, we just verify no error
	})
}
