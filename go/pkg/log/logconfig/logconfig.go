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

// Package logconfig provides component logger configuration for Ray Go
// processes: build a LogOption and apply it to the global zap logger.
//
// It lives under go/pkg/log (not go/internal/common) so that pkg-level code
// such as go/pkg/runtime/api can configure the component logger without a
// pkg -> internal dependency.
package logconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultLoggingDevelopment controls whether the logger runs in development
	// or production mode.
	DefaultLoggingDevelopment = true
	// LOGGING_ROTATE_BYTES is the default log rotation size in bytes (aligned
	// with the Python LOGGING_ROTATE_BYTES constant).
	LOGGING_ROTATE_BYTES = 512 * 1024 * 1024 // 512MB
	// LOGGING_ROTATE_BACKUP_COUNT is the default number of rotated log backups
	// to keep.
	LOGGING_ROTATE_BACKUP_COUNT = 5
)

// RayGoLogFilename returns the log file name for the Go driver/worker process.
// Shared by go/pkg/runtime/api (driver logger) and go/cmd/raygo/default_worker
// (worker logger) so both always produce identical names.
func RayGoLogFilename() string {
	return fmt.Sprintf("ray_go_%d.log", os.Getpid())
}

type LogOption struct {
	// LoggingLevel is the logging level (optional, default: "info").
	LoggingLevel string

	// LoggingFormat is the logging format (optional).
	LoggingFormat string

	// LoggingFilename is the log file name (optional, default: "monitor.log").
	LoggingFilename string

	// LogsDir is the log directory (required).
	LogsDir string

	// LoggingRotateBytes is the log rotation size in bytes (optional, defaults
	// to LOGGING_ROTATE_BYTES).
	LoggingRotateBytes int

	// LoggingRotateBackupCount is the number of rotated log backups (optional, default: 5).
	LoggingRotateBackupCount int

	// StdoutFilepath is the stdout output file path (optional).
	StdoutFilepath string

	// StderrFilepath is the stderr output file path (optional).
	StderrFilepath string
}

// Option modifies a LogOption.
type Option func(*LogOption)

// WithLoggingLevel sets the logging level.
func WithLoggingLevel(level string) Option {
	return func(c *LogOption) {
		c.LoggingLevel = level
	}
}

// WithLoggingFormat sets the logging format.
func WithLoggingFormat(format string) Option {
	return func(c *LogOption) {
		c.LoggingFormat = format
	}
}

func WithLogsDir(logsDir string) Option {
	return func(c *LogOption) {
		if logsDir != "" {
			c.LogsDir = logsDir
		}
	}
}

// WithLoggingFilename sets the log file name.
func WithLoggingFilename(filename string) Option {
	return func(c *LogOption) {
		c.LoggingFilename = filename
	}
}

// WithLoggingRotateBytes sets the log rotation size in bytes.
func WithLoggingRotateBytes(bytes int) Option {
	return func(c *LogOption) {
		if bytes > 0 {
			c.LoggingRotateBytes = bytes
		}
	}
}

// WithLoggingRotateBackupCount sets the number of rotated log backups.
func WithLoggingRotateBackupCount(count int) Option {
	return func(c *LogOption) {
		if count > 0 {
			c.LoggingRotateBackupCount = count
		}
	}
}

// WithStdoutFilepath sets the stdout output file path.
func WithStdoutFilepath(path string) Option {
	return func(c *LogOption) {
		c.StdoutFilepath = path
	}
}

// WithStderrFilepath sets the stderr output file path.
func WithStderrFilepath(path string) Option {
	return func(c *LogOption) {
		c.StderrFilepath = path
	}
}

// NewLogOption creates and returns a new LogOption instance.
func NewLogOption(opts ...Option) *LogOption {
	// Apply defaults.
	cfg := &LogOption{
		LoggingLevel:             "info",
		LoggingRotateBytes:       LOGGING_ROTATE_BYTES,
		LoggingRotateBackupCount: LOGGING_ROTATE_BACKUP_COUNT,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// ParseLogLevel parses a logging level.
func ParseLogLevel(level string) (zapcore.Level, error) {
	switch level {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "critical":
		return zapcore.PanicLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("unsupported log level: %s", level)
	}
}

// SetupComponentLogger configures the component logging system.
func (opt *LogOption) SetupComponentLogger() error {
	// Parse the logging level.
	loggingZapcoreLevel, err := ParseLogLevel(opt.LoggingLevel)
	if err != nil {
		return err
	}

	// Build the full log file path.
	var logOutputPaths []string
	var logErrorOutputPaths []string

	if opt.LogsDir != "" && opt.LoggingFilename != "" {
		logFilePath := filepath.Join(opt.LogsDir, opt.LoggingFilename)
		logOutputPaths = []string{logFilePath}
		logErrorOutputPaths = []string{logFilePath}
	}

	// If stdout/stderr file paths are configured, add them to the output paths.
	if opt.StdoutFilepath != "" {
		if opt.LogsDir != "" && !filepath.IsAbs(opt.StdoutFilepath) {
			logOutputPaths = append(logOutputPaths, filepath.Join(opt.LogsDir, opt.StdoutFilepath))
		} else {
			logOutputPaths = append(logOutputPaths, opt.StdoutFilepath)
		}
	}

	if opt.StderrFilepath != "" {
		if opt.LogsDir != "" && !filepath.IsAbs(opt.StderrFilepath) {
			logErrorOutputPaths = append(logErrorOutputPaths, filepath.Join(opt.LogsDir, opt.StderrFilepath))
		} else {
			logErrorOutputPaths = append(logErrorOutputPaths, opt.StderrFilepath)
		}
	}

	// If no log file path is configured, fall back to the default output.
	if len(logOutputPaths) == 0 {
		logOutputPaths = []string{}
	}
	if len(logErrorOutputPaths) == 0 {
		logErrorOutputPaths = []string{}
	}

	// Convert bytes to MB (lumberjack uses MB as its unit).
	maxSizeMB := opt.LoggingRotateBytes / (1024 * 1024)
	if maxSizeMB <= 0 {
		maxSizeMB = 1
	}

	// Choose the encoder type based on the logging format.
	var encoderType zap.EncoderType
	if opt.LoggingFormat != "" {
		// Use the console encoder when a custom format is configured.
		encoderType = zap.ConsoleEncoder
	} else {
		// Default to the JSON encoder (production mode).
		encoderType = zap.JSONEncoder
	}

	if err := zap.SetupDefaultLogger(
		zap.WithLevel(loggingZapcoreLevel),
		zap.WithEncoder(encoderType),
		zap.WithDevelopment(DefaultLoggingDevelopment),
		zap.WithOutputPaths(logOutputPaths...),
		zap.WithErrorOutputPaths(logErrorOutputPaths...),
		zap.WithRotation(&lumberjack.Options{
			MaxSize:    maxSizeMB,
			MaxBackups: opt.LoggingRotateBackupCount,
			Compress:   true,
			LocalTime:  true,
		}),
	); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	return nil
}
