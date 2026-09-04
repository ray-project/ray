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


package common

import (
	"fmt"
	"github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"go.uber.org/zap/zapcore"
	"path/filepath"
)

type LogOption struct {
	// LoggingLevel log level (optional, default: "info")
	LoggingLevel string

	// LoggingFormat log format (optional)
	LoggingFormat string

	// LoggingFilename log file name (optional, default: "monitor.log")
	LoggingFilename string

	// LogsDir log directory (required)
	LogsDir string

	// LoggingRotateBytes log rotation size (optional, default: 100MB)
	LoggingRotateBytes int

	// LoggingRotateBackupCount number of rotation backups (optional, default: 5)
	LoggingRotateBackupCount int

	// StdoutFilepath stdout output file path (optional)
	StdoutFilepath string

	// StderrFilepath stderr output file path (optional)
	StderrFilepath string
}

// Option is a function type that configures *LogOption.
type Option func(*LogOption)

func WithLoggingLevel(level string) Option {
	return func(c *LogOption) {
		c.LoggingLevel = level
	}
}

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

func WithLoggingFilename(filename string) Option {
	return func(c *LogOption) {
		c.LoggingFilename = filename
	}
}

func WithLoggingRotateBytes(bytes int) Option {
	return func(c *LogOption) {
		if bytes > 0 {
			c.LoggingRotateBytes = bytes
		}
	}
}

func WithLoggingRotateBackupCount(count int) Option {
	return func(c *LogOption) {
		if count > 0 {
			c.LoggingRotateBackupCount = count
		}
	}
}

func WithStdoutFilepath(path string) Option {
	return func(c *LogOption) {
		c.StdoutFilepath = path
	}
}

func WithStderrFilepath(path string) Option {
	return func(c *LogOption) {
		c.StderrFilepath = path
	}
}

// NewLogOption creates and returns a new *LogOption.
func NewLogOption(opts ...Option) *LogOption {
	// Default values.
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
	// Parse the configured log level.
	loggingZapcoreLevel, err := ParseLogLevel(opt.LoggingLevel)
	if err != nil {
		return err
	}

	// Build the full log file paths.
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

	// If no log file path is configured, fall back to empty outputs.
	if len(logOutputPaths) == 0 {
		logOutputPaths = []string{}
	}
	if len(logErrorOutputPaths) == 0 {
		logErrorOutputPaths = []string{}
	}

	// Convert bytes to MB (lumberjack uses MB as the unit).
	maxSizeMB := opt.LoggingRotateBytes / (1024 * 1024)
	if maxSizeMB <= 0 {
		maxSizeMB = 1
	}

	// Choose the encoder type based on the configured log format.
	var encoderType zap.EncoderType
	if opt.LoggingFormat != "" {
		// If a custom format is configured, use the Console encoder.
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
