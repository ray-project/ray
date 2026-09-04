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


// Package zap provides Zap as the default logr implementation.
package zap

import (
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/lumberjack"
)

// ZapError is a Zap-related error type.
type ZapError struct {
	Op   string
	Path string
	Err  error
}

func (e *ZapError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("zap %s: %s: %v", e.Op, e.Path, e.Err)
	}
	return fmt.Sprintf("zap %s: %v", e.Op, e.Err)
}

// New creates a logr.Logger instance using Zap as the underlying implementation.
func New(opts *Options) (logr.Logger, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	zapLogger, err := buildZapLogger(opts)
	if err != nil {
		return logr.Discard(), err
	}

	return zapr.NewLogger(zapLogger), nil
}

// NewRaw creates a raw zap.Logger instance.
func NewRaw(opts *Options) (*zap.Logger, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	return buildZapLogger(opts)
}

// buildZapLogger builds a Zap Logger with Tee output.
func buildZapLogger(opts *Options) (*zap.Logger, error) {
	// stdout and stderr need separate encoders to avoid concurrent write conflicts
	stdoutEncoder := newEncoder(opts)
	stderrEncoder := newEncoder(opts)

	// Create stdout output (Info/Debug level only)
	stdoutWriter := lumberjack.NewWriteSyncer(
		opts.OutputPaths,
		opts.Rotation,
		zapcore.AddSync(os.Stdout),
	)

	// stdout allows only Debug and Info levels (excludes Warn)
	stdoutLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= opts.Level && l < zapcore.WarnLevel
	})

	stdoutCore := zapcore.NewCore(stdoutEncoder, stdoutWriter, stdoutLevel)

	// Create stderr output (Warn/Error level only)
	stderrWriter := lumberjack.NewWriteSyncer(
		opts.ErrorOutputPaths,
		opts.Rotation,
		zapcore.AddSync(os.Stderr),
	)

	// stderr allows only Warn and Error levels
	stderrLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= zapcore.WarnLevel && l >= opts.Level
	})

	stderrCore := zapcore.NewCore(stderrEncoder, stderrWriter, stderrLevel)

	// Use Tee to combine both cores
	teeCore := zapcore.NewTee(stdoutCore, stderrCore)

	// Create Logger with caller
	options := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1), // Skip zapr wrapper layer
	}

	// Add stacktrace for Error level
	if opts.Development {
		options = append(options, zap.Development())
	} else {
		options = append(options, zap.AddStacktrace(zapcore.ErrorLevel))
	}

	return zap.New(teeCore, options...), nil
}

// SetupDefaultLogger initializes the global Logger with default Zap configuration.
// Uses Go options pattern for flexible configuration.
//
// Examples:
//
//	// Default config (development mode)
//	zap.SetupDefaultLogger()
//
//	// Production mode
//	zap.SetupDefaultLogger(zap.WithDevelopment(false))
//
//	// Custom level and output paths
//	zap.SetupDefaultLogger(
//	    zap.WithLevel(zapcore.DebugLevel),
//	    zap.WithOutputPaths("stdout.log"),
//	    zap.WithErrorOutputPaths("stderr.log"),
//	)
func SetupDefaultLogger(opts ...Option) error {
	options := applyOptions(opts...)

	logger, err := New(options)
	if err != nil {
		// Create fallback logger
		fallbackLogger, _ := New(DefaultOptions())
		log.SetLogger(fallbackLogger)

		fallbackLogger.Error(err, "failed to initialize logger, using fallback")
		return err
	}

	log.SetLogger(logger)
	return nil
}

// SetupComponentLogger creates and configures an independent logger for a specific component.
// This is equivalent to Python's setup_component_logger() - each call returns a new logger
// instance that can be configured independently from the global logger and other components.
//
// The function accepts the same options as SetupDefaultLogger, allowing you to customize
// log level, output paths, rotation settings, etc. for each component.
//
// Examples:
//
//	// Create a logger for monitor component with custom configuration
//	monitorLogger, err := zap.SetupComponentLogger(
//	    zap.WithLevel(zapcore.DebugLevel),
//	    zap.WithOutputPaths("/var/log/ray/monitor.log"),
//	    zap.WithRotation(&lumberjack.Options{
//	        MaxSize:    50,
//	        MaxBackups: 10,
//	    }),
//	)
//	if err != nil {
//	    // handle error
//	}
//	monitorLogger.Info("monitor started")
//
//	// Create another logger for dashboard with different configuration
//	dashboardLogger, err := zap.SetupComponentLogger(
//	    zap.WithLevel(zapcore.InfoLevel),
//	    zap.WithOutputPaths("/var/log/ray/dashboard.log"),
//	)
//	dashboardLogger.Info("dashboard initialized")
func SetupComponentLogger(opts ...Option) logr.Logger {
	options := applyOptions(opts...)

	logger, err := New(options)
	if err != nil {
		// Create fallback logger with default options
		fallbackLogger, _ := New(DefaultOptions())

		// Log the error using the fallback logger
		fallbackLogger.Error(err, "failed to create component logger, using fallback")

		// Return the fallback logger instead of Discard
		return fallbackLogger
	}

	return logger
}
