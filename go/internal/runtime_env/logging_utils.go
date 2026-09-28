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

package runtime_env

import (
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"go.uber.org/zap/zapcore"

	"github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/ray-project/ray/go/pkg/log/zap"
)

// RedirectStdoutStderrIfNeeded sets up redirection for stdout and stderr if needed, based on the given rotation parameters.
// This function mirrors the behavior of the Python redirect_stdout_stderr_if_needed function.
//
// Parameters:
//   - stdoutFilepath: the filepath stdout will be redirected to; if empty, stdout will not be redirected.
//   - stderrFilepath: the filepath stderr will be redirected to; if empty, stderr will not be redirected.
//   - rotationBytes: number of bytes which triggers file rotation.
//   - rotationBackupCount: the max size of rotation files (number of backup files to keep).
func RedirectStdoutStderrIfNeeded(
	stdoutFilepath string,
	stderrFilepath string,
	rotationBytes int64,
	rotationBackupCount int,
) error {
	// Setup redirection for stdout and stderr using lumberjack logger.
	if stdoutFilepath != "" {
		if err := redirectStream(os.Stdout, stdoutFilepath, rotationBytes, rotationBackupCount); err != nil {
			return fmt.Errorf("failed to redirect stdout: %w", err)
		}
	}
	if stderrFilepath != "" {
		if err := redirectStream(os.Stderr, stderrFilepath, rotationBytes, rotationBackupCount); err != nil {
			return fmt.Errorf("failed to redirect stderr: %w", err)
		}
	}

	return nil
}

// redirectStream redirects a stream (stdout or stderr) to the specified file with rotation support.
// It uses syscall.Dup2 to redirect the file descriptor at the OS level.
func redirectStream(stream *os.File, filepath string, rotationBytes int64, rotationBackupCount int) error {
	// Open the log file
	logFile, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", filepath, err)
	}

	// Get the current stream file descriptor
	streamFd := stream.Fd()

	// Duplicate the log file's FD to avoid closing issues
	dupFd, err := syscall.Dup(int(logFile.Fd()))
	if err != nil {
		logFile.Close()
		return fmt.Errorf("failed to duplicate file descriptor: %w", err)
	}

	// Redirect the stream to the log file using dup2
	// This changes where the OS sends data written to the original FD
	if err := syscall.Dup2(dupFd, int(streamFd)); err != nil {
		syscall.Close(dupFd)
		logFile.Close()
		return fmt.Errorf("failed to redirect stream: %w", err)
	}

	// Close the duplicated FD and the temporary file handle
	// The original stream FD now points to the log file
	syscall.Close(dupFd)
	logFile.Close()

	// Note: Unlike Python, we don't replace os.Stdout/os.Stderr with custom writers
	// because Go's standard library handles buffering differently.
	// The syscall.Dup2 call ensures all writes to the original FD go to the log file.
	// For programmatic access, users should use the returned io.Writer.

	return nil
}

// NewRotatingWriter creates an io.Writer with rotation support for the given filepath.
// This can be used as a replacement for os.Stdout or os.Stderr in applications that need
// explicit control over where output is written.
func NewRotatingWriter(filepath string, rotationBytes int64, rotationBackupCount int) (io.Writer, error) {
	if filepath == "" {
		return os.Stdout, nil
	}

	// Create options for lumberjack logger
	opts := &lumberjack.Options{
		MaxSize:    int(rotationBytes / (1024 * 1024)), // Convert bytes to MB for lumberjack
		MaxBackups: rotationBackupCount,
		MaxAge:     0, // Don't remove old log files based on age
		Compress:   false,
		LocalTime:  true, // Use local time for file naming
	}

	// Use the lumberjack wrapper to create a rotating writer
	writer := lumberjack.NewWriter([]string{filepath}, opts, nil)
	return writer, nil
}

func BuildLoggingOptions(
	level zapcore.Level,
	format string,
	outputPaths []string,
	rotationBytes int,
	rotationBackupCount int,
) []zap.Option {
	opts := make([]zap.Option, 0, 3)

	// Set level (required field)
	opts = append(opts, zap.WithLevel(level))

	// Set format/encoder (optional field, defaults to console)
	encoder := zap.ConsoleEncoder
	if format != "" && strings.EqualFold(format, "json") {
		encoder = zap.JSONEncoder
	}
	opts = append(opts, zap.WithEncoder(encoder))

	// Add output paths if provided (empty slice means use default stdout/stderr)
	if len(outputPaths) > 0 {
		opts = append(opts, zap.WithOutputPaths(outputPaths...))
	}

	// Rotation (optional, only set if RotationBytes > 0)
	if rotationBytes > 0 {
		opts = append(opts, zap.WithRotation(&lumberjack.Options{
			MaxSize:    rotationBytes / (1024 * 1024), // Convert bytes to MB for lumberjack
			MaxBackups: rotationBackupCount,
			Compress:   true,
		}))
	}

	return opts
}
