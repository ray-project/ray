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

package log_monitor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/spf13/cobra"
)

func TestGetLogMonitorCmdParsesRequiredFlags(t *testing.T) {
	cmd := GetLogMonitorCmd()
	cmd.SetArgs([]string{
		"--session-dir=/tmp/ray/session",
		"--logs-dir=/tmp/ray/session/logs",
		"--gcs-address=127.0.0.1:6379",
		"--cluster-id-hex=0123456789abcdef0123456789abcdef0123456789abcdef01234567",
		"--node-ip-address=10.0.0.8",
	})

	// Disable actual execution by setting a no-op RunE
	var executed bool
	origRunE := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		executed = true
		return nil
	}

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if !executed {
		t.Fatal("expected command to execute")
	}

	// Restore original RunE for validation test
	cmd.RunE = origRunE
}

func TestValidateOptions(t *testing.T) {
	tests := []struct {
		name    string
		opts    *Options
		wantErr bool
	}{
		{
			name: "valid options",
			opts: &Options{
				SessionDir:    "/tmp/ray/session",
				LogsDir:       "/tmp/ray/session/logs",
				GCSAddress:    "127.0.0.1:6379",
				ClusterIDHex:  "0123456789abcdef0123456789abcdef0123456789abcdef01234567",
				NodeIPAddress: "10.0.0.8",
			},
			wantErr: false,
		},
		{
			name:    "missing session-dir",
			opts:    &Options{LogsDir: "/tmp/ray/session/logs", GCSAddress: "127.0.0.1:6379", ClusterIDHex: "0123456789abcdef0123456789abcdef0123456789abcdef01234567"},
			wantErr: true,
		},
		{
			name:    "missing logs-dir",
			opts:    &Options{SessionDir: "/tmp/ray/session", GCSAddress: "127.0.0.1:6379", ClusterIDHex: "0123456789abcdef0123456789abcdef0123456789abcdef01234567"},
			wantErr: true,
		},
		{
			name:    "missing gcs-address",
			opts:    &Options{SessionDir: "/tmp/ray/session", LogsDir: "/tmp/ray/session/logs", ClusterIDHex: "0123456789abcdef0123456789abcdef0123456789abcdef01234567"},
			wantErr: true,
		},
		{
			name:    "missing cluster-id-hex",
			opts:    &Options{SessionDir: "/tmp/ray/session", LogsDir: "/tmp/ray/session/logs", GCSAddress: "127.0.0.1:6379"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOptions(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogMonitorCommandInvokesRunnerWithParsedOptions(t *testing.T) {
	var got *Options
	cmd := newLogMonitorCmd(func(opts *Options) error {
		got = opts
		return nil
	})
	cmd.SetArgs([]string{
		"--session-dir=/tmp/ray/session",
		"--logs-dir=/tmp/ray/session/logs",
		"--gcs-address=127.0.0.1:6379",
		"--cluster-id-hex=0123456789abcdef0123456789abcdef0123456789abcdef01234567",
		"--node-ip-address=10.0.0.8",
		"--logging-level=debug",
		"--logging-format=%(asctime)s %(message)s",
		"--stdout-filepath=/tmp/out",
		"--stderr-filepath=/tmp/err",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got == nil || got.GCSAddress != "127.0.0.1:6379" {
		t.Fatalf("got = %#v", got)
	}
	if got.ClusterIDHex != "0123456789abcdef0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("ClusterIDHex = %q", got.ClusterIDHex)
	}
	if got.NodeIPAddress != "10.0.0.8" {
		t.Fatalf("NodeIPAddress = %q", got.NodeIPAddress)
	}
	if got.Logging.Level != "debug" {
		t.Fatalf("Logging.Level = %q", got.Logging.Level)
	}
	if got.Logging.Format != "%(asctime)s %(message)s" {
		t.Fatalf("Logging.Format = %q", got.Logging.Format)
	}
}

func TestConnectGCSClientWithRetryRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	deps := runnerDeps{
		connectGCSClient: func(opts gcs.ClientOptions) (gcs.Client, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("gcs not ready")
			}
			return nil, nil
		},
		retryTimeout: 50 * time.Millisecond,
		retryDelay:   time.Millisecond,
	}

	if _, err := connectGCSClientWithRetry(context.Background(), deps, gcs.ClientOptions{}); err != nil {
		t.Fatalf("connectGCSClientWithRetry() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDetectAutoscalerV2EnabledWithRetryRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	deps := runnerDeps{
		retryTimeout: 50 * time.Millisecond,
		retryDelay:   time.Millisecond,
	}

	enabled, err := detectAutoscalerV2EnabledWithRetry(context.Background(), deps, func(context.Context) (bool, error) {
		attempts++
		if attempts < 3 {
			return false, errors.New("gcs get not ready")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("detectAutoscalerV2EnabledWithRetry() error = %v", err)
	}
	if !enabled {
		t.Fatal("enabled = false, want true")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestLoggerOutputPaths(t *testing.T) {
	opts := &Options{
		LogsDir:        "/tmp/ray/session/logs",
		StdoutFilepath: "log_monitor.out",
		StderrFilepath: "log_monitor.err",
		Logging: LoggingConfig{
			Filename: "log_monitor.log",
		},
	}

	gotOut, gotErr := loggerOutputPaths(opts)
	wantOut := []string{
		filepath.Join(opts.LogsDir, "log_monitor.log"),
	}
	wantErr := []string{
		filepath.Join(opts.LogsDir, "log_monitor.log"),
	}

	if !reflect.DeepEqual(gotOut, wantOut) {
		t.Fatalf("outputPaths = %#v, want %#v", gotOut, wantOut)
	}
	if !reflect.DeepEqual(gotErr, wantErr) {
		t.Fatalf("errorOutputPaths = %#v, want %#v", gotErr, wantErr)
	}
}

func TestSetupLoggerWritesStdoutAndStderrFiles(t *testing.T) {
	tmpDir := t.TempDir()
	opts := &Options{
		LogsDir:        tmpDir,
		StdoutFilepath: "log_monitor.out",
		StderrFilepath: "log_monitor.err",
		Logging: LoggingConfig{
			Level:    "info",
			Filename: "log_monitor.log",
		},
	}
	restoreStreams := saveAndRestoreStandardStreams(t)
	defer restoreStreams()

	if err := setupLogger(opts); err != nil {
		t.Fatalf("setupLogger() error = %v", err)
	}

	logger := log.WithName("log-monitor-test")
	logger.Info("info message")
	logger.Error(errors.New("boom"), "error message")
	if _, err := fmt.Fprintln(os.Stdout, "stdout message"); err != nil {
		t.Fatalf("stdout write error = %v", err)
	}
	if _, err := fmt.Fprintln(os.Stderr, "stderr message"); err != nil {
		t.Fatalf("stderr write error = %v", err)
	}

	logContent, err := os.ReadFile(filepath.Join(tmpDir, "log_monitor.log"))
	if err != nil {
		t.Fatalf("ReadFile(log_monitor.log) error = %v", err)
	}
	if !strings.Contains(string(logContent), "info message") || !strings.Contains(string(logContent), "error message") {
		t.Fatalf("log_monitor.log missing logger output: %q", string(logContent))
	}

	outContent, err := os.ReadFile(filepath.Join(tmpDir, "log_monitor.out"))
	if err != nil {
		t.Fatalf("ReadFile(log_monitor.out) error = %v", err)
	}
	if !strings.Contains(string(outContent), "stdout message") {
		t.Fatalf("log_monitor.out missing stdout message: %q", string(outContent))
	}
	if strings.Contains(string(outContent), "info message") || strings.Contains(string(outContent), "error message") {
		t.Fatalf("log_monitor.out should not contain logger output: %q", string(outContent))
	}

	errContent, err := os.ReadFile(filepath.Join(tmpDir, "log_monitor.err"))
	if err != nil {
		t.Fatalf("ReadFile(log_monitor.err) error = %v", err)
	}
	if !strings.Contains(string(errContent), "stderr message") {
		t.Fatalf("log_monitor.err missing stderr message: %q", string(errContent))
	}
	if strings.Contains(string(errContent), "info message") || strings.Contains(string(errContent), "error message") {
		t.Fatalf("log_monitor.err should not contain logger output: %q", string(errContent))
	}
}

func saveAndRestoreStandardStreams(t *testing.T) func() {
	t.Helper()

	savedStdout, err := syscall.Dup(syscall.Stdout)
	if err != nil {
		t.Fatalf("Dup(stdout) error = %v", err)
	}
	savedStderr, err := syscall.Dup(syscall.Stderr)
	if err != nil {
		_ = syscall.Close(savedStdout)
		t.Fatalf("Dup(stderr) error = %v", err)
	}

	return func() {
		if err := syscall.Dup2(savedStdout, syscall.Stdout); err != nil {
			t.Fatalf("restore stdout error = %v", err)
		}
		if err := syscall.Dup2(savedStderr, syscall.Stderr); err != nil {
			t.Fatalf("restore stderr error = %v", err)
		}
		_ = syscall.Close(savedStdout)
		_ = syscall.Close(savedStderr)
		os.Stdout = os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")
		os.Stderr = os.NewFile(uintptr(syscall.Stderr), "/dev/stderr")
	}
}
