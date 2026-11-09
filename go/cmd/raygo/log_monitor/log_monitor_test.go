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
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/logmonitor"
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
		connectGCSClient: func(ctx context.Context, opts gcs.ClientOptions) (gcsClient, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("gcs not ready")
			}
			return &fakeGCSClient{}, nil
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

// TestConnectGCSClientWithRetryRetriesConnectionError verifies a connection
// error keeps being retried until the startup timeout elapses.
func TestConnectGCSClientWithRetryRetriesConnectionError(t *testing.T) {
	attempts := 0
	deps := runnerDeps{
		connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
			attempts++
			return nil, errors.New("connection refused")
		},
		retryTimeout: 30 * time.Millisecond,
		retryDelay:   time.Millisecond,
	}

	_, err := connectGCSClientWithRetry(context.Background(), deps, gcs.ClientOptions{})
	if err == nil {
		t.Fatal("connectGCSClientWithRetry() = nil error, want connection error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("error %q does not mention the connection failure", err)
	}
	if attempts < 2 {
		t.Fatalf("attempts = %d, want at least 2 (connection errors must be retried)", attempts)
	}
}

// TestConnectGCSClientWithRetryDoesNotRetryConfigError verifies a client
// construction failure classified as a configuration error is reported after a
// single call instead of being retried until the startup timeout.
func TestConnectGCSClientWithRetryDoesNotRetryConfigError(t *testing.T) {
	attempts := 0
	deps := runnerDeps{
		connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
			attempts++
			return nil, fmt.Errorf("%w: missing client certificate", errGCSClientConfig)
		},
		retryTimeout: 500 * time.Millisecond,
		retryDelay:   time.Millisecond,
	}

	start := time.Now()
	_, err := connectGCSClientWithRetry(context.Background(), deps, gcs.ClientOptions{})
	if err == nil {
		t.Fatal("connectGCSClientWithRetry() = nil error, want configuration error")
	}
	if !errors.Is(err, errGCSClientConfig) {
		t.Fatalf("error %q does not wrap errGCSClientConfig", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (configuration errors must not be retried)", attempts)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("configuration error took %v, want an immediate failure", elapsed)
	}
}

// TestConnectGCSClientWithRetryFailsFastOnDialOptionsError verifies the dial
// options are resolved once before the retry loop, so a TLS misconfiguration
// fails without invoking the connect function at all.
func TestConnectGCSClientWithRetryFailsFastOnDialOptionsError(t *testing.T) {
	t.Setenv("RAY_USE_TLS", "1")
	t.Setenv("RAY_TLS_SERVER_CERT", "")
	t.Setenv("RAY_TLS_SERVER_KEY", "")
	t.Setenv("RAY_TLS_CA_CERT", "")

	attempts := 0
	deps := runnerDeps{
		connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
			attempts++
			return &fakeGCSClient{}, nil
		},
		retryTimeout: 500 * time.Millisecond,
		retryDelay:   time.Millisecond,
	}

	start := time.Now()
	_, err := connectGCSClientWithRetry(context.Background(), deps, gcs.ClientOptions{})
	if err == nil {
		t.Fatal("connectGCSClientWithRetry() = nil error, want configuration error")
	}
	if !errors.Is(err, errGCSClientConfig) {
		t.Fatalf("error %q does not wrap errGCSClientConfig", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (dial options are resolved before the retry loop)", attempts)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("dial option error took %v, want an immediate failure", elapsed)
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

// fakeGCSClient satisfies gcsClient for tests that go through connectGCSClient.
type fakeGCSClient struct {
	values   map[string][]byte
	closed   bool
	err      error
	payloads []gcs.LogBatchPayload
}

func (f *fakeGCSClient) Get(_ context.Context, ns, key string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if v, ok := f.values[ns+"/"+key]; ok {
		return v, nil
	}
	return nil, gcs.ErrKeyNotFound
}

func (f *fakeGCSClient) Close() error { f.closed = true; return nil }

func (f *fakeGCSClient) PublishLogBatch(_ context.Context, payload gcs.LogBatchPayload) error {
	f.payloads = append(f.payloads, payload)
	return nil
}

// TestRunLogMonitorWiresPublishClient verifies the pure-Go gcsClient (which
// itself implements gcs.LogBatchPublisher) is passed through to
// logmonitor.NewRuntimePublisher and that publishing a batch reaches the
// underlying gcsClient.
func TestRunLogMonitorWiresPublishClient(t *testing.T) {
	pub := &fakeGCSClient{}

	monitorStarted := make(chan struct{}, 1)
	var capturedPublisher logmonitor.Publisher
	deps := runnerDeps{
		connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
			return pub, nil
		},
		retryTimeout: 50 * time.Millisecond,
		retryDelay:   time.Millisecond,
		newMonitor: func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error) {
			capturedPublisher = publisher
			return &fakeMonitorRunner{started: monitorStarted}, nil
		},
	}

	opts := &Options{
		SessionDir:    t.TempDir(),
		LogsDir:       t.TempDir(),
		GCSAddress:    "127.0.0.1:6379",
		ClusterIDHex:  "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c",
		NodeIPAddress: "127.0.0.1",
		Logging:       LoggingConfig{Level: "info", Filename: ""},
	}
	if err := runLogMonitorWithDeps(opts, deps); err != nil {
		t.Fatalf("runLogMonitorWithDeps error = %v", err)
	}
	if capturedPublisher == nil {
		t.Fatal("newMonitor did not receive a publisher")
	}
	// Publish a batch through the wrapped runtime publisher.
	if err := capturedPublisher.Publish(logmonitor.LogBatch{IP: "1.2.3.4", JobID: "j1", Lines: []string{"hello"}}); err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(pub.payloads) != 1 {
		t.Fatalf("fake gcs client got %d payloads, want 1", len(pub.payloads))
	}
	if pub.payloads[0].IP != "1.2.3.4" || pub.payloads[0].Lines[0] != "hello" {
		t.Fatalf("payload = %+v, want IP 1.2.3.4 lines [hello]", pub.payloads[0])
	}
}

type fakeMonitorRunner struct {
	started chan struct{}
}

func (f *fakeMonitorRunner) Run(ctx context.Context) error {
	select {
	case f.started <- struct{}{}:
	default:
	}
	return nil
}

// TestRunLogMonitorSkipsPlugin verifies the full flow runs to completion without
// any plugin-related pre-check: runnerDeps no longer carries validatePlugin, so
// runLogMonitorWithDeps wires the pure-Go client and starts the monitor directly.
func TestRunLogMonitorSkipsPlugin(t *testing.T) {
	monitorStarted := make(chan struct{}, 1)
	deps := runnerDeps{
		connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
			return &fakeGCSClient{values: map[string][]byte{}}, nil
		},
		retryTimeout: 50 * time.Millisecond,
		retryDelay:   time.Millisecond,
		newMonitor: func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error) {
			return &fakeMonitorRunner{started: monitorStarted}, nil
		},
	}

	opts := &Options{
		SessionDir:    t.TempDir(),
		LogsDir:       t.TempDir(),
		GCSAddress:    "127.0.0.1:6379",
		ClusterIDHex:  "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c",
		NodeIPAddress: "127.0.0.1",
		Logging:       LoggingConfig{Level: "info", Filename: ""},
	}
	if err := runLogMonitorWithDeps(opts, deps); err != nil {
		t.Fatalf("runLogMonitorWithDeps error = %v", err)
	}
	select {
	case <-monitorStarted:
	default:
		t.Fatal("monitor was not started")
	}
}

func TestAutoscalerV2FromEnv(t *testing.T) {
	tests := []struct {
		name    string
		envVal  string
		unset   bool
		want    bool
		wantSet bool
	}{
		{name: "unset", unset: true, want: false, wantSet: false},
		{name: "one", envVal: "1", want: true, wantSet: true},
		{name: "true", envVal: "true", want: true, wantSet: true},
		{name: "true uppercase", envVal: "TRUE", want: true, wantSet: true},
		{name: "true with surrounding whitespace", envVal: " true ", want: true, wantSet: true},
		// A present but falsy value is still reported as set; only the enabled
		// result is false, so the v2 detection falls through to the GCS KV.
		{name: "zero", envVal: "0", want: false, wantSet: true},
		{name: "false", envVal: "false", want: false, wantSet: true},
		{name: "garbage", envVal: "enabled", want: false, wantSet: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				t.Setenv(autoscalerV2EnabledEnvVar, "")
				os.Unsetenv(autoscalerV2EnabledEnvVar)
			} else {
				t.Setenv(autoscalerV2EnabledEnvVar, tt.envVal)
			}
			got, set := autoscalerV2FromEnv()
			if got != tt.want || set != tt.wantSet {
				t.Fatalf("autoscalerV2FromEnv() = (%v, %v), want (%v, %v)", got, set, tt.want, tt.wantSet)
			}
		})
	}
}

// countingGCSClient wraps fakeGCSClient and counts GCS KV Get calls so a test
// can assert that autoscaler v2 detection never queries GCS when the
// RAY_enable_autoscaler_v2 environment variable short-circuits.
type countingGCSClient struct {
	*fakeGCSClient
	gets int
}

func (c *countingGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	c.gets++
	return c.fakeGCSClient.Get(ctx, ns, key)
}

// TestRunLogMonitorAutoscalerV2EnvShortCircuit verifies that
// runLogMonitorWithDeps only short-circuits on a truthy
// RAY_enable_autoscaler_v2 ("1"/"true"): the env value is passed straight
// through to newMonitor without querying the GCS internal KV. A falsy or
// unset env falls through to the GCS KV detection path, so the env can never
// disable a server-side true.
func TestRunLogMonitorAutoscalerV2EnvShortCircuit(t *testing.T) {
	tests := []struct {
		name     string
		envVal   string
		unset    bool
		wantV2   bool
		wantGets int
	}{
		{name: "env one short-circuits to true", envVal: "1", wantV2: true, wantGets: 0},
		{name: "env true short-circuits to true", envVal: "true", wantV2: true, wantGets: 0},
		{name: "env zero falls through to KV", envVal: "0", wantV2: false, wantGets: 1},
		{name: "env false falls through to KV", envVal: "false", wantV2: false, wantGets: 1},
		{name: "env garbage falls through to KV", envVal: "yes", wantV2: false, wantGets: 1},
		{name: "unset falls through to KV", unset: true, wantV2: false, wantGets: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				t.Setenv(autoscalerV2EnabledEnvVar, "")
				os.Unsetenv(autoscalerV2EnabledEnvVar)
			} else {
				t.Setenv(autoscalerV2EnabledEnvVar, tt.envVal)
			}

			client := &countingGCSClient{fakeGCSClient: &fakeGCSClient{values: map[string][]byte{}}}

			monitorStarted := make(chan struct{}, 1)
			var gotAutoscalerV2 *bool
			deps := runnerDeps{
				connectGCSClient: func(context.Context, gcs.ClientOptions) (gcsClient, error) {
					return client, nil
				},
				retryTimeout: 50 * time.Millisecond,
				retryDelay:   time.Millisecond,
				newMonitor: func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error) {
					gotAutoscalerV2 = &autoscalerV2
					return &fakeMonitorRunner{started: monitorStarted}, nil
				},
			}

			opts := &Options{
				SessionDir:    t.TempDir(),
				LogsDir:       t.TempDir(),
				GCSAddress:    "127.0.0.1:6379",
				ClusterIDHex:  "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c",
				NodeIPAddress: "127.0.0.1",
				Logging:       LoggingConfig{Level: "info", Filename: ""},
			}
			if err := runLogMonitorWithDeps(opts, deps); err != nil {
				t.Fatalf("runLogMonitorWithDeps error = %v", err)
			}
			if gotAutoscalerV2 == nil {
				t.Fatal("newMonitor was not called")
			}
			if *gotAutoscalerV2 != tt.wantV2 {
				t.Fatalf("newMonitor got autoscalerV2 = %v, want %v", *gotAutoscalerV2, tt.wantV2)
			}
			if client.gets != tt.wantGets {
				t.Fatalf("GCS autoscaler v2 detection performed %d Get calls, want %d", client.gets, tt.wantGets)
			}
			select {
			case <-monitorStarted:
			default:
				t.Fatal("monitor was not started")
			}
		})
	}
}

func TestApplyMemoryLimitDefaultAndEnv(t *testing.T) {
	// Restore the process soft memory limit whether the test passes or fails,
	// so a 64 MiB limit is never left behind in the test binary.
	t.Cleanup(func() { debug.SetMemoryLimit(-1) })
	t.Setenv(envMemoryLimit, "")
	t.Setenv("GOMEMLIMIT", "")
	if got := applyMemoryLimit(); got != int64(defaultMemoryLimit) {
		t.Fatalf("applyMemoryLimit() default = %d, want %d", got, defaultMemoryLimit)
	}
	t.Setenv(envMemoryLimit, "134217728") // 128 MiB
	if got := applyMemoryLimit(); got != int64(128<<20) {
		t.Fatalf("applyMemoryLimit() env = %d, want %d", got, 128<<20)
	}
	t.Setenv(envMemoryLimit, "invalid")
	if got := applyMemoryLimit(); got != int64(defaultMemoryLimit) {
		t.Fatalf("applyMemoryLimit() invalid env = %d, want default %d", got, defaultMemoryLimit)
	}
}

func TestApplyMemoryLimitHonorsGOMEMLIMIT(t *testing.T) {
	t.Cleanup(func() { debug.SetMemoryLimit(-1) })
	t.Setenv(envMemoryLimit, "")
	t.Setenv("GOMEMLIMIT", "1GiB")
	if got := applyMemoryLimit(); got != int64(1<<30) {
		t.Fatalf("applyMemoryLimit() GOMEMLIMIT=1GiB = %d, want %d", got, int64(1<<30))
	}
	// "off" disables the limit entirely; it must not fall back to the default.
	t.Setenv("GOMEMLIMIT", "off")
	if got := applyMemoryLimit(); got != math.MaxInt64 {
		t.Fatalf("applyMemoryLimit() GOMEMLIMIT=off = %d, want %d", got, int64(math.MaxInt64))
	}
	t.Setenv("GOMEMLIMIT", "2048M")
	if got := applyMemoryLimit(); got != int64(defaultMemoryLimit) {
		t.Fatalf("applyMemoryLimit() GOMEMLIMIT=2048M = %d, want default %d", got, int64(defaultMemoryLimit))
	}
	t.Setenv("GOMEMLIMIT", "invalid")
	if got := applyMemoryLimit(); got != int64(defaultMemoryLimit) {
		t.Fatalf("applyMemoryLimit() GOMEMLIMIT=invalid = %d, want default %d", got, int64(defaultMemoryLimit))
	}
	// Explicit env still wins over a process-level GOMEMLIMIT.
	t.Setenv("GOMEMLIMIT", "1GiB")
	t.Setenv(envMemoryLimit, "134217728") // 128 MiB
	if got := applyMemoryLimit(); got != int64(128<<20) {
		t.Fatalf("applyMemoryLimit() env over GOMEMLIMIT = %d, want %d", got, int64(128<<20))
	}
}

func TestEnvPositiveInt(t *testing.T) {
	const key = "RAY_LOG_MONITOR_MAX_OPEN_FILES"
	tests := []struct {
		name       string
		envVal     string
		unset      bool
		defaultVal int
		want       int
		wantWarn   bool
	}{
		{name: "unset falls back to default", unset: true, defaultVal: 200, want: 200, wantWarn: false},
		{name: "positive value adopted", envVal: "300", defaultVal: 200, want: 300, wantWarn: false},
		{name: "zero falls back with warning", envVal: "0", defaultVal: 200, want: 200, wantWarn: true},
		{name: "negative falls back with warning", envVal: "-5", defaultVal: 200, want: 200, wantWarn: true},
		{name: "non-numeric falls back with warning", envVal: "abc", defaultVal: 200, want: 200, wantWarn: true},
		{name: "partially parsed falls back with warning", envVal: "5x", defaultVal: 200, want: 200, wantWarn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				t.Setenv(key, "")
				os.Unsetenv(key)
			} else {
				t.Setenv(key, tt.envVal)
			}
			var got int
			stderr := captureStderrString(t, func() {
				got = envPositiveInt(key, tt.defaultVal)
			})
			if got != tt.want {
				t.Fatalf("envPositiveInt(%q, %d) = %d, want %d", key, tt.defaultVal, got, tt.want)
			}
			if tt.wantWarn && !strings.Contains(stderr, key) {
				t.Fatalf("expected stderr warning mentioning %q, got %q", key, stderr)
			}
			if !tt.wantWarn && stderr != "" {
				t.Fatalf("unexpected stderr output %q", stderr)
			}
		})
	}
}

func TestEnvPositiveDuration(t *testing.T) {
	const key = "LOG_NAME_UPDATE_INTERVAL_S"
	tests := []struct {
		name       string
		envVal     string
		unset      bool
		defaultVal time.Duration
		want       time.Duration
		wantWarn   bool
	}{
		{name: "unset falls back to default", unset: true, defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: false},
		{name: "0.5 converts to 500ms", envVal: "0.5", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: false},
		{name: "1.5 converts to 1.5s", envVal: "1.5", defaultVal: 500 * time.Millisecond, want: 1500 * time.Millisecond, wantWarn: false},
		{name: "zero falls back with warning", envVal: "0", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "negative falls back with warning", envVal: "-1", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "non-numeric falls back with warning", envVal: "abc", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "partially parsed falls back with warning", envVal: "5x", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "inf falls back with warning", envVal: "Inf", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "overflowing seconds falls back with warning", envVal: "1e10", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "underflowing seconds falls back with warning", envVal: "1e-12", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
		{name: "nan falls back with warning", envVal: "NaN", defaultVal: 500 * time.Millisecond, want: 500 * time.Millisecond, wantWarn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unset {
				t.Setenv(key, "")
				os.Unsetenv(key)
			} else {
				t.Setenv(key, tt.envVal)
			}
			var got time.Duration
			stderr := captureStderrString(t, func() {
				got = envPositiveDuration(key, tt.defaultVal)
			})
			if got != tt.want {
				t.Fatalf("envPositiveDuration(%q, %v) = %v, want %v", key, tt.defaultVal, got, tt.want)
			}
			if tt.wantWarn && !strings.Contains(stderr, key) {
				t.Fatalf("expected stderr warning mentioning %q, got %q", key, stderr)
			}
			if !tt.wantWarn && stderr != "" {
				t.Fatalf("unexpected stderr output %q", stderr)
			}
		})
	}
}

// captureStderrString runs fn while os.Stderr is redirected to a pipe and
// returns everything fn wrote to stderr.
func captureStderrString(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	os.Stderr = w
	fn()
	_ = w.Close()
	os.Stderr = old
	data, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatalf("ReadAll error = %v", err)
	}
	return string(data)
}

func TestParseGOMEMLIMIT(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{"", 0, false},
		{"-1", 0, false},
		{"  ", 0, false},
		{"off", math.MaxInt64, true},
		{"64", 64, true},
		{"64B", 64, true},
		{"1KiB", 1 << 10, true},
		{"2048M", 0, false},
		{"1GiB", 1 << 30, true},
		{"2TiB", 2 << 40, true},
		{"1.5GiB", 0, false},
		{"1G", 0, false},
		{"1gib", 0, false},
		{"abc", 0, false},
		{"0", 0, false},
	} {
		got, ok := parseGOMEMLIMIT(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Fatalf("parseGOMEMLIMIT(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestEnvPositiveIntWarnsOnInvalidValue verifies a set-but-invalid value falls
// back to the default and warns on stderr instead of failing silently.
func TestEnvPositiveIntWarnsOnInvalidValue(t *testing.T) {
	const key = "RAY_GO_LOG_MONITOR_TEST_INT"
	t.Setenv(key, "not-a-number")

	stderrPath := filepath.Join(t.TempDir(), "stderr")
	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		t.Fatalf("Create(stderr) error = %v", err)
	}
	origStderr := os.Stderr
	os.Stderr = stderrFile

	got := envPositiveInt(key, 7)
	os.Stderr = origStderr
	if err := stderrFile.Close(); err != nil {
		t.Fatalf("Close(stderr) error = %v", err)
	}

	if got != 7 {
		t.Fatalf("envPositiveInt(%q, 7) = %d, want 7", key, got)
	}
	warning, err := os.ReadFile(stderrPath)
	if err != nil {
		t.Fatalf("ReadFile(stderr) error = %v", err)
	}
	if !strings.Contains(string(warning), key) || !strings.Contains(string(warning), "using default 7") {
		t.Fatalf("stderr = %q, want a warning about %s and the default", string(warning), key)
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
