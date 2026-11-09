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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ray-project/ray/go/internal/common"
	gcsgrpc "github.com/ray-project/ray/go/internal/gcs/grpc"
	"github.com/ray-project/ray/go/internal/logmonitor"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log/logconfig"
	lj "github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/ray-project/ray/go/pkg/log/zap"
)

const (
	nodeIPFilename                = "node_ip_address.json"
	autoscalerStateNamespace      = "__autoscaler"
	autoscalerV2EnabledKey        = "__autoscaler_v2_enabled"
	autoscalerV2EnabledEnvVar     = "RAY_enable_autoscaler_v2"
	runtimeEnvLogToDriverEnvVar   = "RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED"
	envMaxLineSize                = "RAY_GO_LOG_MONITOR_MAX_LINE_BYTES"
	envMaxOpenFiles               = "RAY_LOG_MONITOR_MAX_OPEN_FILES"
	envManyFilesThreshold         = "RAY_LOG_MONITOR_MANY_FILES_THRESHOLD"
	envNumLinesToRead             = "RAY_LOG_MONITOR_NUM_LINES_TO_READ"
	envNameUpdateInterval         = "LOG_NAME_UPDATE_INTERVAL_S"
	envMemoryLimit                = "RAY_GO_LOG_MONITOR_MEMLIMIT"
	defaultMemoryLimit            = 64 << 20
	defaultGCSConnectTimeoutMilli = int64(5000)
	defaultGCSStartupRetryTimeout = 30 * time.Second
	defaultGCSStartupRetryDelay   = 250 * time.Millisecond
)

type LoggingConfig struct {
	Level       string
	Format      string
	Filename    string
	RotateBytes int64
	BackupCount int
}

type Options struct {
	SessionDir     string
	LogsDir        string
	GCSAddress     string
	ClusterIDHex   string
	NodeIPAddress  string
	StdoutFilepath string
	StderrFilepath string
	Logging        LoggingConfig
}

type monitorRunner interface {
	Run(context.Context) error
}

// gcsClient is the minimal GCS surface the log monitor needs: internal KV
// reads for autoscaler v2 detection, log batch publishing and lifecycle
// management. Implemented by the pure-Go gRPC client (go/internal/gcs/grpc).
type gcsClient interface {
	Get(ctx context.Context, ns, key string) ([]byte, error)
	PublishLogBatch(ctx context.Context, payload gcs.LogBatchPayload) error
	Close() error
}

type runnerDeps struct {
	loadNodeIPAddress func(string) (string, error)
	connectGCSClient  func(context.Context, gcs.ClientOptions) (gcsClient, error)
	retryTimeout      time.Duration
	retryDelay        time.Duration
	newMonitor        func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error)
}

var defaultRunnerDeps = runnerDeps{
	loadNodeIPAddress: defaultLoadNodeIPAddress,
	connectGCSClient: func(ctx context.Context, opts gcs.ClientOptions) (gcsClient, error) {
		return gcsgrpc.NewClient(ctx, opts)
	},
	retryTimeout: defaultGCSStartupRetryTimeout,
	retryDelay:   defaultGCSStartupRetryDelay,
	newMonitor: func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error) {
		return logmonitor.NewWithConfig(ip, opts.LogsDir, publisher, logmonitor.RuntimeConfig{
			RuntimeEnvToDriver:  runtimeEnvToDriver,
			AutoscalerV2Enabled: autoscalerV2,
			MaxLineSize:         maxLineSizeFromEnv(),
			MaxFilesOpen:        envPositiveInt(envMaxOpenFiles, logmonitor.DefaultMaxFilesOpen()),
			ManyFilesThreshold:  envPositiveInt(envManyFilesThreshold, logmonitor.DefaultManyFilesThreshold()),
			NumLinesToRead:      envPositiveInt(envNumLinesToRead, logmonitor.DefaultNumLinesToRead()),
			NameUpdateInterval:  envPositiveDuration(envNameUpdateInterval, logmonitor.DefaultNameUpdateInterval()),
		})
	},
}

// envPositiveInt reads key as a positive integer, falling back to defaultVal
// when the variable is unset or not a positive integer. A warning is written
// to stderr when the variable is set but invalid, so a misconfiguration (wrong
// name, unit, or value) surfaces instead of silently falling back to the
// default.
func envPositiveInt(key string, defaultVal int) int {
	v, ok := envPositive(key, defaultVal, func(k string) int {
		return common.EnvInteger(k, 0)
	})
	if ok {
		return v
	}
	return defaultVal
}

// envPositive parses key via parse and adopts the parsed value only when it is
// a positive finite number; otherwise a warning is written to stderr (naming
// defaultDisplay as the fallback) when the variable is set but invalid, and
// (value, false) is returned so callers can fall back precisely.
func envPositive[T int | float64](key string, defaultDisplay any, parse func(string) T) (T, bool) {
	v := parse(key)
	if isPositiveFinite(v) {
		return v, true
	}
	if _, ok := os.LookupEnv(key); ok {
		fmt.Fprintf(os.Stderr,
			"log_monitor: ignoring invalid %s (want a positive number); using default %v\n",
			key, defaultDisplay)
	}
	return v, false
}

// isPositiveFinite reports whether v is strictly positive and, for floating
// point values, finite (rejects +Inf/-Inf; NaN already fails the > 0 check).
func isPositiveFinite[T int | float64](v T) bool {
	switch t := any(v).(type) {
	case int:
		return t > 0
	case float64:
		return t > 0 && !math.IsInf(t, 0)
	}
	return false
}

func maxLineSizeFromEnv() int {
	return envPositiveInt(envMaxLineSize, logmonitor.DefaultMaxLineSize())
}

// envPositiveDuration reads key as a positive number of seconds, falling back
// to defaultVal when the variable is unset or not a positive number. A warning
// is written to stderr when the variable is set but invalid, so a
// misconfiguration (wrong name, unit, or value) surfaces instead of silently
// falling back to the default. The returned value is the duration equivalent
// of the value in seconds (0.5 -> 500ms).
func envPositiveDuration(key string, defaultVal time.Duration) time.Duration {
	seconds, ok := envPositive(key, defaultVal, func(k string) float64 {
		return common.EnvFloat(k, 0)
	})
	if !ok {
		return defaultVal
	}
	d := time.Duration(seconds * float64(time.Second))
	if d > 0 {
		return d
	}
	// The seconds value passed envPositive's checks but did not survive the
	// conversion: large values overflow int64 into a negative duration and
	// tiny values underflow to 0. Warn and fall back like the other knobs
	// instead of leaving a caller-side > 0 gate to restore the default
	// silently.
	fmt.Fprintf(os.Stderr,
		"log_monitor: ignoring invalid %s (want a positive number); using default %v\n",
		key, defaultVal)
	return defaultVal
}

// applyMemoryLimit sets the Go soft memory limit for this process. An explicit
// RAY_GO_LOG_MONITOR_MEMLIMIT always wins. Otherwise a process-level GOMEMLIMIT
// is honored as-is so a cluster-wide memory policy is not silently overridden;
// only when neither is set is the built-in default applied.
func applyMemoryLimit() int64 {
	if v := common.EnvInteger(envMemoryLimit, 0); v > 0 {
		limit := int64(v)
		debug.SetMemoryLimit(limit)
		return limit
	}
	if limit, ok := parseGOMEMLIMIT(os.Getenv("GOMEMLIMIT")); ok {
		return limit
	}
	limit := int64(defaultMemoryLimit)
	debug.SetMemoryLimit(limit)
	return limit
}

// parseGOMEMLIMIT parses a Go runtime GOMEMLIMIT value following the runtime's
// parseByteCount rules (an integer byte count with an optional binary unit
// suffix such as "B", "KiB", "MiB", "GiB" or "TiB", matching the regex
// ^[0-9]+(([KMGT]i)?B)?$) plus the "off" sentinel that disables the limit.
// It reports (MaxInt64, true) for "off" and false for empty, "-1" or otherwise
// invalid values so the caller can fall back to its own default. Unlike the
// runtime (which aborts on malformed values at startup), values the runtime
// rejects simply fall back to the default here.
func parseGOMEMLIMIT(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-1" {
		return 0, false
	}
	if s == "off" {
		return math.MaxInt64, true
	}
	i := 0
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
	}
	if i == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	var mult int64 = 1
	switch s[i:] {
	case "", "B":
		mult = 1
	case "KiB":
		mult = 1 << 10
	case "MiB":
		mult = 1 << 20
	case "GiB":
		mult = 1 << 30
	case "TiB":
		mult = 1 << 40
	default:
		return 0, false
	}
	if n > math.MaxInt64/mult {
		return 0, false
	}
	limit := n * mult
	if limit <= 0 {
		return 0, false
	}
	return limit, true
}

func runLogMonitor(opts *Options) error {
	limit := applyMemoryLimit()
	fmt.Fprintf(os.Stderr, "log_monitor: effective soft memory limit %d bytes\n", limit)
	return runLogMonitorWithDeps(opts, defaultRunnerDeps)
}

func runLogMonitorWithDeps(opts *Options, deps runnerDeps) error {
	if err := setupLogger(opts); err != nil {
		return err
	}
	var err error
	nodeIP := opts.NodeIPAddress
	if nodeIP == "" {
		nodeIP, err = deps.loadNodeIPAddress(opts.SessionDir)
		if err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clusterID, err := ids.ClusterIDFromHex(opts.ClusterIDHex)
	if err != nil {
		return fmt.Errorf("parse cluster-id-hex: %w", err)
	}

	client, err := connectGCSClientWithRetry(ctx, deps, gcs.ClientOptions{
		Address:   opts.GCSAddress,
		ClusterID: clusterID,
		TimeoutMs: defaultGCSConnectTimeoutMilli,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	autoscalerV2 := false
	if v, ok := autoscalerV2FromEnv(); ok && v {
		autoscalerV2 = true
	} else {
		autoscalerV2, err = detectAutoscalerV2EnabledWithRetry(ctx, deps, func(ctx context.Context) (bool, error) {
			return detectAutoscalerV2Enabled(ctx, client)
		})
		if err != nil {
			return fmt.Errorf("detect autoscaler v2: %w", err)
		}
	}

	monitor, err := deps.newMonitor(
		nodeIP,
		opts,
		logmonitor.NewRuntimePublisher(ctx, client),
		autoscalerV2,
		os.Getenv(runtimeEnvLogToDriverEnvVar) == "1",
	)
	if err != nil {
		return fmt.Errorf("create log monitor: %w", err)
	}

	if err := monitor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func GetLogMonitorCmd() *cobra.Command {
	return newLogMonitorCmd(runLogMonitor)
}

func newLogMonitorCmd(run func(*Options) error) *cobra.Command {
	opts := &Options{}
	cmd := &cobra.Command{
		Use:   "log-monitor",
		Short: "Run the Go log monitor",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateOptions(opts); err != nil {
				return err
			}
			return run(opts)
		},
	}
	cmd.Flags().StringVar(&opts.SessionDir, "session-dir", "", "Ray session directory")
	cmd.Flags().StringVar(&opts.LogsDir, "logs-dir", "", "Ray logs directory")
	cmd.Flags().StringVar(&opts.GCSAddress, "gcs-address", "", "GCS address")
	cmd.Flags().StringVar(&opts.ClusterIDHex, "cluster-id-hex", "", "Cluster ID in hex.")
	cmd.Flags().StringVar(&opts.NodeIPAddress, "node-ip-address", "", "Node IP address.")
	cmd.Flags().StringVar(&opts.Logging.Level, "logging-level", "info", "Logging level.")
	cmd.Flags().StringVar(&opts.Logging.Format, "logging-format", "", "Logging format.")
	cmd.Flags().StringVar(&opts.Logging.Filename, "logging-filename", "log_monitor.log", "Log filename")
	cmd.Flags().Int64Var(&opts.Logging.RotateBytes, "logging-rotate-bytes", 0, "Log rotation bytes")
	cmd.Flags().IntVar(&opts.Logging.BackupCount, "logging-rotate-backup-count", 0, "Log backup count")
	cmd.Flags().StringVar(&opts.StdoutFilepath, "stdout-filepath", "", "stdout file")
	cmd.Flags().StringVar(&opts.StderrFilepath, "stderr-filepath", "", "stderr file")
	return cmd
}

func validateOptions(opts *Options) error {
	switch {
	case opts.SessionDir == "":
		return fmt.Errorf("session-dir must not be empty")
	case opts.LogsDir == "":
		return fmt.Errorf("logs-dir must not be empty")
	case opts.GCSAddress == "":
		return fmt.Errorf("gcs-address must not be empty")
	case opts.ClusterIDHex == "":
		return fmt.Errorf("cluster-id-hex must not be empty")
	default:
		return nil
	}
}

func setupLogger(opts *Options) error {
	level, err := logconfig.ParseLogLevel(opts.Logging.Level)
	if err != nil {
		return err
	}

	outputPaths, errorOutputPaths := loggerOutputPaths(opts)
	loggerOpts := []zap.Option{
		zap.WithLevel(level),
		zap.WithDevelopment(opts.Logging.Filename == ""),
	}
	if len(outputPaths) > 0 {
		loggerOpts = append(loggerOpts,
			zap.WithOutputPaths(outputPaths...),
			zap.WithErrorOutputPaths(errorOutputPaths...),
		)
		if rotation := rotationOptions(opts.Logging.RotateBytes, opts.Logging.BackupCount); rotation != nil {
			loggerOpts = append(loggerOpts, zap.WithRotation(rotation))
		}
	}
	if err := zap.SetupDefaultLogger(loggerOpts...); err != nil {
		return err
	}
	return redirectProcessStreams(opts)
}

func loggerOutputPaths(opts *Options) ([]string, []string) {
	outputPaths := make([]string, 0, 1)
	errorOutputPaths := make([]string, 0, 1)
	if opts.Logging.Filename != "" {
		logPath := resolveLogPath(opts.LogsDir, opts.Logging.Filename)
		outputPaths = append(outputPaths, logPath)
		errorOutputPaths = append(errorOutputPaths, logPath)
	}
	return outputPaths, errorOutputPaths
}

func redirectProcessStreams(opts *Options) error {
	if err := redirectProcessStream(opts.LogsDir, opts.StdoutFilepath, syscall.Stdout, "/dev/stdout"); err != nil {
		return err
	}
	return redirectProcessStream(opts.LogsDir, opts.StderrFilepath, syscall.Stderr, "/dev/stderr")
}

func redirectProcessStream(logsDir, path string, targetFD int, streamName string) error {
	if path == "" {
		return nil
	}

	resolvedPath := resolveLogPath(logsDir, path)
	streamFile, err := os.OpenFile(resolvedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", resolvedPath, err)
	}
	defer streamFile.Close()

	if err := syscall.Dup2(int(streamFile.Fd()), targetFD); err != nil {
		return fmt.Errorf("redirect %s to %s: %w", streamName, resolvedPath, err)
	}

	switch targetFD {
	case syscall.Stdout:
		os.Stdout = os.NewFile(uintptr(syscall.Stdout), streamName)
	case syscall.Stderr:
		os.Stderr = os.NewFile(uintptr(syscall.Stderr), streamName)
	}
	return nil
}

func resolveLogPath(logsDir, path string) string {
	if filepath.IsAbs(path) || logsDir == "" {
		return path
	}
	return filepath.Join(logsDir, path)
}

func rotationOptions(maxBytes int64, backupCount int) *lj.Options {
	if maxBytes <= 0 {
		return nil
	}
	maxSizeMB := int((maxBytes + (1 << 20) - 1) / (1 << 20))
	if maxSizeMB < 1 {
		maxSizeMB = 1
	}
	return &lj.Options{
		MaxSize:    maxSizeMB,
		MaxBackups: backupCount,
	}
}

func defaultLoadNodeIPAddress(sessionDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(sessionDir, nodeIPFilename))
	if err != nil {
		return "", fmt.Errorf("read cached node ip: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return "127.0.0.1", nil
	}

	var payload struct {
		NodeIPAddress string `json:"node_ip_address"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("parse cached node ip: %w", err)
	}
	if payload.NodeIPAddress == "" {
		return "127.0.0.1", nil
	}
	return payload.NodeIPAddress, nil
}

// errGCSClientConfig marks a connectGCSClient failure caused by local
// misconfiguration (for example invalid TLS settings) rather than transient
// GCS unavailability. connectGCSClientWithRetry reports such an error at once
// instead of retrying: waiting cannot fix a bad configuration.
var errGCSClientConfig = errors.New("gcs client configuration error")

// permanentError marks a failure that retrying cannot fix. retryWithTimeout
// returns it immediately instead of retrying until the deadline.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }

func (e permanentError) Unwrap() error { return e.err }

func retryWithTimeout[T any](
	ctx context.Context,
	timeout time.Duration,
	delay time.Duration,
	op func(context.Context) (T, error),
) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		value, err := op(ctx)
		if err == nil {
			return value, nil
		}
		var permanent permanentError
		if errors.As(err, &permanent) {
			return zero, permanent.err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return zero, lastErr
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
}

func connectGCSClientWithRetry(ctx context.Context, deps runnerDeps, opts gcs.ClientOptions) (gcsClient, error) {
	// Resolve TLS/token options once, outside the retry loop, so the token and
	// certificate files are not re-read on every attempt (each client
	// construction would otherwise re-run DialOptionsFromEnv). A resolution
	// failure is a configuration error, not a transient GCS unavailability, so
	// it is not retried.
	dialOpts, err := gcs.DialOptionsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("connect gcs client: %w: %w", errGCSClientConfig, err)
	}
	opts.DialOptions = &dialOpts
	// The pure-Go gRPC client dials via gcs.DialGCS: when ClientOptions.TimeoutMs
	// is set it waits for the channel to become Ready within that bound, so a
	// GCS that is still starting up fails this call and the retry loop retries
	// until deps.retryTimeout elapses. With a zero timeout the dial is
	// non-blocking and the autoscaler-v2 detection below becomes the effective
	// readiness gate.
	client, err := retryWithTimeout(ctx, deps.retryTimeout, deps.retryDelay, func(ctx context.Context) (gcsClient, error) {
		client, err := deps.connectGCSClient(ctx, opts)
		if err != nil && errors.Is(err, errGCSClientConfig) {
			return nil, permanentError{err}
		}
		return client, err
	})
	if err != nil {
		return nil, fmt.Errorf("connect gcs client: %w", err)
	}
	return client, nil
}

func detectAutoscalerV2EnabledWithRetry(ctx context.Context, deps runnerDeps, detect func(context.Context) (bool, error)) (bool, error) {
	enabled, err := retryWithTimeout(ctx, deps.retryTimeout, deps.retryDelay, func(ctx context.Context) (bool, error) {
		enabled, err := detect(ctx)
		if errors.Is(err, gcs.ErrKeyNotFound) {
			return false, nil
		}
		return enabled, err
	})
	if err != nil {
		if errors.Is(err, gcs.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return enabled, nil
}

// autoscalerV2FromEnv reads RAY_enable_autoscaler_v2 (aligned with Python
// ray._config.enable_autoscaler_v2() and the C++ RayConfig bool semantics:
// "true"/"1" mean enabled). The second return value reports whether the
// environment variable is set at all.
func autoscalerV2FromEnv() (bool, bool) {
	v, ok := os.LookupEnv(autoscalerV2EnabledEnvVar)
	if !ok {
		return false, false
	}
	lv := strings.ToLower(strings.TrimSpace(v))
	return lv == "true" || lv == "1", true
}

func detectAutoscalerV2Enabled(ctx context.Context, client gcsClient) (bool, error) {
	value, err := client.Get(ctx, autoscalerStateNamespace, autoscalerV2EnabledKey)
	if err != nil {
		if errors.Is(err, gcs.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return string(value) == "1", nil
}
