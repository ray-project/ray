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
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ray-project/ray/go/internal/common"
	nativegcs "github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/logmonitor"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	lj "github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/ray-project/ray/go/pkg/log/zap"
)

const (
	nodeIPFilename                = "node_ip_address.json"
	autoscalerStateNamespace      = "__autoscaler"
	autoscalerV2EnabledKey        = "__autoscaler_v2_enabled"
	runtimeEnvLogToDriverEnvVar   = "RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED"
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

type runnerDeps struct {
	loadNodeIPAddress func(string) (string, error)
	connectGCSClient  func(gcs.ClientOptions) (gcs.Client, error)
	retryTimeout      time.Duration
	retryDelay        time.Duration
	newPublishClient  func(gcs.Client) (*nativegcs.LogBatchPublisher, error)
	newMonitor        func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error)
}

var defaultRunnerDeps = runnerDeps{
	loadNodeIPAddress: defaultLoadNodeIPAddress,
	connectGCSClient: func(opts gcs.ClientOptions) (gcs.Client, error) {
		return nativegcs.ConnectClient(opts)
	},
	retryTimeout: defaultGCSStartupRetryTimeout,
	retryDelay:   defaultGCSStartupRetryDelay,
	newPublishClient: func(client gcs.Client) (*nativegcs.LogBatchPublisher, error) {
		return nativegcs.NewLogBatchPublisher(client)
	},
	newMonitor: func(ip string, opts *Options, publisher logmonitor.Publisher, autoscalerV2 bool, runtimeEnvToDriver bool) (monitorRunner, error) {
		return logmonitor.NewWithConfig(ip, opts.LogsDir, publisher, logmonitor.RuntimeConfig{
			RuntimeEnvToDriver:  runtimeEnvToDriver,
			AutoscalerV2Enabled: autoscalerV2,
		})
	},
}

func runLogMonitor(opts *Options) error {
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

	publishClient, err := deps.newPublishClient(client)
	if err != nil {
		return fmt.Errorf("create log publish client: %w", err)
	}

	autoscalerV2, err := detectAutoscalerV2EnabledWithRetry(ctx, deps, func(ctx context.Context) (bool, error) {
		return detectAutoscalerV2Enabled(ctx, client)
	})
	if err != nil {
		return fmt.Errorf("detect autoscaler v2: %w", err)
	}

	monitor, err := deps.newMonitor(
		nodeIP,
		opts,
		logmonitor.NewRuntimePublisher(ctx, publishClient),
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
	level, err := common.ParseLogLevel(opts.Logging.Level)
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

func connectGCSClientWithRetry(ctx context.Context, deps runnerDeps, opts gcs.ClientOptions) (gcs.Client, error) {
	client, err := retryWithTimeout(ctx, deps.retryTimeout, deps.retryDelay, func(context.Context) (gcs.Client, error) {
		return deps.connectGCSClient(opts)
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

func detectAutoscalerV2Enabled(ctx context.Context, client gcs.Client) (bool, error) {
	value, err := client.Get(ctx, autoscalerStateNamespace, autoscalerV2EnabledKey)
	if err != nil {
		if errors.Is(err, gcs.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return string(value) == "1", nil
}
