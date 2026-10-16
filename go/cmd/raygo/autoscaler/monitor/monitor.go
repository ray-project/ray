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

// Package monitor provides the command line interface of the Ray Monitor V2
// process. The monitor is the core monitoring component of the Ray autoscaling
// system; it runs on the head node as a daemon.
package monitor

import (
	"fmt"
	"os"

	v2 "github.com/ray-project/ray/go/internal/autoscaler/v2"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/logconfig"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
	"github.com/spf13/cobra"
)

// MonitorCmd represents the monitor subcommand.
var MonitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "Start a Ray autoscaler monitor process",
	Long: `Start a Ray autoscaler monitor process for Go language runtime.

The monitor process runs on the head node and periodically collects cluster
resource usage data from GCS (Global Control Service), triggers autoscaling
updates, and exports Prometheus metrics.

Usage:
  raygo monitor [flags]

Examples:
  raygo monitor --gcs-address=192.168.1.100:6379 \
                --logs-dir=/tmp/ray/logs \
                --monitor-ip=192.168.1.100`,
	RunE: runMonitor,
}

func init() {
	// GCS address flag (optional).
	MonitorCmd.Flags().String(constant.FlagsGcsAddress, "", "The address (ip:port) of GCS server (--gcs-address).")
	// Autoscaling config flag (optional).
	MonitorCmd.Flags().String(constant.FlagsAutoscalingConfig, "", "The path to the autoscaling config file (--autoscaling-config).")

	// Logging flags.
	MonitorCmd.Flags().String(constant.FlagsLoggingLevel, common.LoggerLevel,
		fmt.Sprintf("Logging level (--logging-level). Choices: %v", common.LoggerLevelChoices))
	MonitorCmd.Flags().String(constant.FlagsLoggingFormat, common.LoggerFormat,
		"The logging format (--logging-format).")
	MonitorCmd.Flags().String(constant.FlagsLoggingFilename, common.MONITOR_LOG_FILE_NAME,
		fmt.Sprintf("Specify the name of log file, log to stdout if set empty (--logging-filename). Default is %q", common.MONITOR_LOG_FILE_NAME))
	MonitorCmd.Flags().String(constant.FlagsLogsDir, "",
		"Specify the path of the logs directory used by Ray processes (--logs-dir).")
	MonitorCmd.Flags().Int(constant.FlagsLoggingRotateBytes, logconfig.LOGGING_ROTATE_BYTES,
		fmt.Sprintf("Specify the max bytes for rotating log file (--logging-rotate-bytes). Default is %d bytes.", logconfig.LOGGING_ROTATE_BYTES))
	MonitorCmd.Flags().Int(constant.FlagsLoggingRotateBackupCount, logconfig.LOGGING_ROTATE_BACKUP_COUNT,
		fmt.Sprintf("Specify the backup count of rotated log file (--logging-rotate-backup-count). Default is %d.", logconfig.LOGGING_ROTATE_BACKUP_COUNT))

	// Monitor flags (optional).
	MonitorCmd.Flags().String(constant.FlagsMonitorIP, "",
		"The IP address of the machine hosting the monitor process, used for exposing Prometheus metrics (--monitor-ip).")

	// Output redirection flags (optional).
	MonitorCmd.Flags().String(constant.FlagsStdoutFilepath, "",
		"The filepath to dump monitor stdout (--stdout-filepath).")
	MonitorCmd.Flags().String(constant.FlagsStderrFilepath, "",
		"The filepath to dump monitor stderr (--stderr-filepath).")

	// Mark the required parameters.
	if err := MonitorCmd.MarkFlagRequired(constant.FlagsGcsAddress); err != nil {
		log.Log.Error(err, "failed to mark flag as required", "flag", constant.FlagsGcsAddress)
	}
	if err := MonitorCmd.MarkFlagRequired(constant.FlagsLogsDir); err != nil {
		log.Log.Error(err, "failed to mark flag as required", "flag", constant.FlagsLogsDir)
	}
}

// main is the entry point of the monitor executable.
func main() {
	if err := MonitorCmd.Execute(); err != nil {
		log.Log.Error(err, "failed to execute monitor command")
		os.Exit(1)
	}
}

// GetMonitorCmd returns the monitor subcommand object.
// It is used to embed the monitor command in other modules.
func GetMonitorCmd() *cobra.Command {
	return MonitorCmd
}

// monitorPanicError converts a recovered panic into the error returned by
// runMonitor so the process exits non-zero, matching the Python monitor's
// non-zero traceback exit. Swallowing the panic would make a dead autoscaler
// look like a clean stop.
func monitorPanicError(r interface{}) error {
	v2.CustomPanicHook(r)
	return fmt.Errorf("monitor panicked: %v", r)
}

// runMonitor runs the monitor command.
func runMonitor(cmd *cobra.Command, args []string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = monitorPanicError(r)
		}
	}()

	// Create and validate the MonitorConfig from the command line flags.
	config, err := v2.NewMonitorV2Config(cmd)
	if err != nil {
		v2.CustomErrorHook(err)
		return fmt.Errorf("failed to parse monitor config: %w", err)
	}

	// Set up the logging system.
	if err = logconfig.NewLogOption(
		logconfig.WithLoggingLevel(config.LoggingLevel),
		logconfig.WithLoggingFormat(config.LoggingFormat),
		logconfig.WithLoggingFilename(config.LoggingFilename),
		logconfig.WithLogsDir(config.LogsDir),
		logconfig.WithLoggingRotateBytes(config.LoggingRotateBytes),
		logconfig.WithLoggingRotateBackupCount(config.LoggingRotateBackupCount),
		logconfig.WithStdoutFilepath(config.StdoutFilepath),
		logconfig.WithStderrFilepath(config.StderrFilepath),
	).SetupComponentLogger(); err != nil {
		v2.CustomErrorHook(err)
		return err
	}

	// Log the startup information.
	log.Log.Info("Starting Ray monitor",
		"gcs_address", config.GcsAddress,
		"autoscaling_config", config.AutoscalingConfig,
		"log_dir", config.LogsDir,
		"monitor_ip", config.MonitorIP)

	// Fail fast on deployment errors (missing/invalid go_runtime.so).
	if err := plugin.CheckPluginLoadable(); err != nil {
		v2.CustomErrorHook(err)
		return err
	}

	// Build the GCS client through the plugin export surface and reuse the same
	// instance for the config reader and the monitor (one-process-one-client).
	gcsClient, err := plugin.LoadGCSClient(gcs.ClientOptions{
		Address:   config.GcsAddress,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 10000,
	})
	if err != nil {
		v2.CustomErrorHook(err)
		return fmt.Errorf("failed to connect to GCS client: %w", err)
	}
	defer gcsClient.Close()

	// Create the config reader and validate it.
	configReader, err := v2.CreateConfigReader(config, gcsClient)
	if err != nil {
		v2.CustomErrorHook(err)
		return err
	}

	monitor, err := v2.NewAutoscalerMonitor(
		config.GcsAddress,
		configReader,
		config.LogsDir,
		config.MonitorIP,
		gcsClient,
	)
	if err != nil {
		v2.CustomErrorHook(err)
		return err
	}

	if err = monitor.Run(); err != nil {
		v2.CustomErrorHook(err)
		return err
	}
	return nil
}
