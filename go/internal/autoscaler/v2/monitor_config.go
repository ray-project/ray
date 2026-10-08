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

package v2

import (
	"fmt"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/spf13/cobra"
)

// MonitorV2Config holds the configuration parameters of the Monitor V2 process.
type MonitorV2Config struct {
	// GcsAddress is the GCS server address in "ip:port" form (required).
	GcsAddress string

	// AutoscalingConfig is the autoscaling config file path (optional).
	AutoscalingConfig string

	// LoggingLevel is the log level (optional, default "info").
	LoggingLevel string

	// LoggingFormat is the log format (optional).
	LoggingFormat string

	// LoggingFilename is the log file name (optional, default "monitor.log").
	LoggingFilename string

	// LogsDir is the log directory (required).
	LogsDir string

	// LoggingRotateBytes is the log rotation size (optional, default 10MB).
	LoggingRotateBytes int

	// LoggingRotateBackupCount is the number of log backups (optional, default 5).
	LoggingRotateBackupCount int

	// MonitorIP is the IP address of the Monitor process (optional).
	MonitorIP string

	// StdoutFilepath is the stdout output file path (optional).
	StdoutFilepath string

	// StderrFilepath is the stderr output file path (optional).
	StderrFilepath string
}

// NewMonitorV2Config creates and validates a MonitorV2Config from command line flags.
func NewMonitorV2Config(cmd *cobra.Command) (*MonitorV2Config, error) {
	log.Log.Info("NewMonitorV2Config", "ip", "NewMonitorV2Config value")
	if cmd == nil {
		return nil, fmt.Errorf("cmd is nil")
	}

	config := &MonitorV2Config{}

	// Parse the GCS address (optional).
	gcsAddress, err := cmd.Flags().GetString(constant.FlagsGcsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsGcsAddress, err)
	}
	if gcsAddress != "" {
		ip, port, err := common.ValidateHostPort(gcsAddress)
		if err != nil {
			return nil, fmt.Errorf("invalid GCS address '%s': %w", gcsAddress, err)
		}
		config.GcsAddress = fmt.Sprintf("%s:%d", ip, port)
		log.Log.Info("GCS address parsed", "ip", ip, "port", port)
	}

	// Parse the autoscaling config file path.
	autoscalingConfig, err := cmd.Flags().GetString(constant.FlagsAutoscalingConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsAutoscalingConfig, err)
	}
	config.AutoscalingConfig = autoscalingConfig
	if autoscalingConfig != "" {
		log.Log.Info("Autoscaling config file", "path", autoscalingConfig)
	}

	// Parse the log level.
	loggingLevel, err := cmd.Flags().GetString(constant.FlagsLoggingLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLoggingLevel, err)
	}
	if loggingLevel == "" {
		loggingLevel = common.LoggerLevel
	}
	// Validate the log level.
	if !isValidLoggingLevel(loggingLevel) {
		return nil, fmt.Errorf("invalid logging level '%s', must be one of: %v", loggingLevel, common.LoggerLevelChoices)
	}
	config.LoggingLevel = loggingLevel

	// Parse the log format.
	loggingFormat, err := cmd.Flags().GetString(constant.FlagsLoggingFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLoggingFormat, err)
	}
	if loggingFormat == "" {
		loggingFormat = common.LoggerFormat
	}
	config.LoggingFormat = loggingFormat

	// Parse the log file name.
	loggingFilename, err := cmd.Flags().GetString(constant.FlagsLoggingFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLoggingFilename, err)
	}
	if loggingFilename == "" {
		loggingFilename = common.MONITOR_LOG_FILE_NAME
	}
	config.LoggingFilename = loggingFilename

	// Parse the log directory (required).
	logsDir, err := cmd.Flags().GetString(constant.FlagsLogsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLogsDir, err)
	}
	if logsDir == "" {
		return nil, fmt.Errorf("--%s must be set", constant.FlagsLogsDir)
	}
	config.LogsDir = logsDir

	// Parse the log rotation size.
	loggingRotateBytes, err := cmd.Flags().GetInt(constant.FlagsLoggingRotateBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLoggingRotateBytes, err)
	}
	if loggingRotateBytes <= 0 {
		loggingRotateBytes = common.MonitorLogRotateBytes()
	}
	config.LoggingRotateBytes = loggingRotateBytes

	// Parse the number of log backups.
	loggingRotateBackupCount, err := cmd.Flags().GetInt(constant.FlagsLoggingRotateBackupCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsLoggingRotateBackupCount, err)
	}
	if loggingRotateBackupCount < 0 {
		loggingRotateBackupCount = common.MonitorLogRotateBackupCount()
	}
	config.LoggingRotateBackupCount = loggingRotateBackupCount

	// Parse the Monitor IP.
	monitorIP, err := cmd.Flags().GetString(constant.FlagsMonitorIP)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsMonitorIP, err)
	}
	config.MonitorIP = monitorIP
	if monitorIP != "" {
		log.Log.Info("Monitor IP", "ip", monitorIP)
	}

	// Parse the stdout file path.
	stdoutFilepath, err := cmd.Flags().GetString(constant.FlagsStdoutFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsStdoutFilepath, err)
	}
	config.StdoutFilepath = stdoutFilepath

	// Parse the stderr file path.
	stderrFilepath, err := cmd.Flags().GetString(constant.FlagsStderrFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get flag '%s': %w", constant.FlagsStderrFilepath, err)
	}
	config.StderrFilepath = stderrFilepath

	return config, nil
}

// isValidLoggingLevel reports whether the log level is valid.
func isValidLoggingLevel(level string) bool {
	for _, valid := range common.LoggerLevelChoices {
		if level == valid {
			return true
		}
	}
	return false
}

// String returns the string representation of the config.
func (c *MonitorV2Config) String() string {
	return fmt.Sprintf("MonitorV2Config{GcsAddress: %s, AutoscalingConfig: %s, LogsDir: %s, LoggingLevel: %s, MonitorIP: %s}",
		c.GcsAddress, c.AutoscalingConfig, c.LogsDir, c.LoggingLevel, c.MonitorIP)
}
