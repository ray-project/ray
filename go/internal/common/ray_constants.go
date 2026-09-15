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
	"os"
	"runtime"
	"strconv"
)

// EnvInteger reads an integer from an environment variable, returning the
// default value if the variable is absent or cannot be parsed.
func EnvInteger(key string, defaultVal int) int {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}
	intVal, err := strconv.Atoi(value)
	if err != nil {
		return defaultVal
	}
	return intVal
}

// EnvFloat reads a float from an environment variable, returning the default
// value if the variable is absent or cannot be parsed.
func EnvFloat(key string, defaultVal float64) float64 {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}
	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return defaultVal
	}
	return floatVal
}

// EnvBool reads a boolean from an environment variable, treating "true" and
// "1" as true and everything else as false.
func EnvBool(key string, defaultVal bool) bool {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}
	return value == "true" || value == "1"
}

func EnvSetByUser(key string) bool {
	_, exists := os.LookupEnv(key)
	return exists
}

// EnvString reads a string from an environment variable, returning the default
// value if the variable is absent.
func EnvString(key string, defaultVal string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}
	return value
}

// Log level configuration (aligned with Python ray_constants.LOGGER_LEVEL).
var (
	// LoggerLevel is the default log level (corresponds to Python:
	// os.environ.get("RAY_LOGGER_LEVEL", "info")).
	LoggerLevel = EnvString("RAY_LOGGER_LEVEL", "info")
	// LoggerLevelChoices is the list of selectable log levels.
	LoggerLevelChoices = []string{"debug", "info", "warning", "error", "critical"}
)

const (
	// DefaultRuntimeEnvTimeoutSeconds is the default runtime env timeout (seconds).
	DefaultRuntimeEnvTimeoutSeconds = 600

	// Keep in sync with max_grpc_message_size in ray_config_def.h.
	GRPC_CPP_MAX_MESSAGE_SIZE = 250 * 1024 * 1024
)

// Log format (aligned with Python ray_constants.LOGGER_FORMAT).
const (
	// LoggerFormat is the default log format.
	LoggerFormat     = "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
	LoggerFormatHelp = "The logging format."
)

const (
	MONITOR_LOG_FILE_NAME = "monitor.log"

	// DefaultLoggingDevelopment is the default development/production log mode.
	DefaultLoggingDevelopment = true
	// Default log rotation size (aligned with Python LOGGING_ROTATE_BYTES).
	LOGGING_ROTATE_BYTES = 512 * 1024 * 1024 // 512MB
	// Default number of log rotation backups.
	LOGGING_ROTATE_BACKUP_COUNT = 5

	REDIS_DEFAULT_USERNAME = ""
	REDIS_DEFAULT_PASSWORD = ""

	DEFAULT_DASHBOARD_IP                = "127.0.0.1"
	DEFAULT_DASHBOARD_PORT              = 8265
	DASHBOARD_ADDRESS                   = "dashboard"
	DASHBOARD_CLIENT_MAX_SIZE           = 100 * 1024 * 1024
	PROMETHEUS_SERVICE_DISCOVERY_FILE   = "prom_metrics_service_discovery.json"
	DEFAULT_DASHBOARD_AGENT_LISTEN_PORT = 52365
)

const (
	// IS_WINDOWS_OR_OSX indicates whether the running platform is Windows or macOS.
	// Note: this constant is always false in Go because it is meant for Python code;
	// Go should detect the platform at runtime via runtime.GOOS.
	IS_WINDOWS_OR_OSX           = false
	ENABLE_RAY_CLUSTERS_ENV_VAR = "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER"
)

func IsWindowsOrOSX() bool {
	return runtime.GOOS == "darwin" || runtime.GOOS == "windows"
}

// EnableRayCluster reports whether Ray cluster mode is enabled.
// By default, cluster mode is not enabled on Windows and macOS unless overridden
// via an environment variable.
func EnableRayCluster() bool {
	defaultValue := !IsWindowsOrOSX()
	return EnvBool(ENABLE_RAY_CLUSTERS_ENV_VAR, defaultValue)
}

// MonitorLogRotateBytes returns the log rotation size, overridable via an
// environment variable.
func MonitorLogRotateBytes() int {
	return EnvInteger("RAY_MONITOR_LOG_ROTATE_BYTES", LOGGING_ROTATE_BYTES)
}

// MonitorLogRotateBackupCount returns the number of log rotation backups,
// overridable via an environment variable.
func MonitorLogRotateBackupCount() int {
	return EnvInteger("RAY_MONITOR_LOG_ROTATE_BACKUP_COUNT", LOGGING_ROTATE_BACKUP_COUNT)
}
