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

// Package constant holds the constants of the Ray Monitor V2 process.
package constant

import (
	"context"
	"math"
	"os"
	"strconv"

	"github.com/ray-project/ray/go/pkg/log"
)

// Command-line flag name constants (mirroring the argparse arguments of the
// Python monitor.py).
const (
	// GCS connection flags.
	FlagsGcsAddress = "gcs-address"

	// Autoscaling configuration flags.
	FlagsAutoscalingConfig = "autoscaling-config"

	// Logging configuration flags.
	FlagsLoggingLevel             = "logging-level"
	FlagsLoggingFormat            = "logging-format"
	FlagsLoggingFilename          = "logging-filename"
	FlagsLogsDir                  = "logs-dir"
	FlagsLoggingRotateBytes       = "logging-rotate-bytes"
	FlagsLoggingRotateBackupCount = "logging-rotate-backup-count"

	// Monitor configuration flags.
	FlagsMonitorIP = "monitor-ip"

	// Output redirection flags.
	FlagsStdoutFilepath = "stdout-filepath"
	FlagsStderrFilepath = "stderr-filepath"

	DISABLE_LAUNCH_CONFIG_CHECK_KEY = "disable_launch_config_check"

	// KV_NAMESPACE_SESSION is the GCS internal KV namespace under which the
	// head node stores the session name. Mirrors Python ray_constants.KV_NAMESPACE_SESSION.
	KV_NAMESPACE_SESSION = "session"
)

// AUTOSCALER_UPDATE_INTERVAL_S is the interval (seconds) at which autoscaling
// updates are performed. Overridable via the AUTOSCALER_UPDATE_INTERVAL_S env
// var. Mirrors Python constants.AUTOSCALER_UPDATE_INTERVAL_S; read once at
// process start (like the Python module-level constant).
var AUTOSCALER_UPDATE_INTERVAL_S = envInteger("AUTOSCALER_UPDATE_INTERVAL_S", 5)

// AUTOSCALER_METRIC_PORT is the port the autoscaler Prometheus metrics are
// exported to. Overridable via the AUTOSCALER_METRIC_PORT env var.
// Mirrors Python constants.AUTOSCALER_METRIC_PORT.
var AUTOSCALER_METRIC_PORT = envInteger("AUTOSCALER_METRIC_PORT", 44217)

// envInteger reads an integer from the env var key, falling back to def when
// the var is unset or not a valid integer. Mirrors Python
// ray.autoscaler._private.constants.env_integer, except that an invalid value
// logs a warning and uses the default instead of crashing the process.
func envInteger(key string, def int) int {
	val, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	if val == "inf" {
		return math.MaxInt
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		log.Log.V(1).Info("Invalid integer env var, using default",
			"key", key, "value", val, "default", def)
		return def
	}
	return n
}

// BASE_READONLY_CONFIG is the base configuration of the read-only provider.
var BASE_READONLY_CONFIG = map[string]interface{}{
	"cluster_name":         "default",
	"max_workers":          0,
	"upscaling_speed":      1.0,
	"docker":               make(map[string]interface{}),
	"idle_timeout_minutes": 0,
	"provider": map[string]interface{}{
		"type":                          "readonly",
		"use_node_id_as_ip":             true,
		DISABLE_LAUNCH_CONFIG_CHECK_KEY: true,
	},
	"auth": make(map[string]interface{}),
	"available_node_types": map[string]interface{}{
		"ray.head.default": map[string]interface{}{
			"resources":   make(map[string]float64),
			"max_workers": 0,
			"node_config": make(map[string]interface{}),
		},
	},
	"head_node_type":                "ray.head.default",
	"file_mounts":                   make(map[string]string),
	"cluster_synced_files":          []string{},
	"file_mounts_sync_continuously": false,
	"rsync_exclude":                 []string{},
	"rsync_filter":                  []string{},
	"initialization_commands":       []string{},
	"setup_commands":                []string{},
	"head_setup_commands":           []string{},
	"worker_setup_commands":         []string{},
	"head_start_ray_commands":       []string{},
	"worker_start_ray_commands":     []string{},
}

var Ctx = context.Background()
