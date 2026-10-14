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

package event

import (
	"os"
	"time"

	"github.com/ray-project/ray/go/internal/common"
)

// This file mirrors python/ray/dashboard/modules/event/event_consts.py.
// Environment overridable values keep the same env var names and defaults.

const (
	// logErrorEventStringLengthLimit mirrors
	// LOG_ERROR_EVENT_STRING_LENGTH_LIMIT: chars retained when logging events
	// that failed to report.
	logErrorEventStringLengthLimit = 1000

	// scanEventStartOffsetSeconds mirrors SCAN_EVENT_START_OFFSET_SECONDS:
	// only files modified after (now - 30 minutes) are monitored.
	scanEventStartOffsetSeconds = -30 * 60

	// concurrentReadLimit mirrors CONCURRENT_READ_LIMIT: max event files read
	// concurrently per scan.
	concurrentReadLimit = 50

	// eventReadLineCountLimit mirrors EVENT_READ_LINE_COUNT_LIMIT: max lines
	// read from a file per scan.
	eventReadLineCountLimit = 200

	// eventAgentRetryTimes mirrors EVENT_AGENT_RETRY_TIMES: times a report
	// batch is retried before it is dropped.
	eventAgentRetryTimes = 10

	// eventAgentCacheSize mirrors EVENT_AGENT_CACHE_SIZE: capacity of the
	// cached events queue holding line batches.
	eventAgentCacheSize = 10240
)

// reportRequestTimeout bounds each report HTTP request. Go's http.DefaultClient
// has no timeout, so an explicit deadline is required to avoid a hung dashboard
// stalling the report loop (and in turn the monitor) forever.
const reportRequestTimeout = 30 * time.Second

// reportRetryBaseDelay is the base delay of the exponential backoff applied
// between report retries; the actual delay grows with each attempt and is
// jittered (see retryBackoffDelay), so a hung dashboard cannot occupy the
// report loop with a tight retry storm while its connection is accepted.
const reportRetryBaseDelay = 100 * time.Millisecond

// reportRetryMaxDelay caps the per-attempt backoff delay between report
// retries.
const reportRetryMaxDelay = 2 * time.Second

// eventSourceAll mirrors EVENT_SOURCE_ALL: source type names from event.proto's
// Event.SourceType enum. Only event log files whose name matches
// "*{source_type}*.log" for one of these are monitored (see isEventLogName).
var eventSourceAll = []string{
	"COMMON",
	"CORE_WORKER",
	"GCS",
	"RAYLET",
	"CLUSTER_LIFECYCLE",
	"AUTOSCALER",
	"JOBS",
}

// scanEventDirIntervalSeconds mirrors SCAN_EVENT_DIR_INTERVAL_SECONDS: seconds
// between two scans of the events directory. Default 2, overridable via
// SCAN_EVENT_DIR_INTERVAL_SECONDS (env_integer).
func scanEventDirIntervalSeconds() int {
	return envInteger("SCAN_EVENT_DIR_INTERVAL_SECONDS", 2)
}

// eventReadLineLengthLimit mirrors EVENT_READ_LINE_LENGTH_LIMIT: a line longer
// than this many bytes (2MB by default) is ignored when reading an event file.
// Overridable via EVENT_READ_LINE_LENGTH_LIMIT (env_integer).
func eventReadLineLengthLimit() int {
	return envInteger("EVENT_READ_LINE_LENGTH_LIMIT", 2*1024*1024)
}

// eventAgentReportIntervalSeconds mirrors
// EVENT_AGENT_REPORT_INTERVAL_SECONDS: seconds between two report events loops.
// Default 0.1, overridable via EVENT_AGENT_REPORT_INTERVAL_SECONDS (env_float).
func eventAgentReportIntervalSeconds() float64 {
	return common.EnvFloat("EVENT_AGENT_REPORT_INTERVAL_SECONDS", 0.1)
}

// gcsRpcTimeoutSeconds mirrors GCS_RPC_TIMEOUT_SECONDS: per-call timeout of the
// GCS InternalKV Get used to resolve the dashboard HTTP address. Default 60,
// overridable via RAY_DASHBOARD_GCS_RPC_TIMEOUT_SECONDS (env_integer).
func gcsRpcTimeoutSeconds() time.Duration {
	return time.Duration(envInteger("RAY_DASHBOARD_GCS_RPC_TIMEOUT_SECONDS", 60)) * time.Second
}

// envInteger reads an integer environment variable, falling back to def when
// unset or not a valid non-negative integer. It reuses common.EnvInteger and
// adds Python's env_integer isdigit() gate: only a non-empty all-digit value is
// accepted, so negative or signed overrides (e.g. "-1") fall back to def.
func envInteger(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if !isUnsignedDecimal(v) {
			return def
		}
	}
	return common.EnvInteger(name, def)
}

// isUnsignedDecimal reports whether s is a non-empty string of ASCII decimal
// digits, mirroring the isdigit() gate in Python's env_integer.
func isUnsignedDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
