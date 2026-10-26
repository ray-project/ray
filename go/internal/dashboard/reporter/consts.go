// Package reporter implements the Go dashboard-agent reporter module,
// mirroring the Python dashboard reporter agent.
package reporter

import (
	"os"
	"strconv"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
)

const (
	// ReporterPrefix is the key prefix used when publishing node resource
	// usage to GCS, aligned with Python reporter_consts.REPORTER_PREFIX. It
	// aliases gcs.ReporterPrefix so both the reporter (publishing) and the
	// head pubsub subscriber (parsing) share a single source of truth.
	ReporterPrefix = gcs.ReporterPrefix
	// DefaultReporterUpdateIntervalMs is the default reporting interval in
	// milliseconds, aligned with Python reporter_consts.REPORTER_UPDATE_INTERVAL_MS.
	DefaultReporterUpdateIntervalMs = 5000
	// ReporterUpdateIntervalMs is the exported reporting interval constant
	// required by the component design contract. It aliases the default value.
	ReporterUpdateIntervalMs = DefaultReporterUpdateIntervalMs
)

const (
	// ReporterChannel is the pubsub channel type used to publish node
	// resource usage messages, aligned with the Python RAY_NODE_RESOURCE_USAGE_CHANNEL.
	ReporterChannel = proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL
)

// Component metric gauge names, aligned with the Python METRICS_GAUGES
// definitions in reporter_agent.py.
const (
	ComponentGaugeCPUPercentage  = "component_cpu_percentage"
	ComponentGaugeMemSharedBytes = "component_mem_shared_bytes"
	ComponentGaugeRSSMB          = "component_rss_mb"
	ComponentGaugeUSSMB          = "component_uss_mb"
	ComponentGaugePSSMB          = "component_pss_mb"
	ComponentGaugeNumFDs         = "component_num_fds"
	ComponentGaugeGPUPercentage  = "component_gpu_percentage"
	ComponentGaugeGPUMemoryMB    = "component_gpu_memory_mb"
)

// reporterUpdateIntervalEnv is the environment variable that overrides the
// default reporting interval, aligned with Python env_integer.
const reporterUpdateIntervalEnv = "REPORTER_UPDATE_INTERVAL_MS"

// UpdateIntervalMs returns the reporting interval in milliseconds. It reads
// the REPORTER_UPDATE_INTERVAL_MS environment variable and falls back to
// DefaultReporterUpdateIntervalMs when the variable is unset or invalid.
// Only pure-digit strings (0-9) are accepted, matching the Python
// env_integer isdigit() semantics; signed or otherwise non-numeric values
// fall back to the default.
func UpdateIntervalMs() int {
	raw, ok := os.LookupEnv(reporterUpdateIntervalEnv)
	if !ok {
		return DefaultReporterUpdateIntervalMs
	}
	if !isAllDigits(raw) {
		return DefaultReporterUpdateIntervalMs
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return DefaultReporterUpdateIntervalMs
	}
	// Reject non-positive values: a zero interval would make time.NewTicker
	// panic (Go panics on d <= 0), so fall back to the default instead of
	// letting a 0 flow into the module's publish loop.
	if v <= 0 {
		return DefaultReporterUpdateIntervalMs
	}
	return v
}

// isAllDigits reports whether s is non-empty and consists only of ASCII
// decimal digits (0-9), matching Python's str.isdigit() used by env_integer.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
