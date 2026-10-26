package reporter

import (
	"testing"

	"github.com/ray-project/ray/go/proto"
)

func TestConstsPrefix(t *testing.T) {
	if ReporterPrefix != "RAY_REPORTER:" {
		t.Fatalf("ReporterPrefix = %q, want %q", ReporterPrefix, "RAY_REPORTER:")
	}
}

func TestConstsUpdateIntervalDefault(t *testing.T) {
	if DefaultReporterUpdateIntervalMs != 5000 {
		t.Fatalf("DefaultReporterUpdateIntervalMs = %d, want 5000", DefaultReporterUpdateIntervalMs)
	}
}

func TestConstsReporterUpdateIntervalMsAlias(t *testing.T) {
	if ReporterUpdateIntervalMs != 5000 {
		t.Fatalf("ReporterUpdateIntervalMs = %d, want 5000", ReporterUpdateIntervalMs)
	}
	if ReporterUpdateIntervalMs != DefaultReporterUpdateIntervalMs {
		t.Fatalf("ReporterUpdateIntervalMs = %d, want DefaultReporterUpdateIntervalMs = %d",
			ReporterUpdateIntervalMs, DefaultReporterUpdateIntervalMs)
	}
}

func TestUpdateIntervalMsDefault(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() = %d, want 5000", got)
	}
}

func TestUpdateIntervalMsEnvOverride(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "3000")
	got := UpdateIntervalMs()
	if got != 3000 {
		t.Fatalf("UpdateIntervalMs() = %d, want 3000", got)
	}
}

func TestUpdateIntervalMsEnvInvalid(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "not-a-number")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() = %d, want default 5000", got)
	}
}

func TestUpdateIntervalMsEnvNegative(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "-100")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() with -100 = %d, want default 5000", got)
	}
}

func TestUpdateIntervalMsEnvZero(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "0")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() with 0 = %d, want default 5000 (a zero interval would panic NewTicker)", got)
	}
}

func TestUpdateIntervalMsEnvSignedPositive(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "+3000")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() with +3000 = %d, want default 5000", got)
	}
}

func TestUpdateIntervalMsEnvOverflow(t *testing.T) {
	t.Setenv("REPORTER_UPDATE_INTERVAL_MS", "99999999999999999999")
	got := UpdateIntervalMs()
	if got != 5000 {
		t.Fatalf("UpdateIntervalMs() with overflowing digits = %d, want default 5000", got)
	}
}

func TestConstsChannel(t *testing.T) {
	if ReporterChannel != proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL {
		t.Fatalf("ReporterChannel = %v, want %v",
			ReporterChannel, proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL)
	}
}

func TestConstsComponentGaugeNames(t *testing.T) {
	want := map[string]string{
		"cpu":            "component_cpu_percentage",
		"memSharedBytes": "component_mem_shared_bytes",
		"rssMb":          "component_rss_mb",
		"ussMb":          "component_uss_mb",
		"pssMb":          "component_pss_mb",
		"numFds":         "component_num_fds",
		"gpuPercentage":  "component_gpu_percentage",
		"gpuMemoryMb":    "component_gpu_memory_mb",
	}
	got := map[string]string{
		"cpu":            ComponentGaugeCPUPercentage,
		"memSharedBytes": ComponentGaugeMemSharedBytes,
		"rssMb":          ComponentGaugeRSSMB,
		"ussMb":          ComponentGaugeUSSMB,
		"pssMb":          ComponentGaugePSSMB,
		"numFds":         ComponentGaugeNumFDs,
		"gpuPercentage":  ComponentGaugeGPUPercentage,
		"gpuMemoryMb":    ComponentGaugeGPUMemoryMB,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("gauge[%s] = %q, want %q", k, got[k], w)
		}
	}
}
