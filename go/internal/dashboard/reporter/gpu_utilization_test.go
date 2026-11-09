package reporter

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// TestParseGPUCSVUnavailableUtilizationIsMinusOne pins the value recorded for a
// device whose utilization nvidia-smi does not report. nvidia-smi prints [N/A]
// there, and Python's gpu_providers.py keeps its -1 sentinel for the same
// condition (utilization = -1 is assigned before every
// nvmlDeviceGetUtilizationRates call and survives the NVMLError). Reporting 0
// instead would claim an idle GPU, which is the opposite of "unknown".
func TestParseGPUCSVUnavailableUtilizationIsMinusOne(t *testing.T) {
	out := []byte("0, GPU-aaa, NVIDIA A10G, [N/A], 1234, 16384\n")

	gpus := parseGPUCsv(out)

	if len(gpus) != 1 {
		t.Fatalf("parseGPUCsv returned %d GPUs, want 1", len(gpus))
	}
	if got := gpus[0].UtilizationGPU; got != gpuUtilizationUnavailable {
		t.Errorf("UtilizationGPU = %d, want %d for an [N/A] device", got, gpuUtilizationUnavailable)
	}
	if gpuUtilizationUnavailable != -1 {
		t.Errorf("gpuUtilizationUnavailable = %d, want -1 to match Python", gpuUtilizationUnavailable)
	}
	// The other fields of the same row must survive; only utilization is unknown.
	if gpus[0].UUID != "GPU-aaa" || gpus[0].Name != "NVIDIA A10G" {
		t.Errorf("row identity lost: %+v", gpus[0])
	}
	if gpus[0].MemoryUsed != 1234 || gpus[0].MemoryTotal != 16384 {
		t.Errorf("memory = %d/%d, want 1234/16384", gpus[0].MemoryUsed, gpus[0].MemoryTotal)
	}
}

// TestParseGPUCSVKeepsRealUtilizationAlongsideUnavailable guards against the
// sentinel leaking into devices that do report a value, including a genuine 0.
func TestParseGPUCSVKeepsRealUtilizationAlongsideUnavailable(t *testing.T) {
	out := []byte(
		"0, GPU-aaa, NVIDIA A10G, [N/A], 1, 100\n" +
			"1, GPU-bbb, NVIDIA A10G, 0, 2, 100\n" +
			"2, GPU-ccc, NVIDIA A10G, 97, 3, 100\n",
	)

	gpus := parseGPUCsv(out)

	if len(gpus) != 3 {
		t.Fatalf("parseGPUCsv returned %d GPUs, want 3", len(gpus))
	}
	want := []int{gpuUtilizationUnavailable, 0, 97}
	for i, w := range want {
		if gpus[i].UtilizationGPU != w {
			t.Errorf("gpu[%d].UtilizationGPU = %d, want %d", i, gpus[i].UtilizationGPU, w)
		}
	}
}

// TestParseGPUCSVUnavailableUtilizationVariants covers the spellings nvidia-smi
// uses for an unreadable field, all of which must map to the sentinel rather
// than to an error or a silent zero.
func TestParseGPUCSVUnavailableUtilizationVariants(t *testing.T) {
	for _, field := range []string{"[N/A]", "N/A", "[Not Supported]", "Not Supported", ""} {
		out := []byte("0, GPU-aaa, NVIDIA A10G, " + field + ", 1, 100\n")
		gpus := parseGPUCsv(out)
		if len(gpus) != 1 {
			t.Fatalf("field %q: parseGPUCsv returned %d GPUs, want 1", field, len(gpus))
		}
		if gpus[0].UtilizationGPU != gpuUtilizationUnavailable {
			t.Errorf("field %q: UtilizationGPU = %d, want %d", field, gpus[0].UtilizationGPU, gpuUtilizationUnavailable)
		}
	}
}

// TestGPUCollectorKeepsUnavailableUtilizationEndToEnd drives the whole collector
// so the sentinel survives the CSV → struct → device pipeline, not just the
// parser in isolation.
func TestGPUCollectorKeepsUnavailableUtilizationEndToEnd(t *testing.T) {
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: []byte("0, GPU-aaa, NVIDIA A10G, [N/A], 1, 100\n")}, noProcQuery)
	defer func() { runCommand = orig }()

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 1 {
		t.Fatalf("Collect() returned %d GPUs, want 1", len(gpus))
	}
	if gpus[0].UtilizationGPU != gpuUtilizationUnavailable {
		t.Errorf("UtilizationGPU = %d, want %d", gpus[0].UtilizationGPU, gpuUtilizationUnavailable)
	}
}

// TestPayloadRendersUnavailableUtilizationAsMinusOne checks the wire shape.
// Python sends utilizationGpu: -1 for this condition, so the payload must carry
// the sentinel as a number and never omit the key or substitute 0 / null.
func TestPayloadRendersUnavailableUtilizationAsMinusOne(t *testing.T) {
	stats := headStats()
	stats["gpus"] = []GpuUtilizationInfo{{
		Index:          0,
		Name:           "NVIDIA A10G",
		UUID:           "GPU-aaa",
		UtilizationGPU: gpuUtilizationUnavailable,
		MemoryUsed:     1,
		MemoryTotal:    100,
		ProcessesPIDs:  map[int]ProcessGPUInfo{},
	}}

	out, err := NewStatsPayloadComposer(stats, 1700000000.0).Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}

	var payload struct {
		Gpus []struct {
			UtilizationGpu *float64 `json:"utilizationGpu"`
		} `json:"gpus"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v\n%s", err, out)
	}
	if len(payload.Gpus) != 1 {
		t.Fatalf("payload has %d gpus, want 1", len(payload.Gpus))
	}
	if payload.Gpus[0].UtilizationGpu == nil {
		t.Fatalf("utilizationGpu is missing or null, want %d", gpuUtilizationUnavailable)
	}
	if *payload.Gpus[0].UtilizationGpu != float64(gpuUtilizationUnavailable) {
		t.Errorf("utilizationGpu = %v, want %d", *payload.Gpus[0].UtilizationGpu, gpuUtilizationUnavailable)
	}
	// Guard the literal spelling too: a nil-collapsing renderer would drop to
	// null above, but an encoder change could also emit the wrong number.
	if !strings.Contains(out, `"utilizationGpu":-1`) {
		t.Errorf("payload does not contain \"utilizationGpu\":-1\n%s", out)
	}
}

// TestNodeGpuUtilizationSumsUnavailableSentinel mirrors the Python _to_records
// guard, which skips only None and therefore adds -1 into
// node_gpus_utilization unchanged. A Go version that skipped the sentinel would
// report 0 for a device whose utilization is unknown, diverging from Python.
func TestNodeGpuUtilizationSumsUnavailableSentinel(t *testing.T) {
	stats := map[string]interface{}{
		"gpus": []GpuUtilizationInfo{
			{Index: 0, Name: "NVIDIA A10G", UtilizationGPU: gpuUtilizationUnavailable, MemoryUsed: 1, MemoryTotal: 100},
			{Index: 1, Name: "NVIDIA A10G", UtilizationGPU: 42, MemoryUsed: 1, MemoryTotal: 100},
		},
	}

	records := gpuRecords(stats, map[string]string{})

	utilization := map[string]float64{}
	for _, r := range records {
		if r.Gauge.Name == GaugeNodeGPUsUtilization {
			utilization[r.Tags["GpuIndex"]] = r.Value
		}
	}
	// Python sums per device inside the per-device loop body, so each device
	// emits its own value; device 0 contributes its -1 verbatim.
	if got := utilization["0"]; got != float64(gpuUtilizationUnavailable) {
		t.Errorf("node_gpus_utilization[GpuIndex=0] = %v, want %d", got, gpuUtilizationUnavailable)
	}
	if got := utilization["1"]; got != 42 {
		t.Errorf("node_gpus_utilization[GpuIndex=1] = %v, want 42", got)
	}
}

// TestUnavailableUtilizationIsNotTreatedAsZero documents the failure mode the
// fix removes: 0 and the sentinel must stay distinguishable, since 0 means an
// idle GPU and -1 means an unknown one.
func TestUnavailableUtilizationIsNotTreatedAsZero(t *testing.T) {
	if gpuUtilizationUnavailable == 0 {
		t.Fatalf("gpuUtilizationUnavailable = 0, which is indistinguishable from an idle GPU")
	}
	if gpuUtilizationUnavailable != -1 {
		t.Fatalf("gpuUtilizationUnavailable = %d, want -1 to match Python", gpuUtilizationUnavailable)
	}
	if math.Signbit(float64(gpuUtilizationUnavailable)) != math.Signbit(-1) {
		t.Errorf("sentinel lost its sign")
	}
}
