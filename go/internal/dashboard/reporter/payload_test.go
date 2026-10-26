package reporter

import (
	"encoding/json"
	"reflect"
	"testing"
)

// processInfo returns a snake_case ProcessInfo-shaped map, matching what the
// collector produces for workers/raylet/agent/gcs/additional.
func processInfo(pid int, cmd string) map[string]interface{} {
	return map[string]interface{}{
		"pid":              pid,
		"create_time":      100.0,
		"cpu_percent":      0.5,
		"cpu_times":        map[string]interface{}{"user": 1.0, "system": 2.0, "children_user": 0.1, "children_system": 0.2},
		"cmdline":          []string{cmd},
		"memory_info":      map[string]interface{}{"rss": 100.0, "vms": 200.0, "pfaults": 1.0, "pageins": 2.0},
		"memory_full_info": map[string]interface{}{"rss": 100.0, "vms": 200.0, "pfaults": 1.0, "pageins": 2.0, "uss": 50.0},
		"num_fds":          3,
		"gpu_memory_usage": 1024,
		"gpu_utilization":  20,
	}
}

// headStats builds a stats map with the same shape as StatsCollector.Collect()
// on a head node, using the production concrete types for nested fields.
func headStats() map[string]interface{} {
	worker := processInfo(42, "ray::worker")
	return map[string]interface{}{
		"now":           int64(1700000000),
		"hostname":      "host-a",
		"ip":            "10.0.0.1",
		"cpu":           12.5,
		"cpus":          []int{8, 8},
		"mem":           []interface{}{int64(100), int64(30), 70.0, int64(70)},
		"shm":           int64(2048),
		"workers":       []interface{}{worker},
		"raylet":        processInfo(1, "raylet"),
		"agent":         processInfo(2, "agent"),
		"bootTime":      1234567890.0,
		"loadAvg":       []interface{}{[]interface{}{1.0, 2.0, 3.0}, []interface{}{0.5, 0.25, 0.375}},
		"disk":          map[string]DiskInfo{"/": {Total: 1000, Used: 400, Free: 600, Percent: 40.0}},
		"disk_io":       DiskIOMetrics{ReadBytes: 10, WriteBytes: 20, ReadCount: 1, WriteCount: 2},
		"disk_io_speed": nil,
		"network":       IOMetrics{Sent: 100, Recv: 200},
		"network_speed": nil,
		"additional":    map[string]interface{}{"extra": processInfo(3, "extra")},
		"cmdline":       []string{"raylet"},
		"gpus": []GpuUtilizationInfo{{
			Index:          0,
			Name:           "Tesla",
			UUID:           "gpu-0",
			UtilizationGPU: 50,
			MemoryUsed:     1024,
			MemoryTotal:    16384,
			ProcessesPIDs: map[int]ProcessGPUInfo{
				42: {PID: 42, GPUMemoryUsage: 512, GPUUtilization: intPtr(10)},
			},
		}},
		"tpus": []TpuUtilizationInfo{{
			Index:                 0,
			Name:                  "tpu-0",
			TPUType:               "v4",
			TPUTopology:           "2x2",
			TensorcoreUtilization: 80,
			HBMUtilization:        70,
			DutyCycle:             60,
			MemoryUsed:            100,
			MemoryTotal:           200,
		}},
		"gcs": processInfo(5, "gcs_server"),
	}
}

func intPtr(v int) *int { return &v }

// topLevelKeys is the canonical set of camelCase keys in the Python
// StatsPayload model (reporter_models.py).
func topLevelKeys() []string {
	return []string{
		"now", "hostname", "ip", "cpu", "cpus", "mem", "shm", "workers",
		"raylet", "agent", "bootTime", "loadAvg", "disk", "diskIo",
		"diskIoSpeed", "gpus", "tpus", "network", "networkSpeed",
		"cmdline", "gcs",
	}
}

func decode(t *testing.T, out string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	return m
}

func TestPayloadComposeHeadTopLevelKeys(t *testing.T) {
	c := NewStatsPayloadComposer(headStats(), 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)
	for _, k := range topLevelKeys() {
		if _, ok := m[k]; !ok {
			t.Errorf("missing top-level camelCase key %q in payload", k)
		}
	}
	// Head nodes must carry gcs.
	if _, ok := m["gcs"]; !ok {
		t.Errorf("head payload missing gcs")
	}
	// snake_case keys must not leak through.
	for _, k := range []string{"disk_io", "disk_io_speed", "network_speed", "processes_pids", "create_time"} {
		if _, ok := m[k]; ok {
			t.Errorf("snake_case key %q leaked into payload", k)
		}
	}
	// now must be the float POSIX timestamp passed to the composer.
	if f, ok := m["now"].(float64); !ok || f != 1700000000.0 {
		t.Errorf("now not set to composer timestamp, got %v", m["now"])
	}
}

func TestPayloadComposeNonHeadNoGCS(t *testing.T) {
	stats := headStats()
	delete(stats, "gcs")
	c := NewStatsPayloadComposer(stats, 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)
	if _, ok := m["gcs"]; ok {
		t.Errorf("non-head payload must not contain gcs")
	}
}

func TestPayloadComposeNestedStructures(t *testing.T) {
	c := NewStatsPayloadComposer(headStats(), 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)

	// disk: mount point -> {total, used, free, percent}
	disk, ok := m["disk"].(map[string]interface{})
	if !ok {
		t.Fatalf("disk not an object: %v", m["disk"])
	}
	root, ok := disk["/"].(map[string]interface{})
	if !ok {
		t.Fatalf("disk[/] not an object: %v", disk["/"])
	}
	for _, k := range []string{"total", "used", "free", "percent"} {
		if _, ok := root[k]; !ok {
			t.Errorf("disk sub-field %q missing", k)
		}
	}
	if got := root["used"].(float64); got != 400 {
		t.Errorf("disk used = %v, want 400", got)
	}

	// mem: [total, available, percent, used]
	mem, ok := m["mem"].([]interface{})
	if !ok || len(mem) != 4 {
		t.Fatalf("mem not a 4-tuple: %v", m["mem"])
	}

	// diskIo: [readBytes, writeBytes, readCount, writeCount]
	diskIO, ok := m["diskIo"].([]interface{})
	if !ok || len(diskIO) != 4 {
		t.Fatalf("diskIo not a 4-tuple: %v", m["diskIo"])
	}

	// network: [sent, received]
	net, ok := m["network"].([]interface{})
	if !ok || len(net) != 2 {
		t.Fatalf("network not a 2-tuple: %v", m["network"])
	}

	// workers[0] must use camelCase ProcessInfo keys.
	workers, ok := m["workers"].([]interface{})
	if !ok || len(workers) == 0 {
		t.Fatalf("workers empty or not a list: %v", m["workers"])
	}
	proc, ok := workers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker not an object: %v", workers[0])
	}
	for _, k := range []string{"pid", "createTime", "cpuPercent", "cpuTimes", "cmdline", "memoryInfo", "memoryFullInfo", "numFds", "gpuMemoryUsage", "gpuUtilization"} {
		if _, ok := proc[k]; !ok {
			t.Errorf("worker ProcessInfo field %q missing", k)
		}
	}
	ct, ok := proc["cpuTimes"].(map[string]interface{})
	if !ok {
		t.Fatalf("cpuTimes not an object: %v", proc["cpuTimes"])
	}
	for _, k := range []string{"user", "system", "childrenUser", "childrenSystem"} {
		if _, ok := ct[k]; !ok {
			t.Errorf("cpuTimes field %q missing", k)
		}
	}
	mi, ok := proc["memoryInfo"].(map[string]interface{})
	if !ok {
		t.Fatalf("memoryInfo not an object: %v", proc["memoryInfo"])
	}
	for _, k := range []string{"rss", "vms", "pfaults", "pageins"} {
		if _, ok := mi[k]; !ok {
			t.Errorf("memoryInfo field %q missing", k)
		}
	}
}

func TestPayloadComposeGPUsAndTPUs(t *testing.T) {
	c := NewStatsPayloadComposer(headStats(), 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)

	gpus, ok := m["gpus"].([]interface{})
	if !ok || len(gpus) != 1 {
		t.Fatalf("gpus not a 1-element list: %v", m["gpus"])
	}
	gpu, ok := gpus[0].(map[string]interface{})
	if !ok {
		t.Fatalf("gpu not an object: %v", gpus[0])
	}
	for _, k := range []string{"index", "name", "uuid", "utilizationGpu", "memoryUsed", "memoryTotal", "processesPids"} {
		if _, ok := gpu[k]; !ok {
			t.Errorf("gpu field %q missing", k)
		}
	}
	pids, ok := gpu["processesPids"].([]interface{})
	if !ok || len(pids) != 1 {
		t.Fatalf("processesPids not converted to a list: %v", gpu["processesPids"])
	}
	pidInfo, ok := pids[0].(map[string]interface{})
	if !ok {
		t.Fatalf("process pid info not an object: %v", pids[0])
	}
	for _, k := range []string{"pid", "gpuMemoryUsage", "gpuUtilization"} {
		if _, ok := pidInfo[k]; !ok {
			t.Errorf("process pid field %q missing", k)
		}
	}

	tpus, ok := m["tpus"].([]interface{})
	if !ok || len(tpus) != 1 {
		t.Fatalf("tpus not a 1-element list: %v", m["tpus"])
	}
	tpu, ok := tpus[0].(map[string]interface{})
	if !ok {
		t.Fatalf("tpu not an object: %v", tpus[0])
	}
	for _, k := range []string{"index", "name", "tpuType", "tpuTopology", "tensorcoreUtilization", "hbmUtilization", "dutyCycle", "memoryUsed", "memoryTotal"} {
		if _, ok := tpu[k]; !ok {
			t.Errorf("tpu field %q missing", k)
		}
	}
}

// TestPayloadComposeDropsAdditional asserts that the "additional" field is not
// emitted in the payload. The Python StatsPayload pydantic model does not
// declare an "additional" field, so pydantic v1 (Extra.ignore) discards it
// during parse_obj before json.dumps. The Go composer must mirror that and drop
// the extra field instead of passing it through.
func TestPayloadComposeDropsAdditional(t *testing.T) {
	c := NewStatsPayloadComposer(headStats(), 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)
	if _, ok := m["additional"]; ok {
		t.Errorf("payload must not contain additional, but found it")
	}
	// The nested additional process must not leak into the payload either.
	for _, k := range []string{"extra"} {
		if _, ok := m[k]; ok {
			t.Errorf("payload leaked additional nested key %q", k)
		}
	}
}

// TestPayloadComposeShmNull asserts that a nil shm value serializes to JSON
// null, matching Python's Optional[int] field when unavailable (e.g. non-k8s).
func TestPayloadComposeShmNull(t *testing.T) {
	stats := headStats()
	stats["shm"] = nil
	c := NewStatsPayloadComposer(stats, 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)
	v, ok := m["shm"]
	if !ok {
		t.Fatalf("payload missing shm key")
	}
	if v != nil {
		t.Errorf("shm = %v, want null", v)
	}
}

// TestPayloadComposeGpusEmptySliceVsNil asserts the serialization of an empty
// gpus slice vs a nil gpus slice. Python always emits an empty list [] for
// gpus (List[GpuUtilizationInfo] with no GPUs), so the Go composer must never
// leak null for the typed slice: both empty and nil slices serialize to [].
func TestPayloadComposeGpusEmptySliceVsNil(t *testing.T) {
	// Empty non-nil slice -> JSON array [].
	stats := headStats()
	stats["gpus"] = []GpuUtilizationInfo{}
	c := NewStatsPayloadComposer(stats, 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	if !containsJSONToken(out, `"gpus":[]`) {
		t.Errorf("empty gpus should serialize to [], got: %s", out)
	}

	// Nil typed slice must also serialize to [] (never null), matching Python's
	// always-list behavior for gpus.
	stats = headStats()
	stats["gpus"] = []GpuUtilizationInfo(nil)
	c = NewStatsPayloadComposer(stats, 1700000000.0)
	out, err = c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	if containsJSONToken(out, `"gpus":null`) {
		t.Errorf("nil gpus must not serialize to null, got: %s", out)
	}
	if !containsJSONToken(out, `"gpus":[]`) {
		t.Errorf("nil gpus should serialize to [] to match Python, got: %s", out)
	}
}

// TestPayloadComposeTpusEmptySliceVsNil mirrors the gpus test for tpus.
func TestPayloadComposeTpusEmptySliceVsNil(t *testing.T) {
	stats := headStats()
	stats["tpus"] = []TpuUtilizationInfo{}
	c := NewStatsPayloadComposer(stats, 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	if !containsJSONToken(out, `"tpus":[]`) {
		t.Errorf("empty tpus should serialize to [], got: %s", out)
	}

	stats = headStats()
	stats["tpus"] = []TpuUtilizationInfo(nil)
	c = NewStatsPayloadComposer(stats, 1700000000.0)
	out, err = c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	if containsJSONToken(out, `"tpus":null`) {
		t.Errorf("nil tpus must not serialize to null, got: %s", out)
	}
	if !containsJSONToken(out, `"tpus":[]`) {
		t.Errorf("nil tpus should serialize to [] to match Python, got: %s", out)
	}
}

// TestPayloadComposeGpuUtilizationNil asserts that a nil
// ProcessGPUInfo.GPUUtilization serializes to JSON null.
func TestPayloadComposeGpuUtilizationNil(t *testing.T) {
	stats := headStats()
	gpus := stats["gpus"].([]GpuUtilizationInfo)
	gpus[0].ProcessesPIDs = map[int]ProcessGPUInfo{
		7: {PID: 7, GPUMemoryUsage: 256, GPUUtilization: nil},
	}
	c := NewStatsPayloadComposer(stats, 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	if !containsJSONToken(out, `"gpuUtilization":null`) {
		t.Errorf("nil gpuUtilization should serialize to null, got: %s", out)
	}
}

func containsJSONToken(s, tok string) bool {
	for i := 0; i+len(tok) <= len(s); i++ {
		if s[i:i+len(tok)] == tok {
			return true
		}
	}
	return false
}

// TestPayloadComposeKeySetAlignsWithPython asserts the emitted key set equals
// the Python StatsPayload model exactly (no extras, no missing), including the
// gcs head-only field.
func TestPayloadComposeKeySetAlignsWithPython(t *testing.T) {
	c := NewStatsPayloadComposer(headStats(), 1700000000.0)
	out, err := c.Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	m := decode(t, out)

	want := map[string]bool{}
	for _, k := range topLevelKeys() {
		want[k] = true
	}
	got := map[string]bool{}
	for k := range m {
		got[k] = true
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("top-level key set mismatch\n want: %v\n got:  %v", want, got)
	}
}
