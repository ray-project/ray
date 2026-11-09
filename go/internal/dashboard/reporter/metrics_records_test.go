package reporter

import (
	"sort"
	"testing"
)

// globalTestTags mirrors the global tags the module merges into every record.
func globalTestTags() map[string]string {
	return map[string]string{"Version": "2.53.0", "SessionName": "session_test"}
}

// recordsFor builds the gauge records of a head-node stats fixture through the
// production record generator, discarding the stale-tracking state so each call
// is independent.
func recordsFor(t *testing.T, stats, clusterStats map[string]interface{}, isHead bool) []gaugeRecord {
	t.Helper()
	state := &recordsState{}
	return state.toRecords(stats, clusterStats, isHead, globalTestTags())
}

// findRecords returns every record of the given gauge name.
func findRecords(records []gaugeRecord, name string) []gaugeRecord {
	out := make([]gaugeRecord, 0, 1)
	for _, r := range records {
		if r.Gauge.Name == name {
			out = append(out, r)
		}
	}
	return out
}

// requireSingleRecord returns the only record of a gauge, failing when the gauge
// is missing or unexpectedly duplicated.
func requireSingleRecord(t *testing.T, records []gaugeRecord, name string) gaugeRecord {
	t.Helper()
	found := findRecords(records, name)
	if len(found) != 1 {
		t.Fatalf("gauge %s: found %d records, want exactly 1; all gauges=%v", name, len(found), gaugeNames(records))
	}
	return found[0]
}

// gaugeNames returns the sorted gauge names present in a record slice, used to
// make failure messages actionable.
func gaugeNames(records []gaugeRecord) []string {
	seen := map[string]bool{}
	for _, r := range records {
		seen[r.Gauge.Name] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// assertTag verifies one tag of a record and reports the full tag set on
// mismatch.
func assertTag(t *testing.T, r gaugeRecord, key, want string) {
	t.Helper()
	if got := r.Tags[key]; got != want {
		t.Errorf("%s tag %q = %q, want %q (tags: %v)", r.Gauge.Name, key, got, want, r.Tags)
	}
}

// metricRecordStats builds a stats map with the production concrete types for a
// head node, sized so every value is individually identifiable in assertions.
func metricRecordStats() map[string]interface{} {
	worker := map[string]interface{}{
		"pid":              42,
		"cmdline":          []string{"ray::Worker.actor"},
		"cpu_percent":      12.5,
		"memory_info":      map[string]interface{}{"rss": 50_000_000.0, "vms": 90_000_000.0, "shared": 4096.0},
		"memory_full_info": map[string]interface{}{"rss": 50_000_000.0, "uss": 30_000_000.0, "pss": 35_000_000.0},
		"num_fds":          7,
		"gpu_memory_usage": 2048,
		"gpu_utilization":  30,
	}
	return map[string]interface{}{
		"ip":         "10.0.0.7",
		"cpu":        37.5,
		"cpus":       []int{8, 4},
		"mem":        []interface{}{int64(100_000), int64(40_000), 60.0, int64(60_000)},
		"host_mem":   []interface{}{int64(70_000), int64(200_000)},
		"swap":       []interface{}{int64(8_000), int64(2_000), 25.0},
		"cgroup_mem": []interface{}{int64(60_000), int64(100_000)},
		"shm":        int64(65536),
		"disk_io": DiskIOMetrics{
			ReadBytes: 1000, WriteBytes: 2000, ReadCount: 11, WriteCount: 22,
		},
		"disk_io_speed": []float64{1.5, 2.5, 3.5, 4.5},
		"disk": map[string]DiskInfo{
			"/": {Total: 1000, Used: 400, Free: 600, Percent: 40.0},
		},
		"network":       IOMetrics{Sent: 111, Recv: 222},
		"network_speed": []float64{5.5, 6.5},
		"gpus": []GpuUtilizationInfo{{
			Index: 0, Name: "Tesla T4", UUID: "gpu-0", UtilizationGPU: 55, MemoryUsed: 1024, MemoryTotal: 16384,
			PowerMW: 70_000, HasPower: true, TemperatureC: 61, HasTemperature: true,
		}},
		"tpus": []TpuUtilizationInfo{{
			Index: 1, Name: "tpu-1", TPUType: "v4", TPUTopology: "2x2",
			TensorcoreUtilization: 80, HBMUtilization: 70, DutyCycle: 60, MemoryUsed: 100, MemoryTotal: 200,
		}},
		"raylet": map[string]interface{}{
			"pid": 1, "cpu_percent": 1.5,
			"memory_info":      map[string]interface{}{"rss": 10_000_000.0, "shared": 2048.0},
			"memory_full_info": map[string]interface{}{"uss": 8_000_000.0, "pss": 9_000_000.0},
			"num_fds":          3,
		},
		"agent": map[string]interface{}{
			"pid": 2, "cpu_percent": 2.5,
			"memory_info":      map[string]interface{}{"rss": 20_000_000.0, "shared": 1024.0},
			"memory_full_info": map[string]interface{}{"uss": 15_000_000.0, "pss": 16_000_000.0},
			"num_fds":          4,
		},
		"gcs": map[string]interface{}{
			"pid": 3, "cpu_percent": 0.5,
			"memory_info":      map[string]interface{}{"rss": 30_000_000.0, "shared": 512.0},
			"memory_full_info": map[string]interface{}{"uss": 25_000_000.0, "pss": 26_000_000.0},
			"num_fds":          5,
		},
		"workers": []interface{}{worker},
		"additional": map[string]interface{}{
			"log_monitor": []interface{}{
				map[string]interface{}{
					"pid": 4, "cpu_percent": 3.5,
					"memory_info":      map[string]interface{}{"rss": 40_000_000.0, "shared": 256.0},
					"memory_full_info": map[string]interface{}{"uss": 1_000_000.0, "pss": 2_000_000.0},
					"num_fds":          6,
				},
			},
		},
	}
}

// TestNodeRecords verifies the 21 node-level gauges carry the node tags and the
// values of the collected stats, matching the Python _to_records node section.
func TestNodeRecords(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, true)

	cases := []struct {
		gauge string
		want  float64
	}{
		{GaugeNodeCPUUtilization, 37.5},
		{GaugeNodeCPUCount, 8},
		{GaugeNodeMemUsed, 60_000},
		{GaugeNodeMemAvailable, 40_000},
		{GaugeNodeMemTotal, 100_000},
		{GaugeNodeMemSharedBytes, 65536},
		{GaugeNodeDiskIORead, 1000},
		{GaugeNodeDiskIOWrite, 2000},
		{GaugeNodeDiskIOReadCount, 11},
		{GaugeNodeDiskIOWriteCount, 22},
		{GaugeNodeDiskIOReadSpeed, 1.5},
		{GaugeNodeDiskIOWriteSpeed, 2.5},
		{GaugeNodeDiskReadIOPS, 3.5},
		{GaugeNodeDiskWriteIOPS, 4.5},
		{GaugeNodeDiskUsage, 400},
		{GaugeNodeDiskFree, 600},
		{GaugeNodeDiskUtilizationPct, 40},
		{GaugeNodeNetworkSent, 111},
		{GaugeNodeNetworkReceived, 222},
		{GaugeNodeNetworkSendSpeed, 5.5},
		{GaugeNodeNetworkReceiveSpeed, 6.5},
	}
	for _, c := range cases {
		r := requireSingleRecord(t, records, c.gauge)
		if r.Value != c.want {
			t.Errorf("%s = %v, want %v", c.gauge, r.Value, c.want)
		}
		assertTag(t, r, "ip", "10.0.0.7")
		assertTag(t, r, "RayNodeType", "head")
		assertTag(t, r, "IsHeadNode", "true")
		assertTag(t, r, "Version", "2.53.0")
		assertTag(t, r, "SessionName", "session_test")
	}
}

// TestNodeRecordsWorkerTags verifies a worker node is tagged as such, matching
// the Python ray_node_type/is_head_node derivation.
func TestNodeRecordsWorkerTags(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, false)
	r := requireSingleRecord(t, records, GaugeNodeCPUUtilization)
	assertTag(t, r, "RayNodeType", "worker")
	assertTag(t, r, "IsHeadNode", "false")
}

// TestNodeRecordsOmitSharedMemoryWhenZero verifies node_mem_shared_bytes is
// dropped when shared memory is unavailable, matching Python's `if shm_used`.
func TestNodeRecordsOmitSharedMemoryWhenZero(t *testing.T) {
	stats := metricRecordStats()
	stats["shm"] = nil
	records := recordsFor(t, stats, nil, false)
	if got := findRecords(records, GaugeNodeMemSharedBytes); len(got) != 0 {
		t.Errorf("node_mem_shared_bytes present with unavailable shm: %v", got)
	}
}

// TestGPUDeviceRecords verifies one GPU produces the four per-device gauges with
// the device tags and the total-minus-used available RAM, matching Python.
func TestGPUDeviceRecords(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, false)

	cases := []struct {
		gauge string
		want  float64
	}{
		{GaugeNodeGPUsAvailable, 1},
		{GaugeNodeGPUsUtilization, 55},
		{GaugeNodeGRAMUsed, 1024},
		{GaugeNodeGRAMAvaliable, 16384 - 1024},
	}
	for _, c := range cases {
		r := requireSingleRecord(t, records, c.gauge)
		if r.Value != c.want {
			t.Errorf("%s = %v, want %v", c.gauge, r.Value, c.want)
		}
		assertTag(t, r, "GpuIndex", "0")
		assertTag(t, r, "GpuDeviceName", "Tesla T4")
	}
}

// TestGPUDeviceRecordsPerDevice verifies every GPU index is recorded
// independently rather than aggregated, matching the Python per-device loop.
func TestGPUDeviceRecordsPerDevice(t *testing.T) {
	stats := metricRecordStats()
	stats["gpus"] = []GpuUtilizationInfo{
		{Index: 0, Name: "Tesla T4", UtilizationGPU: 10, MemoryUsed: 100, MemoryTotal: 1000},
		{Index: 1, Name: "Tesla T4", UtilizationGPU: 90, MemoryUsed: 900, MemoryTotal: 1000},
	}
	records := recordsFor(t, stats, nil, false)

	found := findRecords(records, GaugeNodeGPUsUtilization)
	if len(found) != 2 {
		t.Fatalf("node_gpus_utilization records = %d, want 2", len(found))
	}
	byIndex := map[string]float64{}
	for _, r := range found {
		byIndex[r.Tags["GpuIndex"]] = r.Value
	}
	if byIndex["0"] != 10 || byIndex["1"] != 90 {
		t.Errorf("per-device utilization = %v, want map[0:10 1:90]", byIndex)
	}
}

// TestGPUDeviceRecordsOmitDeviceNameWhenEmpty verifies an unnamed device omits
// the GpuDeviceName tag, matching Python's `if gpu_name` guard.
func TestGPUDeviceRecordsOmitDeviceNameWhenEmpty(t *testing.T) {
	stats := metricRecordStats()
	stats["gpus"] = []GpuUtilizationInfo{{Index: 0, Name: "", UtilizationGPU: 5, MemoryUsed: 1, MemoryTotal: 2}}
	records := recordsFor(t, stats, nil, false)
	r := requireSingleRecord(t, records, GaugeNodeGPUsAvailable)
	if _, ok := r.Tags["GpuDeviceName"]; ok {
		t.Errorf("GpuDeviceName tag present for an unnamed device: %v", r.Tags)
	}
}

// TestTPUDeviceRecords verifies the five per-device TPU gauges and the TPU tag
// set, matching the Python TPU section.
func TestTPUDeviceRecords(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, false)

	cases := []struct {
		gauge string
		want  float64
	}{
		{GaugeTpuTensorcoreUtilization, 80},
		{GaugeTpuMemoryBandwidth, 70},
		{GaugeTpuDutyCycle, 60},
		{GaugeTpuMemoryUsed, 100},
		{GaugeTpuMemoryTotal, 200},
	}
	for _, c := range cases {
		r := requireSingleRecord(t, records, c.gauge)
		if r.Value != c.want {
			t.Errorf("%s = %v, want %v", c.gauge, r.Value, c.want)
		}
		assertTag(t, r, "TpuIndex", "1")
		assertTag(t, r, "TpuDeviceName", "tpu-1")
		assertTag(t, r, "TpuType", "v4")
		assertTag(t, r, "TpuTopology", "2x2")
	}
}

// TestComponentRecordsAggregateAndConvertUnits verifies the component gauges
// aggregate the process stats with RSS/USS/PSS divided by 1e6 (bytes to MB)
// while shared memory stays in bytes, matching the Python
// _generate_system_stats_record unit handling.
func TestComponentRecordsAggregateAndConvertUnits(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, true)

	cpuRecords := findRecords(records, ComponentGaugeCPUPercentage)
	if len(cpuRecords) != 5 {
		t.Fatalf("component_cpu_percentage records = %d, want 5 (gcs, raylet, worker, agent, log_monitor)", len(cpuRecords))
	}

	byComponent := func(rs []gaugeRecord) map[string]gaugeRecord {
		m := map[string]gaugeRecord{}
		for _, r := range rs {
			m[r.Tags["Component"]] = r
		}
		return m
	}
	cpuBy := byComponent(cpuRecords)
	raylet, ok := cpuBy["raylet"]
	if !ok {
		t.Fatalf("no raylet component record; components = %v", mapKeys(cpuBy))
	}
	if raylet.Value != 1.5 {
		t.Errorf("raylet component_cpu_percentage = %v, want 1.5", raylet.Value)
	}
	assertTag(t, raylet, "pid", "1")
	assertTag(t, raylet, "ip", "10.0.0.7")

	rssBy := byComponent(findRecords(records, ComponentGaugeRSSMB))
	rssBytesBy := byComponent(findRecords(records, ComponentGaugeRSSBytes))
	sharedBy := byComponent(findRecords(records, ComponentGaugeSharedBytes))
	ussBy := byComponent(findRecords(records, ComponentGaugeUSSMB))
	ussBytesBy := byComponent(findRecords(records, ComponentGaugeUSSBytes))
	fdsBy := byComponent(findRecords(records, ComponentGaugeNumFDs))

	// RSS and USS are reported both in bytes and in MB (bytes / 1e6); shared
	// memory stays in bytes.
	if got := rssBy["raylet"].Value; got != 10 {
		t.Errorf("raylet component_rss_mb = %v, want 10 (10 MB)", got)
	}
	if got := rssBytesBy["raylet"].Value; got != 1.0e7 {
		t.Errorf("raylet component_rss_bytes = %v, want 1.0e7 (bytes)", got)
	}
	if got := sharedBy["raylet"].Value; got != 2048 {
		t.Errorf("raylet component_shared_bytes = %v, want 2048 (bytes)", got)
	}
	if got := ussBytesBy["raylet"].Value; got != 8.0e6 {
		t.Errorf("raylet component_uss_bytes = %v, want 8.0e6 (bytes)", got)
	}
	if got := ussBy["raylet"].Value; got != 8 {
		t.Errorf("raylet component_uss_mb = %v, want 8", got)
	}
	if got := fdsBy["gcs"].Value; got != 5 {
		t.Errorf("gcs component_num_fds = %v, want 5", got)
	}
}

// recordsOfComponent narrows a record slice to one component name, so component
// gauges -- which are shared across components and separated by the Component
// tag -- can be asserted per component.
func recordsOfComponent(records []gaugeRecord, component string) []gaugeRecord {
	out := make([]gaugeRecord, 0, len(records))
	for _, r := range records {
		if r.Tags["Component"] == component {
			out = append(out, r)
		}
	}
	return out
}

// mapKeys returns the sorted keys of a component record map for failure output.
func mapKeys(m map[string]gaugeRecord) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestComponentRecordsGCSOnlyOnHead verifies the gcs component is only recorded
// on the head node, matching the Python `if self._is_head_node` guard.
func TestComponentRecordsGCSOnlyOnHead(t *testing.T) {
	head := recordsFor(t, metricRecordStats(), nil, true)
	if got := componentRecordCount(head, gcsServerComponent); got != 7 {
		t.Errorf("head gcs component records = %d, want 7", got)
	}
	worker := recordsFor(t, metricRecordStats(), nil, false)
	if got := componentRecordCount(worker, gcsServerComponent); got != 0 {
		t.Errorf("non-head gcs component records = %d, want 0", got)
	}
}

// componentRecordCount counts the records of one component name across the
// component gauges.
func componentRecordCount(records []gaugeRecord, component string) int {
	n := 0
	for _, r := range records {
		if r.Tags["Component"] == component {
			n++
		}
	}
	return n
}

// TestComponentRecordsOmitUSSAndPSSWhenZero verifies the USS and PSS gauges are
// only emitted for positive totals, matching the Python `if total_uss > 0.0`
// and `if total_pss > 0.0` guards. The fixture carries memory_info but no
// memory_full_info at all, which is how a process without a readable
// smaps_rollup is reported.
func TestComponentRecordsOmitUSSWhenZero(t *testing.T) {
	stats := map[string]interface{}{
		"ip": "10.0.0.7",
		"raylet": map[string]interface{}{
			"pid":         1,
			"cpu_percent": 1.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0, "shared": 0.0},
			"num_fds":     1,
		},
	}
	records := recordsFor(t, stats, nil, false)

	// The component gauges that are always emitted must be present, so the test
	// proves the USS absence is a guard rather than a missing fixture.
	if got := findRecords(records, ComponentGaugeRSSMB); len(got) != 1 {
		t.Fatalf("component_rss_mb records = %d, want 1", len(got))
	}
	if got := findRecords(records, ComponentGaugeUSSMB); len(got) != 0 {
		t.Errorf("component_uss_mb emitted for a zero USS total: %v", got)
	}
	if got := findRecords(records, ComponentGaugeUSSBytes); len(got) != 0 {
		t.Errorf("component_uss_bytes emitted for a zero USS total: %v", got)
	}
}

// TestComponentGPUIsRecordedOnlyWhenPositive verifies the component GPU gauges
// are only emitted for positive totals, matching the Python guards.
func TestComponentGPUIsRecordedOnlyWhenPositive(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), nil, false)
	r := requireSingleRecord(t, records, ComponentGaugeGPUMemoryMB)
	if r.Value != 2048 {
		t.Errorf("worker component_gpu_memory_mb = %v, want 2048", r.Value)
	}
	pct := requireSingleRecord(t, records, ComponentGaugeGPUPercentage)
	if pct.Value != 30 {
		t.Errorf("worker component_gpu_percentage = %v, want 30", pct.Value)
	}
	if _, ok := pct.Tags["pid"]; ok {
		t.Errorf("worker component record must not carry a pid tag: %v", pct.Tags)
	}
}

// TestWorkerRecordsGroupByCommandName verifies workers are grouped by the first
// cmdline token and aggregated, matching the Python generate_worker_stats_record
// grouping.
func TestWorkerRecordsGroupByCommandName(t *testing.T) {
	stats := metricRecordStats()
	stats["workers"] = []interface{}{
		map[string]interface{}{
			"pid": 10, "cmdline": []string{"ray::F"}, "cpu_percent": 1.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0, "shared": 0.0},
			"num_fds":     1,
		},
		map[string]interface{}{
			"pid": 11, "cmdline": []string{"ray::F"}, "cpu_percent": 2.0,
			"memory_info": map[string]interface{}{"rss": 3_000_000.0, "shared": 0.0},
			"num_fds":     2,
		},
	}
	records := recordsFor(t, stats, nil, false)

	// The component gauges are shared across all components and distinguished by
	// the Component tag, so the worker group must be selected by its name.
	group := recordsOfComponent(records, "ray::F")
	cpu := requireSingleRecord(t, group, ComponentGaugeCPUPercentage)
	if cpu.Value != 3.0 {
		t.Errorf("grouped worker cpu = %v, want 3.0 (1.0 + 2.0)", cpu.Value)
	}
	if _, ok := cpu.Tags["pid"]; ok {
		t.Errorf("grouped worker record must not carry a pid tag: %v", cpu.Tags)
	}
	// Both workers share the group name, so the group is one record, not two.
	if n := len(findRecords(group, ComponentGaugeCPUPercentage)); n != 1 {
		t.Errorf("worker component_cpu_percentage records = %d, want 1 aggregated group", n)
	}
	rss := requireSingleRecord(t, group, ComponentGaugeRSSMB)
	if rss.Value != 4 {
		t.Errorf("grouped worker component_rss_mb = %v, want 4 (1 MB + 3 MB)", rss.Value)
	}
	fds := requireSingleRecord(t, group, ComponentGaugeNumFDs)
	if fds.Value != 3 {
		t.Errorf("grouped worker component_num_fds = %v, want 3 (1 + 2)", fds.Value)
	}

	// Two distinct cmdline groups must stay separate.
	stats["workers"] = []interface{}{
		map[string]interface{}{
			"pid": 10, "cmdline": []string{"ray::F"}, "cpu_percent": 4.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0}, "num_fds": 1,
		},
		map[string]interface{}{
			"pid": 11, "cmdline": []string{"ray::G"}, "cpu_percent": 6.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0}, "num_fds": 1,
		},
	}
	grouped := recordsFor(t, stats, nil, false)
	if got := requireSingleRecord(t, recordsOfComponent(grouped, "ray::F"), ComponentGaugeCPUPercentage).Value; got != 4 {
		t.Errorf("ray::F group cpu = %v, want 4", got)
	}
	if got := requireSingleRecord(t, recordsOfComponent(grouped, "ray::G"), ComponentGaugeCPUPercentage).Value; got != 6 {
		t.Errorf("ray::G group cpu = %v, want 6", got)
	}
}

// workerOnlyStats builds a minimal stats map whose only component is the worker
// list, so a reset assertion observes exactly the stale-worker records without
// the raylet/agent/additional components of the full fixture.
func workerOnlyStats(workers []interface{}) map[string]interface{} {
	return map[string]interface{}{"ip": "10.0.0.7", "workers": workers}
}

// TestStaleWorkerRecordsResetToZero verifies a worker group that disappeared
// since the previous cycle has all six component gauges reset to zero, matching
// the Python stale-process reset.
func TestStaleWorkerRecordsResetToZero(t *testing.T) {
	state := &recordsState{}
	first := workerOnlyStats([]interface{}{
		map[string]interface{}{
			"pid": 10, "cmdline": []string{"ray::Gone"}, "cpu_percent": 5.0,
			"memory_info":      map[string]interface{}{"rss": 1_000_000.0, "shared": 512.0},
			"memory_full_info": map[string]interface{}{"uss": 2_000_000.0, "pss": 3_000_000.0},
			"num_fds":          1,
		},
	})
	// The first cycle reports the live worker: every component gauge is present.
	live := state.toRecords(first, nil, false, globalTestTags())
	if got := findRecords(live, ComponentGaugeCPUPercentage); len(got) != 1 || got[0].Value != 5.0 {
		t.Fatalf("first cycle component_cpu_percentage = %v, want one record of 5.0", got)
	}

	// The second cycle has no workers, so the previous group is stale.
	records := state.toRecords(workerOnlyStats([]interface{}{}), nil, false, globalTestTags())

	found := findRecords(records, ComponentGaugeCPUPercentage)
	if len(found) != 1 {
		t.Fatalf("reset component_cpu_percentage records = %d, want 1", len(found))
	}
	if found[0].Value != 0 {
		t.Errorf("stale worker cpu = %v, want 0", found[0].Value)
	}
	assertTag(t, found[0], "Component", "ray::Gone")

	for _, gauge := range []string{
		ComponentGaugeSharedBytes, ComponentGaugeRSSMB, ComponentGaugeRSSBytes,
		ComponentGaugeUSSMB, ComponentGaugeUSSBytes, ComponentGaugeNumFDs,
	} {
		reset := findRecords(records, gauge)
		if len(reset) != 1 || reset[0].Value != 0 {
			t.Errorf("stale worker %s not reset to zero: %v", gauge, reset)
		}
	}

	// A third cycle must not repeat the reset: the stale set is consumed.
	third := state.toRecords(workerOnlyStats([]interface{}{}), nil, false, globalTestTags())
	if got := findRecords(third, ComponentGaugeCPUPercentage); len(got) != 0 {
		t.Errorf("stale worker reset repeated on a later cycle: %v", got)
	}
}

// TestStaleGPUWorkerRecordsResetToZero verifies a worker group that stopped
// using a GPU has only the two component GPU gauges reset, matching the Python
// _generate_reseted_gpu_stats_record.
func TestStaleGPUWorkerRecordsResetToZero(t *testing.T) {
	state := &recordsState{}
	first := metricRecordStats()
	first["workers"] = []interface{}{
		map[string]interface{}{
			"pid": 10, "cmdline": []string{"ray::GPUGone"}, "cpu_percent": 1.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0},
			"num_fds":     1, "gpu_memory_usage": 512, "gpu_utilization": 10,
		},
	}
	state.toRecords(first, nil, false, globalTestTags())

	// The same worker is still alive but no longer reports GPU usage.
	second := metricRecordStats()
	second["workers"] = []interface{}{
		map[string]interface{}{
			"pid": 10, "cmdline": []string{"ray::GPUGone"}, "cpu_percent": 1.0,
			"memory_info": map[string]interface{}{"rss": 1_000_000.0},
			"num_fds":     1,
		},
	}
	records := state.toRecords(second, nil, false, globalTestTags())

	for _, gauge := range []string{ComponentGaugeGPUMemoryMB, ComponentGaugeGPUPercentage} {
		found := findRecords(records, gauge)
		var zeroed []gaugeRecord
		for _, r := range found {
			if r.Tags["Component"] == "ray::GPUGone" && r.Value == 0 {
				zeroed = append(zeroed, r)
			}
		}
		if len(zeroed) != 1 {
			t.Errorf("%s: stale GPU worker not reset to zero: %v", gauge, found)
		}
	}
}

// TestStaleAdditionalComponentRecordsResetToZero verifies an additional
// component that disappeared has its gauges reset, matching the Python
// additional-components reset block.
func TestStaleAdditionalComponentRecordsResetToZero(t *testing.T) {
	additionalOnly := func(additional map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"ip": "10.0.0.7", "additional": additional}
	}

	state := &recordsState{}
	first := additionalOnly(map[string]interface{}{
		"log_monitor": []interface{}{
			map[string]interface{}{
				"pid": 4, "cpu_percent": 3.5,
				"memory_info":      map[string]interface{}{"rss": 40_000_000.0, "shared": 256.0},
				"memory_full_info": map[string]interface{}{"uss": 1_000_000.0, "pss": 2_000_000.0},
				"num_fds":          6,
			},
		},
	})
	live := state.toRecords(first, nil, false, globalTestTags())
	if got := findRecords(live, ComponentGaugeCPUPercentage); len(got) != 1 || got[0].Value != 3.5 {
		t.Fatalf("first cycle component_cpu_percentage = %v, want one record of 3.5", got)
	}
	// The additional component carries its pid as a tag, unlike the worker group.
	assertTag(t, findRecords(live, ComponentGaugeCPUPercentage)[0], "pid", "4")

	records := state.toRecords(additionalOnly(map[string]interface{}{}), nil, false, globalTestTags())

	found := findRecords(records, ComponentGaugeCPUPercentage)
	if len(found) != 1 {
		t.Fatalf("reset records = %d, want 1 for the vanished log_monitor", len(found))
	}
	if found[0].Value != 0 {
		t.Errorf("vanished log_monitor cpu = %v, want 0", found[0].Value)
	}
	assertTag(t, found[0], "Component", "log_monitor")
	// The reset record mirrors the reported one, so it carries no pid.
	if pid, ok := found[0].Tags["pid"]; ok {
		t.Errorf("reset record unexpectedly carries pid=%q", pid)
	}
}

// TestClusterRecordsHeadOnlyWithAutoscalerReport verifies the three cluster
// gauges are produced per node type from the autoscaler report, matching the
// Python cluster section.
func TestClusterRecordsHeadOnlyWithAutoscalerReport(t *testing.T) {
	clusterStats := map[string]interface{}{
		"autoscaler_report": map[string]interface{}{
			"active_nodes": map[string]interface{}{"worker": 3, "head": 1},
			"failed_nodes": []interface{}{
				[]interface{}{"10.0.0.1", "worker"},
				[]interface{}{"10.0.0.2", "worker"},
				[]interface{}{"10.0.0.3", "head"},
			},
			"pending_nodes": []interface{}{
				[]interface{}{"10.0.0.4", "worker", "waiting"},
			},
		},
	}
	records := recordsFor(t, metricRecordStats(), clusterStats, true)

	active := findRecords(records, GaugeClusterActiveNodes)
	if len(active) != 2 {
		t.Fatalf("cluster_active_nodes records = %d, want 2", len(active))
	}
	activeBy := map[string]float64{}
	for _, r := range active {
		activeBy[r.Tags["node_type"]] = r.Value
		assertTag(t, r, "Version", "2.53.0")
		assertTag(t, r, "SessionName", "session_test")
	}
	if activeBy["worker"] != 3 || activeBy["head"] != 1 {
		t.Errorf("cluster_active_nodes = %v, want map[worker:3 head:1]", activeBy)
	}

	failedBy := map[string]float64{}
	for _, r := range findRecords(records, GaugeClusterFailedNodes) {
		failedBy[r.Tags["node_type"]] = r.Value
	}
	if failedBy["worker"] != 2 || failedBy["head"] != 1 {
		t.Errorf("cluster_failed_nodes = %v, want map[worker:2 head:1]", failedBy)
	}

	pending := findRecords(records, GaugeClusterPendingNodes)
	if len(pending) != 1 || pending[0].Value != 1 {
		t.Fatalf("cluster_pending_nodes = %v, want one worker record of value 1", pending)
	}
	assertTag(t, pending[0], "node_type", "worker")
}

// TestClusterRecordsOmittedWithoutAutoscalerReport verifies the cluster gauges
// are skipped when the autoscaler has not published a report yet, matching the
// Python `if "autoscaler_report" in cluster_stats` guard.
func TestClusterRecordsOmittedWithoutAutoscalerReport(t *testing.T) {
	records := recordsFor(t, metricRecordStats(), map[string]interface{}{}, true)
	for _, gauge := range []string{GaugeClusterActiveNodes, GaugeClusterFailedNodes, GaugeClusterPendingNodes} {
		if got := findRecords(records, gauge); len(got) != 0 {
			t.Errorf("%s emitted without an autoscaler report: %v", gauge, got)
		}
	}
}

// TestClusterRecordsOmittedOnWorkerNode verifies a non-head node never emits
// cluster gauges, matching the Python `and self._is_head_node` guard.
func TestClusterRecordsOmittedOnWorkerNode(t *testing.T) {
	clusterStats := map[string]interface{}{
		"autoscaler_report": map[string]interface{}{
			"active_nodes": map[string]interface{}{"worker": 3},
		},
	}
	records := recordsFor(t, metricRecordStats(), clusterStats, false)
	if got := findRecords(records, GaugeClusterActiveNodes); len(got) != 0 {
		t.Errorf("cluster_active_nodes emitted on a worker node: %v", got)
	}
}

// TestGlobalTagsMergedIntoEveryRecord verifies the global Version/SessionName
// tags reach every record type, matching the Python record_and_export merge.
func TestGlobalTagsMergedIntoEveryRecord(t *testing.T) {
	clusterStats := map[string]interface{}{
		"autoscaler_report": map[string]interface{}{
			"active_nodes": map[string]interface{}{"worker": 1},
		},
	}
	records := recordsFor(t, metricRecordStats(), clusterStats, true)
	if len(records) == 0 {
		t.Fatal("no records generated")
	}
	for _, r := range records {
		if r.Tags["Version"] != "2.53.0" {
			t.Errorf("%s missing global Version tag: %v", r.Gauge.Name, r.Tags)
		}
		if r.Tags["SessionName"] != "session_test" {
			t.Errorf("%s missing global SessionName tag: %v", r.Gauge.Name, r.Tags)
		}
	}
}

// TestToRecordsCoversAllRecordGauges verifies every non-reset gauge that the
// generator is expected to emit appears in a fully populated head-node cycle,
// so a section silently dropping out of toRecords is caught.
func TestToRecordsCoversAllRecordGauges(t *testing.T) {
	clusterStats := map[string]interface{}{
		"autoscaler_report": map[string]interface{}{
			"active_nodes":  map[string]interface{}{"worker": 1},
			"failed_nodes":  []interface{}{},
			"pending_nodes": []interface{}{},
		},
	}
	records := recordsFor(t, metricRecordStats(), clusterStats, true)
	present := map[string]bool{}
	for _, r := range records {
		present[r.Gauge.Name] = true
	}

	want := []string{
		GaugeClusterActiveNodes,
		GaugeNodeCPUUtilization, GaugeNodeCPUCount,
		GaugeNodeMemUsed, GaugeNodeMemAvailable, GaugeNodeMemTotal, GaugeNodeMemSharedBytes,
		GaugeNodeMemUsedHost, GaugeNodeMemTotalHost,
		GaugeNodeSwapUsed, GaugeNodeSwapTotal, GaugeNodeSwapUtilization,
		GaugeNodeCgroupMemUsed, GaugeNodeCgroupMemTotal,
		GaugeNodeGPUsAvailable, GaugeNodeGPUsUtilization, GaugeNodeGRAMUsed, GaugeNodeGRAMAvaliable,
		GaugeNodeGPUPower, GaugeNodeGPUTemperature,
		GaugeTpuTensorcoreUtilization, GaugeTpuMemoryBandwidth, GaugeTpuDutyCycle, GaugeTpuMemoryUsed, GaugeTpuMemoryTotal,
		GaugeNodeDiskIORead, GaugeNodeDiskIOWrite, GaugeNodeDiskIOReadCount, GaugeNodeDiskIOWriteCount,
		GaugeNodeDiskIOReadSpeed, GaugeNodeDiskIOWriteSpeed, GaugeNodeDiskReadIOPS, GaugeNodeDiskWriteIOPS,
		GaugeNodeDiskUsage, GaugeNodeDiskFree, GaugeNodeDiskUtilizationPct,
		GaugeNodeNetworkSent, GaugeNodeNetworkReceived, GaugeNodeNetworkSendSpeed, GaugeNodeNetworkReceiveSpeed,
		ComponentGaugeCPUPercentage, ComponentGaugeSharedBytes,
		ComponentGaugeRSSMB, ComponentGaugeRSSBytes,
		ComponentGaugeUSSMB, ComponentGaugeUSSBytes, ComponentGaugeNumFDs,
		ComponentGaugeGPUMemoryMB, ComponentGaugeGPUPercentage,
	}
	for _, name := range want {
		if !present[name] {
			t.Errorf("gauge %s missing from a fully populated collection cycle", name)
		}
	}
	t.Logf("emitted %d records across %d distinct gauges", len(records), len(present))
}

// TestCountsByNodeType verifies the autoscaler active_nodes mapping is summed
// per node type.
func TestCountsByNodeType(t *testing.T) {
	got := countsByNodeType(map[string]interface{}{"worker": float64(3), "head": 1})
	if got["worker"] != 3 || got["head"] != 1 {
		t.Errorf("countsByNodeType = %v, want map[worker:3 head:1]", got)
	}
	if len(countsByNodeType(nil)) != 0 {
		t.Errorf("countsByNodeType(nil) = %v, want empty", countsByNodeType(nil))
	}
}

// TestCountsOfPairs verifies the failed/pending node tuples are counted by their
// second element (the node type), as published by the autoscaler.
func TestCountsOfPairs(t *testing.T) {
	got := countsOfPairs([]interface{}{
		[]interface{}{"10.0.0.1", "worker"},
		[]interface{}{"10.0.0.2", "worker"},
		[]interface{}{"10.0.0.3", "head"},
	})
	if got["worker"] != 2 || got["head"] != 1 {
		t.Errorf("countsOfPairs = %v, want map[worker:2 head:1]", got)
	}
	// Malformed entries are skipped rather than panicking.
	if got := countsOfPairs([]interface{}{"not-a-tuple", []interface{}{"only-one"}}); len(got) != 0 {
		t.Errorf("countsOfPairs(malformed) = %v, want empty", got)
	}
}

// TestDifference verifies the stale-name set difference is computed and sorted.
func TestDifference(t *testing.T) {
	got := difference(
		map[string]struct{}{"b": {}, "a": {}, "c": {}},
		map[string]struct{}{"b": {}},
	)
	want := []string{"a", "c"}
	if len(got) != len(want) {
		t.Fatalf("difference = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("difference[%d] = %q, want %q (deterministic order)", i, got[i], want[i])
		}
	}
}

// TestMergeTagsLastWins verifies a key defined by several maps takes the value
// of the last map, matching the Python `{**tags, **global_tags}` merge order.
func TestMergeTagsLastWins(t *testing.T) {
	got := mergeTags(
		map[string]string{"a": "1", "b": "1"},
		map[string]string{"b": "2", "c": "2"},
	)
	want := map[string]string{"a": "1", "b": "2", "c": "2"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("mergeTags[%q] = %q, want %q", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("mergeTags size = %d, want %d (%v)", len(got), len(want), got)
	}
}
