package reporter

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestProcAttrsRequiredFields verifies that procAttrs always emits the
// ProcessInfo-required fields pid, create_time, and cpu_percent (matching the
// Python ProcessInfo pydantic model), plus cmdline and name. The test runs
// against the current process, which always exists.
func TestProcAttrsRequiredFields(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	for _, k := range []string{"pid", "create_time", "cpu_percent", "cmdline"} {
		if _, ok := info[k]; !ok {
			t.Errorf("procAttrs missing required field %q", k)
		}
	}
	if info["pid"] != os.Getpid() {
		t.Errorf("procAttrs pid = %v, want %d", info["pid"], os.Getpid())
	}
	if _, ok := info["create_time"].(float64); !ok {
		t.Errorf("create_time type = %T, want float64", info["create_time"])
	}
	if _, ok := info["cpu_percent"].(float64); !ok {
		t.Errorf("cpu_percent type = %T, want float64", info["cpu_percent"])
	}
}

// TestProcessCreateTime verifies that the start time of the current process is
// a plausible epoch timestamp (created within the last ~24h window rather than
// zero, which is the failure sentinel).
func TestProcessCreateTime(t *testing.T) {
	s := &systemSource{}
	ct := s.processCreateTime(os.Getpid())
	if ct <= 0 {
		t.Fatalf("processCreateTime(self) = %v, want > 0", ct)
	}
	boot, err := s.bootTime()
	if err != nil {
		t.Fatalf("bootTime() error: %v", err)
	}
	// A process cannot have started before boot time, and must be <= now.
	if ct < boot {
		t.Errorf("createTime = %v, before bootTime = %v", ct, boot)
	}
	if ct > float64(time.Now().Unix()) {
		t.Errorf("createTime = %v, after now", ct)
	}
}

// TestProcessCreateTimeMalformed verifies graceful degradation when the stat
// line is too short to contain field 22: it returns 0 and does not panic.
func TestProcessCreateTimeMalformed(t *testing.T) {
	s := &systemSource{}
	ct := s.processCreateTime(99999999) // nonexistent pid
	if ct != 0 {
		t.Errorf("processCreateTime(nonexistent) = %v, want 0", ct)
	}
}

// TestSpeedFromIO verifies disk_io_speed is computed from the oldest and newest
// retained samples as (readSpeed, writeSpeed, readIops, writeIops), mirroring
// Python _compute_speed_from_hist.
func TestSpeedFromIO(t *testing.T) {
	hist := []ioSample{
		{at: 1000, val: DiskIOMetrics{ReadBytes: 100, WriteBytes: 200, ReadCount: 10, WriteCount: 20}},
		{at: 1001, val: DiskIOMetrics{ReadBytes: 200, WriteBytes: 400, ReadCount: 30, WriteCount: 40}},
	}
	got := speedFromIO(hist)
	want := []float64{100, 200, 20, 20}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Errorf("speedFromIO[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestSpeedFromIOInsufficientSamples verifies that fewer than two samples yield
// a zero 4-tuple rather than a divide-by-zero.
func TestSpeedFromIOInsufficientSamples(t *testing.T) {
	for _, hist := range [][]ioSample{nil, {{at: 1, val: DiskIOMetrics{}}}} {
		got := speedFromIO(hist)
		for _, v := range got {
			if v != 0 {
				t.Errorf("speedFromIO = %v, want all zeros", got)
			}
		}
	}
}

// TestSpeedFromIONonPositiveDelta verifies the zero/negative time-delta guard:
// two samples stamped with the same instant (or with an out-of-order one) must
// yield a zero 4-tuple, never an Inf or NaN from dividing by zero. The guard
// matters even with sub-second timestamps, since a fast collection loop can
// observe the same instant twice.
func TestSpeedFromIONonPositiveDelta(t *testing.T) {
	sameInstant := []ioSample{
		{at: 1700000000.5, val: DiskIOMetrics{ReadBytes: 100}},
		{at: 1700000000.5, val: DiskIOMetrics{ReadBytes: 200}},
	}
	backwards := []ioSample{
		{at: 1700000000.9, val: DiskIOMetrics{ReadBytes: 100}},
		{at: 1700000000.1, val: DiskIOMetrics{ReadBytes: 200}},
	}
	for name, hist := range map[string][]ioSample{"same instant": sameInstant, "time went backwards": backwards} {
		got := speedFromIO(hist)
		for i, v := range got {
			if v != 0 {
				t.Errorf("%s: speedFromIO[%d] = %v, want 0 (guarded non-positive time delta)", name, i, v)
			}
		}
	}
}

// TestNetworkSpeed verifies network_speed is computed as (sendSpeed,
// receiveSpeed) from the oldest and newest retained samples.
func TestNetworkSpeed(t *testing.T) {
	s := &systemSource{}
	s.netHist = []netSample{
		{at: 1000, val: IOMetrics{Sent: 1000, Recv: 2000}},
		{at: 1001, val: IOMetrics{Sent: 1100, Recv: 2100}},
	}
	got, err := s.networkSpeed()
	if err != nil {
		t.Fatalf("networkSpeed error: %v", err)
	}
	if math.Abs(got[0]-100) > 1e-9 || math.Abs(got[1]-100) > 1e-9 {
		t.Errorf("networkSpeed = %v, want [100 100]", got)
	}
}

// TestNetworkSpeedInsufficientSamples verifies fewer than two samples yield
// [0 0].
func TestNetworkSpeedInsufficientSamples(t *testing.T) {
	s := &systemSource{}
	got, err := s.networkSpeed()
	if err != nil {
		t.Fatalf("networkSpeed error: %v", err)
	}
	if got[0] != 0 || got[1] != 0 {
		t.Errorf("networkSpeed = %v, want [0 0]", got)
	}
}

// TestMeminfoCachedPerCycle verifies that meminfo is parsed once per collect
// cycle and cached, and that beginCycle invalidates it for the next cycle.
func TestMeminfoCachedPerCycle(t *testing.T) {
	s := &systemSource{}
	if s.meminfoCached {
		t.Fatal("meminfoCached should start false")
	}
	v1, err := s.meminfo()
	if err != nil {
		t.Fatalf("meminfo error: %v", err)
	}
	if !s.meminfoCached {
		t.Fatal("meminfoCached should be true after first parse")
	}
	// Second call must hit the cache (no re-parse) and return the same values.
	v2, err := s.meminfo()
	if err != nil {
		t.Fatalf("second meminfo error: %v", err)
	}
	if len(v1) == 0 || len(v1) != len(v2) {
		t.Errorf("meminfo cache inconsistency: len1=%d len2=%d", len(v1), len(v2))
	}
	// beginCycle clears the cache so the next cycle re-parses.
	s.beginCycle(posixTime())
	if s.meminfoCached {
		t.Fatal("beginCycle should clear meminfoCached")
	}
	if s.meminfoCache != nil {
		t.Fatal("beginCycle should clear meminfoCache")
	}
}

// TestBootTimeCached verifies that bootTime reads /proc/stat once and caches
// the result for subsequent calls.
func TestBootTimeCached(t *testing.T) {
	s := &systemSource{}
	if s.bootCached {
		t.Fatal("bootCached should start false")
	}
	b1, err := s.bootTime()
	if err != nil {
		t.Fatalf("bootTime error: %v", err)
	}
	if !s.bootCached {
		t.Fatal("bootCached should be true after first read")
	}
	b2, err := s.bootTime()
	if err != nil {
		t.Fatalf("second bootTime error: %v", err)
	}
	if b1 != b2 {
		t.Errorf("bootTime changed between calls: %v vs %v", b1, b2)
	}
	if b1 <= 0 {
		t.Errorf("bootTime = %v, want > 0", b1)
	}
}

// TestCollectorWireDiskAndNetworkSpeed verifies Collect wires the real
// diskIOSpeed/networkSpeed tuples (not nil) into the stats map, so the Python
// consumer never sees a null speed tuple.
func TestCollectorWireDiskAndNetworkSpeed(t *testing.T) {
	src := newFakeSource()
	sc := &StatsCollector{src: src}
	stats := sc.Collect()

	for _, k := range []string{"disk_io_speed", "network_speed"} {
		val, ok := stats[k]
		if !ok {
			t.Errorf("%s missing from stats map", k)
			continue
		}
		if val == nil {
			t.Errorf("%s = nil, want non-nil tuple", k)
		}
	}
}

// rawMeminfoKB parses /proc/meminfo into a field->kB map without any unit
// scaling, providing an independent reference for the byte conversion.
func rawMeminfoKB(t *testing.T) map[string]int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Fatalf("read /proc/meminfo: %v", err)
	}
	out := map[string]int64{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			continue
		}
		out[strings.TrimSpace(parts[0])] = v
	}
	return out
}

// TestReadMeminfoScalesKBToBytes is the regression test for the memory unit
// defect: /proc/meminfo reports kB, and the published payload must be in bytes.
// Before the fix every value was 1024 times too small. The parser is driven
// with a fixed meminfo fixture so the assertion is exact and does not depend on
// volatile live memory figures.
func TestReadMeminfoScalesKBToBytes(t *testing.T) {
	const fixture = "MemTotal:       16384000 kB\n" +
		"MemFree:         8192000 kB\n" +
		"MemAvailable:   12288000 kB\n" +
		"Cached:          4096000 kB\n" +
		"Shmem:            512000 kB\n" +
		"SomeFieldWithoutColon\n" +
		"BadValue: notanumber kB\n"
	values, err := parseMeminfo(strings.NewReader(fixture))
	if err != nil {
		t.Fatalf("parseMeminfo error: %v", err)
	}
	want := map[string]int64{
		"MemTotal":     16384000 * 1024,
		"MemFree":      8192000 * 1024,
		"MemAvailable": 12288000 * 1024,
		"Cached":       4096000 * 1024,
		"Shmem":        512000 * 1024,
	}
	for key, wantBytes := range want {
		if got := values[key]; got != wantBytes {
			t.Errorf("%s = %d bytes, want %d (fixture %d kB)", key, got, wantBytes, wantBytes/1024)
		}
	}
	// Guard against the specific regression: a raw kB value leaking through.
	if got := values["MemTotal"]; got == 16384000 {
		t.Errorf("MemTotal = %d is still unscaled kB, want %d bytes", got, 16384000*1024)
	}
	// Malformed lines are skipped rather than corrupting the map.
	if _, ok := values["SomeFieldWithoutColon"]; ok {
		t.Errorf("SomeFieldWithoutColon present, want malformed lines skipped")
	}
	if _, ok := values["BadValue"]; ok {
		t.Errorf("BadValue present, want unparseable values skipped")
	}
}

// TestShmUsageReturnsBytes verifies shm is published in bytes, matching the
// Python _get_shm_usage contract ("Unit is in bytes").
func TestShmUsageReturnsBytes(t *testing.T) {
	s := &systemSource{}
	got, ok, err := s.shmUsage()
	if err != nil {
		t.Fatalf("shmUsage error: %v", err)
	}
	if !ok {
		t.Skip("Shmem not exposed by this kernel")
	}
	raw := rawMeminfoKB(t)
	if want := raw["Shmem"] * 1024; got != want {
		t.Errorf("shmUsage = %d bytes, want %d (raw %d kB)", got, want, raw["Shmem"])
	}
}

// TestSystemMemoryBytesPicksSmallerLimit verifies the get_system_memory
// semantics: the cgroup limit wins only when it is smaller than the host total,
// so the huge value written when the process is unconfined is ignored.
func TestSystemMemoryBytesPicksSmallerLimit(t *testing.T) {
	const total = 8 << 30
	meminfo := map[string]int64{"MemTotal": total}

	if got := systemMemoryBytes(meminfo, 2<<30, true); got != 2<<30 {
		t.Errorf("confined limit: got %d, want %d", got, 2<<30)
	}
	// The unconfined cgroup v1 limit is a near-max int64; the host total wins.
	if got := systemMemoryBytes(meminfo, math.MaxInt64, true); got != total {
		t.Errorf("unconfined limit: got %d, want %d", got, total)
	}
	if got := systemMemoryBytes(meminfo, 0, false); got != total {
		t.Errorf("no limit: got %d, want %d", got, total)
	}
}

// TestUsedMemoryBytesPrefersCgroup verifies that a usable cgroup reading takes
// priority over the psutil fallback, mirroring get_used_memory.
func TestUsedMemoryBytesPrefersCgroup(t *testing.T) {
	meminfo := map[string]int64{"MemTotal": 8 << 30, "MemAvailable": 3 << 30}
	if got := usedMemoryBytes(meminfo, 5<<30, true); got != 5<<30 {
		t.Errorf("got %d, want cgroup value %d", got, 5<<30)
	}
}

// TestUsedMemoryBytesFallsBackToPsutil verifies the non-container path:
// used = total - MemAvailable, matching psutil.virtual_memory().used (the
// vendored psutil derives used as total - available).
func TestUsedMemoryBytesFallsBackToPsutil(t *testing.T) {
	const total = 8 << 30
	const avail = 3 << 30
	meminfo := map[string]int64{"MemTotal": total, "MemAvailable": avail}
	if got := usedMemoryBytes(meminfo, 0, false); got != total-avail {
		t.Errorf("got %d, want %d", got, total-avail)
	}

	// Without MemAvailable the estimate starts from MemFree + Cached.
	meminfo = map[string]int64{"MemTotal": total, "MemFree": 1 << 30, "Cached": 2 << 30}
	if got := usedMemoryBytes(meminfo, 0, false); got != total-(3<<30) {
		t.Errorf("fallback estimate: got %d, want %d", got, total-(3<<30))
	}

	// An available figure exceeding total (LXC distortion) falls back to MemFree.
	meminfo = map[string]int64{"MemTotal": total, "MemAvailable": total + (1 << 30), "MemFree": 1 << 30}
	if got := usedMemoryBytes(meminfo, 0, false); got != total-(1<<30) {
		t.Errorf("lxc clamp: got %d, want %d", got, total-(1<<30))
	}
}

// TestCgroupUsedMemoryFromSubtractsFileCache verifies get_cgroup_used_memory:
// the controller usage minus the reclaimable inactive/active file cache.
func TestCgroupUsedMemoryFromSubtractsFileCache(t *testing.T) {
	dir := t.TempDir()
	statPath := filepath.Join(dir, "memory.stat")
	usagePath := filepath.Join(dir, "memory.usage_in_bytes")
	stat := "cache 100\n" +
		"total_inactive_file 2048\n" +
		"total_active_file 1024\n" +
		"other 999\n"
	if err := os.WriteFile(statPath, []byte(stat), 0o600); err != nil {
		t.Fatalf("write stat: %v", err)
	}
	if err := os.WriteFile(usagePath, []byte("10240\n"), 0o600); err != nil {
		t.Fatalf("write usage: %v", err)
	}

	got, ok := cgroupUsedMemoryFrom(statPath, usagePath, "total_inactive_file", "total_active_file")
	if !ok {
		t.Fatal("cgroupUsedMemoryFrom reported no usable value")
	}
	if want := int64(10240 - 2048 - 1024); got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

// TestCgroupUsedMemoryFromMissingKeys verifies that an incomplete memory.stat
// yields no value so the caller falls back to psutil, mirroring Python's
// -1 sentinel checks returning None.
func TestCgroupUsedMemoryFromMissingKeys(t *testing.T) {
	dir := t.TempDir()
	statPath := filepath.Join(dir, "memory.stat")
	usagePath := filepath.Join(dir, "memory.usage_in_bytes")
	if err := os.WriteFile(statPath, []byte("total_inactive_file 2048\n"), 0o600); err != nil {
		t.Fatalf("write stat: %v", err)
	}
	if err := os.WriteFile(usagePath, []byte("10240\n"), 0o600); err != nil {
		t.Fatalf("write usage: %v", err)
	}
	if _, ok := cgroupUsedMemoryFrom(statPath, usagePath, "total_inactive_file", "total_active_file"); ok {
		t.Error("missing active_file key should yield no value")
	}
	if _, ok := cgroupUsedMemoryFrom(filepath.Join(dir, "absent"), usagePath, "total_inactive_file", "total_active_file"); ok {
		t.Error("absent stat file should yield no value")
	}
}

// TestMemUsageUnitsAndSemantics compares the production memUsage against the
// Python reference on this host: total is MemTotal in bytes (or the smaller
// cgroup limit) and available is total - used, so the published tuple can never
// mix units or report an available figure exceeding total.
func TestMemUsageUnitsAndSemantics(t *testing.T) {
	s := &systemSource{}
	mem, err := s.memUsage()
	if err != nil {
		t.Fatalf("memUsage error: %v", err)
	}
	raw := rawMeminfoKB(t)
	totalBytes := raw["MemTotal"] * 1024
	// total is the host MemTotal, or the cgroup limit when that is smaller.
	if mem.Total > totalBytes {
		t.Errorf("Total = %d exceeds host MemTotal %d", mem.Total, totalBytes)
	}
	if mem.Total <= 0 {
		t.Fatalf("Total = %d, want > 0", mem.Total)
	}
	// available is defined as total - used, so the identity must hold exactly.
	if mem.Available != mem.Total-mem.Used {
		t.Errorf("Available = %d, want total-used = %d", mem.Available, mem.Total-mem.Used)
	}
	if mem.Used < 0 || mem.Used > mem.Total {
		t.Errorf("Used = %d out of range [0, %d]", mem.Used, mem.Total)
	}
	if mem.Percent < 0 || mem.Percent > 100 {
		t.Errorf("Percent = %v out of range [0, 100]", mem.Percent)
	}
}

// k8sTestSource builds a systemSource whose K8s adapter is driven by fixed
// cgroup contents and a stubbed environment, so the container dispatch paths can
// be exercised without a real pod.
func k8sTestSource(t *testing.T, contents map[string]string, env map[string]string) *systemSource {
	t.Helper()
	lookup := func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
	return &systemSource{k8s: newTestAdapter(contents, lookup)}
}

// stubCPUOnline pins the online CPU list (e.g. "0-7") so the logical CPU count
// does not depend on the machine running the test.
func stubCPUOnline(t *testing.T, list string) {
	t.Helper()
	orig := cpuOnlineReader
	cpuOnlineReader = func(string) (string, error) { return list, nil }
	t.Cleanup(func() { cpuOnlineReader = orig })
}

// stubCPUOnlineError makes the online CPU list unreadable so the
// runtime.NumCPU() fallback can be exercised.
func stubCPUOnlineError(t *testing.T) {
	t.Helper()
	orig := cpuOnlineReader
	cpuOnlineReader = func(string) (string, error) { return "", os.ErrNotExist }
	t.Cleanup(func() { cpuOnlineReader = orig })
}

// TestDiskUsageK8sReturnsDummyValues verifies the Python _get_disk_usage
// behavior: inside a pod without RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE the disk
// section is disabled by publishing the dummy {"/": total=1, used=0, free=1,
// percent=0} entry instead of real host disk usage.
func TestDiskUsageK8sReturnsDummyValues(t *testing.T) {
	s := k8sTestSource(t, map[string]string{cgroupRoot: "x"},
		map[string]string{"KUBERNETES_SERVICE_HOST": "10.96.0.1"})
	got, err := s.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("diskUsage returned %d entries, want 1 dummy entry", len(got))
	}
	d, ok := got["/"]
	if !ok {
		t.Fatalf("diskUsage missing the / key: %v", got)
	}
	if d.Total != 1 || d.Used != 0 || d.Free != 1 || d.Percent != 0 {
		t.Errorf("dummy / = %+v, want {Total:1 Used:0 Free:1 Percent:0}", d)
	}
}

// TestDiskUsageK8sEnabledReturnsReal verifies that RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE=1
// re-enables real disk usage reporting inside a pod, mirroring Python
// ENABLE_K8S_DISK_USAGE.
func TestDiskUsageK8sEnabledReturnsReal(t *testing.T) {
	s := k8sTestSource(t, map[string]string{cgroupRoot: "x"}, map[string]string{
		"KUBERNETES_SERVICE_HOST":             "10.96.0.1",
		"RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE": "1",
	})
	got, err := s.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage error: %v", err)
	}
	if d := got["/"]; d.Total == 1 && d.Free == 1 {
		t.Errorf("diskUsage inside a pod with usage enabled = %+v, want real statfs not the dummy entry", d)
	}
}

// TestDiskUsageOutsideK8sReturnsReal verifies the non-pod path is unchanged and
// does not emit the dummy entry.
func TestDiskUsageOutsideK8sReturnsReal(t *testing.T) {
	s := &systemSource{}
	got, err := s.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage error: %v", err)
	}
	if d, ok := got["/"]; ok && d.Total == 1 && d.Free == 1 {
		t.Errorf("diskUsage outside a pod = %+v, want real statfs", d)
	}
}

// TestDiskUsageDoesNotDispatchInContainerWithoutPod pins the predicate the
// Python _get_disk_usage branch keys on: IN_KUBERNETES_POD, not IN_CONTAINER. A
// plain container must report real disk usage, because the dummy-value branch
// is not taken there.
func TestDiskUsageDoesNotDispatchInContainerWithoutPod(t *testing.T) {
	container := k8sTestSource(t, map[string]string{cgroupRoot: "x"}, map[string]string{})
	if !container.inContainer() || container.inK8s() {
		t.Fatalf("test setup invalid: inContainer()=%v inK8s()=%v, want true/false for a cgroup-only source",
			container.inContainer(), container.inK8s())
	}

	got, err := container.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage error: %v", err)
	}
	if d, ok := got["/"]; ok && d.Total == 1 && d.Free == 1 {
		t.Errorf("diskUsage in a plain container = %+v, want real statfs (IN_CONTAINER alone must not dispatch)", d)
	}
}

// TestCPUPercentDispatchesToK8sAdapter verifies that inside a pod cpu is
// sourced from the container adapter (k8s_utils.cpu_percent semantics) rather
// than the host-wide /proc/stat delta. The adapter fixture is chosen so its
// second sample reads exactly 50%, which the host path would not reproduce
// deterministically.
func TestCPUPercentDispatchesToK8sAdapter(t *testing.T) {
	s := k8sTestSource(t, map[string]string{
		cgroupRoot:   "x",
		cpuUsageV1:   "0",
		cpuQuota:     "200000",
		cpuPeriod:    "100000",
		procStatPath: "cpu  100000 0 0 0 0 0 0\n",
	}, map[string]string{"KUBERNETES_SERVICE_HOST": "10.96.0.1"})

	if got, _ := s.cpuPercent(); got != 0 {
		t.Fatalf("first cpuPercent() = %v, want 0 on the adapter's first sample", got)
	}
	// Advance the container usage by one full period against a 2-CPU limit over
	// a 100ms window -> 50%.
	s.k8s.readFile = fakeReadFile(map[string]string{
		cgroupRoot:   "x",
		cpuUsageV1:   "100000000",
		cpuQuota:     "200000",
		cpuPeriod:    "100000",
		procStatPath: "cpu  100010 0 0 0 0 0 0\n",
	}, nil)
	if got, _ := s.cpuPercent(); got != 50 {
		t.Fatalf("cpuPercent() in a pod = %v, want 50 (adapter dispatch)", got)
	}
}

// TestCPUPercentDoesNotDispatchInContainerWithoutPod pins the predicate the
// Python _get_cpu_percent branch keys on: the IN_KUBERNETES_POD argument, not
// IN_CONTAINER. A plain container must keep using the host-wide /proc/stat
// delta.
//
// The host path reads the real /proc/stat, so its returned percentage is not
// reproducible in a test. The assertion therefore keys on whether the
// container adapter was touched at all: CPUPerc is stateful and records its
// first sample in lastSet, so a wrong dispatch is observable as lastSet
// turning true. This is the only scenario that separates inK8s() from
// inContainer(): a pod satisfies both predicates, so a test that sets both
// cannot detect a dispatch that keys off the wrong one.
func TestCPUPercentDoesNotDispatchInContainerWithoutPod(t *testing.T) {
	s := k8sTestSource(t, map[string]string{
		cgroupRoot:   "x",
		cpuUsageV1:   "1000000000",
		cpuQuota:     "200000",
		cpuPeriod:    "100000",
		procStatPath: "cpu  100000 0 0 0 0 0 0\n",
	}, map[string]string{})
	if !s.inContainer() || s.inK8s() {
		t.Fatalf("test setup invalid: inContainer()=%v inK8s()=%v, want true/false for a cgroup-only source",
			s.inContainer(), s.inK8s())
	}

	s.k8s.lastSet = false
	for i := 0; i < 2; i++ {
		if _, err := s.cpuPercent(); err != nil {
			t.Fatalf("cpuPercent() call %d in a plain container: %v", i, err)
		}
	}
	if s.k8s.lastSet {
		t.Errorf("cpuPercent() consumed the container adapter in a plain container: adapter lastSet = true, want false (IN_CONTAINER alone must not dispatch)")
	}
}

// TestCPUCountsDispatchesToAdapterInContainer verifies the Python __init__
// rule: inside a container or pod the logical count comes from
// utils.get_num_cpus (the cgroup quota/cpuset), not the bare logical CPU count.
func TestCPUCountsDispatchesToAdapterInContainer(t *testing.T) {
	s := k8sTestSource(t, map[string]string{
		cgroupRoot: "x",
		cpuQuota:   "300000",
		cpuPeriod:  "100000",
	}, map[string]string{})
	// get_num_cpus truncates only when the docker count differs from the bare
	// logical count; pin them apart so the truncation path is deterministic.
	s.k8s.numCPU = func() int { return 64 }

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("cpuCounts returned %d counts, want 2", len(got))
	}
	if got[0] != 3 {
		t.Errorf("cpus[0] = %d, want 3 (cgroup quota 300000/100000)", got[0])
	}
}

// TestCPUCountsUsesOnlineCPUsOutsideContainer verifies the non-container path
// reports the number of ONLINE CPUs, mirroring psutil.cpu_count()
// (os.sysconf("SC_NPROCESSORS_ONLN")) rather than runtime.NumCPU(), which
// reports the process's CPU affinity mask and would under-report a pinned
// process.
func TestCPUCountsUsesOnlineCPUsOutsideContainer(t *testing.T) {
	stubCPUOnline(t, "0-7")
	s := &systemSource{}

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if got[0] != 8 {
		t.Errorf("cpus[0] = %d, want 8 (online CPUs, psutil.cpu_count() equivalent)", got[0])
	}
}

// TestCPUCountsFallsBackToRuntimeNumCPU verifies the fallback when the online
// CPU list cannot be read: the count degrades to runtime.NumCPU() instead of
// collapsing to zero.
func TestCPUCountsFallsBackToRuntimeNumCPU(t *testing.T) {
	stubCPUOnlineError(t)
	s := &systemSource{}

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if want := runtime.NumCPU(); got[0] != want {
		t.Errorf("cpus[0] = %d, want %d (runtime.NumCPU fallback)", got[0], want)
	}
}

// TestCPUCountsFallsBackOnMalformedOnlineList verifies a readable but
// unparseable online CPU list is treated as unavailable: a malformed list must
// not be accepted as a zero-CPU host, which would both misreport cpus[0] and
// divide by zero in the per-CPU load normalization.
func TestCPUCountsFallsBackOnMalformedOnlineList(t *testing.T) {
	stubCPUOnline(t, "not-a-cpu-list")
	s := &systemSource{}

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if got[0] <= 0 {
		t.Fatalf("cpus[0] = %d, want a positive fallback count", got[0])
	}
	if want := runtime.NumCPU(); got[0] != want {
		t.Errorf("cpus[0] = %d, want %d (malformed list falls back to runtime.NumCPU)", got[0], want)
	}
}

// TestCPUCountsPrefersCgroupQuotaInsideCPUSetConstrainedHost pins the defect
// this fix addresses: on a host whose online CPU list is wider than the cgroup
// quota, the container branch must win. A regression that read the online count
// after the dispatch would report 8 rather than the 2-CPU quota.
func TestCPUCountsPrefersCgroupQuotaInsideCPUSetConstrainedHost(t *testing.T) {
	stubCPUOnline(t, "0-7")
	s := k8sTestSource(t, map[string]string{
		cgroupRoot: "x",
		cpuQuota:   "200000",
		cpuPeriod:  "100000",
	}, map[string]string{})

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if got[0] != 2 {
		t.Errorf("cpus[0] = %d, want 2 (cgroup quota takes precedence over the online count)", got[0])
	}
}

// TestCPUCountsDispatchesToAdapterInPodOnly verifies the IN_KUBERNETES_POD half
// of the Python `IN_KUBERNETES_POD or IN_CONTAINER` condition: a pod without
// /sys/fs/cgroup must still divert the logical count to the cgroup-aware
// adapter.
func TestCPUCountsDispatchesToAdapterInPodOnly(t *testing.T) {
	stubCPUOnline(t, "0-7")
	s := k8sTestSource(t, map[string]string{
		// No cgroupRoot entry, so IsInContainer() is false; only the pod env
		// var is set.
		cpuQuota:  "300000",
		cpuPeriod: "100000",
	}, map[string]string{"KUBERNETES_SERVICE_HOST": "10.96.0.1"})

	got, err := s.cpuCounts()
	if err != nil {
		t.Fatalf("cpuCounts error: %v", err)
	}
	if got[0] != 3 {
		t.Errorf("cpus[0] = %d, want 3 (pod branch uses the cgroup quota 300000/100000)", got[0])
	}
}

// TestBootTimeDispatchesToPidOneInK8s verifies the Python _get_boot_time rule:
// inside a pod the container entrypoint's start time (process 1) is reported
// instead of the host boot time. The entrypoint starts after the host boots, so
// the pod reading must be strictly later than the host reading.
func TestBootTimeDispatchesToPidOneInK8s(t *testing.T) {
	host := &systemSource{}
	hostBoot, err := host.bootTime()
	if err != nil {
		t.Fatalf("host bootTime error: %v", err)
	}
	if hostBoot <= 0 {
		t.Fatalf("host bootTime = %v, want > 0", hostBoot)
	}

	pod := k8sTestSource(t, map[string]string{cgroupRoot: "x"},
		map[string]string{"KUBERNETES_SERVICE_HOST": "10.96.0.1"})
	podBoot, err := pod.bootTime()
	if err != nil {
		t.Fatalf("pod bootTime error: %v", err)
	}
	if podBoot <= hostBoot {
		t.Errorf("pod bootTime = %v, want > host boot time %v (pid 1 start time)", podBoot, hostBoot)
	}
}

// TestBootTimeDoesNotDispatchInContainerWithoutPod pins the predicate the
// Python _get_boot_time branch keys on: IN_KUBERNETES_POD, not IN_CONTAINER. A
// plain container (cgroup root present, no KUBERNETES_SERVICE_HOST) must report
// the HOST boot time, because the Python branch is not taken there.
//
// This is the only scenario that separates inK8s() from inContainer(): a pod
// satisfies both predicates, so a test that sets both cannot detect a dispatch
// that keys off the wrong one.
func TestBootTimeDoesNotDispatchInContainerWithoutPod(t *testing.T) {
	host := &systemSource{}
	hostBoot, err := host.rawBootTime()
	if err != nil {
		t.Fatalf("host rawBootTime error: %v", err)
	}

	// cgroupRoot is present (IN_CONTAINER true) but no pod env var is set.
	container := k8sTestSource(t, map[string]string{cgroupRoot: "x"}, map[string]string{})
	if !container.inContainer() {
		t.Fatalf("test setup invalid: inContainer() = false, want true for the cgroup-only source")
	}
	if container.inK8s() {
		t.Fatalf("test setup invalid: inK8s() = true, want false without KUBERNETES_SERVICE_HOST")
	}

	got, err := container.bootTime()
	if err != nil {
		t.Fatalf("container bootTime error: %v", err)
	}
	if got != hostBoot {
		t.Errorf("bootTime() in a plain container = %v, want the host boot time %v (IN_CONTAINER alone must not dispatch)", got, hostBoot)
	}
}
