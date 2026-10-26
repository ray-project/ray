package reporter

import (
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

// fakeReadFile returns a readFile injection backed by a path->content map so
// tests can emulate cgroup v1/v2 and host proc files without touching the real
// filesystem.
func fakeReadFile(contents map[string]string, missing map[string]bool) readFileFn {
	return func(path string) ([]byte, error) {
		if missing != nil && missing[path] {
			return nil, os.ErrNotExist
		}
		if content, ok := contents[path]; ok {
			return []byte(content), nil
		}
		return nil, os.ErrNotExist
	}
}

// newTestAdapter builds a K8sAdapter wired to default cgroup paths, fake file
// contents, and an env lookup so it never reads the host filesystem.
func newTestAdapter(contents map[string]string, env func(string) (string, bool)) *K8sAdapter {
	return &K8sAdapter{
		cpuUsageV1: cpuUsageV1,
		cpuUsageV2: cpuUsageV2,
		cpuQuota:   cpuQuota,
		cpuPeriod:  cpuPeriod,
		memUsageV1: memUsageV1,
		memLimitV1: memLimitV1,
		memUsageV2: memUsageV2,
		memLimitV2: memLimitV2,
		netDev:     netDevPath,
		procStat:   procStatPath,
		envLookup:  env,
		fileExists: func(path string) bool { return contents[path] != "" },
		readFile:   fakeReadFile(contents, nil),
	}
}

func TestK8sAdapterIsK8sFalse(t *testing.T) {
	a := newTestAdapter(nil, func(string) (string, bool) { return "", false })
	if a.IsK8s() {
		t.Fatalf("IsK8s() = true, want false when KUBERNETES_SERVICE_HOST is unset")
	}
}

func TestK8sAdapterIsK8sTrue(t *testing.T) {
	a := newTestAdapter(nil, func(key string) (string, bool) {
		if key == "KUBERNETES_SERVICE_HOST" {
			return "10.96.0.1", true
		}
		return "", false
	})
	if !a.IsK8s() {
		t.Fatalf("IsK8s() = false, want true when KUBERNETES_SERVICE_HOST is set")
	}
}

func TestK8sAdapterIsInContainerTrue(t *testing.T) {
	contents := map[string]string{cgroupRoot: "ignored"}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	if !a.IsInContainer() {
		t.Fatalf("IsInContainer() = false, want true when /sys/fs/cgroup exists")
	}
}

func TestK8sAdapterIsInContainerFalse(t *testing.T) {
	a := newTestAdapter(map[string]string{}, func(string) (string, bool) { return "", false })
	if a.IsInContainer() {
		t.Fatalf("IsInContainer() = true, want false when /sys/fs/cgroup is absent")
	}
}

// TestCPUPercFirstCallZero verifies that the first CPUPerc call returns 0.0,
// since a CPU usage delta is not yet available, matching the Python k8s_utils.
func TestK8sAdapterCPUPercFirstCallZero(t *testing.T) {
	a := newTestAdapter(nil, func(string) (string, bool) { return "", false })
	if got := a.CPUPerc(); got != 0 {
		t.Fatalf("CPUPerc() first call = %v, want 0", got)
	}
}

// TestCPUPercComputesDelta exercises the quota/period CPU calculation: with a 2
// CPU limit (quota=200000us, period=100000us) and a usage delta of one full
// period (100ms) over a 100ms sample window, the container is using 50% of its
// available capacity.
func TestK8sAdapterCPUPercComputesDelta(t *testing.T) {
	contents := map[string]string{
		cpuUsageV1: "0",
		cpuQuota:   "200000",
		cpuPeriod:  "100000",
		// System tick sum starts at 100000 (100ms of host time in ticks at
		// 100 ticks/second would be 10000, but the absolute value is irrelevant;
		// only the delta matters).
		procStatPath: "cpu  100000 0 0 0 0 0 0\n",
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	if got := a.CPUPerc(); got != 0 {
		t.Fatalf("CPUPerc() first call = %v, want 0", got)
	}

	// Advance usage by 100ms (one full period of a 100000us period) and advance
	// host system time by 100ms (10 ticks) so the window elapsed is 100ms.
	contents[cpuUsageV1] = "100000000"
	contents[procStatPath] = "cpu  100010 0 0 0 0 0 0\n"
	if got := a.CPUPerc(); got != 50 {
		t.Fatalf("CPUPerc() after 100ms delta = %v, want 50", got)
	}
}

// TestCPUPercV2 verifies cgroup v2 parsing from cpu.stat when the v1 cpuacct
// file is absent.
func TestK8sAdapterCPUPercV2(t *testing.T) {
	a := &K8sAdapter{
		cpuUsageV1: cpuUsageV1,
		cpuUsageV2: cpuUsageV2,
		cpuQuota:   cpuQuota,
		cpuPeriod:  cpuPeriod,
		procStat:   procStatPath,
		envLookup:  func(string) (string, bool) { return "", false },
		fileExists: func(string) bool { return true },
		readFile: fakeReadFile(map[string]string{
			cpuUsageV2: "usage_usec 100000\nuser_usec 0\nsystem_usec 0\n",
			cpuQuota:   "200000",
			cpuPeriod:  "100000",
			procStatPath: "cpu  100000 0 0 0 0 0 0\n",
		}, map[string]bool{cpuUsageV1: true}),
	}
	if got := a.CPUPerc(); got != 0 {
		t.Fatalf("CPUPerc() first call = %v, want 0", got)
	}
	a.readFile = fakeReadFile(map[string]string{
		cpuUsageV2: "usage_usec 200000\nuser_usec 0\nsystem_usec 0\n",
		cpuQuota:   "200000",
		cpuPeriod:  "100000",
		procStatPath: "cpu  100010 0 0 0 0 0 0\n",
	}, map[string]bool{cpuUsageV1: true})
	if got := a.CPUPerc(); got != 50 {
		t.Fatalf("CPUPerc() v2 after delta = %v, want 50", got)
	}
}

// TestK8sAdapterCPUPercMultiWindowLargeDelta exercises a real multi-window
// sample where the elapsed window spans far more than a single cgroup period.
// The old implementation divided by a fixed quota (period * cpus), so the
// percentage grew linearly with the window size and sat at 100% for even small
// real loads. The fixed implementation divides by the actual elapsed host
// system time, so a 500ms window using one full CPU against a 2-CPU limit
// reads as 50%, never saturating at 100%.
func TestK8sAdapterCPUPercMultiWindowLargeDelta(t *testing.T) {
	contents := map[string]string{
		cpuUsageV1:   "0",
		cpuQuota:     "200000",
		cpuPeriod:    "100000",
		procStatPath: "cpu  100000 0 0 0 0 0 0\n",
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	if got := a.CPUPerc(); got != 0 {
		t.Fatalf("CPUPerc() first call = %v, want 0", got)
	}

	// 500ms window (50 ticks), one full CPU consumed for the whole window
	// (500ms of container CPU time) against a 2-CPU limit -> 50%.
	contents[cpuUsageV1] = "500000000"
	contents[procStatPath] = "cpu  100050 0 0 0 0 0 0\n"
	if got := a.CPUPerc(); got != 50 {
		t.Fatalf("CPUPerc() = %v, want 50 (one CPU of a 2-CPU limit across a 500ms window)", got)
	}

	// Second window: another 500ms elapsed, one full CPU consumed -> still 50%.
	contents[cpuUsageV1] = "1000000000"
	contents[procStatPath] = "cpu  100100 0 0 0 0 0 0\n"
	if got := a.CPUPerc(); got != 50 {
		t.Fatalf("CPUPerc() second window = %v, want 50", got)
	}
}

// TestK8sAdapterCPUPercMultiCoreHostNormalizesByHostCPUs verifies that the
// CPUPerc calculation normalizes the host system-time delta by the number of
// host cores (mirroring Python k8s_utils.system_delta = raw / _host_num_cpus()).
// On a 32-core host with a 2-CPU container limit that is fully saturated (both
// CPUs busy for the whole window), the percentage must read near 100%. Without
// host-core normalization the aggregate /proc/stat "cpu" delta (the sum across
// all 32 cores) would understate usage by ~32x, yielding ~3.125% instead.
func TestK8sAdapterCPUPercMultiCoreHostNormalizesByHostCPUs(t *testing.T) {
	// 32 host cores: one aggregate "cpu" line plus cpu0..cpu31.
	hostLines := "cpu  100000 0 0 0 0 0 0\n"
	for i := 0; i < 32; i++ {
		hostLines += "cpu" + strconv.Itoa(i) + " 1000 0 0 0 0 0 0\n"
	}
	contents := map[string]string{
		cpuUsageV1:   "0",
		cpuQuota:     "200000", // 2-CPU limit
		cpuPeriod:    "100000",
		procStatPath: hostLines,
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	if got := a.CPUPerc(); got != 0 {
		t.Fatalf("CPUPerc() first call = %v, want 0", got)
	}

	// Container fully saturates both CPUs for one tick (2e7 ns = 2 * 1e7 ns),
	// while the aggregate host "cpu" line advances by 32 ticks (one per core).
	contents[cpuUsageV1] = "20000000"
	contents[procStatPath] = "cpu  100032 0 0 0 0 0 0\n" + strings.Repeat("cpuN 1000 0 0 0 0 0 0\n", 32)
	if got := a.CPUPerc(); got < 99 || got > 100 {
		t.Fatalf("CPUPerc() saturated 2-CPU container on 32-core host = %v, want ~100 (not 3.125)", got)
	}
}

func TestK8sAdapterMemStatsV1(t *testing.T) {
	contents := map[string]string{
		memUsageV1: "1073741824", // 1 GiB used
		memLimitV1: "2147483648", // 2 GiB limit
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	got, err := a.MemStats()
	if err != nil {
		t.Fatalf("MemStats() error = %v", err)
	}
	if got.Total != 2147483648 {
		t.Errorf("MemStats().Total = %d, want 2147483648", got.Total)
	}
	if got.Used != 1073741824 {
		t.Errorf("MemStats().Used = %d, want 1073741824", got.Used)
	}
	if got.Available != 1073741824 {
		t.Errorf("MemStats().Available = %d, want 1073741824", got.Available)
	}
	if got.Percent != 50 {
		t.Errorf("MemStats().Percent = %v, want 50", got.Percent)
	}
}

func TestK8sAdapterMemStatsV2(t *testing.T) {
	a := &K8sAdapter{
		memUsageV1: memUsageV1,
		memLimitV1: memLimitV1,
		memUsageV2: memUsageV2,
		memLimitV2: memLimitV2,
		envLookup:  func(string) (string, bool) { return "", false },
		fileExists: func(string) bool { return true },
		readFile: fakeReadFile(map[string]string{
			memUsageV2: "1073741824",
			memLimitV2: "2147483648",
		}, map[string]bool{memUsageV1: true, memLimitV1: true}),
	}
	got, err := a.MemStats()
	if err != nil {
		t.Fatalf("MemStats() error = %v", err)
	}
	if got.Total != 2147483648 {
		t.Errorf("MemStats().Total = %d, want 2147483648", got.Total)
	}
	if got.Used != 1073741824 {
		t.Errorf("MemStats().Used = %d, want 1073741824", got.Used)
	}
}

// TestK8sAdapterMemStatsV2UnlimitedMax verifies that a cgroup v2 memory.max of
// the literal "max" (unlimited memory, a common K8s pod without a memory
// limit) does not error and reports a near-zero percentage rather than a
// false 100% usage alarm.
func TestK8sAdapterMemStatsV2UnlimitedMax(t *testing.T) {
	a := &K8sAdapter{
		memUsageV1: memUsageV1,
		memLimitV1: memLimitV1,
		memUsageV2: memUsageV2,
		memLimitV2: memLimitV2,
		envLookup:  func(string) (string, bool) { return "", false },
		fileExists: func(string) bool { return true },
		readFile: fakeReadFile(map[string]string{
			memUsageV2: "1073741824",
			memLimitV2: "max",
		}, map[string]bool{memUsageV1: true, memLimitV1: true}),
	}
	got, err := a.MemStats()
	if err != nil {
		t.Fatalf("MemStats() error = %v, want no error for unlimited memory.max", err)
	}
	if got.Used != 1073741824 {
		t.Errorf("MemStats().Used = %d, want 1073741824", got.Used)
	}
	if got.Total != math.MaxInt64 {
		t.Errorf("MemStats().Total = %d, want %d (unlimited sentinel)", got.Total, int64(math.MaxInt64))
	}
	if got.Percent != 0 {
		t.Errorf("MemStats().Percent = %v, want 0 for unlimited memory", got.Percent)
	}
}

func TestK8sAdapterMemStatsDegradesOnReadError(t *testing.T) {
	a := newTestAdapter(nil, func(string) (string, bool) { return "", false })
	got, err := a.MemStats()
	if err == nil {
		t.Fatalf("MemStats() error = nil, want error when cgroup files are unreadable")
	}
	if got != (MemStats{}) {
		t.Errorf("MemStats() = %+v, want zero value on error", got)
	}
}

func TestK8sAdapterNetworkStats(t *testing.T) {
	contents := map[string]string{
		netDevPath: "Inter-|   Receive                                                |  Transmit\n" +
			" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n" +
			"    lo: 1000 10 0 0 0 0 0 0       2000 20 0 0 0 0 0 0\n" +
			"  eth0: 50000 100 0 0 0 0 0 0      90000 150 0 0 0 0 0 0\n" +
			"  eth1: 30000 60 0 0 0 0 0 0       10000 30 0 0 0 0 0 0\n",
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	got, err := a.NetworkStats()
	if err != nil {
		t.Fatalf("NetworkStats() error = %v", err)
	}
	// loopback is excluded; only eth0 + eth1 count.
	if got.Recv != 80000 {
		t.Errorf("NetworkStats().Recv = %d, want 80000", got.Recv)
	}
	if got.Sent != 100000 {
		t.Errorf("NetworkStats().Sent = %d, want 100000", got.Sent)
	}
}

func TestK8sAdapterNetworkStatsDegradesOnReadError(t *testing.T) {
	a := newTestAdapter(nil, func(string) (string, bool) { return "", false })
	got, err := a.NetworkStats()
	if err == nil {
		t.Fatalf("NetworkStats() error = nil, want error when /proc/net/dev is unreadable")
	}
	if got != (IOMetrics{}) {
		t.Errorf("NetworkStats() = %+v, want zero value on error", got)
	}
}

// TestK8sAdapterContainerCPUCapacityUnlimitedQuotaFallbackHost verifies that
// when the cgroup quota is unlimited (negative) or absent, the capacity falls
// back to the host CPU count parsed from /proc/stat.
func TestK8sAdapterContainerCPUCapacityUnlimitedQuotaFallbackHost(t *testing.T) {
	contents := map[string]string{
		cpuQuota:     "-1",
		cpuPeriod:    "100000",
		procStatPath: "cpu  100000 0 0 0 0 0 0\n" +
			"cpu0 1000 0 0 0 0 0 0\n" +
			"cpu1 1000 0 0 0 0 0 0\n",
	}
	a := newTestAdapter(contents, func(string) (string, bool) { return "", false })
	if got := a.containerCPUCapacity(); got != 2 {
		t.Fatalf("containerCPUCapacity() = %d, want 2 (host CPUs from /proc/stat)", got)
	}
}

// TestK8sAdapterContainerCPUCapacityFallbackOne verifies that when the quota is
// unlimited and /proc/stat is unreadable, the capacity falls back to at least 1.
func TestK8sAdapterContainerCPUCapacityFallbackOne(t *testing.T) {
	a := newTestAdapter(nil, func(string) (string, bool) { return "", false })
	if got := a.containerCPUCapacity(); got != 1 {
		t.Fatalf("containerCPUCapacity() = %d, want 1 (fallback floor)", got)
	}
}

// TestNewK8sAdapterDefaults ensures the production constructor wires the real
// env lookup and filesystem readers rather than nil functions.
func TestNewK8sAdapterDefaults(t *testing.T) {
	a := NewK8sAdapter()
	if a.envLookup == nil {
		t.Fatalf("NewK8sAdapter() envLookup = nil, want real lookup")
	}
	if a.fileExists == nil {
		t.Fatalf("NewK8sAdapter() fileExists = nil, want real check")
	}
	if a.readFile == nil {
		t.Fatalf("NewK8sAdapter() readFile = nil, want real reader")
	}
	if !strings.HasSuffix(a.cpuUsageV1, "cpuacct.usage") {
		t.Errorf("NewK8sAdapter() cpuUsageV1 = %q, want default cgroup v1 path", a.cpuUsageV1)
	}
}
