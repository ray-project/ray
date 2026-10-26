package reporter

import (
	"math"
	"os"
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

	for _, k := range []string{"pid", "create_time", "cpu_percent", "cmdline", "name"} {
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
// cycle and cached, and that resetCycleCache invalidates it for the next cycle.
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
	// resetCycleCache clears the cache so the next cycle re-parses.
	s.resetCycleCache()
	if s.meminfoCached {
		t.Fatal("resetCycleCache should clear meminfoCached")
	}
	if s.meminfoCache != nil {
		t.Fatal("resetCycleCache should clear meminfoCache")
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
