package reporter

import (
	"math"
	"testing"
	"time"
)

// cycleFakeSource is a statsSource whose disk/network readers stamp their
// speed-history samples the way the production systemSource does: with the
// timestamp handed to beginCycle, not with a fresh clock read.
type cycleFakeSource struct {
	fakeSource
	captured     []float64
	diskIOSeq    []DiskIOMetrics
	networkSeq   []IOMetrics
	diskIOCalls  int
	networkCalls int
	diskIOHist   []ioSample
	netHist      []netSample
}

func (f *cycleFakeSource) beginCycle(now float64) {
	f.captured = append(f.captured, now)
}

// diskIO returns the next scripted counter sample and records it with the cycle
// timestamp, mirroring systemSource.diskIO -> recordDiskIO.
func (f *cycleFakeSource) diskIO() (DiskIOMetrics, error) {
	var m DiskIOMetrics
	if f.diskIOCalls < len(f.diskIOSeq) {
		m = f.diskIOSeq[f.diskIOCalls]
	}
	f.diskIOCalls++
	f.diskIOHist = append(f.diskIOHist, ioSample{at: f.cycleAt(), val: m})
	return m, nil
}

func (f *cycleFakeSource) network() (IOMetrics, error) {
	var m IOMetrics
	if f.networkCalls < len(f.networkSeq) {
		m = f.networkSeq[f.networkCalls]
	}
	f.networkCalls++
	f.netHist = append(f.netHist, netSample{at: f.cycleAt(), val: m})
	return m, nil
}

func (f *cycleFakeSource) cycleAt() float64 {
	if len(f.captured) == 0 {
		return 0
	}
	return f.captured[len(f.captured)-1]
}

// diskIOSpeed and networkSpeed mirror the production readers by computing from
// the retained history.
func (f *cycleFakeSource) diskIOSpeed() ([]float64, error) {
	return speedFromIO(f.diskIOHist), nil
}

func (f *cycleFakeSource) networkSpeed() ([]float64, error) {
	return speedFromIOMetrics(f.netHist), nil
}

// speedFromIOMetrics is the network counterpart of speedFromIO, kept local so
// the fake does not depend on unexported production fields.
func speedFromIOMetrics(hist []netSample) []float64 {
	if len(hist) < 2 {
		return []float64{0, 0}
	}
	then, now := hist[0], hist[len(hist)-1]
	dt := now.at - then.at
	if dt <= 0 {
		return []float64{0, 0}
	}
	return []float64{
		float64(now.val.Sent-then.val.Sent) / dt,
		float64(now.val.Recv-then.val.Recv) / dt,
	}
}

// TestPosixTimeKeepsFractionalSeconds verifies the cycle clock is a POSIX
// timestamp in float seconds with sub-second resolution, mirroring
// dashboard.utils.to_posix_time. A whole-second value would make two cycles
// inside the same second indistinguishable.
func TestPosixTimeKeepsFractionalSeconds(t *testing.T) {
	got := posixTime()
	// It must be a wall-clock epoch timestamp, not a truncated sub-second
	// remainder: a value under 1e9 cannot be a POSIX timestamp for any
	// realistic date.
	if got < 1e9 {
		t.Fatalf("posixTime() = %v, want a POSIX epoch timestamp (>= 1e9)", got)
	}
	// And it must track the real clock, so the epoch base is the actual
	// current time rather than an arbitrary constant.
	epoch := float64(time.Now().Unix())
	if math.Abs(got-epoch) > 5 {
		t.Errorf("posixTime() = %v, want within 5s of the wall clock %v", got, epoch)
	}

	// The fractional part may legitimately be zero if the clock happened to
	// tick over between two reads, so sample a few times and require at least
	// one non-integral reading. A truncated implementation can never produce
	// one.
	fractional := false
	for i := 0; i < 50; i++ {
		v := posixTime()
		if v != math.Trunc(v) {
			fractional = true
			break
		}
	}
	if !fractional {
		t.Error("posixTime() returned only whole seconds over 50 reads, want sub-second precision")
	}
}

// TestCollectorNowKeepsFractionalSeconds verifies the published "now" field is
// not truncated to a whole second. This is the sub-second timestamp defect: time.Now().Unix()
// quantized the timestamp, so speed deltas computed from two cycles in the same
// second became zero.
func TestCollectorNowKeepsFractionalSeconds(t *testing.T) {
	sc := &StatsCollector{src: newFakeSource(), now: func() float64 { return 1700000000.75 }}

	stats := sc.Collect()
	now, ok := stats["now"].(float64)
	if !ok {
		t.Fatalf("now type = %T, want float64", stats["now"])
	}
	if now != 1700000000.75 {
		t.Errorf("now = %v, want 1700000000.75 (the cycle clock value must be reported verbatim)", now)
	}
}

// TestCollectorUsesOneTimestampPerCycle pins the Python parity contract: a
// collection cycle computes to_posix_time(datetime.utcnow()) once and reuses it
// for the payload and both speed histories. Go must call the cycle clock
// exactly once per Collect, so a cycle cannot straddle two instants.
func TestCollectorUsesOneTimestampPerCycle(t *testing.T) {
	calls := 0
	sc := &StatsCollector{
		src: newFakeSource(),
		now: func() float64 { calls++; return 1700000000 + float64(calls) },
	}

	stats := sc.Collect()

	if calls != 1 {
		t.Errorf("cycle clock called %d times during one Collect, want exactly 1", calls)
	}
	if now, _ := stats["now"].(float64); now != 1700000001 {
		t.Errorf("now = %v, want 1700000001 (the single cycle reading)", now)
	}
}

// TestCollectorSpeedKeepsFractionalDt verifies the speed delta is computed with
// sub-second precision. Two cycles 0.5s apart with a 10-byte counter delta must
// yield 20 B/s; a whole-second timestamp makes dt 0 and the speed 0.
func TestCollectorSpeedKeepsFractionalDt(t *testing.T) {
	src := &cycleFakeSource{
		diskIOSeq: []DiskIOMetrics{
			{ReadBytes: 0, WriteBytes: 0, ReadCount: 0, WriteCount: 0},
			{ReadBytes: 10, WriteBytes: 20, ReadCount: 2, WriteCount: 4},
		},
		networkSeq: []IOMetrics{
			{Sent: 0, Recv: 0},
			{Sent: 100, Recv: 200},
		},
	}
	// Two cycles half a second apart, both inside the same whole second.
	timestamps := []float64{1700000000.0, 1700000000.5}
	i := 0
	sc := &StatsCollector{
		src: src,
		now: func() float64 {
			v := timestamps[i]
			i++
			return v
		},
	}

	sc.Collect()
	stats := sc.Collect()

	diskSpeed, ok := stats["disk_io_speed"].([]float64)
	if !ok {
		t.Fatalf("disk_io_speed type = %T, want []float64", stats["disk_io_speed"])
	}
	// 10 bytes / 0.5s = 20 B/s.
	if math.Abs(diskSpeed[0]-20) > 1e-9 {
		t.Errorf("disk read speed = %v, want 20 B/s (10 bytes over a 0.5s cycle gap)", diskSpeed[0])
	}
	if math.Abs(diskSpeed[2]-4) > 1e-9 {
		t.Errorf("disk read iops = %v, want 4 (2 reads over a 0.5s cycle gap)", diskSpeed[2])
	}

	netSpeed, ok := stats["network_speed"].([]float64)
	if !ok {
		t.Fatalf("network_speed type = %T, want []float64", stats["network_speed"])
	}
	if math.Abs(netSpeed[0]-200) > 1e-9 {
		t.Errorf("network send speed = %v, want 200 B/s (100 bytes over a 0.5s cycle gap)", netSpeed[0])
	}
}

// TestCollectorSpeedSamplesShareTheCycleTimestamp verifies the speed-history
// samples carry the cycle timestamp, so the published "now" and the sample
// instants agree (Python stamps both histories with the same `now`).
func TestCollectorSpeedSamplesShareTheCycleTimestamp(t *testing.T) {
	src := &cycleFakeSource{}
	sc := &StatsCollector{src: src, now: func() float64 { return 1700000000.25 }}

	sc.Collect()

	if len(src.captured) != 1 {
		t.Fatalf("beginCycle called %d times, want 1", len(src.captured))
	}
	if src.captured[0] != 1700000000.25 {
		t.Errorf("beginCycle timestamp = %v, want 1700000000.25", src.captured[0])
	}
	if len(src.diskIOHist) != 1 || len(src.netHist) != 1 {
		t.Fatalf("hist sizes = %d, %d, want 1 each after one cycle",
			len(src.diskIOHist), len(src.netHist))
	}
	if got := src.diskIOHist[0].at; got != 1700000000.25 {
		t.Errorf("disk ioSample.at = %v, want the cycle timestamp 1700000000.25", got)
	}
	if got := src.netHist[0].at; got != 1700000000.25 {
		t.Errorf("netSample.at = %v, want the cycle timestamp 1700000000.25", got)
	}
}

// TestSystemSourceSpeedSamplesUseCycleTimestamp verifies the production
// systemSource stamps its history with the timestamp beginCycle received,
// rather than reading the clock again inside recordDiskIO/recordNetwork.
func TestSystemSourceSpeedSamplesUseCycleTimestamp(t *testing.T) {
	s := &systemSource{}

	s.beginCycle(1700000000.125)
	if s.cycleTimestamp != 1700000000.125 {
		t.Fatalf("cycleTimestamp = %v, want 1700000000.125", s.cycleTimestamp)
	}

	s.recordDiskIO(DiskIOMetrics{ReadBytes: 1})
	s.recordNetwork(IOMetrics{Sent: 1})

	if len(s.diskIOHist) != 1 || s.diskIOHist[0].at != 1700000000.125 {
		t.Errorf("diskIOHist = %+v, want one sample stamped 1700000000.125", s.diskIOHist)
	}
	if len(s.netHist) != 1 || s.netHist[0].at != 1700000000.125 {
		t.Errorf("netHist = %+v, want one sample stamped 1700000000.125", s.netHist)
	}
}

// TestSystemSourceNetworkSpeedGuardsZeroDelta verifies the production network
// speed path returns zeros instead of Inf/NaN when two samples carry the same
// cycle timestamp, mirroring the disk-IO guard.
func TestSystemSourceNetworkSpeedGuardsZeroDelta(t *testing.T) {
	s := &systemSource{}
	s.beginCycle(1700000000.5)
	s.recordNetwork(IOMetrics{Sent: 100, Recv: 200})
	s.beginCycle(1700000000.5)
	s.recordNetwork(IOMetrics{Sent: 200, Recv: 400})

	got, err := s.networkSpeed()
	if err != nil {
		t.Fatalf("networkSpeed: %v", err)
	}
	for i, v := range got {
		if v != 0 {
			t.Errorf("networkSpeed[%d] = %v, want 0 (guarded zero time delta)", i, v)
		}
	}
}

// TestStatsCollectorDefaultsToSystemClock verifies a StatsCollector built
// without an explicit cycle clock still reports a real timestamp, so struct
// literals in tests and any future construction path keep working.
func TestStatsCollectorDefaultsToSystemClock(t *testing.T) {
	sc := &StatsCollector{src: newFakeSource()}

	stats := sc.Collect()
	now, ok := stats["now"].(float64)
	if !ok {
		t.Fatalf("now type = %T, want float64", stats["now"])
	}
	if now < 1e9 {
		t.Errorf("now = %v, want a POSIX epoch timestamp from the default clock", now)
	}
	if diff := math.Abs(now - float64(time.Now().Unix())); diff > 5 {
		t.Errorf("now = %v, want within 5s of the wall clock", now)
	}
}
