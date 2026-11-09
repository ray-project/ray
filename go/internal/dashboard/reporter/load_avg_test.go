package reporter

import (
	"encoding/json"
	"strings"
	"testing"
)

// marshalStats is a test helper that renders a Collect result the way the
// reporter publishes it, so assertions see the JSON shapes the head node
// actually receives rather than Go types.
func marshalStats(t *testing.T, stats map[string]interface{}) map[string]interface{} {
	t.Helper()

	raw, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	return out
}

// loadAvgOf returns the marshalled loadAvg pair from a stats map.
func loadAvgOf(t *testing.T, stats map[string]interface{}) []interface{} {
	t.Helper()

	m := marshalStats(t, stats)
	pair, ok := m["loadAvg"].([]interface{})
	if !ok {
		t.Fatalf("loadAvg after JSON = %v (%T), want a 2-element array", m["loadAvg"], m["loadAvg"])
	}
	if len(pair) != 2 {
		t.Fatalf("loadAvg length = %d, want 2 [load, per_cpu_load]", len(pair))
	}
	return pair
}

// TestLoadAvgPerCPUIsNullWithoutCPUCount pins the required shape: when the logical
// CPU count is 0, Python _get_load_avg sets per_cpu_load = None and json.dumps
// renders it as null. A Go empty slice serializes as [] instead, so the second
// element must be untyped nil to reach the wire as null.
func TestLoadAvgPerCPUIsNullWithoutCPUCount(t *testing.T) {
	s := &systemSource{
		cpuCountsOverride: func() ([]int, error) { return []int{0, 0}, nil },
	}

	load, err := s.loadAvg()
	if err != nil {
		t.Fatalf("loadAvg: %v", err)
	}
	if load.PerCPULoad != nil {
		t.Fatalf("PerCPULoad = %v, want nil when the logical CPU count is 0", load.PerCPULoad)
	}

	wire := loadAvgWire(load)
	if wire[1] != nil {
		t.Errorf("loadAvgWire[1] = %v (%T), want nil", wire[1], wire[1])
	}
}

// TestLoadAvgPerCPUEncodesAsJSONNull verifies the nil above reaches the wire as
// null rather than [] — the actual defect, which only shows up after
// serialization.
func TestLoadAvgPerCPUEncodesAsJSONNull(t *testing.T) {
	s := &systemSource{
		cpuCountsOverride: func() ([]int, error) { return []int{0, 0}, nil },
	}
	load, err := s.loadAvg()
	if err != nil {
		t.Fatalf("loadAvg: %v", err)
	}

	raw, err := json.Marshal(loadAvgWire(load))
	if err != nil {
		t.Fatalf("marshal loadAvg: %v", err)
	}
	got := string(raw)
	// The load values depend on the host, so assert the shape rather than the
	// numbers: the pair must close with ",null]".
	if !strings.HasSuffix(got, ",null]") {
		t.Errorf("loadAvg JSON = %s, want the per-CPU element to be null, not []", got)
	}
	if strings.Contains(got, ",[]]") {
		t.Errorf("loadAvg JSON = %s, want null rather than an empty array", got)
	}
}

// TestLoadAvgPerCPUArrayWhenCPUsKnown is the control: with a positive logical
// count the second element is a 3-element array, so the null above is not the
// result of the field being dropped entirely.
func TestLoadAvgPerCPUArrayWhenCPUsKnown(t *testing.T) {
	s := &systemSource{
		cpuCountsOverride: func() ([]int, error) { return []int{2, 1}, nil },
	}

	load, err := s.loadAvg()
	if err != nil {
		t.Fatalf("loadAvg: %v", err)
	}
	wire := loadAvgWire(load)
	perCPU, ok := wire[1].([]interface{})
	if !ok {
		t.Fatalf("loadAvgWire[1] = %v (%T), want a 3-element array", wire[1], wire[1])
	}
	if len(perCPU) != 3 {
		t.Fatalf("per_cpu_load length = %d, want 3", len(perCPU))
	}
}

// TestCollectLoadAvgNullWithoutCPUCount verifies the whole cycle, so the defect
// cannot be reintroduced by a change in Collect's assembly rather than in
// loadAvg itself.
func TestCollectLoadAvgNullWithoutCPUCount(t *testing.T) {
	c := &StatsCollector{
		src: &systemSource{
			cpuCountsOverride: func() ([]int, error) { return []int{0, 0}, nil },
		},
		hostname: "host-a",
		ip:       "10.0.0.1",
		now:      func() float64 { return 1700000000.0 },
	}

	pair := loadAvgOf(t, c.Collect())
	if pair[1] != nil {
		t.Errorf("loadAvg[1] = %v (%T), want null when cpus[0] is 0", pair[1], pair[1])
	}
}

// TestCollectLoadAvgNullMatchesPythonForEveryProbeFailure pins the source of the
// null: loadAvg must report nil per-CPU whenever cpuCounts reports a zero
// logical count, regardless of how the load values themselves parsed.
func TestCollectLoadAvgNullMatchesPythonForEveryProbeFailure(t *testing.T) {
	for _, c := range []struct {
		name    string
		counts  []int
		wantNil bool
	}{
		{"zero logical", []int{0, 0}, true},
		{"zero logical with physical", []int{0, 8}, true},
		{"positive logical", []int{8, 4}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := &systemSource{
				cpuCountsOverride: func() ([]int, error) { return c.counts, nil },
			}
			load, err := s.loadAvg()
			if err != nil {
				t.Fatalf("loadAvg: %v", err)
			}
			if gotNil := load.PerCPULoad == nil; gotNil != c.wantNil {
				t.Errorf("PerCPULoad nil = %v, want %v for counts %v", gotNil, c.wantNil, c.counts)
			}
		})
	}
}

// TestPerCPULoadWireNumberFormatting verifies the per-CPU values keep their
// round2 precision through the wire conversion, so switching the return type to
// interface{} did not change the numeric content.
func TestPerCPULoadWireNumberFormatting(t *testing.T) {
	wire := perCPULoadWire([]float64{0.5, 0.25, 0.375})
	arr, ok := wire.([]interface{})
	if !ok {
		t.Fatalf("perCPULoadWire = %v (%T), want []interface{}", wire, wire)
	}
	want := []float64{0.5, 0.25, 0.375}
	for i, w := range want {
		got, ok := arr[i].(float64)
		if !ok || got != w {
			t.Errorf("perCPULoadWire[%d] = %v, want %v", i, arr[i], w)
		}
	}
}

// TestLoadAvgReadsRealProcLoadavg exercises the production read path against the
// live /proc/loadavg: the load triple must be populated and, with one logical
// CPU, per-CPU load must be the load itself.
func TestLoadAvgReadsRealProcLoadavg(t *testing.T) {
	s := &systemSource{
		cpuCountsOverride: func() ([]int, error) { return []int{1, 1}, nil },
	}

	load, err := s.loadAvg()
	if err != nil {
		t.Fatalf("loadAvg: %v", err)
	}
	if load.Load1 < 0 || load.Load5 < 0 || load.Load15 < 0 {
		t.Errorf("load = %v/%v/%v, want non-negative", load.Load1, load.Load5, load.Load15)
	}
	if len(load.PerCPULoad) != 3 {
		t.Fatalf("PerCPULoad length = %d, want 3 with one logical CPU", len(load.PerCPULoad))
	}
	// Dividing by one CPU must leave the values unchanged, modulo round2.
	if got := round2(load.Load1); load.PerCPULoad[0] != got {
		t.Errorf("PerCPULoad[0] = %v, want %v for a single logical CPU", load.PerCPULoad[0], got)
	}
}
