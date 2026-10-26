package reporter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeGetMetrics returns a fixed body and error so tests never perform a real
// HTTP request to the TPU device plugin.
func fakeGetMetrics(body []byte, err error) httpGetter {
	return func(url string) ([]byte, error) {
		return body, err
	}
}

func TestTPUCollectorParsesPrometheus(t *testing.T) {
	body := []byte(
		"# HELP tensorcore_utilization_node TensorCore utilization\n" +
			"# TYPE tensorcore_utilization_node gauge\n" +
			`tensorcore_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.8` + "\n" +
			`memory_bandwidth_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.5` + "\n" +
			`duty_cycle_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.2` + "\n" +
			`memory_used{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 1234` + "\n" +
			`memory_total{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 4096` + "\n",
	)
	orig := getMetrics
	getMetrics = fakeGetMetrics(body, nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 1 {
		t.Fatalf("Collect() returned %d TPUs, want 1", len(tpus))
	}
	t0 := tpus[0]
	if t0.Index != 0 {
		t.Errorf("tpu[0].Index = %d, want 0", t0.Index)
	}
	if t0.Name != "4804690994094478883-0" {
		t.Errorf("tpu[0].Name = %q, want 4804690994094478883-0", t0.Name)
	}
	if t0.TPUType != "tpu-v6e-slice" {
		t.Errorf("tpu[0].TPUType = %q, want tpu-v6e-slice", t0.TPUType)
	}
	if t0.TPUTopology != "2x4" {
		t.Errorf("tpu[0].TPUTopology = %q, want 2x4", t0.TPUTopology)
	}
	if t0.TensorcoreUtilization != 0.8 {
		t.Errorf("tpu[0].TensorcoreUtilization = %v, want 0.8", t0.TensorcoreUtilization)
	}
	if t0.HBMUtilization != 0.5 {
		t.Errorf("tpu[0].HBMUtilization = %v, want 0.5", t0.HBMUtilization)
	}
	if t0.DutyCycle != 0.2 {
		t.Errorf("tpu[0].DutyCycle = %v, want 0.2", t0.DutyCycle)
	}
	if t0.MemoryUsed != 1234 {
		t.Errorf("tpu[0].MemoryUsed = %d, want 1234", t0.MemoryUsed)
	}
	if t0.MemoryTotal != 4096 {
		t.Errorf("tpu[0].MemoryTotal = %d, want 4096", t0.MemoryTotal)
	}
}

func TestTPUCollectorMergesMultipleDevicesSorted(t *testing.T) {
	body := []byte(
		`tensorcore_utilization_node{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 0.3` + "\n" +
			`tensorcore_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.9` + "\n",
	)
	orig := getMetrics
	getMetrics = fakeGetMetrics(body, nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 2 {
		t.Fatalf("Collect() returned %d TPUs, want 2", len(tpus))
	}
	if tpus[0].Index != 0 {
		t.Errorf("tpus[0].Index = %d, want 0 (sorted)", tpus[0].Index)
	}
	if tpus[1].Index != 1 {
		t.Errorf("tpus[1].Index = %d, want 1 (sorted)", tpus[1].Index)
	}
}

func TestTPUCollectorDegradesOnHTTPError(t *testing.T) {
	orig := getMetrics
	getMetrics = fakeGetMetrics(nil, errors.New("connection refused"))
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if tpus == nil {
		t.Fatalf("Collect() returned nil, want non-nil empty slice")
	}
	if len(tpus) != 0 {
		t.Errorf("Collect() returned %d TPUs on error, want 0", len(tpus))
	}
}

func TestTPUCollectorDegradesOnNoAddr(t *testing.T) {
	orig := getMetrics
	getMetrics = fakeGetMetrics([]byte("unused"), nil)
	defer func() { getMetrics = orig }()

	// No device plugin address must yield an empty result without any call to
	// the HTTP getter.
	c := NewTPUCollectorWithEndpoint("")
	tpus := c.Collect()

	if tpus == nil || len(tpus) != 0 {
		t.Errorf("Collect() returned %v on empty addr, want empty", tpus)
	}
}

func TestTPUCollectorDegradesOnMalformedBody(t *testing.T) {
	orig := getMetrics
	getMetrics = fakeGetMetrics([]byte("not valid prometheus \x00 metrics"), nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 0 {
		t.Errorf("Collect() returned %d TPUs on malformed body, want 0", len(tpus))
	}
}

// TestTPUCollectorAcceptsBothMetricNamings ensures both the device plugin
// naming with the "_node" suffix and the Python reference naming without it
// are recognized. This guards against collection silently returning empty when
// the actual /metrics output differs from the comment in reporter_agent.py.
func TestTPUCollectorAcceptsBothMetricNamings(t *testing.T) {
	body := []byte(
		`tensorcore_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.8` + "\n" +
			`memory_bandwidth_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.5` + "\n" +
			`duty_cycle_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.2` + "\n" +
			`memory_used_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 1234` + "\n" +
			`memory_total_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 4096` + "\n" +
			`tensorcore_utilization{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 0.3` + "\n" +
			`memory_bandwidth_utilization{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 0.4` + "\n" +
			`duty_cycle{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 0.1` + "\n" +
			`memory_used{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 5678` + "\n" +
			`memory_total{accelerator_id="4804690994094478883-1",model="tpu-v6e-slice",tpu_topology="2x4"} 8192` + "\n",
	)
	orig := getMetrics
	getMetrics = fakeGetMetrics(body, nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 2 {
		t.Fatalf("Collect() returned %d TPUs, want 2 (both namings recognized)", len(tpus))
	}

	// Accelerator 0 uses the "_node" suffixed names.
	t0 := tpus[0]
	if t0.Index != 0 {
		t.Errorf("tpus[0].Index = %d, want 0", t0.Index)
	}
	if t0.TensorcoreUtilization != 0.8 {
		t.Errorf("tpus[0].TensorcoreUtilization = %v, want 0.8", t0.TensorcoreUtilization)
	}
	if t0.HBMUtilization != 0.5 {
		t.Errorf("tpus[0].HBMUtilization = %v, want 0.5", t0.HBMUtilization)
	}
	if t0.DutyCycle != 0.2 {
		t.Errorf("tpus[0].DutyCycle = %v, want 0.2", t0.DutyCycle)
	}
	if t0.MemoryUsed != 1234 {
		t.Errorf("tpus[0].MemoryUsed = %d, want 1234", t0.MemoryUsed)
	}
	if t0.MemoryTotal != 4096 {
		t.Errorf("tpus[0].MemoryTotal = %d, want 4096", t0.MemoryTotal)
	}

	// Accelerator 1 uses the plain (Python) names.
	t1 := tpus[1]
	if t1.Index != 1 {
		t.Errorf("tpus[1].Index = %d, want 1", t1.Index)
	}
	if t1.TensorcoreUtilization != 0.3 {
		t.Errorf("tpus[1].TensorcoreUtilization = %v, want 0.3", t1.TensorcoreUtilization)
	}
	if t1.HBMUtilization != 0.4 {
		t.Errorf("tpus[1].HBMUtilization = %v, want 0.4", t1.HBMUtilization)
	}
	if t1.DutyCycle != 0.1 {
		t.Errorf("tpus[1].DutyCycle = %v, want 0.1", t1.DutyCycle)
	}
	if t1.MemoryUsed != 5678 {
		t.Errorf("tpus[1].MemoryUsed = %d, want 5678", t1.MemoryUsed)
	}
	if t1.MemoryTotal != 8192 {
		t.Errorf("tpus[1].MemoryTotal = %d, want 8192", t1.MemoryTotal)
	}
}

// TestTPUCollectorNoDoubleCountOnNodeVariant ensures that when the device
// plugin emits both the plain metric and its "_node" variant for the same
// accelerator, the utilization is overwritten rather than summed. Summing
// would push the value past 100% (double counting) for a single field.
func TestTPUCollectorNoDoubleCountOnNodeVariant(t *testing.T) {
	body := []byte(
		`tensorcore_utilization{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.6` + "\n" +
			`tensorcore_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.8` + "\n" +
			`memory_bandwidth_utilization_node{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.5` + "\n",
	)
	orig := getMetrics
	getMetrics = fakeGetMetrics(body, nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 1 {
		t.Fatalf("Collect() returned %d TPUs, want 1", len(tpus))
	}
	if got := tpus[0].TensorcoreUtilization; got != 0.8 {
		t.Errorf("TensorcoreUtilization = %v, want 0.8 (plain + _node must not sum to 1.4)", got)
	}
	if got := tpus[0].HBMUtilization; got != 0.5 {
		t.Errorf("HBMUtilization = %v, want 0.5", got)
	}
}

// TestTPUCollectorSkipsUnparseableAcceleratorID ensures samples whose
// accelerator_id cannot be parsed into an index are skipped rather than being
// merged into index 0.
func TestTPUCollectorSkipsUnparseableAcceleratorID(t *testing.T) {
	body := []byte(
		`tensorcore_utilization{accelerator_id="not-an-index",model="tpu-v6e-slice",tpu_topology="2x4"} 0.8` + "\n" +
			`duty_cycle{accelerator_id="malformed",model="tpu-v6e-slice",tpu_topology="2x4"} 0.2` + "\n" +
			`tensorcore_utilization{accelerator_id="4804690994094478883-0",model="tpu-v6e-slice",tpu_topology="2x4"} 0.5` + "\n",
	)
	orig := getMetrics
	getMetrics = fakeGetMetrics(body, nil)
	defer func() { getMetrics = orig }()

	c := NewTPUCollectorWithEndpoint("127.0.0.1:8888")
	tpus := c.Collect()

	if len(tpus) != 1 {
		t.Fatalf("Collect() returned %d TPUs, want 1 (malformed ids skipped)", len(tpus))
	}
	if tpus[0].Index != 0 {
		t.Errorf("tpus[0].Index = %d, want 0", tpus[0].Index)
	}
	if tpus[0].TensorcoreUtilization != 0.5 {
		t.Errorf("tpus[0].TensorcoreUtilization = %v, want 0.5", tpus[0].TensorcoreUtilization)
	}
	if tpus[0].DutyCycle != 0.0 {
		t.Errorf("tpus[0].DutyCycle = %v, want 0.0 (malformed duty_cycle skipped)", tpus[0].DutyCycle)
	}
}

// TestGetMetricsUsesTimeout ensures the production httpGetter uses a client
// with a timeout so a hung endpoint does not block collection indefinitely.
func TestGetMetricsUsesTimeout(t *testing.T) {
	if tpuHTTPClient.Timeout != httpTimeout {
		t.Fatalf("tpuHTTPClient.Timeout = %v, want %v", tpuHTTPClient.Timeout, httpTimeout)
	}

	// A server that never responds within the timeout must make getMetrics fail
	// quickly rather than hang.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	start := time.Now()
	_, err := getMetrics(srv.URL)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("getMetrics() returned nil error for a hung endpoint, want timeout error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("getMetrics() took %v, want it to return on timeout well before the endpoint responds", elapsed)
	}
}
