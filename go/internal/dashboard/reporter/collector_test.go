package reporter

import (
	"math"
	"testing"
	"time"
)

type fakeSource struct {
	cpuP        float64
	cpus        []int
	mem         MemInfo
	shm         int64
	shmOK       bool
	disk        map[string]DiskInfo
	diskIOM     DiskIOMetrics
	diskIOSpd   []float64
	net         IOMetrics
	netSpd      []float64
	load        LoadAvgInfo
	boot        float64
	worker      []interface{}
	rayletM     map[string]interface{}
	agentM      map[string]interface{}
	gcsM        map[string]interface{}
	additionalM map[string]interface{}

	errs map[string]error
}

func (f *fakeSource) cpuPercent() (float64, error) {
	if err := f.errs["cpu"]; err != nil {
		return 0, err
	}
	return f.cpuP, nil
}
func (f *fakeSource) resetCycleCache() {}
func (f *fakeSource) cpuCounts() ([]int, error) {
	if err := f.errs["cpus"]; err != nil {
		return nil, err
	}
	return f.cpus, nil
}
func (f *fakeSource) memUsage() (MemInfo, error) {
	if err := f.errs["mem"]; err != nil {
		return MemInfo{}, err
	}
	return f.mem, nil
}
func (f *fakeSource) shmUsage() (int64, bool, error) {
	if err := f.errs["shm"]; err != nil {
		return 0, false, err
	}
	return f.shm, f.shmOK, nil
}
func (f *fakeSource) diskUsage() (map[string]DiskInfo, error) {
	if err := f.errs["disk"]; err != nil {
		return nil, err
	}
	return f.disk, nil
}
func (f *fakeSource) diskIO() (DiskIOMetrics, error) {
	if err := f.errs["diskIO"]; err != nil {
		return DiskIOMetrics{}, err
	}
	return f.diskIOM, nil
}
func (f *fakeSource) diskIOSpeed() ([]float64, error) {
	if err := f.errs["diskIOSpeed"]; err != nil {
		return nil, err
	}
	return f.diskIOSpd, nil
}
func (f *fakeSource) network() (IOMetrics, error) {
	if err := f.errs["network"]; err != nil {
		return IOMetrics{}, err
	}
	return f.net, nil
}
func (f *fakeSource) networkSpeed() ([]float64, error) {
	if err := f.errs["networkSpeed"]; err != nil {
		return nil, err
	}
	return f.netSpd, nil
}
func (f *fakeSource) loadAvg() (LoadAvgInfo, error) {
	if err := f.errs["load"]; err != nil {
		return LoadAvgInfo{}, err
	}
	return f.load, nil
}
func (f *fakeSource) bootTime() (float64, error) {
	if err := f.errs["boot"]; err != nil {
		return 0, err
	}
	return f.boot, nil
}
func (f *fakeSource) workers() ([]interface{}, error) {
	if err := f.errs["workers"]; err != nil {
		return nil, err
	}
	return f.worker, nil
}
func (f *fakeSource) raylet() (map[string]interface{}, error) {
	if err := f.errs["raylet"]; err != nil {
		return nil, err
	}
	return f.rayletM, nil
}
func (f *fakeSource) agent() (map[string]interface{}, error) {
	if err := f.errs["agent"]; err != nil {
		return nil, err
	}
	return f.agentM, nil
}
func (f *fakeSource) gcs() (map[string]interface{}, error) {
	if err := f.errs["gcs"]; err != nil {
		return nil, err
	}
	return f.gcsM, nil
}
func (f *fakeSource) additional() (map[string]interface{}, error) {
	if err := f.errs["additional"]; err != nil {
		return nil, err
	}
	return f.additionalM, nil
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		cpuP: 12.5,
		cpus: []int{8, 8},
		mem:  MemInfo{Total: 100, Available: 30, Percent: 70.0, Used: 70},
		shm:  2048, shmOK: true,
		disk: map[string]DiskInfo{
			"/":    {Total: 1000, Used: 400, Free: 600, Percent: 40.0},
			"/tmp": {Total: 500, Used: 100, Free: 400, Percent: 20.0},
		},
		diskIOM: DiskIOMetrics{ReadBytes: 10, WriteBytes: 20, ReadCount: 1, WriteCount: 2},
		net:     IOMetrics{Sent: 100, Recv: 200},
		load:    LoadAvgInfo{Load1: 1.0, Load5: 2.0, Load15: 3.0, PerCPULoad: []float64{0.5, 0.25, 0.375}},
		boot:    1234567890.0,
		worker:  []interface{}{map[string]interface{}{"pid": 42}},
		rayletM: map[string]interface{}{"pid": 1, "cmdline": []string{"raylet"}},
		agentM:  map[string]interface{}{"pid": 2},
		gcsM:    map[string]interface{}{"pid": 3},
		additionalM: map[string]interface{}{
			"gcs_server": []interface{}{map[string]interface{}{"pid": 4}},
		},
		errs: map[string]error{},
	}
}

func TestCollectorReturnsFullStatsMap(t *testing.T) {
	src := newFakeSource()
	sc := &StatsCollector{
		hostname: "host-a",
		ip:       "1.2.3.4",
		isHead:   true,
		src:      src,
	}

	stats := sc.Collect()

	if got := stats["hostname"]; got != "host-a" {
		t.Errorf("hostname = %v, want host-a", got)
	}
	if got := stats["ip"]; got != "1.2.3.4" {
		t.Errorf("ip = %v, want 1.2.3.4", got)
	}
	if got := stats["cpu"]; got != 12.5 {
		t.Errorf("cpu = %v, want 12.5", got)
	}
	if got, want := stats["cpus"], []int{8, 8}; !equalIntSlice(got, want) {
		t.Errorf("cpus = %v, want %v", got, want)
	}
	if got := stats["shm"]; got != int64(2048) {
		t.Errorf("shm = %v, want 2048", got)
	}
	if got := stats["bootTime"]; got != 1234567890.0 {
		t.Errorf("bootTime = %v, want 1234567890.0", got)
	}
	if got := stats["workers"]; len(got.([]interface{})) != 1 {
		t.Errorf("workers = %v, want 1 entry", got)
	}
	if _, ok := stats["gcs"]; !ok {
		t.Errorf("gcs missing on head node")
	}
	for _, key := range []string{"gpus", "tpus"} {
		val, ok := stats[key]
		if !ok {
			t.Errorf("%s missing from stats map", key)
			continue
		}
		gpuList, ok := val.([]interface{})
		if !ok {
			t.Errorf("%s type = %T, want []interface{}", key, val)
			continue
		}
		if gpuList == nil {
			t.Errorf("%s = nil, want non-nil empty slice", key)
		}
		if len(gpuList) != 0 {
			t.Errorf("%s length = %d, want 0", key, len(gpuList))
		}
	}

	now, ok := stats["now"].(int64)
	if !ok {
		t.Fatalf("now type = %T, want int64", stats["now"])
	}
	cur := time.Now().Unix()
	if now < cur-5 || now > cur+5 {
		t.Errorf("now = %d, outside reasonable window around %d (want epoch seconds)", now, cur)
	}
}

func TestCollectorMemDiskNetworkLoad(t *testing.T) {
	src := newFakeSource()
	sc := &StatsCollector{src: src}

	stats := sc.Collect()

	mem, ok := stats["mem"].([]interface{})
	if !ok {
		t.Fatalf("mem type = %T, want []interface{} (aligned with Python tuple)", stats["mem"])
	}
	if len(mem) != 4 {
		t.Fatalf("mem length = %d, want 4 [total, available, percent, used]", len(mem))
	}
	if mem[0] != int64(100) || mem[1] != int64(30) || mem[2] != float64(70.0) || mem[3] != int64(70) {
		t.Errorf("mem = %v, want [100 30 70 70]", mem)
	}

	disk, ok := stats["disk"].(map[string]DiskInfo)
	if !ok {
		t.Fatalf("disk type = %T", stats["disk"])
	}
	if disk["/"].Total != 1000 {
		t.Errorf("disk[/].Total = %v, want 1000", disk["/"].Total)
	}

	net, ok := stats["network"].(IOMetrics)
	if !ok {
		t.Fatalf("network type = %T", stats["network"])
	}
	if net.Sent != 100 || net.Recv != 200 {
		t.Errorf("network = %+v", net)
	}

	loadAvg, ok := stats["loadAvg"].([]interface{})
	if !ok {
		t.Fatalf("loadAvg type = %T, want []interface{} (aligned with Python tuple)", stats["loadAvg"])
	}
	if len(loadAvg) != 2 {
		t.Fatalf("loadAvg length = %d, want 2 [load, per_cpu_load]", len(loadAvg))
	}
	load, ok := loadAvg[0].([]interface{})
	if !ok || len(load) != 3 {
		t.Fatalf("loadAvg[0] = %v (%T), want [l1 l5 l15]", loadAvg[0], loadAvg[0])
	}
	if load[0] != float64(1.0) || load[2] != float64(3.0) {
		t.Errorf("load = %v, want [1 2 3]", load)
	}
	perCPU, ok := loadAvg[1].([]interface{})
	if !ok || len(perCPU) != 3 {
		t.Fatalf("loadAvg[1] = %v (%T), want [p1 p2 p3]", loadAvg[1], loadAvg[1])
	}
	if perCPU[0] != float64(0.5) || perCPU[2] != float64(0.375) {
		t.Errorf("per_cpu_load = %v, want [0.5 0.25 0.375]", perCPU)
	}
}

func TestCollectorNoGCSTOnNonHead(t *testing.T) {
	sc := &StatsCollector{isHead: false, src: newFakeSource()}
	stats := sc.Collect()
	if _, ok := stats["gcs"]; ok {
		t.Errorf("gcs present on non-head node")
	}
}

func TestCollectorDegradesOnSourceError(t *testing.T) {
	src := newFakeSource()
	src.errs["cpu"] = errFake
	src.errs["mem"] = errFake
	sc := &StatsCollector{isHead: true, src: src}

	stats := sc.Collect()
	if got := stats["cpu"]; got != float64(0) {
		t.Errorf("cpu on error = %v, want 0", got)
	}
	mem, ok := stats["mem"].([]interface{})
	if !ok {
		t.Fatalf("mem on error type = %T, want []interface{}", stats["mem"])
	}
	for i, v := range mem {
		if v != int64(0) && v != float64(0) {
			t.Errorf("mem[%d] on error = %v, want 0", i, v)
		}
	}
}

func TestCollectorSHMNilWhenUnsupported(t *testing.T) {
	src := newFakeSource()
	src.shmOK = false
	sc := &StatsCollector{src: src}
	stats := sc.Collect()
	if stats["shm"] != nil {
		t.Errorf("shm = %v, want nil", stats["shm"])
	}
}

func TestMemPercentMatchesPythonRounding(t *testing.T) {
	cases := []struct {
		used, total int64
		want        float64
	}{
		{70, 100, 70.0},
		{1, 3, 33.3}, // round(0.333..., 3)*100 = 33.3
		{1, 8, 12.5}, // round(0.125, 3)*100 = 12.5
		{2, 3, 66.7}, // round(0.666..., 3)*100 = 66.7
		{0, 100, 0.0},
		{100, 100, 100.0},
	}
	const eps = 1e-9
	for _, c := range cases {
		got := memPercent(c.used, c.total)
		if math.Abs(got-c.want) > eps {
			t.Errorf("memPercent(%d, %d) = %v, want %v", c.used, c.total, got, c.want)
		}
	}
}

var errFake = &fakeErr{}

type fakeErr struct{}

func (e *fakeErr) Error() string { return "fake source error" }

func equalIntSlice(got interface{}, want []int) bool {
	gs, ok := got.([]int)
	if !ok {
		return false
	}
	if len(gs) != len(want) {
		return false
	}
	for i := range gs {
		if gs[i] != want[i] {
			return false
		}
	}
	return true
}
