package reporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// unusedPID returns a pid with no /proc entry, so inspecting it models a
// process that has already exited (the Python NoSuchProcess case).
func unusedPID(t *testing.T) int {
	t.Helper()
	max := 65535
	if raw, err := os.ReadFile("/proc/sys/kernel/pid_max"); err == nil {
		if parsed, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			max = parsed
		}
	}
	for pid := max - 1; pid > 1; pid-- {
		if _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); err != nil {
			return pid
		}
	}
	t.Fatalf("no unused pid found below pid_max=%d", max)
	return 0
}

// zombiePID starts a short-lived child and returns its pid while it is still an
// unreaped zombie, mirroring psutil.STATUS_ZOMBIE. The caller must reap it with
// the returned function.
func zombiePID(t *testing.T) (int, func()) {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	reap := func() { _ = cmd.Wait() }

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if state, err := processState(cmd.Process.Pid); err == nil && state == procStateZombie {
			return cmd.Process.Pid, reap
		}
		time.Sleep(5 * time.Millisecond)
	}
	reap()
	t.Fatalf("child %d never reached zombie state", cmd.Process.Pid)
	return 0, nil
}

// TestWorkersSkipsExitedPids verifies worker discovery reports the live pid and
// drops a pid whose process is gone, matching the Python handling of
// psutil.NoSuchProcess in _async_get_worker_processes. It also asserts the
// collected entry carries the fields the payload and the component gauges need.
func TestWorkersSkipsExitedPids(t *testing.T) {
	self := os.Getpid()
	gone := unusedPID(t)

	s := &systemSource{rayletWorkerPIDs: func() ([]int32, error) {
		return []int32{int32(self), int32(gone)}, nil
	}}

	workers, err := s.workers()
	if err != nil {
		t.Fatalf("workers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("workers = %v, want exactly 1 entry (the live pid)", workers)
	}

	info, ok := workers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker entry type = %T, want map[string]interface{}", workers[0])
	}
	if got := info["pid"]; got != self {
		t.Errorf("worker pid = %v, want %d", got, self)
	}
	if got, ok := info["create_time"].(float64); !ok || got <= 0 {
		t.Errorf("worker create_time = %v (%T), want a positive float64", info["create_time"], info["create_time"])
	}
	cmdline, ok := info["cmdline"].([]string)
	if !ok || len(cmdline) == 0 {
		t.Fatalf("worker cmdline = %v (%T), want a non-empty []string", info["cmdline"], info["cmdline"])
	}
	if _, ok := info["num_fds"].(int); !ok {
		t.Errorf("worker num_fds = %v (%T), want int", info["num_fds"], info["num_fds"])
	}
	if _, ok := info["memory_info"].(map[string]interface{}); !ok {
		t.Errorf("worker memory_info = %v (%T), want map[string]interface{}", info["memory_info"], info["memory_info"])
	}
}

// TestWorkersSkipsZombies verifies a zombie worker is dropped, matching the
// Python `if w.status() == psutil.STATUS_ZOMBIE: continue` guard.
func TestWorkersSkipsZombies(t *testing.T) {
	pid, reap := zombiePID(t)
	defer reap()

	s := &systemSource{rayletWorkerPIDs: func() ([]int32, error) {
		return []int32{int32(pid)}, nil
	}}

	workers, err := s.workers()
	if err != nil {
		t.Fatalf("workers: %v", err)
	}
	if len(workers) != 0 {
		t.Errorf("workers = %v, want empty: pid %d is a zombie", workers, pid)
	}
}

// TestWorkersReturnsEmptyWhenRayletUnconfigured verifies worker discovery
// degrades to an empty (non-nil) list when no raylet address is configured, so
// the stats payload always carries a "workers" list.
func TestWorkersReturnsEmptyWhenRayletUnconfigured(t *testing.T) {
	s := &systemSource{}

	workers, err := s.workers()
	if err != nil {
		t.Fatalf("workers: %v", err)
	}
	if workers == nil {
		t.Fatal("workers = nil, want non-nil empty slice")
	}
	if len(workers) != 0 {
		t.Errorf("workers = %v, want empty", workers)
	}
}

// TestWorkersReturnsEmptyWhenRayletEmptyAndOnError verifies the two remaining
// degradation paths: a raylet that reports no worker, and a raylet call that
// fails. Both yield an empty list without an error, matching the Python
// `if not pids: return []` and the (GetTimeoutError, RpcError) handler.
func TestWorkersReturnsEmptyWhenRayletEmptyAndOnError(t *testing.T) {
	cases := []struct {
		name    string
		fetcher workerPIDFetcher
	}{
		{"no pids", func() ([]int32, error) { return nil, nil }},
		{"fetch error", func() ([]int32, error) { return nil, errors.New("raylet unreachable") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &systemSource{rayletWorkerPIDs: tc.fetcher}

			workers, err := s.workers()
			if err != nil {
				t.Fatalf("workers: %v", err)
			}
			if len(workers) != 0 {
				t.Errorf("workers = %v, want empty", workers)
			}
		})
	}
}

// TestAttachWorkerGPUInfo verifies worker entries receive gpu_memory_usage and
// gpu_utilization summed across every device the process uses, and that a
// process without GPU usage still carries both fields set to zero (the Python
// ProcessInfo payload always includes them).
func TestAttachWorkerGPUInfo(t *testing.T) {
	util := 20
	stats := map[string]interface{}{
		"workers": []interface{}{
			map[string]interface{}{"pid": 100, "cmdline": []string{"ray::A"}},
			map[string]interface{}{"pid": 200, "cmdline": []string{"ray::B"}},
		},
		"gpus": []GpuUtilizationInfo{
			{Index: 0, ProcessesPIDs: map[int]ProcessGPUInfo{
				100: {PID: 100, GPUMemoryUsage: 512, GPUUtilization: &util},
				200: {PID: 200, GPUMemoryUsage: 1024},
			}},
			{Index: 1, ProcessesPIDs: map[int]ProcessGPUInfo{
				100: {PID: 100, GPUMemoryUsage: 256, GPUUtilization: &util},
			}},
		},
	}

	attachWorkerGPUInfo(stats)

	workers := stats["workers"].([]interface{})
	first := workers[0].(map[string]interface{})
	if got := first["gpu_memory_usage"]; got != float64(768) {
		t.Errorf("worker 100 gpu_memory_usage = %v, want 768 (512+256 summed across devices)", got)
	}
	if got := first["gpu_utilization"]; got != float64(40) {
		t.Errorf("worker 100 gpu_utilization = %v, want 40 (20+20 summed across devices)", got)
	}

	second := workers[1].(map[string]interface{})
	if got := second["gpu_memory_usage"]; got != float64(1024) {
		t.Errorf("worker 200 gpu_memory_usage = %v, want 1024", got)
	}
	if got := second["gpu_utilization"]; got != float64(0) {
		t.Errorf("worker 200 gpu_utilization = %v, want 0 (nil utilization counts as zero)", got)
	}
}

// TestAttachWorkerGPUInfoWithoutGPUs verifies every worker still carries both
// GPU fields, set to zero, when the node has no GPU at all.
func TestAttachWorkerGPUInfoWithoutGPUs(t *testing.T) {
	stats := map[string]interface{}{
		"workers": []interface{}{map[string]interface{}{"pid": 100, "cmdline": []string{"ray::A"}}},
		"gpus":    []GpuUtilizationInfo{},
	}

	attachWorkerGPUInfo(stats)

	worker := stats["workers"].([]interface{})[0].(map[string]interface{})
	for _, key := range []string{"gpu_memory_usage", "gpu_utilization"} {
		got, ok := worker[key]
		if !ok {
			t.Errorf("worker is missing %q, want it present with value 0", key)
			continue
		}
		if got != float64(0) {
			t.Errorf("worker %s = %v, want 0", key, got)
		}
	}
}

// stubNodeManager is an in-process NodeManagerService that answers
// GetWorkerPIDs with the currently configured pid list or error. The fields are
// mutex-guarded because the serving goroutine reads them while the test writes.
type stubNodeManager struct {
	proto.UnimplementedNodeManagerServiceServer
	mu   sync.Mutex
	pids []int32
	err  error
}

func (s *stubNodeManager) GetWorkerPIDs(context.Context, *proto.GetWorkerPIDsRequest) (*proto.GetWorkerPIDsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return &proto.GetWorkerPIDsReply{Pids: s.pids}, nil
}

// setPids replaces the pid list the stub reports, for tests that drive several
// collection cycles through one stub.
func (s *stubNodeManager) setPids(pids []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pids = pids
}

// startStubRaylet serves the given stub on a loopback port and returns the port.
func startStubRaylet(t *testing.T, stub *stubNodeManager) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterNodeManagerServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address %q: %v", listener.Addr().String(), err)
	}
	parsed, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse port %q: %v", port, err)
	}
	return parsed
}

// TestCollectorWorkersOverRayletGRPC verifies the whole discovery path against a
// real gRPC node manager: NewStatsCollector wires the configured node manager
// port, the GetWorkerPIDs reply is mapped onto process info, and the resulting
// stats feed the component gauges and the process list the node detail API
// reads.
func TestCollectorWorkersOverRayletGRPC(t *testing.T) {
	self := os.Getpid()
	port := startStubRaylet(t, &stubNodeManager{pids: []int32{int32(self)}})

	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", NodeManagerPort: port}
	stats := NewStatsCollector(cfg).Collect()

	workers, ok := stats["workers"].([]interface{})
	if !ok {
		t.Fatalf("workers type = %T, want []interface{}", stats["workers"])
	}
	if len(workers) != 1 {
		t.Fatalf("workers = %v, want exactly 1 entry", workers)
	}
	worker, ok := workers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker entry type = %T, want map[string]interface{}", workers[0])
	}
	if got := worker["pid"]; got != self {
		t.Errorf("worker pid = %v, want %d", got, self)
	}

	cmdline, ok := worker["cmdline"].([]string)
	if !ok || len(cmdline) == 0 {
		t.Fatalf("worker cmdline = %v (%T), want a non-empty []string", worker["cmdline"], worker["cmdline"])
	}
	component := cmdline[0]

	// The same stats must produce a per-worker component gauge set, which is
	// what the node detail page and the per-task/per-actor views read.
	records := recordsFor(t, stats, nil, false)
	cpuRecords := findRecords(records, ComponentGaugeCPUPercentage)
	if len(cpuRecords) == 0 {
		t.Fatalf("no %s record generated for the discovered worker", ComponentGaugeCPUPercentage)
	}
	found := false
	for _, r := range cpuRecords {
		if r.Tags["Component"] == component {
			found = true
		}
	}
	if !found {
		t.Errorf("no %s record tagged Component=%q (components: %v)",
			ComponentGaugeCPUPercentage, component, componentNames(cpuRecords))
	}
}

// TestCollectorWorkersDegradesWhenRayletFails verifies a failing node manager
// leaves the worker list empty without failing the collection cycle, matching
// the Python agent which catches the RPC error and returns [].
func TestCollectorWorkersDegradesWhenRayletFails(t *testing.T) {
	port := startStubRaylet(t, &stubNodeManager{err: fmt.Errorf("no raylet here")})

	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", NodeManagerPort: port}
	stats := NewStatsCollector(cfg).Collect()

	workers, ok := stats["workers"].([]interface{})
	if !ok {
		t.Fatalf("workers type = %T, want []interface{}", stats["workers"])
	}
	if len(workers) != 0 {
		t.Errorf("workers = %v, want empty when the raylet call fails", workers)
	}
	// The rest of the cycle must still be complete.
	for _, key := range []string{"ip", "cpu", "mem", "gpus", "tpus"} {
		if _, ok := stats[key]; !ok {
			t.Errorf("%q missing from stats after a raylet failure", key)
		}
	}
}

// TestCollectorWorkersEmptyWhenNodeManagerPortUnset verifies a config without a
// node manager port yields an empty worker list instead of dialing an invalid
// address.
func TestCollectorWorkersEmptyWhenNodeManagerPortUnset(t *testing.T) {
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1"}
	stats := NewStatsCollector(cfg).Collect()

	workers, ok := stats["workers"].([]interface{})
	if !ok {
		t.Fatalf("workers type = %T, want []interface{}", stats["workers"])
	}
	if len(workers) != 0 {
		t.Errorf("workers = %v, want empty when no node manager port is configured", workers)
	}
}

// TestPublishOnceCarriesDiscoveredWorkers verifies the complete production path
// in one cycle: the raylet reply becomes worker process info, the publish-time
// step adds the GPU fields the Python ProcessInfo model requires, the worker
// feeds the per-component gauges, and the published payload carries the process
// list the node detail API reads. This is the end-to-end shape of the workers fix:
// workers is no longer permanently empty.
func TestPublishOnceCarriesDiscoveredWorkers(t *testing.T) {
	self := os.Getpid()
	port := startStubRaylet(t, &stubNodeManager{pids: []int32{int32(self)}})

	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", NodeManagerPort: port}
	m, pub := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	// The published payload must carry the discovered process.
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(pub.lastPayload()), &decoded); err != nil {
		t.Fatalf("decode published payload: %v", err)
	}
	workers, ok := decoded["workers"].([]interface{})
	if !ok || len(workers) != 1 {
		t.Fatalf("published payload workers = %v, want exactly 1 entry", decoded["workers"])
	}
	worker, ok := workers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("published worker type = %T, want map[string]interface{}", workers[0])
	}
	if got := worker["pid"]; got != float64(self) {
		t.Errorf("published worker pid = %v, want %d", got, self)
	}
	// The camelCase ProcessInfo fields the node detail UI consumes, including
	// the two GPU fields the publish-time merge supplies.
	for _, key := range []string{"pid", "createTime", "cpuPercent", "cmdline", "memoryInfo", "gpuMemoryUsage", "gpuUtilization"} {
		if _, ok := worker[key]; !ok {
			t.Errorf("published worker is missing %q (keys: %v)", key, workerKeys(worker))
		}
	}
}

// TestPublishOnceWorkersResetWhenRayletReportsNone verifies the stale-reset path:
// after a cycle that reported a worker, a cycle whose raylet reports no worker
// must zero that component's gauges. This matches the Python
// generate_worker_stats_record reset of _latest_worker_proc_names via
// _generate_reseted_stats_record, which writes zeros rather than unregistering
// the series.
func TestPublishOnceWorkersResetWhenRayletReportsNone(t *testing.T) {
	self := os.Getpid()
	stub := &stubNodeManager{pids: []int32{int32(self)}}
	port := startStubRaylet(t, stub)

	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", NodeManagerPort: port}
	m, pub := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("first publishOnce: %v", err)
	}

	// Recover the component name the worker was grouped under.
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(pub.lastPayload()), &decoded); err != nil {
		t.Fatalf("decode published payload: %v", err)
	}
	workers := decoded["workers"].([]interface{})
	cmdline := workers[0].(map[string]interface{})["cmdline"].([]interface{})
	component := cmdline[0].(string)

	// RSS is asserted rather than cpu_percent: the first cpu_percent sample of
	// any process is 0 by the psutil interval=None contract, so it could not
	// distinguish "reported" from "reset".
	if got := componentGaugeValue(t, m, ComponentGaugeRSSMB, component); got <= 0 {
		t.Fatalf("component %q rss gauge = %v after the first cycle, want > 0", component, got)
	}

	// Make the raylet report no worker at all. The process stays alive, so only
	// the discovery result changes.
	stub.setPids(nil)
	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("second publishOnce: %v", err)
	}

	if got := componentGaugeValue(t, m, ComponentGaugeRSSMB, component); got != 0 {
		t.Errorf("component %q rss gauge = %v after the raylet reported no worker, want 0 (reset)", component, got)
	}
}

// componentGaugeValue reads one component gauge series from the module registry,
// returning -1 when the series does not exist.
func componentGaugeValue(t *testing.T, m *ReporterModule, gauge, component string) float64 {
	t.Helper()
	families, err := m.metricsService.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "ray_"+gauge {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "Component" && label.GetValue() == component {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

// workerKeys returns the sorted keys of a decoded worker map, for failure messages.
func workerKeys(worker map[string]interface{}) []string {
	keys := make([]string, 0, len(worker))
	for k := range worker {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// componentNames lists the distinct Component tags of a record set, for failure
// messages.
func componentNames(records []gaugeRecord) []string {
	seen := map[string]struct{}{}
	names := make([]string, 0, len(records))
	for _, r := range records {
		name := r.Tags["Component"]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

// TestBeginCycleEvictsDepartedPids verifies the CPU-time baselines of
// processes that vanished are dropped at the start of the next cycle, so the
// baseline map stays bounded by the live process set rather than growing with
// every worker that ever ran.
func TestBeginCycleEvictsDepartedPids(t *testing.T) {
	s := &systemSource{}
	live := os.Getpid()
	departed := unusedPID(t)
	times := processCPUTimes{user: 1.5, system: 0.5}

	// Cycle 1 samples both pids, seeding a baseline for each.
	s.processCPUPercent(live, times)
	s.processCPUPercent(departed, times)
	if len(s.procCPUPrev) != 2 {
		t.Fatalf("procCPUPrev size after cycle 1 = %d, want 2", len(s.procCPUPrev))
	}

	// Cycle 2 samples only the live pid. Eviction is driven by the previous
	// cycle's samples, so both baselines are still present at its start: the
	// departed pid has not yet missed a cycle.
	s.beginCycle(posixTime())
	if len(s.procCPUPrev) != 2 {
		t.Fatalf("procCPUPrev size at the start of cycle 2 = %d, want 2 (both were sampled last cycle)", len(s.procCPUPrev))
	}
	s.processCPUPercent(live, times)

	// Cycle 3 evicts the baseline the departed pid failed to refresh in cycle 2,
	// while the live pid's baseline survives.
	s.beginCycle(posixTime())
	if _, ok := s.procCPUPrev[departed]; ok {
		t.Errorf("procCPUPrev still holds pid %d, want it evicted after it stopped being sampled", departed)
	}
	if _, ok := s.procCPUPrev[live]; !ok {
		t.Errorf("procCPUPrev dropped the live pid %d, want its baseline retained", live)
	}

	// A continuously sampled process keeps a live baseline, so its next sample
	// must not be treated as a first observation returning 0.
	if got := s.processCPUPercent(live, processCPUTimes{user: 3.5, system: 0.5}); got <= 0 {
		t.Errorf("cpu_percent for a continuously sampled process = %v, want > 0", got)
	}
}
