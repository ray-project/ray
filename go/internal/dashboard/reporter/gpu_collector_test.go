package reporter

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// fakeCommand returns a commandRunner backed by a fixed output and error so
// tests never invoke the real nvidia-smi binary.
func fakeCommand(out []byte, err error) commandRunner {
	return func(name string, arg ...string) ([]byte, error) {
		return out, err
	}
}

// smiResponse is the canned answer of one nvidia-smi invocation.
type smiResponse struct {
	out []byte
	err error
}

// fakeNvidiaSmi returns a commandRunner that answers the device query with gpu
// and the per-process query with proc. The collector issues both, so a fake
// that ignored the arguments would feed device CSV output to the process parser.
func fakeNvidiaSmi(gpu, proc smiResponse) commandRunner {
	return func(name string, arg ...string) ([]byte, error) {
		for _, a := range arg {
			if strings.HasPrefix(a, "--query-compute-apps") {
				return proc.out, proc.err
			}
		}
		return gpu.out, gpu.err
	}
}

// noProcQuery is the per-process answer of tests that only exercise the device
// query: the query succeeds but reports no process.
var noProcQuery = smiResponse{}

func TestGPUCollectorParsesCSV(t *testing.T) {
	csvOut := []byte(
		"0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n" +
			"1, GPU-5678efgh, NVIDIA GeForce RTX 2080, 0, 0, 8192\n",
	)
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: csvOut}, noProcQuery)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d GPUs, want 2", len(gpus))
	}

	g0 := gpus[0]
	if g0.Index != 0 {
		t.Errorf("gpu[0].Index = %d, want 0", g0.Index)
	}
	if g0.UUID != "GPU-1234abcd" {
		t.Errorf("gpu[0].UUID = %q, want GPU-1234abcd", g0.UUID)
	}
	if g0.Name != "NVIDIA A100-SXM4-40GB" {
		t.Errorf("gpu[0].Name = %q, want NVIDIA A100-SXM4-40GB", g0.Name)
	}
	if g0.UtilizationGPU != 45 {
		t.Errorf("gpu[0].UtilizationGPU = %d, want 45", g0.UtilizationGPU)
	}
	if g0.MemoryUsed != 1234 {
		t.Errorf("gpu[0].MemoryUsed = %d, want 1234", g0.MemoryUsed)
	}
	if g0.MemoryTotal != 40960 {
		t.Errorf("gpu[0].MemoryTotal = %d, want 40960", g0.MemoryTotal)
	}
	if g0.ProcessesPIDs == nil {
		t.Errorf("gpu[0].ProcessesPIDs = nil, want non-nil empty map")
	}

	g1 := gpus[1]
	if g1.Index != 1 {
		t.Errorf("gpu[1].Index = %d, want 1", g1.Index)
	}
	if g1.UUID != "GPU-5678efgh" {
		t.Errorf("gpu[1].UUID = %q, want GPU-5678efgh", g1.UUID)
	}
	if g1.Name != "NVIDIA GeForce RTX 2080" {
		t.Errorf("gpu[1].Name = %q, want NVIDIA GeForce RTX 2080", g1.Name)
	}
}

func TestGPUCollectorDegradesOnCommandError(t *testing.T) {
	orig := runCommand
	runCommand = fakeCommand(nil, errors.New("nvidia-smi not found"))
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if gpus == nil {
		t.Fatalf("Collect() returned nil, want non-nil empty slice")
	}
	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs on error, want 0", len(gpus))
	}
}

func TestGPUCollectorDegradesOnMalformedCSV(t *testing.T) {
	// An unclosed quote makes the CSV unreadable, which must degrade to empty.
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: []byte("\"unclosed, 0, GPU-1, NVIDIA A100, 10, 5, 100\n")}, noProcQuery)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs on malformed input, want 0", len(gpus))
	}
}

func TestGPUCollectorSkipsShortRecords(t *testing.T) {
	// Rows with fewer than 6 fields are not usable GPU records and are skipped,
	// so a uniform short output degrades to empty rather than producing garbage.
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: []byte("0, GPU-1, NVIDIA A100\n1, GPU-2, NVIDIA B200\n")}, noProcQuery)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs, want 0 (short records skipped)", len(gpus))
	}
}

// TestGPUQueryArgsAreWhitelisted pins both nvidia-smi argument lists. The
// arguments are the only strings reaching the command line, so a change that
// started interpolating external data there must fail this test.
func TestGPUQueryArgsAreWhitelisted(t *testing.T) {
	device := strings.Join(gpuQueryArgs, " ")
	if device != "--query-gpu=index,uuid,name,utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu --format=csv,noheader,nounits" {
		t.Errorf("gpuQueryArgs = %q, want the fixed device query", device)
	}
	proc := strings.Join(gpuProcessQueryArgs, " ")
	if proc != "--query-compute-apps=pid,gpu_uuid,used_memory --format=csv,noheader,nounits" {
		t.Errorf("gpuProcessQueryArgs = %q, want the fixed per-process query", proc)
	}
}

func TestParseGPUProcessCsv(t *testing.T) {
	// Two devices, one of them with two processes. nvidia-smi emits the device
	// uuid, which is how a row is tied back to a device: the numeric index is
	// not part of the per-process query.
	out := []byte(
		"1234, GPU-1234abcd, 512\n" +
			"5678, GPU-1234abcd, 1024\n" +
			"9999, GPU-5678efgh, 256\n",
	)

	byUUID := parseGPUProcessCsv(out)

	if len(byUUID) != 2 {
		t.Fatalf("parseGPUProcessCsv grouped %d devices, want 2: %v", len(byUUID), byUUID)
	}
	first := byUUID["GPU-1234abcd"]
	if len(first) != 2 {
		t.Fatalf("GPU-1234abcd has %d processes, want 2: %v", len(first), first)
	}
	if got := first[1234]; got.PID != 1234 || got.GPUMemoryUsage != 512 {
		t.Errorf("process 1234 = %+v, want pid 1234 memory 512", got)
	}
	if got := first[5678]; got.PID != 5678 || got.GPUMemoryUsage != 1024 {
		t.Errorf("process 5678 = %+v, want pid 5678 memory 1024", got)
	}
	// nvidia-smi has no per-process SM utilization, so the pointer stays nil,
	// matching Python's older-API fallback where gpu_utilization is None.
	for pid, proc := range first {
		if proc.GPUUtilization != nil {
			t.Errorf("process %d GPUUtilization = %v, want nil", pid, *proc.GPUUtilization)
		}
	}
	second := byUUID["GPU-5678efgh"]
	if got := second[9999]; got.GPUMemoryUsage != 256 {
		t.Errorf("process 9999 = %+v, want memory 256", got)
	}
}

func TestParseGPUProcessCsvSkipsInvalidRows(t *testing.T) {
	// A short row, a non-numeric pid, a missing uuid and an unusable memory
	// field must not produce process entries.
	out := []byte(
		"1234, GPU-1\n" +
			"not-a-pid, GPU-1, 512\n" +
			"4321, , 512\n" +
			"8765, GPU-1, N/A\n",
	)

	byUUID := parseGPUProcessCsv(out)

	if len(byUUID) != 1 {
		t.Fatalf("parseGPUProcessCsv returned %d devices, want 1 (only GPU-1): %v", len(byUUID), byUUID)
	}
	processes := byUUID["GPU-1"]
	if len(processes) != 1 {
		t.Fatalf("GPU-1 has %d processes, want 1 (only the N/A memory row): %v", len(processes), processes)
	}
	if got := processes[8765]; got.PID != 8765 || got.GPUMemoryUsage != 0 {
		t.Errorf("process 8765 = %+v, want pid 8765 with memory degraded to 0", got)
	}
}

func TestParseGPUProcessCsvDegradesOnMalformedInput(t *testing.T) {
	if byUUID := parseGPUProcessCsv([]byte("\"unclosed, 1, GPU-1, 512\n")); byUUID != nil {
		t.Errorf("parseGPUProcessCsv on malformed input = %v, want nil", byUUID)
	}
	if byUUID := parseGPUProcessCsv(nil); len(byUUID) != 0 {
		t.Errorf("parseGPUProcessCsv on empty input = %v, want no devices", byUUID)
	}
}

// TestGPUCollectorAttachesProcessesByUUID is the core of the GPU-processes fix: the device
// records must carry their processes so the worker and actor GPU fields derived
// from them stop being constantly zero.
func TestGPUCollectorAttachesProcessesByUUID(t *testing.T) {
	device := []byte(
		"0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n" +
			"1, GPU-5678efgh, NVIDIA A100-SXM4-40GB, 10, 256, 40960\n",
	)
	proc := []byte(
		"1234, GPU-1234abcd, 512\n" +
			"1234, GPU-5678efgh, 256\n" +
			"5678, GPU-1234abcd, 1024\n" +
			"4321, GPU-unplugged, 2048\n",
	)
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: device}, smiResponse{out: proc})
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	c.inContainer = func() bool { return false }
	gpus := c.Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d GPUs, want 2", len(gpus))
	}
	if got := len(gpus[0].ProcessesPIDs); got != 2 {
		t.Fatalf("gpu[0] has %d processes, want 2: %v", got, gpus[0].ProcessesPIDs)
	}
	if got := gpus[0].ProcessesPIDs[1234]; got.GPUMemoryUsage != 512 {
		t.Errorf("gpu[0] process 1234 = %+v, want memory 512", got)
	}
	if got := gpus[0].ProcessesPIDs[5678]; got.GPUMemoryUsage != 1024 {
		t.Errorf("gpu[0] process 5678 = %+v, want memory 1024", got)
	}
	// The pid using both devices appears on each of them, which is what makes
	// attachWorkerGPUInfo sum its usage across devices.
	if got := gpus[1].ProcessesPIDs[1234]; got.GPUMemoryUsage != 256 {
		t.Errorf("gpu[1] process 1234 = %+v, want memory 256", got)
	}
	// A process on a device absent from the device query has nowhere to attach.
	for _, gpu := range gpus {
		if _, ok := gpu.ProcessesPIDs[4321]; ok {
			t.Errorf("gpu %d attached process 4321 of unknown device %q", gpu.Index, "GPU-unplugged")
		}
	}
}

// TestCollectFeedsNonZeroWorkerGPUUsage covers the defect end to end: the GPU
// numbers the reporter publishes for a worker are derived from
// ProcessesPIDs, so this guards the whole chain from nvidia-smi to the worker
// fields.
func TestCollectFeedsNonZeroWorkerGPUUsage(t *testing.T) {
	device := []byte("0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n")
	proc := []byte("4242, GPU-1234abcd, 512\n")
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: device}, smiResponse{out: proc})
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	c.inContainer = func() bool { return false }

	stats := map[string]interface{}{
		"workers": []interface{}{
			map[string]interface{}{"pid": 4242, "cmdline": []string{"ray::A"}},
			map[string]interface{}{"pid": 7, "cmdline": []string{"ray::B"}},
		},
		"gpus": c.Collect(),
	}
	attachWorkerGPUInfo(stats)

	workers := stats["workers"].([]interface{})
	owner := workers[0].(map[string]interface{})
	if got := owner["gpu_memory_usage"]; got != float64(512) {
		t.Errorf("worker 4242 gpu_memory_usage = %v, want 512", got)
	}
	// Utilization stays zero because nvidia-smi cannot report per-process SM
	// usage; the memory figure alone must no longer be zero.
	if got := owner["gpu_utilization"]; got != float64(0) {
		t.Errorf("worker 4242 gpu_utilization = %v, want 0 (unavailable per process)", got)
	}
	other := workers[1].(map[string]interface{})
	if got := other["gpu_memory_usage"]; got != float64(0) {
		t.Errorf("worker 7 gpu_memory_usage = %v, want 0", got)
	}
}

func TestGPUCollectorKeepsDevicesWhenProcessQueryFails(t *testing.T) {
	// A driver without the per-process query must not lose device utilization.
	device := []byte("0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n")
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: device},
		smiResponse{err: errors.New("exit status 1")})
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 1 {
		t.Fatalf("Collect() returned %d GPUs, want 1", len(gpus))
	}
	if gpus[0].UtilizationGPU != 45 {
		t.Errorf("gpu[0].UtilizationGPU = %d, want 45", gpus[0].UtilizationGPU)
	}
	if gpus[0].ProcessesPIDs == nil {
		t.Errorf("gpu[0].ProcessesPIDs = nil, want non-nil empty map")
	}
	if len(gpus[0].ProcessesPIDs) != 0 {
		t.Errorf("gpu[0] has %d processes after a failed process query, want 0", len(gpus[0].ProcessesPIDs))
	}
}

func TestGPUCollectorSkipsProcessQueryWhenNoDevice(t *testing.T) {
	// With no device there is nothing to attach processes to, and the second
	// nvidia-smi invocation must not happen at all.
	orig := runCommand
	defer func() { runCommand = orig }()
	calls := 0
	runCommand = func(name string, arg ...string) ([]byte, error) {
		calls++
		return []byte(""), nil
	}

	c := NewGPUCollector()
	if gpus := c.Collect(); len(gpus) != 0 {
		t.Fatalf("Collect() returned %d GPUs, want 0", len(gpus))
	}
	if calls != 1 {
		t.Errorf("nvidia-smi was invoked %d times, want 1 (no device to query processes for)", calls)
	}
}

func TestParseSchedHostPID(t *testing.T) {
	tests := []struct {
		name      string
		firstLine string
		wantPID   int
		wantOK    bool
	}{
		{name: "plain", firstLine: "bash (1279649, #threads: 1)", wantPID: 1279649, wantOK: true},
		{name: "truncated comm", firstLine: "python3 (42, #threads: 12)", wantPID: 42, wantOK: true},
		{name: "comm with spaces and parens", firstLine: "ray::Worker (id=7) (321, #threads: 3)", wantPID: 321, wantOK: true},
		{name: "empty comm", firstLine: " (88, #threads: 1)", wantPID: 88, wantOK: true},
		{name: "no pid", firstLine: "bash (no pid here)", wantOK: false},
		{name: "no parens", firstLine: "bash 1234 #threads: 1", wantOK: false},
		{name: "empty", firstLine: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pid, ok := parseSchedHostPID(tt.firstLine)
			if ok != tt.wantOK {
				t.Fatalf("parseSchedHostPID(%q) ok = %v, want %v", tt.firstLine, ok, tt.wantOK)
			}
			if ok && pid != tt.wantPID {
				t.Errorf("parseSchedHostPID(%q) pid = %d, want %d", tt.firstLine, pid, tt.wantPID)
			}
		})
	}
}

// TestParseSchedHostPIDMatchesLiveProc guards the sched format against the real
// kernel, so the fixture strings above cannot drift away from reality unseen.
// The parsed value is the outer-namespace pid, which equals os.Getpid() only
// when the process is not pid-remapped; the assertion is therefore limited to
// what holds in both cases, i.e. a positive pid parsed from a real first line.
func TestParseSchedHostPIDMatchesLiveProc(t *testing.T) {
	path := filepath.Join(procDirPath, "self", schedFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	firstLine := string(raw)
	if i := strings.IndexByte(firstLine, '\n'); i >= 0 {
		firstLine = firstLine[:i]
	}

	hostPID, ok := parseSchedHostPID(firstLine)
	if !ok {
		t.Fatalf("parseSchedHostPID(%q) failed to parse the live sched first line", firstLine)
	}
	if hostPID <= 0 {
		t.Errorf("parseSchedHostPID(%q) = %d, want a positive pid", firstLine, hostPID)
	}

	// The live /proc must resolve that host pid without any fixture, which is
	// what the production scan relies on.
	mapping := (&GPUCollector{procDir: procDirPath}).hostToContainerPIDs(map[int]struct{}{hostPID: {}})
	if _, ok := mapping[hostPID]; !ok {
		t.Fatalf("hostToContainerPIDs over the real /proc did not resolve host pid %d (line %q); mapping = %v",
			hostPID, firstLine, mapping)
	}
}

func TestParseDecimalPID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantPID int
		wantOK  bool
	}{
		{name: "digits", input: "1234", wantPID: 1234, wantOK: true},
		{name: "empty", input: "", wantOK: false},
		{name: "letters", input: "abc", wantOK: false},
		{name: "mixed", input: "12a", wantOK: false},
		{name: "negative", input: "-12", wantOK: false},
		{name: "plus", input: "+12", wantOK: false},
		{name: "float", input: "1.5", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pid, ok := parseDecimalPID(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("parseDecimalPID(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if ok && pid != tt.wantPID {
				t.Errorf("parseDecimalPID(%q) = %d, want %d", tt.input, pid, tt.wantPID)
			}
		})
	}
}

// writeSchedProc builds one /proc-like entry holding a sched file whose first
// line records hostPID.
func writeSchedProc(t *testing.T, root, containerPID string, hostPID int) {
	t.Helper()
	dir := filepath.Join(root, containerPID)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	content := "bash (" + strconv.Itoa(hostPID) + ", #threads: 1)\nother line\n"
	if err := os.WriteFile(filepath.Join(dir, schedFileName), []byte(content), 0o644); err != nil {
		t.Fatalf("write sched of %s: %v", containerPID, err)
	}
}

func TestHostToContainerPIDs(t *testing.T) {
	root := t.TempDir()
	writeSchedProc(t, root, "100", 9001)
	writeSchedProc(t, root, "200", 9002)
	// Entries that must be ignored: a non-numeric name and a process whose sched
	// file is missing (it exited between the directory read and the file read).
	if err := os.Mkdir(filepath.Join(root, "notapid"), 0o755); err != nil {
		t.Fatalf("mkdir notapid: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "300"), 0o755); err != nil {
		t.Fatalf("mkdir 300: %v", err)
	}

	c := &GPUCollector{procDir: root}
	mapping := c.hostToContainerPIDs(map[int]struct{}{9001: {}, 9002: {}, 9003: {}})

	if len(mapping) != 2 {
		t.Fatalf("hostToContainerPIDs = %v, want exactly the two resolvable pids", mapping)
	}
	if got := mapping[9001]; got != 100 {
		t.Errorf("host 9001 -> %d, want 100", got)
	}
	if got := mapping[9002]; got != 200 {
		t.Errorf("host 9002 -> %d, want 200", got)
	}
	if _, ok := mapping[9003]; ok {
		t.Errorf("host 9003 was mapped although no process reports it: %v", mapping)
	}
}

func TestHostToContainerPIDsUnreadableRoot(t *testing.T) {
	c := &GPUCollector{procDir: filepath.Join(t.TempDir(), "missing")}
	if mapping := c.hostToContainerPIDs(map[int]struct{}{1: {}}); mapping != nil {
		t.Errorf("hostToContainerPIDs on an unreadable root = %v, want nil", mapping)
	}
}

func TestTranslateToContainerPIDs(t *testing.T) {
	root := t.TempDir()
	writeSchedProc(t, root, "100", 9001)

	c := &GPUCollector{procDir: root}
	byUUID := map[string]map[int]ProcessGPUInfo{
		"GPU-1": {
			9001: {PID: 9001, GPUMemoryUsage: 512},
			9002: {PID: 9002, GPUMemoryUsage: 256},
		},
	}

	translated := c.translateToContainerPIDs(byUUID)

	processes := translated["GPU-1"]
	if len(processes) != 2 {
		t.Fatalf("translated GPU-1 has %d processes, want 2: %v", len(processes), processes)
	}
	mapped := processes[100]
	if mapped.PID != 100 {
		t.Errorf("mapped process PID = %d, want 100 (the container pid)", mapped.PID)
	}
	if mapped.GPUMemoryUsage != 512 {
		t.Errorf("mapped process memory = %d, want 512 preserved", mapped.GPUMemoryUsage)
	}
	if _, ok := processes[9001]; ok {
		t.Errorf("host pid 9001 survived the translation: %v", processes)
	}
	// An unmapped host pid keeps its original key, matching Python.
	unmapped := processes[9002]
	if unmapped.PID != 9002 || unmapped.GPUMemoryUsage != 256 {
		t.Errorf("unmapped process = %+v, want pid 9002 memory 256 kept verbatim", unmapped)
	}
}

// TestTranslateToContainerPIDsScansOnceForAllDevices pins the deliberate
// optimization: the union of pids is resolved with a single /proc scan, and a
// pid shared by two devices maps identically on both.
func TestTranslateToContainerPIDsScansOnceForAllDevices(t *testing.T) {
	root := t.TempDir()
	writeSchedProc(t, root, "100", 9001)

	c := &GPUCollector{procDir: root}
	byUUID := map[string]map[int]ProcessGPUInfo{
		"GPU-1": {9001: {PID: 9001, GPUMemoryUsage: 512}},
		"GPU-2": {9001: {PID: 9001, GPUMemoryUsage: 256}},
	}

	translated := c.translateToContainerPIDs(byUUID)

	for uuid, wantMemory := range map[string]int64{"GPU-1": 512, "GPU-2": 256} {
		processes := translated[uuid]
		if len(processes) != 1 {
			t.Fatalf("%s has %d processes, want 1: %v", uuid, len(processes), processes)
		}
		got, ok := processes[100]
		if !ok {
			t.Fatalf("%s is missing container pid 100: %v", uuid, processes)
		}
		if got.GPUMemoryUsage != wantMemory {
			t.Errorf("%s process 100 memory = %d, want %d", uuid, got.GPUMemoryUsage, wantMemory)
		}
	}
}

func TestGPUCollectorAppliesContainerPIDTranslation(t *testing.T) {
	// Inside a container nvidia-smi reports host pids while the raylet reports
	// container pids, so without the translation no worker ever matches its GPU
	// usage. This asserts the whole Collect path under that condition.
	root := t.TempDir()
	writeSchedProc(t, root, "100", 9001)

	device := []byte("0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n")
	proc := []byte("9001, GPU-1234abcd, 512\n")
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: device}, smiResponse{out: proc})
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	c.procDir = root
	c.inContainer = func() bool { return true }
	gpus := c.Collect()

	if len(gpus) != 1 {
		t.Fatalf("Collect() returned %d GPUs, want 1", len(gpus))
	}
	processes := gpus[0].ProcessesPIDs
	if _, ok := processes[9001]; ok {
		t.Fatalf("host pid 9001 survived into the device processes: %v", processes)
	}
	got, ok := processes[100]
	if !ok {
		t.Fatalf("device processes are missing the container pid 100: %v", processes)
	}
	if got.GPUMemoryUsage != 512 {
		t.Errorf("container process 100 memory = %d, want 512", got.GPUMemoryUsage)
	}

	// Negative control: the container pid is what the raylet reports, so this
	// worker is matched only because of the translation above.
	stats := map[string]interface{}{
		"workers": []interface{}{map[string]interface{}{"pid": 100, "cmdline": []string{"ray::A"}}},
		"gpus":    gpus,
	}
	attachWorkerGPUInfo(stats)
	worker := stats["workers"].([]interface{})[0].(map[string]interface{})
	if got := worker["gpu_memory_usage"]; got != float64(512) {
		t.Errorf("worker 100 gpu_memory_usage = %v, want 512", got)
	}
}

func TestGPUCollectorKeepsHostPIDsOutsideContainer(t *testing.T) {
	// Outside a container the raylet pids are host pids, so the translation must
	// not run and nvidia-smi's pids are the ones that match.
	root := t.TempDir()
	writeSchedProc(t, root, "100", 9001)

	device := []byte("0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n")
	proc := []byte("9001, GPU-1234abcd, 512\n")
	orig := runCommand
	runCommand = fakeNvidiaSmi(smiResponse{out: device}, smiResponse{out: proc})
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	c.procDir = root
	c.inContainer = func() bool { return false }
	gpus := c.Collect()

	processes := gpus[0].ProcessesPIDs
	if _, ok := processes[100]; ok {
		t.Fatalf("container pid 100 appeared although the process is not in a container: %v", processes)
	}
	if got, ok := processes[9001]; !ok || got.GPUMemoryUsage != 512 {
		t.Errorf("device processes = %v, want host pid 9001 with memory 512", processes)
	}
}
