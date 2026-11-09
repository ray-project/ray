package reporter

import (
	"errors"
	"strings"
	"testing"
)

// errMIGListFailed stands in for a failing nvidia-smi invocation without pulling
// in the real binary.
var errMIGListFailed = errors.New("nvidia-smi failed")

// migSmiResponse bundles the answers of the four nvidia-smi invocations a MIG
// node produces: the device query, the MIG mode probe, the "-L" listing and the
// plain table.
type migSmiResponse struct {
	gpu   smiResponse
	mode  smiResponse
	list  smiResponse
	plain smiResponse
	proc  smiResponse
}

// fakeMigNvidiaSmi dispatches a commandRunner call on the nvidia-smi argument
// list. The plain invocation carries no arguments, so it is distinguishable only
// by arity and therefore checked last.
func fakeMigNvidiaSmi(r migSmiResponse) commandRunner {
	return func(name string, arg ...string) ([]byte, error) {
		for _, a := range arg {
			switch {
			case strings.HasPrefix(a, "--query-compute-apps"):
				return r.proc.out, r.proc.err
			case a == "-L":
				return r.list.out, r.list.err
			case strings.Contains(a, "mig.mode.current"):
				return r.mode.out, r.mode.err
			}
		}
		if len(arg) == 0 {
			return r.plain.out, r.plain.err
		}
		return r.gpu.out, r.gpu.err
	}
}

// migDeviceCSV is one A100 without MIG (index 0) and one with MIG enabled
// (index 1). MIG is enabled on the second GPU so the fixture also proves the
// index offset: Python computes gpu_index*1000+mig_index, which for GPU 0 would
// collide with the whole-device index and hide whether the offset was applied.
const migDeviceCSV = "" +
	"0, GPU-aaaa, NVIDIA A100-SXM4-40GB, 95, 40960, 40960\n" +
	"1, GPU-bbbb, NVIDIA A100-SXM4-40GB, 12, 1024, 40960\n"

// migModeCSV enables MIG on GPU 1 only.
const migModeCSV = "" +
	"0, GPU-aaaa, NVIDIA A100-SXM4-40GB, Disabled\n" +
	"1, GPU-bbbb, NVIDIA A100-SXM4-40GB, Enabled\n"

// migListOutput lists two MIG devices on GPU 1.
const migListOutput = "" +
	"GPU 0: A100-SXM4-40GB (UUID: GPU-aaaa)\n" +
	"GPU 1: A100-SXM4-40GB (UUID: GPU-bbbb)\n" +
	"  MIG 3g.20gb Device 0: (UUID: MIG-0000)\n" +
	"  MIG 1g.5gb Device 1: (UUID: MIG-1111)\n"

// migPlainOutput is the MIG devices table of the plain nvidia-smi output, with
// the per-instance memory and SM utilization of the two GPU 1 instances.
const migPlainOutput = `
+-----------------------------------------------------------------------------+
| MIG devices:                                                                |
+------------------+----------------------+-----------+-----------------------+
| GPU  GI  CI  MIG |         Memory-Usage |        Vol|         Shared        |
|                  |                      |  Sm   Mem |                       |
|==================+======================+===========+=======================|
|    1    1    0    0  |   512MiB / 20224MiB |     37     41 |   3   0   2   0   0 |
|    1    2    0    1  |     0MiB /  4864MiB |      0      0 |   1   0   1   0   0 |
+------------------+----------------------+-----------+-----------------------+
`

// TestParseMIGModeCsv pins that only the "Enabled" spelling counts as MIG being
// on. nvidia-smi reports "Disabled" for a normal GPU and "[N/A]" on a driver that
// predates MIG, and Python treats both as "not enabled" because
// nvmlDeviceGetMigMode fails or returns 0 there.
func TestParseMIGModeCsv(t *testing.T) {
	out := []byte("" +
		"0, GPU-aaaa, NVIDIA A100-SXM4-40GB, Enabled\n" +
		"1, GPU-bbbb, NVIDIA A100-SXM4-40GB, Disabled\n" +
		"2, GPU-cccc, NVIDIA A100-SXM4-40GB, [N/A]\n")

	rows := parseMIGModeCsv(out)

	if len(rows) != 3 {
		t.Fatalf("parseMIGModeCsv returned %d rows, want 3", len(rows))
	}
	wantEnabled := map[int]bool{0: true, 1: false, 2: false}
	for _, row := range rows {
		if got := row.enabled; got != wantEnabled[row.index] {
			t.Errorf("gpu %d enabled = %v, want %v", row.index, got, wantEnabled[row.index])
		}
	}
	if rows[0].uuid != "GPU-aaaa" || rows[0].name != "NVIDIA A100-SXM4-40GB" {
		t.Errorf("row identity lost: %+v", rows[0])
	}
}

// TestParseMIGList pins the "-L" parse: every MIG line belongs to the GPU line
// above it, and a GPU without MIG lines contributes nothing.
func TestParseMIGList(t *testing.T) {
	listings := parseMIGList([]byte(migListOutput))

	if len(listings) != 2 {
		t.Fatalf("parseMIGList returned %d listings, want 2", len(listings))
	}
	want := []migListing{
		{parentIndex: 1, migIndex: 0, profile: "3g.20gb", uuid: "MIG-0000"},
		{parentIndex: 1, migIndex: 1, profile: "1g.5gb", uuid: "MIG-1111"},
	}
	for i, w := range want {
		if listings[i] != w {
			t.Errorf("listing[%d] = %+v, want %+v", i, listings[i], w)
		}
	}
}

// TestParseMIGListWithoutMIG pins that a node without MIG instances yields an
// empty result, which is what keeps the non-MIG path untouched.
func TestParseMIGListWithoutMIG(t *testing.T) {
	out := []byte("" +
		"GPU 0: NVIDIA A10G (UUID: GPU-aaaa)\n" +
		"GPU 1: NVIDIA A10G (UUID: GPU-bbbb)\n")

	if listings := parseMIGList(out); len(listings) != 0 {
		t.Fatalf("parseMIGList returned %d listings, want 0: %+v", len(listings), listings)
	}
}

// TestParseMIGListSkipsUnattributableLines pins that a MIG line with no GPU line
// above it is dropped instead of being attributed to an arbitrary GPU.
func TestParseMIGListSkipsUnattributableLines(t *testing.T) {
	out := []byte("" +
		"  MIG 3g.20gb Device 0: (UUID: MIG-orphan)\n" +
		"GPU 1: A100-SXM4-40GB (UUID: GPU-bbbb)\n" +
		"  MIG 1g.5gb Device 0: (UUID: MIG-real)\n")

	listings := parseMIGList(out)

	if len(listings) != 1 {
		t.Fatalf("parseMIGList returned %d listings, want 1: %+v", len(listings), listings)
	}
	if listings[0].parentIndex != 1 || listings[0].uuid != "MIG-real" {
		t.Errorf("listing = %+v, want parent 1 / MIG-real", listings[0])
	}
}

// TestParseMIGTable pins the per-instance memory and SM utilization parse of the
// plain nvidia-smi table, keyed by parent GPU index and MIG device id.
func TestParseMIGTable(t *testing.T) {
	stats := parseMIGTable([]byte(migPlainOutput))

	if len(stats) != 2 {
		t.Fatalf("parseMIGTable returned %d entries, want 2: %+v", len(stats), stats)
	}
	first, ok := stats[[2]int{1, 0}]
	if !ok {
		t.Fatalf("no entry for GPU 1 MIG device 0: %+v", stats)
	}
	if first.memoryUsed != 512 || first.memoryTotal != 20224 || first.utilization != 37 {
		t.Errorf("GPU 1 device 0 = %+v, want 512/20224 @37%%", first)
	}
	second, ok := stats[[2]int{1, 1}]
	if !ok {
		t.Fatalf("no entry for GPU 1 MIG device 1: %+v", stats)
	}
	if second.memoryUsed != 0 || second.memoryTotal != 4864 || second.utilization != 0 {
		t.Errorf("GPU 1 device 1 = %+v, want 0/4864 @0%%", second)
	}
}

// TestParseMIGTableUnavailableUtilization pins that an instance whose Vol/Sm
// column reads N/A keeps the unavailable sentinel instead of claiming 0%, which
// is the same distinction already established for whole GPUs.
func TestParseMIGTableUnavailableUtilization(t *testing.T) {
	row := "|    0    1    0    0  |   512MiB / 20224MiB |    N/A      0 |   3   0   2   0   0 |\n"

	stats := parseMIGTable([]byte(row))

	entry, ok := stats[[2]int{0, 0}]
	if !ok {
		t.Fatalf("no entry parsed from %q: %+v", row, stats)
	}
	if entry.utilization != gpuUtilizationUnavailable {
		t.Errorf("utilization = %d, want %d for an N/A column", entry.utilization, gpuUtilizationUnavailable)
	}
	if entry.memoryUsed != 512 || entry.memoryTotal != 20224 {
		t.Errorf("memory = %d/%d, want 512/20224", entry.memoryUsed, entry.memoryTotal)
	}
}

// TestMIGDeviceName pins the name composition. Python reads the MIG name from
// nvmlDeviceGetName, which returns "<parent name> MIG <profile>"; nvidia-smi
// only exposes the parent name and the profile separately.
func TestMIGDeviceName(t *testing.T) {
	if got := migDeviceName("NVIDIA A100-SXM4-40GB", "3g.20gb"); got != "NVIDIA A100-SXM4-40GB MIG 3g.20gb" {
		t.Errorf("migDeviceName = %q, want the parent name followed by the profile", got)
	}
	if got := migDeviceName("", "3g.20gb"); got != "MIG 3g.20gb" {
		t.Errorf("migDeviceName with no parent name = %q, want %q", got, "MIG 3g.20gb")
	}
}

// TestGPUCollectorExpandsMIGInstances pins the whole MIG substitution: a
// MIG-enabled GPU is replaced by one record per instance with index
// parent*1000+migIndex, while a GPU without MIG keeps its whole-device record.
// Python _get_pynvml_gpu_usage does exactly this and never reports both.
func TestGPUCollectorExpandsMIGInstances(t *testing.T) {
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:   smiResponse{out: []byte(migDeviceCSV)},
		mode:  smiResponse{out: []byte(migModeCSV)},
		list:  smiResponse{out: []byte(migListOutput)},
		plain: smiResponse{out: []byte(migPlainOutput)},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 3 {
		t.Fatalf("Collect() returned %d devices, want 3 (1 whole GPU + 2 MIG): %+v", len(gpus), gpus)
	}

	whole := gpus[0]
	if whole.Index != 0 || whole.UUID != "GPU-aaaa" {
		t.Errorf("first device = index %d uuid %q, want the non-MIG GPU 0", whole.Index, whole.UUID)
	}
	if whole.UtilizationGPU != 95 || whole.MemoryTotal != 40960 {
		t.Errorf("non-MIG device lost its whole-device figures: %+v", whole)
	}

	mig0 := gpus[1]
	if mig0.Index != 1000 || mig0.UUID != "MIG-0000" {
		t.Errorf("second device = index %d uuid %q, want index 1000 uuid MIG-0000", mig0.Index, mig0.UUID)
	}
	if mig0.Name != "NVIDIA A100-SXM4-40GB MIG 3g.20gb" {
		t.Errorf("MIG device name = %q, want the parent name followed by the profile", mig0.Name)
	}
	if mig0.UtilizationGPU != 37 || mig0.MemoryUsed != 512 || mig0.MemoryTotal != 20224 {
		t.Errorf("MIG device = %d%% %d/%d, want 37%% 512/20224",
			mig0.UtilizationGPU, mig0.MemoryUsed, mig0.MemoryTotal)
	}

	mig1 := gpus[2]
	if mig1.Index != 1001 || mig1.UUID != "MIG-1111" {
		t.Errorf("third device = index %d uuid %q, want index 1001 uuid MIG-1111", mig1.Index, mig1.UUID)
	}
}

// TestGPUCollectorMigProcessesAttachByMigUUID pins that a MIG instance's
// processes are the compute apps whose gpu_uuid is the MIG uuid, and that their
// per-process utilization stays nil, which is what Python reports for MIG
// (nvml documents per-process utilization as unavailable in MIG mode).
func TestGPUCollectorMigProcessesAttachByMigUUID(t *testing.T) {
	proc := "4242, MIG-0000, 300\n4243, MIG-1111, 64\n"
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:   smiResponse{out: []byte(migDeviceCSV)},
		mode:  smiResponse{out: []byte(migModeCSV)},
		list:  smiResponse{out: []byte(migListOutput)},
		plain: smiResponse{out: []byte(migPlainOutput)},
		proc:  smiResponse{out: []byte(proc)},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 3 {
		t.Fatalf("Collect() returned %d devices, want 3", len(gpus))
	}
	if len(gpus[0].ProcessesPIDs) != 0 {
		t.Errorf("non-MIG GPU 0 gained %d processes, want 0", len(gpus[0].ProcessesPIDs))
	}
	first, ok := gpus[1].ProcessesPIDs[4242]
	if !ok {
		t.Fatalf("MIG instance 1000 has no process 4242: %+v", gpus[1].ProcessesPIDs)
	}
	if first.GPUMemoryUsage != 300 {
		t.Errorf("process 4242 memory = %d, want 300", first.GPUMemoryUsage)
	}
	if first.GPUUtilization != nil {
		t.Errorf("process 4242 utilization = %d, want nil for MIG", *first.GPUUtilization)
	}
	if _, ok := gpus[2].ProcessesPIDs[4243]; !ok {
		t.Errorf("MIG instance 1001 has no process 4243: %+v", gpus[2].ProcessesPIDs)
	}
}

// TestGPUCollectorDropsEnabledGPUWhenInstancesUnreadable pins that a MIG-enabled
// GPU is not reported as a whole device when the instance listing fails. Python
// extends the result with the instances and skips the whole-GPU record, so a
// whole-device record here would report figures Python never produces for MIG.
func TestGPUCollectorDropsEnabledGPUWhenInstancesUnreadable(t *testing.T) {
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:  smiResponse{out: []byte(migDeviceCSV)},
		mode: smiResponse{out: []byte(migModeCSV)},
		list: smiResponse{out: []byte(""), err: errMIGListFailed},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 1 {
		t.Fatalf("Collect() returned %d devices, want only the non-MIG GPU: %+v", len(gpus), gpus)
	}
	if gpus[0].Index != 0 {
		t.Errorf("surviving device index = %d, want 0 (the MIG-disabled GPU)", gpus[0].Index)
	}
}

// TestGPUCollectorKeepsWholeGpuWhenMigDisabled pins the non-MIG path: a node
// whose probe reports Disabled must report its devices unchanged, even though
// the probe itself succeeded.
func TestGPUCollectorKeepsWholeGpuWhenMigDisabled(t *testing.T) {
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:  smiResponse{out: []byte(migDeviceCSV)},
		mode: smiResponse{out: []byte("0, GPU-aaaa, NVIDIA A100-SXM4-40GB, Disabled\n1, GPU-bbbb, NVIDIA A100-SXM4-40GB, Disabled\n")},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d devices, want 2: %+v", len(gpus), gpus)
	}
	for i, want := range []int{0, 1} {
		if gpus[i].Index != want {
			t.Errorf("device[%d].Index = %d, want %d", i, gpus[i].Index, want)
		}
	}
}

// TestGPUCollectorIgnoresListingForMigDisabledGpu pins that the mode probe and
// the listing must agree before a device is reported as MIG. A listing that
// mentions instances for a GPU whose mode reads Disabled is stale; honouring it
// would report device indices Python never produces for that GPU.
func TestGPUCollectorIgnoresListingForMigDisabledGpu(t *testing.T) {
	mode := "" +
		"0, GPU-aaaa, NVIDIA A100-SXM4-40GB, Disabled\n" +
		"1, GPU-bbbb, NVIDIA A100-SXM4-40GB, Enabled\n"
	list := "" +
		"GPU 0: A100-SXM4-40GB (UUID: GPU-aaaa)\n" +
		"  MIG 1g.5gb Device 0: (UUID: MIG-stale)\n" +
		"GPU 1: A100-SXM4-40GB (UUID: GPU-bbbb)\n" +
		"  MIG 3g.20gb Device 0: (UUID: MIG-0000)\n"
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:  smiResponse{out: []byte(migDeviceCSV)},
		mode: smiResponse{out: []byte(mode)},
		list: smiResponse{out: []byte(list)},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d devices, want 1 whole GPU + 1 MIG: %+v", len(gpus), gpus)
	}
	if gpus[0].Index != 0 || gpus[0].UUID != "GPU-aaaa" {
		t.Errorf("first device = index %d uuid %q, want the whole GPU 0 despite its stale listing", gpus[0].Index, gpus[0].UUID)
	}
	if gpus[1].Index != 1000 || gpus[1].UUID != "MIG-0000" {
		t.Errorf("second device = index %d uuid %q, want the MIG instance of GPU 1", gpus[1].Index, gpus[1].UUID)
	}
}

// TestMIGInstancesOnlyCoversEnabledGPUs pins the instance builder on its own: a
// listing entry for a GPU whose mode probe says Disabled must not produce an
// instance, because the record's index and name would describe a device that
// does not exist.
func TestMIGInstancesOnlyCoversEnabledGPUs(t *testing.T) {
	list := "" +
		"GPU 0: A100-SXM4-40GB (UUID: GPU-aaaa)\n" +
		"  MIG 1g.5gb Device 0: (UUID: MIG-stale)\n" +
		"GPU 1: A100-SXM4-40GB (UUID: GPU-bbbb)\n" +
		"  MIG 3g.20gb Device 0: (UUID: MIG-0000)\n"
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		list:  smiResponse{out: []byte(list)},
		plain: smiResponse{out: []byte(migPlainOutput)},
	})

	enabled := map[int]migModeRow{1: {index: 1, uuid: "GPU-bbbb", name: "NVIDIA A100-SXM4-40GB", enabled: true}}
	instances := (&GPUCollector{}).migInstances(enabled)

	if len(instances) != 1 {
		t.Fatalf("migInstances returned %d instances, want 1: %+v", len(instances), instances)
	}
	if instances[0].parentIndex != 1 || instances[0].info.UUID != "MIG-0000" {
		t.Errorf("instance = %+v, want the GPU 1 instance only", instances[0])
	}
}

// TestGPUCollectorMigDegradesWithWholeGpuProbeFailure pins that a failing MIG
// mode probe is treated as "no MIG", matching Python, which swallows the
// NVMLError from nvmlDeviceGetMigMode and reports the regular GPU record.
func TestGPUCollectorMigDegradesWithWholeGpuProbeFailure(t *testing.T) {
	orig := runCommand
	defer func() { runCommand = orig }()
	runCommand = fakeMigNvidiaSmi(migSmiResponse{
		gpu:  smiResponse{out: []byte(migDeviceCSV)},
		mode: smiResponse{out: nil, err: errMIGListFailed},
	})

	gpus := NewGPUCollector().Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d devices, want 2 whole GPUs: %+v", len(gpus), gpus)
	}
	for i, want := range []int{0, 1} {
		if gpus[i].Index != want {
			t.Errorf("device[%d].Index = %d, want %d", i, gpus[i].Index, want)
		}
	}
}
