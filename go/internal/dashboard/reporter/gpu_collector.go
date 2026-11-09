package reporter

import (
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/pkg/log"
)

// procDirPath is the process filesystem scanned by the container pid
// translation, mirroring the Python os.listdir("/proc").
const procDirPath = "/proc"

// schedFileName is the proc file whose first line records the host pid of a
// container-local process, mirroring the Python /proc/<pid>/sched read.
const schedFileName = "sched"

// gpuUtilizationUnavailable is the sentinel Python reports for a device whose
// utilization it could not read. gpu_providers.py initializes utilization to -1
// before each nvmlDeviceGetUtilizationRates call and keeps that value when the
// call raises NVMLError, so -1 reaches the payload and is summed into
// node_gpus_utilization unchanged (the _to_records guard only skips None, which
// production code never produces). nvidia-smi signals the same condition by
// printing [N/A] instead of a number.
const gpuUtilizationUnavailable = -1

// ProcessGPUInfo holds GPU usage for a single process, aligned with the
// Python ProcessGPUInfo TypedDict in gpu_providers.py.
type ProcessGPUInfo struct {
	PID            int
	GPUMemoryUsage int64
	GPUUtilization *int
}

// GpuUtilizationInfo holds GPU utilization for a single GPU device, aligned
// with the Python GpuUtilizationInfo TypedDict in gpu_providers.py. Memory
// fields are in MiB and UtilizationGPU is a percentage (0-100), matching the
// nvidia-smi --query-gpu output, or gpuUtilizationUnavailable when the device
// does not report one.
type GpuUtilizationInfo struct {
	Index          int
	Name           string
	UUID           string
	UtilizationGPU int
	MemoryUsed     int64
	MemoryTotal    int64
	// PowerMW and TemperatureC are optional: nvidia-smi reports [N/A] for a
	// device that does not expose them, and the Python provider maps that to a
	// None it then skips. The Has flags carry that distinction.
	PowerMW        int64
	HasPower       bool
	TemperatureC   int64
	HasTemperature bool
	ProcessesPIDs  map[int]ProcessGPUInfo
}

// commandRunner abstracts executing an external command and returning its
// combined stdout+stderr. It is a package-level variable so tests can inject
// a fake and avoid invoking the real nvidia-smi binary.
type commandRunner func(name string, arg ...string) ([]byte, error)

// runCommand is the production commandRunner, wrapping os/exec.
var runCommand commandRunner = func(name string, arg ...string) ([]byte, error) {
	return exec.Command(name, arg...).CombinedOutput()
}

// gpuQueryArgs is the fixed argument list passed to nvidia-smi. Keeping the
// arguments as a constant whitelist prevents any external string from being
// injected into the command line.
var gpuQueryArgs = []string{
	"--query-gpu=index,uuid,name,utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu",
	"--format=csv,noheader,nounits",
}

// gpuProcessQueryArgs is the fixed argument list of the per-process query. It
// mirrors the NVML process queries of Python _get_gpu_info: gpu_uuid ties a row
// to a device and used_memory (MiB under nounits) becomes gpu_memory_usage.
// nvidia-smi exposes no per-process SM utilization, so GPUUtilization stays nil,
// which is exactly the shape Python produces through its older-API fallback
// (nvmlDeviceGetComputeRunningProcesses / nvmlDeviceGetGraphicsRunningProcesses),
// where gpu_utilization is None.
var gpuProcessQueryArgs = []string{
	"--query-compute-apps=pid,gpu_uuid,used_memory",
	"--format=csv,noheader,nounits",
}

// migModeQueryArgs is the fixed argument list of the MIG mode probe. nvidia-smi
// documents mig.mode.current as a --query-gpu field, reporting Enabled or
// Disabled per physical GPU. nvidia-smi exposes no CSV path for a MIG instance
// itself, so the instances are listed with migListArgs below.
var migModeQueryArgs = []string{
	"--query-gpu=index,uuid,name,mig.mode.current",
	"--format=csv,noheader,nounits",
}

// migListArgs lists every GPU and, for a MIG-enabled one, its MIG devices in a
// stable line-oriented text form ("nvidia-smi -L"):
//
//	GPU 0: A100-SXM4-40GB (UUID: GPU-e86cb44c-...)
//	  MIG 3g.20gb Device 0: (UUID: MIG-c7384736-...)
//
// This is the only documented way to enumerate MIG instances, because
// --query-gpu has no per-instance fields.
var migListArgs = []string{"-L"}

// migIndexBase separates a MIG instance index from the parent GPU index, so MIG
// devices and whole GPUs never collide in the reported index space. Python
// _get_mig_device_info computes gpu_index * 1000 + mig_index for the same reason.
const migIndexBase = 1000

// GPUCollector collects GPU utilization by parsing nvidia-smi output.
type GPUCollector struct {
	// procDir is the process filesystem scanned for the host -> container pid
	// translation. It is a field so tests can point the scan at a fixture tree
	// instead of the live /proc.
	procDir string
	// inContainer mirrors Python IN_CONTAINER in gpu_providers.py (the presence
	// of the cgroup mount). It is injectable so the translation can be exercised
	// without a container, and because the test host may itself be a container.
	inContainer func() bool
}

// NewGPUCollector builds a GPUCollector using the default nvidia-smi command.
func NewGPUCollector() *GPUCollector {
	k8s := NewK8sAdapter()
	return &GPUCollector{
		procDir:     procDirPath,
		inContainer: k8s.IsInContainer,
	}
}

// Collect returns GPU utilization for all devices. It degrades to a non-nil
// empty slice when nvidia-smi is unavailable or the command fails, so callers
// never observe an error.
func (c *GPUCollector) Collect() []GpuUtilizationInfo {
	out, err := runCommand("nvidia-smi", gpuQueryArgs...)
	if err != nil {
		return []GpuUtilizationInfo{}
	}

	gpus := parseGPUCsv(out)
	if len(gpus) == 0 {
		return gpus
	}

	// A MIG-enabled GPU reports the whole-device figures, but the workloads run
	// on its MIG instances. Python replaces such a GPU with one record per
	// instance (gpu_index * 1000 + mig_index) and never reports both. The same
	// substitution is applied here; devices that do not report MIG mode keep
	// their whole-GPU record.
	gpus = c.expandMigDevices(gpus)
	return c.attachGPUProcesses(gpus)
}

// expandMigDevices replaces every MIG-enabled device with one record per MIG
// instance, mirroring Python _get_pynvml_gpu_usage: when nvmlDeviceGetMigMode
// reports MIG enabled the provider extends the result with _get_mig_devices and
// skips the whole-GPU record. A node with no MIG instance keeps its devices
// untouched, so the non-MIG path is unaffected.
//
// nvidia-smi offers no CSV path for MIG instances, so the instances come from
// the line-oriented "nvidia-smi -L" listing and their figures from the
// per-instance columns of the plain nvidia-smi table.
func (c *GPUCollector) expandMigDevices(gpus []GpuUtilizationInfo) []GpuUtilizationInfo {
	enabled := c.migEnabledGPUs()
	if len(enabled) == 0 {
		return gpus
	}

	instances := c.migInstances(enabled)
	if len(instances) == 0 {
		// MIG is on but no instance could be read. Keeping the whole-GPU records
		// would report figures Python never produces for a MIG device, so the
		// enabled devices are dropped rather than misreported.
		kept := make([]GpuUtilizationInfo, 0, len(gpus))
		for _, gpu := range gpus {
			if _, mig := enabled[gpu.Index]; !mig {
				kept = append(kept, gpu)
			}
		}
		return kept
	}

	out := make([]GpuUtilizationInfo, 0, len(gpus)+len(instances))
	for _, gpu := range gpus {
		if _, mig := enabled[gpu.Index]; !mig {
			out = append(out, gpu)
			continue
		}
		for _, instance := range instances {
			if instance.parentIndex == gpu.Index {
				out = append(out, instance.info)
			}
		}
	}
	return out
}

// migEnabledGPUs returns the indices of the physical GPUs whose MIG mode is
// currently enabled. A failing probe means "no MIG", matching Python, which
// treats an NVMLError from nvmlDeviceGetMigMode as "not enabled".
func (c *GPUCollector) migEnabledGPUs() map[int]migModeRow {
	out, err := runCommand("nvidia-smi", migModeQueryArgs...)
	if err != nil {
		return nil
	}

	enabled := map[int]migModeRow{}
	for _, row := range parseMIGModeCsv(out) {
		if row.enabled {
			enabled[row.index] = row
		}
	}
	return enabled
}

// migInstances builds one record per MIG instance of the enabled GPUs. The
// instances are enumerated by "nvidia-smi -L" and their figures read from the
// MIG devices table of the plain nvidia-smi output; a failed or unparsable
// source leaves the corresponding field at its unavailable sentinel instead of
// dropping the instance, so a MIG node still reports its device list.
func (c *GPUCollector) migInstances(enabled map[int]migModeRow) []migInstance {
	out, err := runCommand("nvidia-smi", migListArgs...)
	if err != nil {
		log.Log.V(1).Info("failed to list MIG devices, MIG-enabled GPUs report nothing",
			"error", err.Error())
		return nil
	}
	listings := parseMIGList(out)
	if len(listings) == 0 {
		return nil
	}

	// The table is only needed for the per-instance memory and utilization; a
	// failure degrades those fields rather than the instance list.
	table := map[[2]int]migTableStats{}
	if plain, err := runCommand("nvidia-smi"); err == nil {
		table = parseMIGTable(plain)
	}

	instances := make([]migInstance, 0, len(listings))
	for _, listing := range listings {
		parent, migEnabled := enabled[listing.parentIndex]
		if !migEnabled {
			// The listing mentions an instance of a GPU whose mode reads
			// Disabled; reporting it would invent a device index Python never
			// produces for that GPU.
			continue
		}
		stats, hasStats := table[[2]int{listing.parentIndex, listing.migIndex}]
		info := GpuUtilizationInfo{
			Index:          listing.parentIndex*migIndexBase + listing.migIndex,
			Name:           migDeviceName(parent.name, listing.profile),
			UUID:           listing.uuid,
			UtilizationGPU: gpuUtilizationUnavailable,
			ProcessesPIDs:  map[int]ProcessGPUInfo{},
		}
		if hasStats {
			info.MemoryUsed = stats.memoryUsed
			info.MemoryTotal = stats.memoryTotal
			info.UtilizationGPU = stats.utilization
		}
		instances = append(instances, migInstance{parentIndex: listing.parentIndex, info: info})
	}
	return instances
}

// migDeviceName reproduces the NVML device name of a MIG instance, which is the
// parent device name followed by the instance profile, e.g.
// "NVIDIA A100-SXM4-40GB MIG 3g.20gb". Python reads that string straight from
// nvmlDeviceGetName; nvidia-smi only exposes the two halves separately.
func migDeviceName(parentName, profile string) string {
	if parentName == "" {
		return "MIG " + profile
	}
	return parentName + " MIG " + profile
}

// attachGPUProcesses fills the ProcessesPIDs of every device from the
// per-process query. A failing query is logged and leaves every device with an
// empty process map rather than dropping the device, so a node whose
// per-process query is unsupported still reports its device-level utilization.
func (c *GPUCollector) attachGPUProcesses(gpus []GpuUtilizationInfo) []GpuUtilizationInfo {
	out, err := runCommand("nvidia-smi", gpuProcessQueryArgs...)
	if err != nil {
		log.Log.V(1).Info("failed to query per-process GPU usage, worker GPU usage stays zero",
			"error", err.Error())
		return gpus
	}

	byUUID := parseGPUProcessCsv(out)
	if len(byUUID) == 0 {
		return gpus
	}
	if c.isInContainer() {
		byUUID = c.translateToContainerPIDs(byUUID)
	}

	for i := range gpus {
		if processes, ok := byUUID[gpus[i].UUID]; ok {
			gpus[i].ProcessesPIDs = processes
		}
	}
	return gpus
}

// isInContainer reports whether the processes use container-local pids,
// mirroring Python IN_CONTAINER in gpu_providers.py. The Kubernetes-pod check of
// reporter_agent.py is deliberately not consulted, matching gpu_providers.py.
func (c *GPUCollector) isInContainer() bool {
	if c.inContainer == nil {
		return NewK8sAdapter().IsInContainer()
	}
	return c.inContainer()
}

// translateToContainerPIDs rewrites the host pids nvidia-smi reports into the
// container-local pids the raylet reports, mirroring Python
// _transform_host_pids_to_container_pids. Raylet worker pids and nvidia-smi
// process pids are only comparable in the same pid namespace, so without this
// translation no worker ever matches its GPU usage inside a container.
//
// The host -> container mapping is resolved once for the union of all pids.
// Python resolves it per device, which repeats the /proc scan per GPU; the
// mapping of a pid does not depend on the device using it, so a single scan
// yields the same result.
func (c *GPUCollector) translateToContainerPIDs(byUUID map[string]map[int]ProcessGPUInfo) map[string]map[int]ProcessGPUInfo {
	wanted := map[int]struct{}{}
	for _, processes := range byUUID {
		for pid := range processes {
			wanted[pid] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return byUUID
	}

	mapping := c.hostToContainerPIDs(wanted)
	if len(mapping) == 0 {
		return byUUID
	}

	translated := make(map[string]map[int]ProcessGPUInfo, len(byUUID))
	for uuid, processes := range byUUID {
		out := make(map[int]ProcessGPUInfo, len(processes))
		for hostPID, info := range processes {
			containerPID, ok := mapping[hostPID]
			if !ok {
				// An unmapped host pid keeps its original entry, matching Python.
				out[hostPID] = info
				continue
			}
			info.PID = containerPID
			out[containerPID] = info
		}
		translated[uuid] = out
	}
	return translated
}

// hostToContainerPIDs returns the container pid of every wanted host pid. Inside
// a container the same process has two pids: procDir is indexed by the
// container-local pid, while the first line of its sched file names the host pid
// in parentheses. Unresolvable pids are absent from the result. The scan stops
// once every wanted pid has been mapped, mirroring the Python early exit.
func (c *GPUCollector) hostToContainerPIDs(wanted map[int]struct{}) map[int]int {
	entries, err := os.ReadDir(c.procDir)
	if err != nil {
		return nil
	}

	mapping := make(map[int]int, len(wanted))
	for _, entry := range entries {
		containerPID, ok := parseDecimalPID(entry.Name())
		if !ok {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(c.procDir, entry.Name(), schedFileName))
		if err != nil {
			// The process exited between the directory read and this read.
			continue
		}
		firstLine := string(raw)
		if i := strings.IndexByte(firstLine, '\n'); i >= 0 {
			firstLine = firstLine[:i]
		}
		hostPID, ok := parseSchedHostPID(firstLine)
		if !ok {
			continue
		}
		if _, want := wanted[hostPID]; !want {
			continue
		}
		mapping[hostPID] = containerPID
		if len(mapping) == len(wanted) {
			break
		}
	}
	return mapping
}

// parseDecimalPID parses a /proc entry name that is a plain sequence of ASCII
// digits, mirroring the Python str.isdigit() filter on the /proc listing.
// strconv.Atoi alone is not enough: it also accepts a leading sign.
func parseDecimalPID(name string) (int, bool) {
	if name == "" {
		return 0, false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return 0, false
		}
	}
	pid, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	return pid, true
}

// schedHostPIDPattern extracts the host pid from the first line of a sched file,
// which looks like "<comm> (<host_pid>, #threads: <n>)". The command name may
// itself contain spaces and parentheses and is truncated by the kernel, so the
// pattern searches for the first "(<digits>," exactly as the Python
// re.search(r"\(([0-9]+),") does.
var schedHostPIDPattern = regexp.MustCompile(`\(([0-9]+),`)

// parseSchedHostPID returns the host pid recorded in a sched file's first line.
func parseSchedHostPID(firstLine string) (int, bool) {
	match := schedHostPIDPattern.FindStringSubmatch(firstLine)
	if match == nil {
		return 0, false
	}
	pid, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return pid, true
}

// parseGPUCsv parses the nvidia-smi --query-gpu CSV output into GPU utilization
// records. Malformed or short records are skipped; unparseable numeric fields
// default to zero, except utilization, which defaults to
// gpuUtilizationUnavailable. nvidia-smi prints [N/A] for a utilization the
// device does not expose, and reporting that as 0 would claim an idle GPU,
// whereas Python reports -1 for the same condition.
// ProcessesPIDs starts empty and is filled by attachGPUProcesses.
func parseGPUCsv(out []byte) []GpuUtilizationInfo {
	r := csv.NewReader(strings.NewReader(string(out)))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return []GpuUtilizationInfo{}
	}

	gpus := make([]GpuUtilizationInfo, 0, len(records))
	for _, rec := range records {
		if len(rec) < 6 {
			continue
		}
		index, _ := strconv.Atoi(strings.TrimSpace(rec[0]))
		uuid := strings.TrimSpace(rec[1])
		name := strings.TrimSpace(rec[2])
		util, utilErr := strconv.Atoi(strings.TrimSpace(rec[3]))
		if utilErr != nil {
			// [N/A] or any non-numeric value means the device did not report
			// its utilization, which Python encodes as -1 rather than 0.
			util = gpuUtilizationUnavailable
		}
		memUsed, _ := strconv.ParseInt(strings.TrimSpace(rec[4]), 10, 64)
		memTotal, _ := strconv.ParseInt(strings.TrimSpace(rec[5]), 10, 64)
		// power.draw is reported in watts with a fractional part; NVML (and the
		// Python provider) reports milliwatts, so the value is scaled here. A
		// driver without the sensor reports [N/A], which parses to the "no
		// value" the record layer skips.
		powerMW, hasPower := parseGPUWattsAsMilliwatts(rec, 6)
		temperature, hasTemperature := parseGPUInt(rec, 7)
		gpus = append(gpus, GpuUtilizationInfo{
			Index:          index,
			UUID:           uuid,
			Name:           name,
			UtilizationGPU: util,
			MemoryUsed:     memUsed,
			MemoryTotal:    memTotal,
			PowerMW:        powerMW,
			HasPower:       hasPower,
			TemperatureC:   temperature,
			HasTemperature: hasTemperature,
			ProcessesPIDs:  map[int]ProcessGPUInfo{},
		})
	}
	return gpus
}

// parseGPUProcessCsv parses the nvidia-smi --query-compute-apps CSV output into
// a map from device uuid to its processes. Records with fewer than three fields,
// a non-numeric pid, or no uuid are skipped. A pid repeated for the same device
// keeps the last row, matching the Python dict assignment. used_memory is in MiB
// under nounits; a driver that reports it as N/A degrades to zero, matching the
// Python `if usedGpuMemory else 0`.
func parseGPUProcessCsv(out []byte) map[string]map[int]ProcessGPUInfo {
	r := csv.NewReader(strings.NewReader(string(out)))
	r.TrimLeadingSpace = true
	// Accept a varying field count so one malformed row is skipped below instead
	// of making ReadAll reject the whole output. nvidia-smi itself always emits a
	// uniform row width.
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil
	}

	byUUID := map[string]map[int]ProcessGPUInfo{}
	for _, rec := range records {
		if len(rec) < 3 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(rec[0]))
		if err != nil {
			continue
		}
		uuid := strings.TrimSpace(rec[1])
		if uuid == "" {
			continue
		}
		memory, _ := strconv.ParseInt(strings.TrimSpace(rec[2]), 10, 64)
		if byUUID[uuid] == nil {
			byUUID[uuid] = map[int]ProcessGPUInfo{}
		}
		byUUID[uuid][pid] = ProcessGPUInfo{PID: pid, GPUMemoryUsage: memory}
	}
	return byUUID
}

// migModeRow is one row of the MIG mode probe: a physical GPU and whether MIG
// is currently enabled on it.
type migModeRow struct {
	index   int
	uuid    string
	name    string
	enabled bool
}

// parseMIGModeCsv parses the --query-gpu=index,uuid,name,mig.mode.current rows.
// nvidia-smi reports the mode as "Enabled" or "Disabled" (also "[N/A]" on a
// driver that predates MIG), so only the enabled spelling counts as enabled.
func parseMIGModeCsv(out []byte) []migModeRow {
	r := csv.NewReader(strings.NewReader(string(out)))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil
	}

	rows := make([]migModeRow, 0, len(records))
	for _, rec := range records {
		if len(rec) < 4 {
			continue
		}
		index, err := strconv.Atoi(strings.TrimSpace(rec[0]))
		if err != nil {
			continue
		}
		rows = append(rows, migModeRow{
			index:   index,
			uuid:    strings.TrimSpace(rec[1]),
			name:    strings.TrimSpace(rec[2]),
			enabled: strings.EqualFold(strings.TrimSpace(rec[3]), "Enabled"),
		})
	}
	return rows
}

// migInstance is a MIG instance resolved from the "-L" listing, carrying the
// parent GPU index it belongs to alongside the reported record.
type migInstance struct {
	parentIndex int
	info        GpuUtilizationInfo
}

// migListing is one MIG device line of "nvidia-smi -L": the parent GPU index,
// the instance index within that GPU, the instance profile, and its uuid.
type migListing struct {
	parentIndex int
	migIndex    int
	profile     string
	uuid        string
}

// migListGPULine matches a parent GPU line of "nvidia-smi -L", e.g.
// "GPU 0: A100-SXM4-40GB (UUID: GPU-e86cb44c-...)". Only the index is used: the
// parent name comes from the MIG mode query, which already carries it.
var migListGPULine = regexp.MustCompile(`^GPU\s+(\d+):`)

// migListMIGLine matches a MIG device line of "nvidia-smi -L", e.g.
// "  MIG 3g.20gb Device 0: (UUID: MIG-c7384736-...)".
var migListMIGLine = regexp.MustCompile(`^\s*MIG\s+(.*?)\s+Device\s+(\d+):\s*\(UUID:\s*([^)]+)\)\s*$`)

// parseMIGList parses the line-oriented "nvidia-smi -L" output into the MIG
// instances it lists. Only GPUs that have MIG devices produce entries, so a node
// without MIG yields nothing. Malformed lines are skipped rather than aborting,
// because the listing is human-oriented and may gain fields in future drivers.
func parseMIGList(out []byte) []migListing {
	var listings []migListing
	parentIndex := -1
	for _, line := range strings.Split(string(out), "\n") {
		if m := migListGPULine.FindStringSubmatch(line); m != nil {
			index, err := strconv.Atoi(m[1])
			if err != nil {
				parentIndex = -1
				continue
			}
			parentIndex = index
			continue
		}
		if m := migListMIGLine.FindStringSubmatch(line); m != nil {
			if parentIndex < 0 {
				// A MIG line without a preceding GPU line cannot be attributed.
				continue
			}
			migIndex, err := strconv.Atoi(m[2])
			if err != nil {
				continue
			}
			listings = append(listings, migListing{
				parentIndex: parentIndex,
				migIndex:    migIndex,
				profile:     strings.TrimSpace(m[1]),
				uuid:        strings.TrimSpace(m[3]),
			})
		}
	}
	return listings
}

// migTableRow matches one row of the "MIG devices" table of the plain
// nvidia-smi output, e.g.
//
//	|  0    1   0   0  |     11MiB / 20224MiB | 42      0 |  3   0    2    0    0 |
//
// The leading columns are GPU index, GI id, CI id and MIG device id; the memory
// column reports "<used>MiB / <total>MiB"; the first Vol column is SM (compute)
// utilization. The trailing Shared columns are not used.
var migTableRow = regexp.MustCompile(
	`^\|\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*\|\s*(\d+)\s*MiB\s*/\s*(\d+)\s*MiB\s*\|\s*(\d+|\S+)\s+\S+\s*\|`)

// migTableStats is the per-MIG-device detail read from the plain nvidia-smi
// table, keyed by parent GPU index and MIG device id.
type migTableStats struct {
	memoryUsed  int64
	memoryTotal int64
	utilization int
}

// parseMIGTable extracts per-MIG-device memory and SM utilization from the plain
// nvidia-smi output. Rows that do not match, or whose utilization is not
// numeric, are skipped and leave the caller with the unavailable sentinel.
func parseMIGTable(out []byte) map[[2]int]migTableStats {
	stats := map[[2]int]migTableStats{}
	for _, line := range strings.Split(string(out), "\n") {
		m := migTableRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		gpuIndex, err1 := strconv.Atoi(m[1])
		migDev, err2 := strconv.Atoi(m[4])
		used, err3 := strconv.ParseInt(m[5], 10, 64)
		total, err4 := strconv.ParseInt(m[6], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			continue
		}
		entry := migTableStats{memoryUsed: used, memoryTotal: total, utilization: gpuUtilizationUnavailable}
		// The Vol/SM column reports "N/A" for an instance that has not sampled yet.
		if util, err := strconv.Atoi(m[7]); err == nil {
			entry.utilization = util
		}
		stats[[2]int{gpuIndex, migDev}] = entry
	}
	return stats
}

// parseGPUWattsAsMilliwatts reads a watts field and scales it to milliwatts,
// reporting false when the field is absent or the driver answered [N/A].
func parseGPUWattsAsMilliwatts(rec []string, idx int) (int64, bool) {
	if idx >= len(rec) {
		return 0, false
	}
	raw := strings.TrimSpace(rec[idx])
	if raw == "" {
		return 0, false
	}
	watts, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return int64(watts * 1000), true
}

// parseGPUInt reads an integer field, reporting false when the field is absent
// or not a number.
func parseGPUInt(rec []string, idx int) (int64, bool) {
	if idx >= len(rec) {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(rec[idx]), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
