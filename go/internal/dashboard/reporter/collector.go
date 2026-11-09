package reporter

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// MemInfo holds host memory usage, aligned with Python _get_mem_usage which
// returns (total, available, percent, used).
type MemInfo struct {
	Total     int64
	Available int64
	Percent   float64
	Used      int64
}

// DiskInfo holds usage for a single mount point, aligned with psutil disk_usage.
type DiskInfo struct {
	Total   int64
	Used    int64
	Free    int64
	Percent float64
}

// IOMetrics holds network byte counters, aligned with Python _get_network_stats.
type IOMetrics struct {
	Sent int64
	Recv int64
}

// DiskIOMetrics holds disk IO byte and op counters, aligned with Python
// _get_disk_io_stats.
type DiskIOMetrics struct {
	ReadBytes  int64
	WriteBytes int64
	ReadCount  int64
	WriteCount int64
}

// LoadAvgInfo holds the 1/5/15 minute load averages and the per-CPU load.
type LoadAvgInfo struct {
	Load1      float64
	Load5      float64
	Load15     float64
	PerCPULoad []float64
}

// statsSource abstracts all host resource collection so that Collect can be
// unit tested against a fake instead of reading real /proc, /sys, or device
// state. Each method may return an error; Collect degrades the corresponding
// field to its zero value rather than failing the whole cycle, matching the
// Python semantics.
type statsSource interface {
	beginCycle(now float64)
	cpuPercent() (float64, error)
	cpuCounts() ([]int, error)
	memUsage() (MemInfo, error)
	hostMemUsage() (HostMemInfo, error)
	swapUsage() (SwapInfo, error)
	cgroupMemUsage() (CgroupMemInfo, error)
	shmUsage() (int64, bool, error)
	diskUsage() (map[string]DiskInfo, error)
	diskIO() (DiskIOMetrics, error)
	diskIOSpeed() ([]float64, error)
	network() (IOMetrics, error)
	networkSpeed() ([]float64, error)
	loadAvg() (LoadAvgInfo, error)
	bootTime() (float64, error)
	workers() ([]interface{}, error)
	raylet() (map[string]interface{}, error)
	agent() (map[string]interface{}, error)
	gcs() (map[string]interface{}, error)
	additional() (map[string]interface{}, error)
}

// StatsCollector periodically gathers physical resource statistics for the
// node, mirroring Python ReporterAgent._async_collect_stats.
type StatsCollector struct {
	hostname string
	ip       string
	isHead   bool
	src      statsSource
	// now returns the current POSIX timestamp with sub-second precision. It is
	// a field so tests can drive the collection cycle deterministically;
	// time.Now is the production value. The same instant is used for the
	// published "now" field and for the disk/network speed sample timestamps,
	// mirroring Python which computes to_posix_time(datetime.utcnow()) once per
	// _async_collect_stats.
	now func() float64
}

// cycleNow returns the timestamp for the current collection cycle, defaulting
// to the system clock when no override was configured (e.g. a StatsCollector
// built as a struct literal in a test).
func (c *StatsCollector) cycleNow() float64 {
	if c.now != nil {
		return c.now()
	}
	return posixTime()
}

// posixTime returns the current time as a POSIX timestamp in float seconds,
// mirroring dashboard.utils.to_posix_time. Sub-second precision is required:
// two collection cycles can fall inside the same wall-clock second (a short
// publishInterval, a retry, or clock granularity), and truncating to whole
// seconds would make the speed history delta zero and yield a spurious 0
// speed.
func posixTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// Collect gathers a full resource snapshot and returns it as a map whose keys
// and value shapes align with the Python _async_collect_stats result. It never
// returns an error: individual collection failures degrade to zero/empty
// values instead of failing the whole cycle, so there is no error to
// propagate.
func (c *StatsCollector) Collect() map[string]interface{} {
	now := c.cycleNow()
	c.src.beginCycle(now)
	cpu, _ := c.src.cpuPercent()
	cpus, _ := c.src.cpuCounts()
	mem, _ := c.src.memUsage()
	shm, shmOK, _ := c.src.shmUsage()
	hostMem, _ := c.src.hostMemUsage()
	swap, _ := c.src.swapUsage()
	cgroupMem, _ := c.src.cgroupMemUsage()
	disk, _ := c.src.diskUsage()
	diskIO, _ := c.src.diskIO()
	diskIOSpeed, _ := c.src.diskIOSpeed()
	network, _ := c.src.network()
	networkSpeed, _ := c.src.networkSpeed()
	load, _ := c.src.loadAvg()
	boot, _ := c.src.bootTime()
	workers, _ := c.src.workers()
	raylet, _ := c.src.raylet()
	agentInfo, _ := c.src.agent()
	additional, _ := c.src.additional()

	var shmVal interface{}
	if shmOK {
		shmVal = shm
	}

	stats := map[string]interface{}{
		"now":           now,
		"hostname":      c.hostname,
		"ip":            c.ip,
		"cpu":           cpu,
		"cpus":          cpus,
		"mem":           []interface{}{mem.Total, mem.Available, mem.Percent, mem.Used},
		"host_mem":      []interface{}{hostMem.Used, hostMem.Total},
		"swap":          swapStat(swap),
		"cgroup_mem":    cgroupMemStat(cgroupMem),
		"shm":           shmVal,
		"workers":       workers,
		"raylet":        raylet,
		"agent":         agentInfo,
		"bootTime":      boot,
		"loadAvg":       loadAvgWire(load),
		"disk":          disk,
		"disk_io":       diskIO,
		"disk_io_speed": diskIOSpeed,
		"network":       network,
		"network_speed": networkSpeed,
		"additional":    additional,
		"cmdline":       cmdlineOf(raylet),
		"gpus":          []interface{}{},
		"tpus":          []interface{}{},
	}

	if c.isHead {
		gcs, _ := c.src.gcs()
		stats["gcs"] = gcs
	}

	return stats
}

// loadAvgWire renders loadAvg as [load, per_cpu_load] arrays, matching how the
// Python (load, per_cpu_load) tuple serializes to JSON.
func loadAvgWire(l LoadAvgInfo) []interface{} {
	return []interface{}{
		[]interface{}{l.Load1, l.Load5, l.Load15},
		perCPULoadWire(l.PerCPULoad),
	}
}

// perCPULoadWire renders the per-CPU load as a JSON array, or as null when it
// could not be computed. Python _get_load_avg assigns per_cpu_load = None when
// the logical CPU count is 0, and json.dumps renders that as null; an empty Go
// slice would serialize as [] instead, so the field type must stay untyped.
func perCPULoadWire(p []float64) interface{} {
	if len(p) == 0 {
		return nil
	}
	out := make([]interface{}, 0, len(p))
	for _, v := range p {
		out = append(out, v)
	}
	return out
}

// cmdlineOf returns the raylet process cmdline if available, otherwise empty.
func cmdlineOf(raylet map[string]interface{}) []string {
	if raylet == nil {
		return []string{}
	}
	cmd, ok := raylet["cmdline"].([]string)
	if !ok {
		return []string{}
	}
	return cmd
}

// NewStatsCollector builds a collector for the given agent config using the
// real host resource source.
func NewStatsCollector(cfg agent.Config) *StatsCollector {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = cfg.NodeIP
	}
	return &StatsCollector{
		hostname: hostname,
		ip:       cfg.NodeIP,
		isHead:   cfg.IsHead,
		src:      newSystemSource(cfg),
		now:      posixTime,
	}
}

// systemSource is the production statsSource reading /proc, /sys, and the
// filesystem, mirroring the non-K8s paths of the Python reporter agent.
type systemSource struct {
	cfg      agent.Config
	hostname string
	// k8s adapts resource collection to a container/cgroup environment. Python
	// dispatches cpu / cpus[0] / bootTime / disk through container-aware paths
	// when running in a pod; the adapter supplies those values. It is always
	// non-nil for the production source and nil in unit tests that stub inputs.
	k8s *K8sAdapter
	// lastCPU/lastSet are mutable between calls; they are only accessed from a
	// single goroutine's periodic collection loop.
	lastCPU cpuTimes
	lastSet bool
	// cpuCache memoizes the [logical, physical] CPU count on first use. Both
	// logical and physical CPU counts are stable for the process lifetime, so
	// caching avoids re-reading /proc/cpuinfo on every collect cycle.
	cpuCache []int
	// cpuCountsOverride, when non-nil, replaces the CPU-count probe for this
	// source. Tests use it to drive the "logical count is zero" branch, which
	// the production probe cannot reach because it always falls back to
	// runtime.NumCPU(). It is nil for the production source.
	cpuCountsOverride func() ([]int, error)
	// gcsPIDFetcher returns the gcs_server pid published in the GCS internal KV
	// store, mirroring the Python _run_loop read of GCS_PID_KEY. It is nil on a
	// non-head node or when no GCS client is configured, in which case gcs()
	// reports an empty dict. Because the pid is read fresh every cycle, a
	// restarted gcs_server is picked up without any cache invalidation.
	gcsPIDFetcher func() (int, bool)
	// bootCached/bootValue memoize the boot timestamp. btime is constant for
	// the process lifetime, so re-reading /proc/stat on every collect cycle
	// (which already happens for cpuPercent) is wasteful.
	bootCached bool
	bootValue  float64
	// meminfoCache/meminfoCached memoize a single parse of /proc/meminfo per
	// collect cycle. memUsage and shmUsage both read the same file, so caching
	// avoids parsing it twice each cycle. The cache is reset at the start of
	// each Collect via beginCycle.
	meminfoCache  map[string]int64
	meminfoCached bool
	// diskIOHist/netHist retain the most recent (timestamp, counter) samples
	// used to compute disk_io_speed / network_speed, mirroring Python's
	// _disk_io_stats_hist and _network_stats_hist.
	diskIOHist []ioSample
	netHist    []netSample
	// cycleTimestamp is the POSIX timestamp of the current collection cycle,
	// set by beginCycle. The disk and network speed samples are stamped with
	// it so that one cycle observes a single instant, mirroring Python which
	// appends the same `now` to both history lists in _async_collect_stats.
	cycleTimestamp float64
	// procCPUPrev holds the most recent CPU-time sample per pid, used to
	// compute each process's cpu_percent from the delta against the previous
	// collection cycle, mirroring the cache psutil keeps on each Process
	// object. Entries are dropped at the start of each cycle for pids that
	// were not sampled during the previous cycle, bounding the map to the set
	// of live processes.
	procCPUPrev map[int]procCPUSample
	// sampledPids collects the pids inspected during the current cycle so the
	// next cycle can evict the procCPUPrev entries of processes that are gone,
	// mirroring the Python self._workers pop of stale entries. It is reset at
	// the start of each Collect via beginCycle.
	sampledPids map[int]struct{}
	// rayletWorkerPIDs queries the raylet for the live worker pids, mirroring
	// Python RayletClient.async_get_worker_pids. It is nil when no raylet
	// address is configured, in which case worker discovery degrades to an
	// empty list.
	rayletWorkerPIDs workerPIDFetcher
}

// ioSample is a (timestamp, disk-IO counter) sample retained for speed
// computation.
type ioSample struct {
	at  float64
	val DiskIOMetrics
}

// netSample is a (timestamp, network counter) sample retained for speed
// computation.
type netSample struct {
	at  float64
	val IOMetrics
}

type cpuTimes struct {
	idle  uint64
	total uint64
}

func newSystemSource(cfg agent.Config) *systemSource {
	hostname, _ := os.Hostname()
	src := &systemSource{cfg: cfg, hostname: hostname, k8s: NewK8sAdapter()}
	// A zero port means the raylet address was not configured; worker discovery
	// then degrades to an empty list without logging an error every cycle.
	if cfg.NodeManagerPort > 0 {
		src.rayletWorkerPIDs = newRayletWorkerPIDFetcher(cfg.NodeIP, cfg.NodeManagerPort)
	}
	// Only the head node publishes the gcs_server pid, mirroring the Python
	// _run_loop which fetches GCS_PID_KEY under `if self._is_head_node`.
	if cfg.IsHead && cfg.GCS != nil {
		src.gcsPIDFetcher = newGCSPIDFetcher(cfg.GCS)
	}
	return src
}

// gcsPIDFetchTimeout bounds the GCS internal KV read used to locate the
// gcs_server process, mirroring the Python GCS_RPC_TIMEOUT_SECONDS default.
const gcsPIDFetchTimeout = 60 * time.Second

// newGCSPIDFetcher builds a fetcher that reads the gcs_server pid from the GCS
// internal KV store, mirroring the Python _run_loop read of GCS_PID_KEY. A
// missing value or an unparsable one reports ok=false so the caller omits the
// gcs entry instead of failing the collection cycle; Python likewise leaves
// _gcs_pid as None and _get_gcs returns an empty dict.
func newGCSPIDFetcher(client gcs.Client) func() (int, bool) {
	return func() (int, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), gcsPIDFetchTimeout)
		defer cancel()

		raw, err := client.Get(ctx, "", gcsPIDKey)
		if err != nil {
			log.Log.Error(err, "failed to get gcs pid from internal kv")
			return 0, false
		}
		if len(raw) == 0 {
			return 0, false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			log.Log.Error(err, "failed to parse gcs pid from internal kv", "value", string(raw))
			return 0, false
		}
		return pid, true
	}
}

// beginCycle starts a collection cycle at the given POSIX timestamp. It clears
// per-cycle caches (meminfo) so values reflect the current cycle, and records
// the cycle timestamp used to stamp the disk and network speed samples.
// Long-lived memoized values (boot time, cpu counts) are intentionally
// retained.
//
// It also evicts the CPU-time baselines of processes that were not sampled
// during the previous cycle, so procCPUPrev stays bounded to the live process
// set as workers come and go. Live processes are sampled every cycle, so their
// baseline survives and cpu_percent remains continuous.
func (s *systemSource) beginCycle(now float64) {
	s.cycleTimestamp = now
	s.meminfoCache = nil
	s.meminfoCached = false

	for pid := range s.procCPUPrev {
		if _, live := s.sampledPids[pid]; !live {
			delete(s.procCPUPrev, pid)
		}
	}
	s.sampledPids = nil
}

// readProcFile reads a whole proc file and returns its trimmed content.
func readProcFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(data)), nil
}

// cpuOnlineReader reads /sys/devices/system/cpu/online. It is a package-level
// seam so tests can pin the host CPU topology instead of depending on the
// machine the test runs on, following the runCommand/lookPath idiom.
var cpuOnlineReader = readProcFile

// cpuCounts returns [logical, physical] CPU counts, mirroring Python
// psutil.cpu_count(logical) and psutil.cpu_count(logical=False). The result is
// cached on first use because CPU counts are stable for the process lifetime
// and reading /proc/cpuinfo on every collect cycle is wasteful.
//
// The logical count is the number of ONLINE CPUs, matching psutil.cpu_count()
// (os.sysconf("SC_NPROCESSORS_ONLN")), not runtime.NumCPU(), which reports the
// process's CPU affinity mask and would under-report a pinned process. Inside a
// container or pod it is replaced by the cgroup quota/cpuset, mirroring the
// Python __init__ branch on IN_KUBERNETES_POD or IN_CONTAINER.
func (s *systemSource) cpuCounts() ([]int, error) {
	if s.cpuCountsOverride != nil {
		return s.cpuCountsOverride()
	}
	if s.cpuCache != nil {
		return s.cpuCache, nil
	}
	logical, ok := onlineCPUs(cpuOnlineReader)
	if !ok {
		logical = runtime.NumCPU()
	}
	if s.inContainer() {
		// psutil does not report a meaningful logical CPU count inside a pod or
		// container, so Python diverts to utils.get_num_cpus(), which honors
		// the cgroup CFS quota / cpuset. The physical count still comes from
		// the host view.
		logical = int(s.k8s.NumCPUs())
	}
	physical := logical
	if count, ok := readPhysicalCPUs(); ok {
		physical = count
	}
	s.cpuCache = []int{logical, physical}
	return s.cpuCache, nil
}

// readPhysicalCPUs counts physical cores from /proc/cpuinfo by unique
// (physical id, core id) pairs.
func readPhysicalCPUs() (int, bool) {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	seen := map[string]struct{}{}
	count := 0
	phys := ""
	core := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "physical id"):
			phys = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		case strings.HasPrefix(line, "core id"):
			core = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		case strings.HasPrefix(line, "processor"):
			if phys != "" && core != "" {
				key := phys + "-" + core
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					count++
				}
			}
			phys, core = "", ""
		}
	}
	if count == 0 {
		return 0, false
	}
	return count, true
}

// cpuPercent returns the CPU usage percentage since the previous call,
// mirroring psutil.cpu_percent() (no interval). The first call returns 0.
func (s *systemSource) cpuPercent() (float64, error) {
	if s.inK8s() {
		// Python _get_cpu_percent(IN_KUBERNETES_POD) diverts to
		// k8s_utils.cpu_percent(), which measures the container against its
		// cgroup CPU limit instead of the host-wide /proc/stat delta. CPUPerc
		// is stateful (it consumes the previous sample) and must be called
		// exactly once per collect cycle.
		return s.k8s.CPUPerc(), nil
	}
	now, err := readCPUTimes()
	if err != nil {
		return 0, err
	}
	if !s.lastSet {
		s.lastCPU = now
		s.lastSet = true
		return 0, nil
	}
	deltaIdle := now.idle - s.lastCPU.idle
	deltaTotal := now.total - s.lastCPU.total
	s.lastCPU = now
	if deltaTotal == 0 {
		return 0, nil
	}
	return 100.0 * float64(deltaTotal-deltaIdle) / float64(deltaTotal), nil
}

// readCPUTimes parses the aggregate cpu line from /proc/stat.
func readCPUTimes() (cpuTimes, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		var vals [8]uint64
		for i := 1; i < len(fields) && i <= 8; i++ {
			v, _ := strconv.ParseUint(fields[i], 10, 64)
			vals[i-1] = v
		}
		idle := vals[3] + vals[4] // idle + iowait
		var total uint64
		for _, v := range vals {
			total += v
		}
		return cpuTimes{idle: idle, total: total}, nil
	}
	return cpuTimes{}, sc.Err()
}

// memUsage returns total/available/percent/used in bytes, mirroring Python
// ReporterAgent._get_mem_usage: total comes from get_system_memory (the cgroup
// memory limit when it is set and smaller than the host total), used comes from
// get_used_memory (the cgroup memory controller's usage minus reclaimable file
// cache, otherwise the psutil used value), and available is total - used.
func (s *systemSource) memUsage() (MemInfo, error) {
	values, err := s.meminfo()
	if err != nil {
		return MemInfo{}, err
	}
	if _, ok := values["MemTotal"]; !ok {
		return MemInfo{}, os.ErrNotExist
	}
	limit, haveLimit := cgroupMemLimitBytes()
	total := systemMemoryBytes(values, limit, haveLimit)
	cgroupUsed, haveCgroupUsed := cgroupUsedMemory()
	used := usedMemoryBytes(values, cgroupUsed, haveCgroupUsed)
	return MemInfo{
		Total:     total,
		Available: total - used,
		Percent:   memPercent(used, total),
		Used:      used,
	}, nil
}

// systemMemoryBytes mirrors Python ray._common.utils.get_system_memory: the
// cgroup memory limit wins when it is set and smaller than the host total,
// because the limit file reports a very large value when the process is not
// actually confined by cgroups.
func systemMemoryBytes(meminfo map[string]int64, limitBytes int64, haveLimit bool) int64 {
	psutilTotal := meminfo["MemTotal"]
	if haveLimit && limitBytes < psutilTotal {
		return limitBytes
	}
	return psutilTotal
}

// usedMemoryBytes mirrors Python ray._private.utils.get_used_memory: the cgroup
// memory controller's usage minus reclaimable file cache when that controller
// is present, otherwise psutil's used value (total - available).
func usedMemoryBytes(meminfo map[string]int64, cgroupUsed int64, haveCgroupUsed bool) int64 {
	if haveCgroupUsed {
		return cgroupUsed
	}
	total := meminfo["MemTotal"]
	avail, ok := meminfo["MemAvailable"]
	if !ok || avail == 0 {
		// Kernels before 3.14 do not expose MemAvailable, and some kernels
		// report it as 0. psutil falls back to free + cached, the same
		// approximation its calculate_avail_vmem starts from; the reporter only
		// needs the estimate, not psutil's /proc/zoneinfo watermark refinement.
		avail = meminfo["MemFree"] + meminfo["Cached"]
	}
	if avail > total {
		// Symptomatic of running in an LXC container, where the host and
		// container values are distorted; psutil falls back to MemFree.
		avail = meminfo["MemFree"]
	}
	used := total - avail
	if used < 0 {
		used = 0
	}
	return used
}

// memPercent mirrors Python round(used/total, 3)*100: the ratio is rounded to
// 3 decimal places before scaling by 100, so the rounding applies to the ratio
// rather than to the percentage.
func memPercent(used, total int64) float64 {
	if total <= 0 {
		return 0
	}
	ratio := float64(used) / float64(total)
	return roundToDecimals(ratio, 3) * 100
}

// shmUsage returns the shared memory size in bytes, and whether shm is
// supported, mirroring Python _get_shm_usage.
func (s *systemSource) shmUsage() (int64, bool, error) {
	values, err := s.meminfo()
	if err != nil {
		return 0, false, err
	}
	v, ok := values["Shmem"]
	if !ok {
		return 0, false, nil
	}
	return v, true, nil
}

// meminfo returns the parsed /proc/meminfo map, cached once per collect cycle
// so that memUsage and shmUsage share a single file read. The cache is reset
// by beginCycle at the start of each Collect.
func (s *systemSource) meminfo() (map[string]int64, error) {
	if s.meminfoCached {
		return s.meminfoCache, nil
	}
	values, err := readMeminfo()
	if err != nil {
		return nil, err
	}
	s.meminfoCache = values
	s.meminfoCached = true
	return values, nil
}

// readMeminfo parses /proc/meminfo into a field->bytes map. The kernel reports
// these values in kB, so each is scaled by 1024 to match the byte units the
// Python reporter publishes (psutil does the same conversion).
func readMeminfo() (map[string]int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMeminfo(f)
}

// parseMeminfo parses meminfo-formatted content into a field->bytes map,
// scaling the kernel's kB values by 1024. It is separated from readMeminfo so
// the unit conversion can be asserted deterministically against a fixed input
// rather than two live reads of /proc/meminfo, whose volatile fields drift.
func parseMeminfo(r io.Reader) (map[string]int64, error) {
	out := map[string]int64{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		valStr := strings.Fields(parts[1])
		if len(valStr) == 0 {
			continue
		}
		v, err := strconv.ParseInt(valStr[0], 10, 64)
		if err != nil {
			continue
		}
		out[key] = v * 1024
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// cgroup memory controller paths, aligned with the constants Python reads in
// get_system_memory (ray._common.utils) and get_used_memory
// (ray._private.utils). cgroup v1 and v2 are probed in that order, mirroring
// Python's if/elif.
const (
	cgroupMemLimitV1 = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cgroupMemUsageV1 = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	cgroupMemStatV1  = "/sys/fs/cgroup/memory/memory.stat"
	cgroupMemLimitV2 = "/sys/fs/cgroup/memory.max"
	cgroupMemUsageV2 = "/sys/fs/cgroup/memory.current"
	cgroupMemStatV2  = "/sys/fs/cgroup/memory.stat"
	cgroupInactiveV1 = "total_inactive_file"
	cgroupActiveV1   = "total_active_file"
	cgroupInactiveV2 = "inactive_file"
	cgroupActiveV2   = "active_file"

	// cgroup v2 swap controller files and the v1 memsw controller, mirroring
	// the paths get_cgroup_aware_swap_memory reads.
	cgroupV2SwapMax         = "/sys/fs/cgroup/memory.swap.max"
	cgroupV2SwapCurrentPath = "/sys/fs/cgroup/memory.swap.current"
	cgroupV1MemswLimit      = "/sys/fs/cgroup/memory/memory.memsw.limit_in_bytes"
	cgroupV1MemswUsage      = "/sys/fs/cgroup/memory/memory.memsw.usage_in_bytes"
)

// cgroupMemLimitBytes returns the cgroup memory limit in bytes, preferring
// cgroup v1 and falling back to cgroup v2, mirroring Python get_system_memory.
// The second result is false when no usable limit file exists; a v2 limit of
// "max" (unset) is also treated as absent. Callers compare the limit against the
// host total and keep the smaller value, so the very large v1 value written when
// the process is unconfined does not distort the total.
func cgroupMemLimitBytes() (int64, bool) {
	if v, ok := readCgroupInt64(cgroupMemLimitV1); ok {
		return v, true
	}
	text, err := readProcFile(cgroupMemLimitV2)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		// A v2 limit of "max" means unset, so there is no usable limit.
		return 0, false
	}
	return v, true
}

// cgroupUsedMemory returns the cgroup memory controller's used bytes, mirroring
// Python get_cgroup_used_memory: the controller's usage minus the reclaimable
// file page cache (inactive + active file), which the OOM killer treats as
// available. cgroup v1 is preferred and v2 is the fallback. The second result
// is false when the controller files are absent or unparseable, in which case
// the caller falls back to the psutil used value.
func cgroupUsedMemory() (int64, bool) {
	if used, ok := cgroupUsedMemoryV1(); ok {
		return used, true
	}
	return cgroupUsedMemoryV2()
}

// cgroupUsedMemoryV1 implements the cgroup v1 branch of get_used_memory using
// the total_inactive_file / total_active_file keys.
func cgroupUsedMemoryV1() (int64, bool) {
	return cgroupUsedMemoryFrom(cgroupMemStatV1, cgroupMemUsageV1, cgroupInactiveV1, cgroupActiveV1)
}

// cgroupUsedMemoryV2 implements the cgroup v2 branch of get_used_memory using
// the inactive_file / active_file keys.
func cgroupUsedMemoryV2() (int64, bool) {
	return cgroupUsedMemoryFrom(cgroupMemStatV2, cgroupMemUsageV2, cgroupInactiveV2, cgroupActiveV2)
}

// cgroupUsedMemoryFrom reads the cgroup memory.stat and usage files and returns
// usage minus the inactive and active file bytes. It reports false when any of
// the three values is missing, matching Python which returns None so the caller
// can fall back to psutil.
func cgroupUsedMemoryFrom(statPath, usagePath, inactiveKey, activeKey string) (int64, bool) {
	stat, err := readProcFile(statPath)
	if err != nil {
		return 0, false
	}
	var inactiveFile, activeFile int64
	var haveInactive, haveActive bool
	for _, line := range strings.Split(stat, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case inactiveKey:
			inactiveFile, haveInactive = v, true
		case activeKey:
			activeFile, haveActive = v, true
		}
	}
	usageText, err := readProcFile(usagePath)
	if err != nil {
		return 0, false
	}
	usage, err := strconv.ParseInt(usageText, 10, 64)
	if err != nil {
		return 0, false
	}
	if !haveInactive || !haveActive {
		return 0, false
	}
	return usage - inactiveFile - activeFile, true
}

// readCgroupInt64 reads a cgroup file holding a single decimal integer,
// reporting false when the file is absent or does not parse.
func readCgroupInt64(path string) (int64, bool) {
	text, err := readProcFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// diskUsage returns per-mount disk usage for the root and temp directories,
// mirroring Python _get_disk_usage.
func (s *systemSource) diskUsage() (map[string]DiskInfo, error) {
	if s.inK8s() && !s.k8s.K8sDiskUsageEnabled() {
		// Python disables disk display inside a pod unless explicitly enabled,
		// reporting dummy values so the dashboard shows no host disk usage.
		return map[string]DiskInfo{
			"/": {Total: 1, Free: 1},
		}, nil
	}
	paths := map[string]string{
		"/": "/",
	}
	// Python keys the second entry on get_user_temp_dir(), which is RAY_TMPDIR,
	// else TMPDIR, else /tmp. It is NOT the session temp dir the agent receives
	// in --temp-dir (that one is <user temp dir>/ray), so using cfg.TempDir here
	// would publish an entry under the wrong key and measure the wrong mount.
	tmp := userTempDir()
	paths[tmp] = tmp

	result := map[string]DiskInfo{}
	for key, p := range paths {
		info, err := diskUsageOf(p)
		if err != nil {
			continue
		}
		result[key] = info
	}
	if len(result) == 0 {
		return nil, os.ErrNotExist
	}
	return result, nil
}

// userTempDir resolves the directory Python's get_user_temp_dir returns:
// RAY_TMPDIR when set, else TMPDIR, else /tmp. It is a package-level seam so the
// diskUsage key can be tested against a pinned environment instead of the one the
// test process happens to run under.
var userTempDir = func() string {
	if dir := os.Getenv("RAY_TMPDIR"); dir != "" {
		return dir
	}
	if dir := os.Getenv("TMPDIR"); dir != "" {
		return dir
	}
	return "/tmp"
}

// statfs is a package-level seam so the percent formula can be exercised against
// a pinned block layout, including the root-reserved blocks a real mount may not
// exhibit. It is the raw statfs(2) wrapper.
var statfs = func(path string) (syscall.Statfs_t, error) {
	var st syscall.Statfs_t
	err := syscall.Statfs(path, &st)
	return st, err
}

// diskUsageOf uses statfs to compute usage of a single path, matching
// psutil.disk_usage on Linux.
//
// psutil's `used` and `free` cover only the non-root pool: used is derived from
// f_bfree while free is f_bavail, so the root-reserved blocks are deliberately
// excluded from both. Its percent is therefore used/(used+free), not used/total
// — the latter would count the reserved blocks in the denominator. On a typical
// ext4 /home with a 5% reserve the two disagree by several percentage points.
//
// psutil computes percent as round(used / (used + free) * 100, 1) (see
// _psposix.disk_usage and _common.usage_percent), i.e. the already-scaled
// percentage is rounded, not the ratio. Both the quantity and the precision
// matter: rounding the ratio to three places and then dividing by ten picks a
// different neighbour for ties such as used=49, free=31, where the percentage is
// 61.25000000000001 and Python reports 61.3.
func diskUsageOf(path string) (DiskInfo, error) {
	st, err := statfs(path)
	if err != nil {
		return DiskInfo{}, err
	}
	blockSize := int64(st.Frsize)
	if blockSize == 0 {
		// Some network filesystems report a zero f_frsize but a usable f_bsize.
		blockSize = int64(st.Bsize)
	}
	total := int64(st.Blocks) * blockSize
	free := int64(st.Bavail) * blockSize
	used := int64(st.Blocks-st.Bfree) * blockSize
	percent := 0.0
	if used+free > 0 {
		percent = roundToDecimals(float64(used)/float64(used+free)*100, 1)
	}
	return DiskInfo{
		Total:   total,
		Used:    used,
		Free:    free,
		Percent: percent,
	}, nil
}

// diskstatsPath and sysBlockDir are package-level seams so the aggregate disk
// IO counter can be exercised against a pinned /proc/diskstats and /sys/block
// layout instead of whatever block devices the test host happens to have.
var (
	diskstatsPath = "/proc/diskstats"
	sysBlockDir   = "/sys/block"
)

// isStorageDevice reports whether name is a whole block device rather than a
// partition, mirroring psutil._pslinux.is_storage_device:
//
//	os.access("/sys/block/" + name.replace('/', '!'), os.F_OK)
//
// Only whole devices have an entry under /sys/block, so partitions (vda1, ...)
// are filtered out here — the same reason psutil's perdisk=False aggregates
// exclude them.
func isStorageDevice(name string) bool {
	_, err := os.Stat(filepath.Join(sysBlockDir, strings.ReplaceAll(name, "/", "!")))
	return err == nil
}

// diskIO reads aggregate disk IO counters from /proc/diskstats, mirroring
// Python _get_disk_io_stats. Returns (0,0,0,0) on diskless hosts.
func (s *systemSource) diskIO() (DiskIOMetrics, error) {
	f, err := os.Open(diskstatsPath)
	if err != nil {
		return DiskIOMetrics{}, err
	}
	defer f.Close()

	var out DiskIOMetrics
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 14 {
			continue
		}
		// psutil.disk_io_counters() is called with perdisk=False, so partitions
		// must be skipped; otherwise their counters are added on top of the
		// whole device that already contains them.
		if !isStorageDevice(fields[2]) {
			continue
		}
		// fields: major minor name reads reads_merged sectors_read ... writes ...
		// Sector counts are in 512-byte units.
		reads, _ := strconv.ParseInt(fields[3], 10, 64)
		sectorsRead, _ := strconv.ParseInt(fields[5], 10, 64)
		writes, _ := strconv.ParseInt(fields[7], 10, 64)
		sectorsWrite, _ := strconv.ParseInt(fields[9], 10, 64)
		out.ReadCount += reads
		out.WriteCount += writes
		out.ReadBytes += sectorsRead * 512
		out.WriteBytes += sectorsWrite * 512
	}
	if err := sc.Err(); err != nil {
		return DiskIOMetrics{}, err
	}
	s.recordDiskIO(out)
	return out, nil
}

// maxSpeedSamples bounds the retained disk/network counter history used to
// compute speeds, mirroring Python's hist that is trimmed to 7 samples.
const maxSpeedSamples = 7

// recordDiskIO appends a disk-IO counter sample to the speed history, keeping
// at most the most recent samples (mirroring Python's bounded hist). The sample
// is stamped with the current cycle timestamp so that a collect cycle and its
// published "now" field share one instant.
func (s *systemSource) recordDiskIO(m DiskIOMetrics) {
	s.diskIOHist = append(s.diskIOHist, ioSample{at: s.cycleTimestamp, val: m})
	if len(s.diskIOHist) > maxSpeedSamples {
		s.diskIOHist = s.diskIOHist[len(s.diskIOHist)-maxSpeedSamples:]
	}
}

// diskIOSpeed computes disk_io_speed as (readSpeed, writeSpeed, readIops,
// writeIops) from the retained counter history, mirroring Python
// _compute_speed_from_hist. When fewer than two samples exist it returns zeros.
func (s *systemSource) diskIOSpeed() ([]float64, error) {
	return speedFromIO(s.diskIOHist), nil
}

// speedFromIO computes per-field rates from a history of (timestamp, counter)
// samples using the oldest and newest sample, mirroring Python
// _compute_speed_from_hist.
func speedFromIO(hist []ioSample) []float64 {
	if len(hist) < 2 {
		return []float64{0, 0, 0, 0}
	}
	then, now := hist[0], hist[len(hist)-1]
	dt := now.at - then.at
	if dt <= 0 {
		return []float64{0, 0, 0, 0}
	}
	prev, cur := then.val, now.val
	return []float64{
		float64(cur.ReadBytes-prev.ReadBytes) / dt,
		float64(cur.WriteBytes-prev.WriteBytes) / dt,
		float64(cur.ReadCount-prev.ReadCount) / dt,
		float64(cur.WriteCount-prev.WriteCount) / dt,
	}
}

// network reads aggregate byte counters from /proc/net/dev for interfaces
// whose name starts with "e", mirroring Python _get_network_stats.
func (s *systemSource) network() (IOMetrics, error) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return IOMetrics{}, err
	}
	defer f.Close()

	var out IOMetrics
	sc := bufio.NewScanner(f)
	// skip header lines (2 lines)
	line := 0
	for sc.Scan() {
		line++
		if line <= 2 {
			continue
		}
		fields := strings.Fields(sc.Text())
		if len(fields) < 10 {
			continue
		}
		iface := strings.TrimSuffix(fields[0], ":")
		if !strings.HasPrefix(iface, "e") {
			continue
		}
		recv, _ := strconv.ParseInt(fields[1], 10, 64)
		sent, _ := strconv.ParseInt(fields[9], 10, 64)
		out.Recv += recv
		out.Sent += sent
	}
	if err := sc.Err(); err != nil {
		return IOMetrics{}, err
	}
	s.recordNetwork(out)
	return out, nil
}

// recordNetwork appends a network counter sample to the speed history, keeping
// at most the most recent samples. The sample is stamped with the current cycle
// timestamp so that a collect cycle and its published "now" field share one
// instant.
func (s *systemSource) recordNetwork(m IOMetrics) {
	s.netHist = append(s.netHist, netSample{at: s.cycleTimestamp, val: m})
	if len(s.netHist) > maxSpeedSamples {
		s.netHist = s.netHist[len(s.netHist)-maxSpeedSamples:]
	}
}

// networkSpeed computes network_speed as (sendSpeed, receiveSpeed) from the
// retained counter history, mirroring Python _compute_speed_from_hist. When
// fewer than two samples exist it returns zeros.
func (s *systemSource) networkSpeed() ([]float64, error) {
	if len(s.netHist) < 2 {
		return []float64{0, 0}, nil
	}
	then, now := s.netHist[0], s.netHist[len(s.netHist)-1]
	dt := now.at - then.at
	if dt <= 0 {
		return []float64{0, 0}, nil
	}
	prev, cur := then.val, now.val
	return []float64{
		float64(cur.Sent-prev.Sent) / dt,
		float64(cur.Recv-prev.Recv) / dt,
	}, nil
}

// loadAvg reads the 1/5/15 minute load averages from /proc/loadavg,
// mirroring Python _get_load_avg.
func (s *systemSource) loadAvg() (LoadAvgInfo, error) {
	content, err := readProcFile("/proc/loadavg")
	if err != nil {
		return LoadAvgInfo{}, err
	}
	fields := strings.Fields(content)
	if len(fields) < 3 {
		return LoadAvgInfo{}, os.ErrNotExist
	}
	l1, _ := strconv.ParseFloat(fields[0], 64)
	l5, _ := strconv.ParseFloat(fields[1], 64)
	l15, _ := strconv.ParseFloat(fields[2], 64)

	var perCPU []float64
	{
		counts, _ := s.cpuCounts()
		if len(counts) > 0 && counts[0] > 0 {
			perCPU = []float64{
				round2(l1 / float64(counts[0])),
				round2(l5 / float64(counts[0])),
				round2(l15 / float64(counts[0])),
			}
		}
	}
	return LoadAvgInfo{Load1: l1, Load5: l5, Load15: l15, PerCPULoad: perCPU}, nil
}

// roundToDecimals rounds f to n decimal places the way Python's round(f, n)
// does: correct decimal rounding of the value's shortest representation, with
// ties broken to even.
//
// math.Round and math.RoundToEven cannot reproduce that, because both round the
// value after rescaling it by a power of ten that is usually not exactly
// representable. The rescale can push a would-be tie off the tie, or create one
// that is not there in decimal, so the wrong neighbour is chosen: Python gives
// round(2.675, 2) = 2.67 where math.Round(2.675*100)/100 gives 2.68, and
// round(0.6375 * 100, 1) = 63.7 where math.RoundToEven on the ratio times 1000
// gives 63.8. Formatting to the target precision and parsing back avoids the
// rescale entirely, and strconv breaks ties to even like Python.
//
// Only the rounding is shared: the scale-then-round ordering stays with each
// caller, because the Python call sites differ — round(x/cpus, 2),
// round(used/total, 3) * 100 and round(used/(used+free) * 100, 1) are three
// distinct operations on three distinct quantities.
func roundToDecimals(f float64, n int) float64 {
	out, err := strconv.ParseFloat(strconv.FormatFloat(f, 'f', n, 64), 64)
	if err != nil {
		// Unreachable: a value formatted by FormatFloat is always parseable.
		return f
	}
	return out
}

func round2(f float64) float64 {
	return roundToDecimals(f, 2)
}

// bootTime returns the node boot timestamp, mirroring Python
// ReporterAgent._get_boot_time: inside a K8s pod the container entrypoint's
// start time (process 1) is reported instead of the host boot time.
func (s *systemSource) bootTime() (float64, error) {
	if s.inK8s() {
		// psutil.Process(pid=1).create_time(); the raw host boot time is still
		// needed to turn the starttime tick count into an epoch timestamp.
		return s.processCreateTime(1), nil
	}
	return s.rawBootTime()
}

// rawBootTime reads the boot timestamp from /proc/stat, mirroring
// Python psutil.boot_time(). The value is cached on first use because btime is
// constant for the process lifetime. This is the un-dispatched host boot time;
// bootTime wraps it with the K8s container-entrypoint behavior.
func (s *systemSource) rawBootTime() (float64, error) {
	if s.bootCached {
		return s.bootValue, nil
	}
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && fields[0] == "btime" {
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, err
			}
			s.bootValue = v
			s.bootCached = true
			return v, nil
		}
	}
	return 0, os.ErrNotExist
}

// inK8s reports whether the process runs inside a Kubernetes pod, mirroring
// Python IN_KUBERNETES_POD. It is false (never panics) when no adapter is set,
// which is the case for unit-test sources that stub their inputs.
func (s *systemSource) inK8s() bool {
	return s.k8s != nil && s.k8s.IsK8s()
}

// inContainer reports whether the process runs inside a container, mirroring
// Python IN_CONTAINER. Python's CPU-count branch is taken for a container as
// well as a pod, because psutil does not report a meaningful logical CPU count
// in either case.
func (s *systemSource) inContainer() bool {
	return s.k8s != nil && (s.k8s.IsInContainer() || s.k8s.IsK8s())
}

// workers returns the process info of the live worker and driver processes
// registered with the raylet, mirroring Python _async_get_workers. The pids come
// from the raylet over gRPC; each pid is then inspected like any other process.
// A pid whose process has already exited, or whose process is a zombie, is
// skipped, matching the Python handling of NoSuchProcess and STATUS_ZOMBIE.
//
// GPU usage is deliberately not attached here: it depends on the device
// snapshot, which is collected separately and merged onto these entries by
// attachWorkerGPUInfo.
func (s *systemSource) workers() ([]interface{}, error) {
	if s.rayletWorkerPIDs == nil {
		return []interface{}{}, nil
	}
	pids, err := s.rayletWorkerPIDs()
	if err != nil {
		return []interface{}{}, nil
	}

	workers := make([]interface{}, 0, len(pids))
	for _, pid := range pids {
		state, err := processState(int(pid))
		if err != nil {
			continue
		}
		if state == procStateZombie {
			continue
		}
		workers = append(workers, s.procAttrs(int(pid)))
	}
	return workers, nil
}

// procStateZombie is the /proc/<pid>/stat state character of a zombie process,
// equivalent to psutil.STATUS_ZOMBIE.
const procStateZombie = byte('Z')

// processState returns the state character of a process. It reads the field
// immediately following the comm field of /proc/<pid>/stat, which is the first
// element returned by procStatFields. It reports an error when the process has
// exited or its stat line cannot be read, so the caller treats such a pid as
// gone.
func processState(pid int) (byte, error) {
	fields, err := procStatFields(pid)
	if err != nil {
		return 0, err
	}
	if len(fields) == 0 || len(fields[0]) == 0 {
		return 0, fmt.Errorf("no process state for pid %d", pid)
	}
	return fields[0][0], nil
}

// isZombie reports whether a process has become a zombie, mirroring
// psutil.STATUS_ZOMBIE. It is a package-level seam so the ancestor walk's zombie
// guard can be exercised without manufacturing a real zombie process.
var isZombie = func(pid int) bool {
	state, err := processState(pid)
	return err == nil && state == procStateZombie
}

// raylet returns process info for the raylet process, mirroring Python
// _get_raylet.
func (s *systemSource) raylet() (map[string]interface{}, error) {
	pid := findRayletByParentChain()
	if pid <= 0 {
		return nil, nil
	}
	return s.procAttrs(pid), nil
}

// gcs returns process info for the GCS server on head nodes, mirroring
// Python _get_gcs, which resolves the pid from the GCS internal KV store rather
// than by inspecting the process table.
func (s *systemSource) gcs() (map[string]interface{}, error) {
	if s.gcsPIDFetcher == nil {
		// No GCS client (non-head node) or no pid published yet: Python returns
		// an empty dict rather than omitting the key.
		return map[string]interface{}{}, nil
	}
	pid, ok := s.gcsPIDFetcher()
	if !ok || pid <= 0 {
		return map[string]interface{}{}, nil
	}
	return s.procAttrs(pid), nil
}

// agent returns process info for the current process, mirroring Python
// _get_agent.
func (s *systemSource) agent() (map[string]interface{}, error) {
	return s.procAttrs(os.Getpid()), nil
}

// findRayletByParentChain locates the raylet by walking up the parent chain
// from this process, mirroring the Python _get_raylet_proc.
//
// The walk starts at the parent, so the agent never matches itself, exactly as
// Python starts at curr_proc.parent(). Matching is done on the process name
// (comm) rather than the full cmdline: every worker and the agent itself carry
// --raylet-name=<socket> in their argv, so a cmdline-substring match would
// select an arbitrary one of them instead of the raylet.
//
// The loop stops at pid 1 and treats it as "no raylet" for two reasons at once:
// Python's `while candidate` ends when the chain runs out, and Python explicitly
// rejects pid 1 because an init process named raylet must not be reported as
// the node's raylet. A matched process that has become a zombie is likewise
// rejected, matching the Python STATUS_ZOMBIE check.
func findRayletByParentChain() int {
	pid := parentPID(os.Getpid())
	for pid > 1 {
		name, err := commName(pid)
		if err != nil {
			// The ancestor exited between the ppid read and the comm read, so
			// the chain is broken and there is no raylet to report.
			return 0
		}
		if strings.Contains(name, "raylet") {
			if isZombie(pid) {
				return 0
			}
			return pid
		}
		pid = parentPID(pid)
	}
	return 0
}

// procAttrs returns process info for a pid, matching the fields the Python
// reporter collects with psutil's PSUTIL_PROCESS_ATTRS: pid, create_time,
// cpu_percent, cpu_times, cmdline, memory_info, memory_full_info and num_fds.
// The pid is always present even when the process cannot be inspected, so
// callers can identify the intended target, and each additional field degrades
// independently to an omitted key. The returned map uses snake_case keys;
// toGoogleStyle converts them to the camelCase ProcessInfo fields of the
// Python StatsPayload model.
//
// The process name (/proc/<pid>/comm) is deliberately absent: psutil's
// as_dict(attrs=PSUTIL_PROCESS_ATTRS) never returns it, so it is not part of
// PSUTIL_PROCESS_ATTRS, and the Python ProcessInfo model has no field for it.
// commName stays available for the raylet discovery, which needs the name as an
// input rather than as a reported field.
func (s *systemSource) procAttrs(pid int) map[string]interface{} {
	info := map[string]interface{}{
		"pid":         pid,
		"create_time": s.processCreateTime(pid),
		"cpu_percent": 0.0,
	}

	// cpu_percent and cpu_times share a single /proc/<pid>/stat read.
	if times, ok := readProcessCPUTimes(pid); ok {
		info["cpu_times"] = map[string]interface{}{
			"user":            times.user,
			"system":          times.system,
			"children_user":   times.childrenUser,
			"children_system": times.childrenSystem,
		}
		info["cpu_percent"] = s.processCPUPercent(pid, times)
	}

	if mem, err := readProcMemoryInfo(pid); err == nil {
		info["memory_info"] = mem
	}
	if mem, err := readProcFullMemoryInfo(pid); err == nil {
		info["memory_full_info"] = mem
	}
	if n, err := countProcFDs(pid); err == nil {
		info["num_fds"] = n
	}

	cmd, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err == nil {
		info["cmdline"] = splitCmdline(cmd)
	}
	return info
}

// commName reads /proc/<pid>/comm, the process name psutil reports through
// Process.name() and the field the Python _get_raylet_proc matches against.
// It is a package-level seam so tests can pin a process tree instead of
// depending on the one the test runs under, following the cpuOnlineReader
// idiom.
var commName = readCommName

// readCommName is the production commName, reading /proc/<pid>/comm.
func readCommName(pid int) (string, error) {
	return readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "comm"))
}

// parentPID reads the parent pid of a process from /proc/<pid>/stat. It is a
// package-level seam alongside commName so the ancestor walk can be tested
// against a pinned process tree.
var parentPID = readParentPID

// readParentPID is the production parentPID. The ppid is field 4 overall, which
// is index 1 within the post-comm fields (the stat line's comm field may itself
// contain spaces and parentheses, so the fields are taken after the last ')').
// A process whose stat line cannot be parsed reports ppid 0, which terminates
// the ancestor walk.
func readParentPID(pid int) int {
	fields, err := procStatFields(pid)
	if err != nil || len(fields) < 2 {
		return 0
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0
	}
	return ppid
}

// procStatFields returns the numeric fields of /proc/<pid>/stat that follow the
// comm field, i.e. starting at field 3 (state). The comm field is enclosed in
// parentheses and may itself contain spaces or parentheses, so the fields are
// located after the last ')'.
func procStatFields(pid int) ([]string, error) {
	raw, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return nil, err
	}
	rp := strings.LastIndex(raw, ")")
	if rp < 0 || rp+1 >= len(raw) {
		return nil, fmt.Errorf("malformed stat line for pid %d", pid)
	}
	return strings.Fields(raw[rp+1:]), nil
}

// processCreateTime returns the epoch-second start time of a process by reading
// its starttime (field 22) from /proc/<pid>/stat and adding the boot time,
// mirroring psutil's create_time. It returns 0 if the process or its starttime
// cannot be read.
func (s *systemSource) processCreateTime(pid int) float64 {
	fields, err := procStatFields(pid)
	if err != nil {
		return 0
	}
	// starttime is field 22 overall, which is index 19 within the post-comm
	// fields (22 - 3).
	if len(fields) < 20 {
		return 0
	}
	startTicks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0
	}
	// Use the raw host boot time, not bootTime() itself: inside a pod
	// bootTime() delegates to processCreateTime(1), so calling it here would
	// recurse.
	boot, err := s.rawBootTime()
	if err != nil {
		return 0
	}
	return boot + float64(startTicks)/clockTicksPerSecond
}

// clockTicksPerSecond is the USER_HZ value used for the CPU-time fields of
// /proc/<pid>/stat, matching psutil's os.sysconf("SC_CLK_TCK") on Linux.
const clockTicksPerSecond = 100.0

// processCPUTimes holds the CPU times of one process in seconds, mirroring the
// psutil pcputimes fields declared by the Python CpuTimes model.
type processCPUTimes struct {
	user           float64
	system         float64
	childrenUser   float64
	childrenSystem float64
}

// readProcessCPUTimes reads the utime/stime/cutime/cstime fields of
// /proc/<pid>/stat (fields 14-17, indices 11-14 after the state field),
// mirroring psutil's Process.cpu_times. It reports false when the process stat
// line cannot be read or is too short.
func readProcessCPUTimes(pid int) (processCPUTimes, bool) {
	fields, err := procStatFields(pid)
	if err != nil {
		return processCPUTimes{}, false
	}
	return cpuTimesFromStatFields(fields)
}

// cpuTimesFromStatFields maps the utime/stime/cutime/cstime fields of one
// /proc/<pid>/stat snapshot to seconds. It is the seam that makes the field
// offsets testable: a test that read a live process twice would race the tick
// counter and could compare two different snapshots.
func cpuTimesFromStatFields(fields []string) (processCPUTimes, bool) {
	if len(fields) < 15 {
		return processCPUTimes{}, false
	}
	return processCPUTimes{
		user:           ticksToSeconds(fields[11]),
		system:         ticksToSeconds(fields[12]),
		childrenUser:   ticksToSeconds(fields[13]),
		childrenSystem: ticksToSeconds(fields[14]),
	}, true
}

// ticksToSeconds converts a USER_HZ clock-tick field to seconds; an unparseable
// field degrades to 0 rather than failing the whole stat read.
func ticksToSeconds(field string) float64 {
	ticks, err := strconv.ParseFloat(field, 64)
	if err != nil {
		return 0
	}
	return ticks / clockTicksPerSecond
}

// procCPUSample is the previous observation of one process's CPU time and the
// monotonic instant it was taken at, used to compute cpu_percent from the delta
// between two collection cycles.
type procCPUSample struct {
	at     time.Time
	user   float64
	system float64
}

// processCPUPercent returns the CPU utilization of a process since the previous
// collection cycle, mirroring psutil's Process.cpu_percent(interval=None): the
// process CPU-time delta is divided by the elapsed monotonic time (the
// multiplication and division by the CPU count cancel out), and the first
// observation returns 0. A non-positive elapsed time also returns 0. The result
// is rounded to one decimal place like psutil.
func (s *systemSource) processCPUPercent(pid int, times processCPUTimes) float64 {
	now := time.Now()
	if s.procCPUPrev == nil {
		s.procCPUPrev = map[int]procCPUSample{}
	}
	if s.sampledPids == nil {
		s.sampledPids = map[int]struct{}{}
	}
	s.sampledPids[pid] = struct{}{}
	prev, ok := s.procCPUPrev[pid]
	s.procCPUPrev[pid] = procCPUSample{at: now, user: times.user, system: times.system}
	if !ok {
		return 0.0
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return 0.0
	}
	delta := (times.user - prev.user) + (times.system - prev.system)
	return roundToDecimals(delta/elapsed*100, 1)
}

// readProcMemoryInfo returns the psutil memory_info equivalent of a process:
// rss, vms and shared in bytes, derived from /proc/<pid>/statm whose first
// three fields are page counts (size, resident, shared).
func readProcMemoryInfo(pid int) (map[string]interface{}, error) {
	raw, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "statm"))
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(raw)
	if len(fields) < 3 {
		return nil, fmt.Errorf("malformed statm line for pid %d", pid)
	}
	pageSize := int64(os.Getpagesize())
	return map[string]interface{}{
		"vms":    pagesToBytes(fields[0], pageSize),
		"rss":    pagesToBytes(fields[1], pageSize),
		"shared": pagesToBytes(fields[2], pageSize),
	}, nil
}

// pagesToBytes converts a page-count field to bytes; an unparseable field
// degrades to 0 rather than failing the whole statm read.
func pagesToBytes(field string, pageSize int64) int64 {
	pages, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		return 0
	}
	return pages * pageSize
}

// readProcFullMemoryInfo returns the psutil memory_full_info equivalent of a
// process: memory_info plus the uss and pss totals. Like psutil it prefers
// /proc/<pid>/smaps_rollup and falls back to the slower /proc/<pid>/smaps when
// the rollup file is unavailable for the pid.
func readProcFullMemoryInfo(pid int) (map[string]interface{}, error) {
	uss, pss, err := readSmapsUsage(pid)
	if err != nil {
		return nil, err
	}
	mem, err := readProcMemoryInfo(pid)
	if err != nil {
		return nil, err
	}
	mem["uss"] = uss
	mem["pss"] = pss
	return mem, nil
}

// readSmapsUsage returns the USS and PSS byte totals of a process from
// smaps_rollup, falling back to smaps.
func readSmapsUsage(pid int) (uss, pss int64, err error) {
	base := filepath.Join("/proc", strconv.Itoa(pid))
	raw, err := readProcFile(filepath.Join(base, "smaps_rollup"))
	if err != nil {
		raw, err = readProcFile(filepath.Join(base, "smaps"))
		if err != nil {
			return 0, 0, err
		}
	}
	uss, pss, _ = parseSmapsUsage(raw)
	return uss, pss, nil
}

// parseSmapsUsage sums the USS and PSS byte totals from a smaps_rollup or smaps
// content. USS is the sum of the Private_Clean / Private_Dirty / Private_Hugetlb
// lines and PSS the Pss line; the kB values on those lines are converted to
// bytes. The rollup file carries a single Pss line while smaps carries one per
// mapping, so both are summed.
func parseSmapsUsage(raw string) (uss, pss, swap int64) {
	for _, line := range strings.Split(raw, "\n") {
		switch {
		case strings.HasPrefix(line, "Private_"):
			uss += smapsLineBytes(line)
		case strings.HasPrefix(line, "Pss:"):
			pss += smapsLineBytes(line)
		case strings.HasPrefix(line, "Swap:"):
			swap += smapsLineBytes(line)
		}
	}
	return uss, pss, swap
}

// smapsLineBytes returns the kB value of a single smaps line in bytes.
// The Pss_Anon / Pss_File / Pss_Shmem breakdown lines of smaps_rollup do not
// match the "Pss:" prefix and are therefore excluded.
func smapsLineBytes(line string) int64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	kb, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return kb * 1024
}

// countProcFDs returns the number of open file descriptors of a process,
// mirroring psutil's Process.num_fds on Linux (the entry count of
// /proc/<pid>/fd).
func countProcFDs(pid int) (int, error) {
	entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func splitCmdline(raw string) []string {
	if raw == "" {
		return []string{}
	}
	return strings.Split(raw, "\x00")
}

const (
	// countSwapInMemoryMonitorEnvVar gates the swap gauges, mirroring the
	// Python RAY_count_swap_in_memory_monitor check: swap that is not counted
	// by the memory monitor has nothing to contribute to the dashboards.
	countSwapInMemoryMonitorEnvVar = "RAY_count_swap_in_memory_monitor"
)

var (
	// errSwapDisabled says swap reporting is switched off.
	errSwapDisabled = errors.New("swap reporting disabled")
	// errNoCgroupMem says the host has no cgroup memory controller to report.
	errNoCgroupMem = errors.New("no cgroup memory controller")
)

// envTrue reports whether an environment variable holds a Ray bool true value
// ("1" or "true", case-insensitive), matching Python's env_bool.
func envTrue(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true":
		return true
	default:
		return false
	}
}

// HostMemInfo is the host's physical memory, independent of any cgroup limit,
// mirroring the psutil.virtual_memory pair the Python reporter reports as
// node_mem_used_host / node_mem_total_host.
type HostMemInfo struct {
	Used  int64
	Total int64
}

// hostMemUsage reads the host memory totals from /proc/meminfo. Unlike
// memUsage it never consults the cgroup: reporting the confined and the
// physical figure side by side is the point of the two host gauges.
func (s *systemSource) hostMemUsage() (HostMemInfo, error) {
	values, err := s.meminfo()
	if err != nil {
		return HostMemInfo{}, err
	}
	total, ok := values["MemTotal"]
	if !ok {
		return HostMemInfo{}, os.ErrNotExist
	}
	avail, ok := values["MemAvailable"]
	if !ok || avail == 0 {
		avail = values["MemFree"] + values["Cached"]
	}
	used := total - avail
	if used < 0 {
		used = 0
	}
	return HostMemInfo{Used: used, Total: total}, nil
}

// SwapInfo is the swap figure the Python reporter reports as node_swap_*,
// already clamped the way it clamps: a used value outside [0, total] is
// impossible and would make the utilization nonsensical.
type SwapInfo struct {
	Total   int64
	Used    int64
	Percent float64
}

// swapUsage mirrors ReporterAgent._get_swap_usage: swap is only reported when
// RAY_count_swap_in_memory_monitor asks for it, because a constant zero series
// on every node is noise on the dashboards.
func (s *systemSource) swapUsage() (SwapInfo, error) {
	if !envTrue(countSwapInMemoryMonitorEnvVar) {
		return SwapInfo{}, errSwapDisabled
	}
	total, used := s.cgroupAwareSwap()
	if used < 0 {
		used = 0
	}
	if used > total {
		used = total
	}
	percent := 0.0
	if total > 0 {
		percent = roundToDecimals(float64(used)/float64(total), 3) * 100
	}
	return SwapInfo{Total: total, Used: used, Percent: percent}, nil
}

// cgroupAwareSwap mirrors get_cgroup_aware_swap_memory: inside a container the
// figures must be the cgroup's own swap, because host swap includes every other
// workload on the machine; outside one they are the host's. The branches are
// ordered the way Python orders them — cgroup v2 swap files first, then the
// v1 memsw controller, then the host — and a cgroup branch that cannot be read
// reports no swap rather than leaking a host figure into a cgroup-scoped one.
func (s *systemSource) cgroupAwareSwap() (int64, int64) {
	if total, used, ok := cgroupV2Swap(); ok {
		return total, used
	}
	if total, used, ok := cgroupV1Swap(); ok {
		return total, used
	}
	values, err := s.meminfo()
	if err != nil {
		return 0, 0
	}
	total := values["SwapTotal"]
	return total, total - values["SwapFree"]
}

// cgroupV2Swap implements the cgroup v2 branch. A numeric memory.swap.max is the
// cap; "max", a value that overflows int64, or a missing memory.swap.max means
// the cgroup imposes no cap, so the host total stands in while used still comes
// from this cgroup's memory.swap.current. A zero cap means swap is disabled for
// the cgroup, and a read error reports no swap at all.
func cgroupV2Swap() (int64, int64, bool) {
	text, err := readProcFile(cgroupV2SwapMax)
	if err != nil {
		return 0, 0, false
	}
	raw := strings.TrimSpace(text)
	max, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil {
		// Unlimited: the practical cap is whatever the host has. The used figure
		// stays cgroup-scoped, so another workload's swap is never counted here.
		hostTotal, _ := hostSwapFromMeminfo()
		return hostTotal, readCgroupV2SwapCurrent(), true
	}
	if max == 0 {
		// Swap disabled for this cgroup. Unlike the unlimited case, do not read
		// memory.swap.current: a stale or transitioning value there is not swap
		// this cgroup may use.
		return 0, 0, true
	}
	return max, readCgroupV2SwapCurrent(), true
}

// cgroupV1Swap derives swap from the memsw controller, which counts RAM and
// swap together: the swap cap is the combined limit minus the RAM limit, and
// the used figure is the combined usage minus the RAM usage. An unset memsw
// limit reads as a near-int64 sentinel, so the combined limit is capped at
// host RAM + host swap the way the C++ memory monitor caps it.
func cgroupV1Swap() (int64, int64, bool) {
	memswLimit, ok := readCgroupInt64(cgroupV1MemswLimit)
	if !ok {
		return 0, 0, false
	}
	memswUsage, ok := readCgroupInt64(cgroupV1MemswUsage)
	if !ok {
		return 0, 0, false
	}
	hostRAM, _ := hostMemFromMeminfo()
	if ramLimit, ok := readCgroupInt64(cgroupMemLimitV1); ok {
		hostSwap, _ := hostSwapFromMeminfo()
		memswLimit = minInt64(memswLimit, hostRAM+hostSwap)
		used := int64(0)
		if ramUsage, ok := readCgroupInt64(cgroupMemUsageV1); ok {
			used = memswUsage - ramUsage
			if used < 0 {
				used = 0
			}
		}
		return maxInt64(0, memswLimit-ramLimit), used, true
	}
	// Without a readable RAM limit the swap share cannot be derived from the
	// combined counter, so there is no swap to report.
	return 0, 0, true
}

// readCgroupV2SwapCurrent reads this cgroup's swap usage, treating a missing file
// as zero, mirroring Python's _read_cgroup_v2_swap_current.
func readCgroupV2SwapCurrent() int64 {
	if v, ok := readCgroupInt64(cgroupV2SwapCurrentPath); ok {
		return v
	}
	return 0
}

// hostSwapFromMeminfo returns the host swap total and used from /proc/meminfo.
func hostSwapFromMeminfo() (int64, int64) {
	values, err := readMeminfo()
	if err != nil {
		return 0, 0
	}
	total := values["SwapTotal"]
	return total, total - values["SwapFree"]
}

// hostMemFromMeminfo returns the host physical memory total from /proc/meminfo.
func hostMemFromMeminfo() (int64, int64) {
	values, err := readMeminfo()
	if err != nil {
		return 0, 0
	}
	avail, ok := values["MemAvailable"]
	if !ok || avail == 0 {
		avail = values["MemFree"] + values["Cached"]
	}
	return values["MemTotal"], values["MemTotal"] - avail
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// CgroupMemInfo is the container memory figure reported as node_cgroup_mem_*.
type CgroupMemInfo struct {
	Used  int64
	Total int64
}

// cgroupMemUsage reports the container memory usage and limit, the pair Python
// reads from ray._common.utils.get_cgroup_mem_stats. A host without a cgroup
// memory controller has nothing to report, which is what the nil-able stat
// entry expresses.
func (s *systemSource) cgroupMemUsage() (CgroupMemInfo, error) {
	values, err := s.meminfo()
	if err != nil {
		return CgroupMemInfo{}, err
	}
	used, haveUsed := cgroupUsedMemory()
	limit, haveLimit := cgroupMemLimitBytes()
	if !haveUsed {
		return CgroupMemInfo{}, errNoCgroupMem
	}
	// Python get_cgroup_mem_stats reports min(limit, system_total) and falls
	// back to system_total when there is no usable limit, so a host whose
	// cgroup is unconstrained (cgroup v1 writes a near-int64 value, cgroup v2
	// writes "max") reports the physical total instead of a sentinel that
	// would make every used/total ratio zero or infinite.
	limit = systemMemoryBytes(values, limit, haveLimit)
	return CgroupMemInfo{Used: used, Total: limit}, nil
}

// swapStat renders the swap entry: the tuple the record layer reads, or nil
// when there is nothing to report. A zero total is nothing to report even when
// the flag is on, mirroring the Python guard that keeps constant zeros off the
// dashboards.
func swapStat(info SwapInfo) interface{} {
	if info.Total <= 0 {
		return nil
	}
	return []interface{}{info.Total, info.Used, info.Percent}
}

// cgroupMemStat renders the cgroup memory entry, or nil when the host has no
// cgroup memory controller.
func cgroupMemStat(info CgroupMemInfo) interface{} {
	if info.Total <= 0 && info.Used <= 0 {
		return nil
	}
	return []interface{}{info.Used, info.Total}
}
