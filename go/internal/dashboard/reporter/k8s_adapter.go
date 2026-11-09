package reporter

import (
	"bufio"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// cgroup v1 and v2 file paths used to derive container CPU, memory, and network
// metrics. They are package-level so tests can construct an adapter with
// overridden paths/readers without touching the host filesystem.
const (
	cgroupRoot   = "/sys/fs/cgroup"
	cpuUsageV1   = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
	cpuUsageV2   = "/sys/fs/cgroup/cpu.stat"
	cpuQuota     = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	cpuPeriod    = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
	cpuMaxV2     = "/sys/fs/cgroup/cpu.max"
	cpusetV1     = "/sys/fs/cgroup/cpuset/cpuset.cpus"
	memUsageV1   = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	memLimitV1   = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	memUsageV2   = "/sys/fs/cgroup/memory.current"
	memLimitV2   = "/sys/fs/cgroup/memory.max"
	netDevPath   = "/proc/net/dev"
	procStatPath = "/proc/stat"
	// cpuOnlinePath lists the online CPUs (e.g. "0-111"), the source
	// os.sysconf("SC_NPROCESSORS_ONLN") resolves to and therefore what
	// psutil.cpu_count() reports.
	cpuOnlinePath = "/sys/devices/system/cpu/online"
	k8sEnvVar     = "KUBERNETES_SERVICE_HOST"
	// k8sDiskUsageEnvVar re-enables real disk usage reporting inside a pod,
	// mirroring Python ENABLE_K8S_DISK_USAGE.
	k8sDiskUsageEnvVar = "RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE"
	// multiprocCPUEnvVar forces the plain logical CPU count, mirroring the
	// Python RAY_USE_MULTIPROCESSING_CPU_COUNT escape hatch.
	multiprocCPUEnvVar = "RAY_USE_MULTIPROCESSING_CPU_COUNT"
)

// readFileFn abstracts reading a whole file so tests can mock cgroup v1/v2 and
// proc files without touching the real filesystem, following the commandRunner
// injection style used by gpu_collector.go.
type readFileFn func(path string) ([]byte, error)

// osReadFile is the production readFile, wrapping os.ReadFile.
var osReadFile readFileFn = func(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// K8sAdapter adapts CPU, memory, and network collection to run inside a
// container/cgroup environment, mirroring the Python k8s_utils.py and the
// container-aware reporter paths. It exposes injectable path and env readers
// so unit tests can exercise parsing without a real cgroup.
type K8sAdapter struct {
	cpuUsageV1 string
	cpuUsageV2 string
	cpuQuota   string
	cpuPeriod  string
	cpuMax     string
	cpuset     string
	memUsageV1 string
	memLimitV1 string
	memUsageV2 string
	memLimitV2 string
	netDev     string
	procStat   string
	envLookup  func(string) (string, bool)
	fileExists func(string) bool
	readFile   readFileFn
	// numCPU returns multiprocessing.cpu_count()'s equivalent, injectable so
	// tests can pin the logical count without depending on the test host.
	numCPU  func() int
	lastCPU int64
	lastSet bool
	// lastSystemTime tracks the last host system CPU time (in nanoseconds) read
	// from /proc/stat, and lastSystemSet records whether it has been captured.
	// They mirror Python k8s_utils.cpu_percent()'s last_system_usage so the CPU
	// percentage can be normalized by the elapsed window rather than a fixed
	// cgroup quota.
	lastSystemTime int64
	lastSystemSet  bool
	hostCPUs       int
	// numCPUs caches the container's logical CPU count so repeated reads (every
	// collect cycle calls get_num_cpus for cpus[0] and CPUPerc) do not re-walk
	// the cgroup files.
	numCPUsCache float64
	numCPUsSet   bool
}

// NewK8sAdapter builds a K8sAdapter using the real environment and filesystem,
// wired to the default cgroup paths.
func NewK8sAdapter() *K8sAdapter {
	return &K8sAdapter{
		cpuUsageV1: cpuUsageV1,
		cpuUsageV2: cpuUsageV2,
		cpuQuota:   cpuQuota,
		cpuPeriod:  cpuPeriod,
		cpuMax:     cpuMaxV2,
		cpuset:     cpusetV1,
		memUsageV1: memUsageV1,
		memLimitV1: memLimitV1,
		memUsageV2: memUsageV2,
		memLimitV2: memLimitV2,
		netDev:     netDevPath,
		procStat:   procStatPath,
		envLookup:  os.LookupEnv,
		fileExists: func(path string) bool {
			_, err := os.Stat(path)
			return err == nil
		},
		readFile: osReadFile,
		hostCPUs: -1,
	}
}

// IsK8s reports whether the process runs inside a Kubernetes pod, detected by
// the presence of the KUBERNETES_SERVICE_HOST environment variable, mirroring
// the Python IN_KUBERNETES_POD check.
func (a *K8sAdapter) IsK8s() bool {
	if a.envLookup == nil {
		a.envLookup = os.LookupEnv
	}
	_, ok := a.envLookup(k8sEnvVar)
	return ok
}

// K8sDiskUsageEnabled reports whether real disk usage should be reported inside
// a pod, mirroring Python ENABLE_K8S_DISK_USAGE: the env var must equal "1".
func (a *K8sAdapter) K8sDiskUsageEnabled() bool {
	if a.envLookup == nil {
		a.envLookup = os.LookupEnv
	}
	v, _ := a.envLookup(k8sDiskUsageEnvVar)
	return v == "1"
}

// IsInContainer reports whether the process runs inside a container, detected
// by the existence of the cgroup root directory /sys/fs/cgroup, mirroring the
// Python IN_CONTAINER = os.path.exists("/sys/fs/cgroup") check.
func (a *K8sAdapter) IsInContainer() bool {
	if a.fileExists == nil {
		a.fileExists = func(path string) bool {
			_, err := os.Stat(path)
			return err == nil
		}
	}
	return a.fileExists(cgroupRoot)
}

// readFileString reads a file and returns its trimmed content, or an error if
// the file is unreadable.
func (a *K8sAdapter) readFileString(path string) (string, error) {
	if a.readFile == nil {
		a.readFile = osReadFile
	}
	data, err := a.readFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// cpuUsageNanos returns the total CPU time consumed by the container in
// nanoseconds. It reads cpuacct.usage in cgroup v1, falling back to the
// usage_usec field of cpu.stat in cgroup v2, mirroring Python _cpu_usage.
func (a *K8sAdapter) cpuUsageNanos() (int64, error) {
	if text, err := a.readFileString(a.cpuUsageV1); err == nil {
		return strconv.ParseInt(text, 10, 64)
	}
	text, err := a.readFileString(a.cpuUsageV2)
	if err != nil {
		return 0, err
	}
	first := strings.SplitN(text, "\n", 2)[0]
	fields := strings.Fields(first)
	if len(fields) < 2 {
		return 0, os.ErrNotExist
	}
	usec, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	// Convert microseconds to nanoseconds.
	return usec * 1000, nil
}

// NumCPUs returns the number of CPUs available to the container, mirroring
// Python ray._private.utils.get_num_cpus (with truncation, as the reporter
// calls it): the plain logical CPU count, replaced by the cgroup-derived
// docker count when that is set and differs. The result is truncated to an
// integer and cached for the process lifetime.
func (a *K8sAdapter) NumCPUs() float64 {
	if a.numCPUsSet {
		return a.numCPUsCache
	}
	a.numCPUsCache = a.computeNumCPUs()
	a.numCPUsSet = true
	return a.numCPUsCache
}

// computeNumCPUs is the uncached get_num_cpus body.
func (a *K8sAdapter) computeNumCPUs() float64 {
	logical := float64(a.logicalCPUs())
	if a.envHas(multiprocCPUEnvVar) {
		// RAY_USE_MULTIPROCESSING_CPU_COUNT=1 short-circuits the cgroup probe,
		// mirroring Python's escape hatch for containers with a wrong cgroup.
		return logical
	}
	docker := a.dockerCPUs()
	if docker < 0 {
		// dockerCPUs returns a negative sentinel when no cgroup limit applies.
		return logical
	}
	if docker == logical {
		return logical
	}
	// get_num_cpus truncates the fractional docker count by default.
	return float64(int(docker))
}

// logicalCPUs returns multiprocessing.cpu_count(), the logical CPU count.
func (a *K8sAdapter) logicalCPUs() int {
	if a.numCPU != nil {
		return a.numCPU()
	}
	if n, ok := onlineCPUs(a.readFileString); ok {
		return n
	}
	return runtime.NumCPU()
}

// onlineCPUs returns the number of online CPUs parsed from
// /sys/devices/system/cpu/online, mirroring psutil.cpu_count() (which resolves
// os.sysconf("SC_NPROCESSORS_ONLN")). It is deliberately not runtime.NumCPU():
// that reports the process's CPU affinity mask, so a process pinned with
// taskset or constrained by cpuset would report a smaller count than Python.
// The read is injectable so tests do not depend on the host topology.
func onlineCPUs(read func(string) (string, error)) (int, bool) {
	text, err := read(cpuOnlinePath)
	if err != nil {
		return 0, false
	}
	return parseCPUList(strings.TrimSpace(text))
}

// parseCPUList counts the CPUs in a Linux cpu list such as "0-3,8" (5 CPUs) or
// "0-111" (112 CPUs), the format of both /sys/devices/system/cpu/online and the
// cpuset controller's cpuset.cpus. An empty or malformed list yields ok=false.
func parseCPUList(text string) (int, bool) {
	if text == "" {
		return 0, false
	}
	count := 0
	for _, part := range strings.Split(text, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		start, err := strconv.Atoi(strings.TrimSpace(lo))
		if err != nil {
			return 0, false
		}
		if !isRange {
			count++
			continue
		}
		end, err := strconv.Atoi(strings.TrimSpace(hi))
		if err != nil || end < start {
			return 0, false
		}
		count += end - start + 1
	}
	if count == 0 {
		return 0, false
	}
	return count, true
}

// dockerCPUs mirrors Python _get_docker_cpus: the number of vCPUs the container
// is limited to, the smaller of the CFS quota (cpu.cfs_quota_us/period, or
// cpu.max on cgroup v2) and the cpuset cpu count. It returns -1 when neither
// limit is set, matching the Python None result the caller maps back to the
// bare logical count.
func (a *K8sAdapter) dockerCPUs() float64 {
	quota, haveQuota := a.cpuQuotaCPUs()
	cpuset, haveCpuset := a.cpusetCPUs()
	if haveQuota && haveCpuset {
		return math.Min(quota, cpuset)
	}
	if haveQuota {
		return quota
	}
	if haveCpuset {
		return cpuset
	}
	return -1
}

// cpuQuotaCPUs returns the CFS quota expressed in CPUs. It reads
// cpu.cfs_quota_us / cpu.cfs_period_us on cgroup v1 and falls back to the
// "quota period" pair in cpu.max on cgroup v2. A negative or unset quota
// reports false, and a zero quota is rounded up to 1, mirroring Python.
func (a *K8sAdapter) cpuQuotaCPUs() (float64, bool) {
	var quota, period float64
	if q, e1 := a.readFloat64(a.cpuQuota); e1 == nil {
		if p, e2 := a.readFloat64(a.cpuPeriod); e2 == nil {
			quota, period = q, p
		}
	}
	if period == 0 {
		// cgroup v2: cpu.max holds "<quota> <period>"; a quota of "max" means
		// the CFS limit is unset.
		if text, err := a.readFileString(a.cpuMax); err == nil {
			fields := strings.Fields(text)
			if len(fields) < 2 {
				return 0, false
			}
			q, e1 := strconv.ParseFloat(fields[0], 64)
			p, e2 := strconv.ParseFloat(fields[1], 64)
			if e1 != nil || e2 != nil {
				return 0, false
			}
			quota, period = q, p
		}
	}
	if period <= 0 {
		return 0, false
	}
	cpus := quota / period
	if cpus < 0 {
		return 0, false
	}
	if cpus == 0 {
		// Round up so a sub-CPU limit still reports one usable CPU.
		cpus = 1
	}
	return cpus, true
}

// cpusetCPUs returns the number of CPUs in the cpuset controller's cpu list
// (e.g. "0-3,8" -> 5), mirroring Python _get_docker_cpus's cpuset parsing.
func (a *K8sAdapter) cpusetCPUs() (float64, bool) {
	text, err := a.readFileString(a.cpuset)
	if err != nil {
		return 0, false
	}
	count, ok := parseCPUList(text)
	if !ok {
		return 0, false
	}
	return float64(count), true
}

// envHas reports whether an environment variable is present, regardless of
// value, mirroring Python's `in os.environ` checks.
func (a *K8sAdapter) envHas(key string) bool {
	if a.envLookup == nil {
		a.envLookup = os.LookupEnv
	}
	_, ok := a.envLookup(key)
	return ok
}

// readFloat64 reads a file and parses its trimmed content as a float64.
func (a *K8sAdapter) readFloat64(path string) (float64, error) {
	text, err := a.readFileString(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(text, 64)
}

// readInt64 reads a file and parses its trimmed content as an int64.
func (a *K8sAdapter) readInt64(path string) (int64, error) {
	text, err := a.readFileString(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(text, 10, 64)
}

// readMemLimit reads a memory limit file, treating the cgroup v2 literal "max"
// (unlimited memory) as an effectively unbounded limit so callers report a
// near-zero percentage rather than erroring. A K8s pod without a memory limit
// commonly reports memory.max as "max".
func (a *K8sAdapter) readMemLimit(path string) (int64, error) {
	text, err := a.readFileString(path)
	if err != nil {
		return 0, err
	}
	if text == "max" {
		return math.MaxInt64, nil
	}
	return strconv.ParseInt(text, 10, 64)
}

// readHostNumCPUs counts the number of CPU lines in /proc/stat minus the
// aggregate first line, mirroring Python _host_num_cpus. The proc path is
// injectable via the procStat field (defaulting to /proc/stat).
func (a *K8sAdapter) readHostNumCPUs() int {
	text, err := a.readFileString(a.procStatPath())
	if err != nil {
		return 0
	}
	count := 0
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) > 0 && strings.HasPrefix(fields[0], "cpu") {
			count++
		}
	}
	// Subtract 1 for the aggregate "cpu" summary line.
	if count > 1 {
		return count - 1
	}
	return 0
}

// readSystemTime returns the total host CPU time in nanoseconds, derived from
// the aggregate "cpu" summary line of /proc/stat, mirroring Python
// _system_usage. It sums the first seven clock-tick fields and converts ticks
// to nanoseconds (100 ticks per second * 1e9 ns per second = 1e7 ns per tick).
func (a *K8sAdapter) readSystemTime() (int64, error) {
	text, err := a.readFileString(a.procStatPath())
	if err != nil {
		return 0, err
	}
	first := strings.SplitN(text, "\n", 2)[0]
	fields := strings.Fields(first)
	if len(fields) < 8 || fields[0] != "cpu" {
		return 0, os.ErrNotExist
	}
	var total int64
	for _, f := range fields[1:8] {
		v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return 0, err
		}
		total += v
	}
	return total * 1e7, nil
}

// procStatPath returns the injectable /proc/stat path, defaulting to the
// standard location when unset.
func (a *K8sAdapter) procStatPath() string {
	if a.procStat == "" {
		return procStatPath
	}
	return a.procStat
}

// CPUPerc returns the CPU usage percentage of the container relative to its
// cgroup CPU limit, mirroring Python k8s_utils.cpu_percent(). The container CPU
// usage delta is divided by the elapsed host system time (the /proc/stat "cpu"
// summary delta, i.e. the sample window) and normalized by the container's CPU
// capacity, so full use of a multi-CPU limit reads as 100% regardless of the
// sample window length. The first call returns 0.0 because no delta is
// available yet.
func (a *K8sAdapter) CPUPerc() float64 {
	cpuUsage, err := a.cpuUsageNanos()
	if err != nil {
		return 0
	}
	systemTime, err := a.readSystemTime()
	if err != nil {
		return 0
	}
	if !a.lastSet {
		a.lastCPU = cpuUsage
		a.lastSystemTime = systemTime
		a.lastSet = true
		a.lastSystemSet = true
		return 0
	}
	usageDelta := cpuUsage - a.lastCPU
	systemDelta := systemTime - a.lastSystemTime
	a.lastCPU = cpuUsage
	a.lastSystemTime = systemTime

	if systemDelta <= 0 {
		return 0
	}
	// Python divides by get_num_cpus(), i.e. the container's logical CPU count
	// as constrained by the cgroup quota/cpuset, so full use of a 2-CPU limit
	// reads as 100% rather than 200%.
	cpus := a.NumCPUs()
	if cpus <= 0 {
		return 0
	}
	// Mirror Python k8s_utils.cpu_percent: the raw host system-time delta from
	// the aggregate /proc/stat "cpu" line is the sum across all host cores, so
	// divide it by the number of host CPUs to obtain the per-core (wall-clock
	// equivalent) elapsed window before computing the quotient. Without this
	// normalization the percentage is understated by ~the host core count.
	hostCPUs := a.hostCPUs
	if hostCPUs <= 0 {
		hostCPUs = a.readHostNumCPUs()
		if hostCPUs > 0 {
			a.hostCPUs = hostCPUs
		}
	}
	if hostCPUs <= 0 {
		// A real Linux host always reports per-core lines (cpu0..cpuN) in
		// /proc/stat, so a zero count is unreachable outside synthetic inputs;
		// treat it as a single-core host rather than dividing by zero.
		hostCPUs = 1
	}
	// usageDelta over the elapsed per-core system window, normalized by CPU
	// capacity, is the percentage of the container's CPU limit actually consumed.
	percent := float64(usageDelta) / (float64(systemDelta) / float64(hostCPUs)) * 100 / cpus
	// Python rounds to one decimal place with round(..., 1), which breaks ties
	// to even; roundToDecimals reproduces that, unlike math.Round.
	percent = roundToDecimals(percent, 1)
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	return percent
}

// MemStats holds container memory usage, aligned with the Python
// _get_mem_usage (total, available, percent, used) shape but viewed from the
// cgroup memory limit/usage.
type MemStats struct {
	Total     int64
	Used      int64
	Available int64
	Percent   float64
}

// MemStats reads container memory usage and limit from the cgroup v1 memory
// files, falling back to cgroup v2 (memory.current / memory.max).
func (a *K8sAdapter) MemStats() (MemStats, error) {
	usage, err1 := a.readInt64(a.memUsageV1)
	limit, err2 := a.readInt64(a.memLimitV1)
	if err1 != nil || err2 != nil {
		usage, err1 = a.readInt64(a.memUsageV2)
		limit, err2 = a.readMemLimit(a.memLimitV2)
		if err1 != nil || err2 != nil {
			return MemStats{}, os.ErrNotExist
		}
	}
	if limit < 0 {
		limit = usage
	}
	available := limit - usage
	if available < 0 {
		available = 0
	}
	return MemStats{
		Total:     limit,
		Used:      usage,
		Available: available,
		Percent:   memPercent(usage, limit),
	}, nil
}

// NetworkStats returns the aggregate receive/transmit byte counters for the
// container's network interfaces (excluding loopback), mirroring the
// Python _get_network_stats but scoped to container-facing interfaces.
func (a *K8sAdapter) NetworkStats() (IOMetrics, error) {
	text, err := a.readFileString(a.netDev)
	if err != nil {
		return IOMetrics{}, err
	}
	var out IOMetrics
	sc := bufio.NewScanner(strings.NewReader(text))
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
		if iface == "lo" {
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
	return out, nil
}
