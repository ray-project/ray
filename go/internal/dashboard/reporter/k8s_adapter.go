package reporter

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
)

// cgroup v1 and v2 file paths used to derive container CPU, memory, and network
// metrics. They are package-level so tests can construct an adapter with
// overridden paths/readers without touching the host filesystem.
const (
	cgroupRoot = "/sys/fs/cgroup"
	cpuUsageV1 = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
	cpuUsageV2 = "/sys/fs/cgroup/cpu.stat"
	cpuQuota   = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	cpuPeriod  = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
	memUsageV1 = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	memLimitV1 = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	memUsageV2 = "/sys/fs/cgroup/memory.current"
	memLimitV2 = "/sys/fs/cgroup/memory.max"
	netDevPath = "/proc/net/dev"
	procStatPath = "/proc/stat"
	k8sEnvVar  = "KUBERNETES_SERVICE_HOST"
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
	memUsageV1 string
	memLimitV1 string
	memUsageV2 string
	memLimitV2 string
	netDev     string
	procStat   string
	envLookup  func(string) (string, bool)
	fileExists func(string) bool
	readFile   readFileFn
	lastCPU    int64
	lastSet    bool
	// lastSystemTime tracks the last host system CPU time (in nanoseconds) read
	// from /proc/stat, and lastSystemSet records whether it has been captured.
	// They mirror Python k8s_utils.cpu_percent()'s last_system_usage so the CPU
	// percentage can be normalized by the elapsed window rather than a fixed
	// cgroup quota.
	lastSystemTime int64
	lastSystemSet  bool
	hostCPUs       int
}

// NewK8sAdapter builds a K8sAdapter using the real environment and filesystem,
// wired to the default cgroup paths.
func NewK8sAdapter() *K8sAdapter {
	return &K8sAdapter{
		cpuUsageV1: cpuUsageV1,
		cpuUsageV2: cpuUsageV2,
		cpuQuota:   cpuQuota,
		cpuPeriod:  cpuPeriod,
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

// containerCPUCapacity returns the number of CPUs the container is limited to,
// derived from cpu.cfs_quota_us / cpu.cfs_period_us. A negative quota (-1)
// means unlimited; in that case the host CPU count from /proc/stat is used as
// a fallback. At least 1 is always returned.
func (a *K8sAdapter) containerCPUCapacity() int {
	quota, err1 := a.readInt64(a.cpuQuota)
	period, err2 := a.readInt64(a.cpuPeriod)
	if err1 == nil && err2 == nil && period > 0 {
		if quota > 0 {
			cpus := int(quota / period)
			if cpus > 0 {
				return cpus
			}
		}
	}
	if a.hostCPUs > 0 {
		return a.hostCPUs
	}
	if cpus := a.readHostNumCPUs(); cpus > 0 {
		a.hostCPUs = cpus
		return cpus
	}
	return 1
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
	cpus := a.containerCPUCapacity()
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
		// Host core count unavailable; skip normalization (equivalent to a
		// single-core host) so the percentage stays well-defined.
		hostCPUs = 1
	}
	// usageDelta over the elapsed per-core system window, normalized by CPU
	// capacity, is the percentage of the container's CPU limit actually consumed.
	percent := float64(usageDelta) / (float64(systemDelta) / float64(hostCPUs)) * 100 / float64(cpus)
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
