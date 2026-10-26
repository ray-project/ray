package reporter

import (
	"bufio"
	"bytes"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
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
	resetCycleCache()
	cpuPercent() (float64, error)
	cpuCounts() ([]int, error)
	memUsage() (MemInfo, error)
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
}

// Collect gathers a full resource snapshot and returns it as a map whose keys
// and value shapes align with the Python _async_collect_stats result. It never
// returns an error: individual collection failures degrade to zero/empty
// values instead of failing the whole cycle, so there is no error to
// propagate.
func (c *StatsCollector) Collect() map[string]interface{} {
	c.src.resetCycleCache()
	cpu, _ := c.src.cpuPercent()
	cpus, _ := c.src.cpuCounts()
	mem, _ := c.src.memUsage()
	shm, shmOK, _ := c.src.shmUsage()
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
		"now":           time.Now().Unix(),
		"hostname":      c.hostname,
		"ip":            c.ip,
		"cpu":           cpu,
		"cpus":          cpus,
		"mem":           []interface{}{mem.Total, mem.Available, mem.Percent, mem.Used},
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

func perCPULoadWire(p []float64) []interface{} {
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
	}
}

// systemSource is the production statsSource reading /proc, /sys, and the
// filesystem, mirroring the non-K8s paths of the Python reporter agent.
type systemSource struct {
	cfg      agent.Config
	hostname string
	// lastCPU/lastSet are mutable between calls; they are only accessed from a
	// single goroutine's periodic collection loop.
	lastCPU cpuTimes
	lastSet bool
	// cpuCache memoizes the [logical, physical] CPU count on first use. Both
	// logical and physical CPU counts are stable for the process lifetime, so
	// caching avoids re-reading /proc/cpuinfo on every collect cycle.
	cpuCache []int
	// procCache is the /proc cmdline scan result for the current collect cycle.
	// findProcessByCmdline scans all /proc entries and is called up to twice
	// per cycle (raylet and gcs), so the scan is cached to avoid the repeated
	// full-directory walk.
	procCache map[string]int
	// bootCached/bootValue memoize the boot timestamp. btime is constant for
	// the process lifetime, so re-reading /proc/stat on every collect cycle
	// (which already happens for cpuPercent) is wasteful.
	bootCached bool
	bootValue  float64
	// meminfoCache/meminfoCached memoize a single parse of /proc/meminfo per
	// collect cycle. memUsage and shmUsage both read the same file, so caching
	// avoids parsing it twice each cycle. The cache is reset at the start of
	// each Collect via resetCycleCache.
	meminfoCache  map[string]int64
	meminfoCached bool
	// diskIOHist/netHist retain the most recent (timestamp, counter) samples
	// used to compute disk_io_speed / network_speed, mirroring Python's
	// _disk_io_stats_hist and _network_stats_hist.
	diskIOHist []ioSample
	netHist    []netSample
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
	return &systemSource{cfg: cfg, hostname: hostname}
}

// resetCycleCache clears per-cycle caches (meminfo) at the start of each
// Collect so values reflect the current cycle. Long-lived memoized values
// (boot time, cpu counts, proc cache) are intentionally retained.
func (s *systemSource) resetCycleCache() {
	s.meminfoCache = nil
	s.meminfoCached = false
}

// readProcFile reads a whole proc file and returns its trimmed content.
func readProcFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(data)), nil
}

// cpuCounts returns [logical, physical] CPU counts, mirroring Python
// psutil.cpu_count(logical) and psutil.cpu_count(logical=False). The result is
// cached on first use because CPU counts are stable for the process lifetime
// and reading /proc/cpuinfo on every collect cycle is wasteful.
func (s *systemSource) cpuCounts() ([]int, error) {
	if s.cpuCache != nil {
		return s.cpuCache, nil
	}
	logical := runtime.NumCPU()
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

// memUsage reads /proc/meminfo and returns total/available/percent/used,
// mirroring Python _get_mem_usage.
func (s *systemSource) memUsage() (MemInfo, error) {
	values, err := s.meminfo()
	if err != nil {
		return MemInfo{}, err
	}
	total, ok := values["MemTotal"]
	if !ok {
		return MemInfo{}, os.ErrNotExist
	}
	available, ok := values["MemAvailable"]
	if !ok {
		available = total
	}
	used := total - available
	percent := memPercent(used, total)
	return MemInfo{
		Total:     total,
		Available: available,
		Percent:   percent,
		Used:      used,
	}, nil
}

// memPercent mirrors Python round(used/total, 3)*100: the ratio is rounded to
// 3 decimal places before scaling by 100.
func memPercent(used, total int64) float64 {
	if total <= 0 {
		return 0
	}
	ratio := float64(used) / float64(total)
	return math.Round(ratio*1000) / 1000 * 100
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
// by resetCycleCache at the start of each Collect.
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

// readMeminfo parses /proc/meminfo into a field->KB map (keys in kB).
func readMeminfo() (map[string]int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]int64{}
	sc := bufio.NewScanner(f)
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
		out[key] = v
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// diskUsage returns per-mount disk usage for the root and temp directories,
// mirroring Python _get_disk_usage.
func (s *systemSource) diskUsage() (map[string]DiskInfo, error) {
	paths := map[string]string{
		"/": "/",
	}
	tmp := s.cfg.TempDir
	if tmp == "" {
		tmp = os.TempDir()
	}
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

// diskUsageOf uses statfs to compute usage of a single path.
func diskUsageOf(path string) (DiskInfo, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return DiskInfo{}, err
	}
	total := int64(st.Blocks) * int64(st.Bsize)
	free := int64(st.Bavail) * int64(st.Bsize)
	used := int64(st.Blocks-st.Bfree) * int64(st.Bsize)
	percent := 0.0
	if total > 0 {
		percent = float64(used) / float64(total) * 100
	}
	return DiskInfo{
		Total:   total,
		Used:    used,
		Free:    free,
		Percent: percent,
	}, nil
}

// diskIO reads aggregate disk IO counters from /proc/diskstats, mirroring
// Python _get_disk_io_stats. Returns (0,0,0,0) on diskless hosts.
func (s *systemSource) diskIO() (DiskIOMetrics, error) {
	f, err := os.Open("/proc/diskstats")
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
// at most the most recent samples (mirroring Python's bounded hist).
func (s *systemSource) recordDiskIO(m DiskIOMetrics) {
	s.diskIOHist = append(s.diskIOHist, ioSample{at: float64(time.Now().Unix()), val: m})
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
// at most the most recent samples.
func (s *systemSource) recordNetwork(m IOMetrics) {
	s.netHist = append(s.netHist, netSample{at: float64(time.Now().Unix()), val: m})
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

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}

// bootTime reads the boot timestamp from /proc/stat, mirroring
// Python psutil.boot_time(). The value is cached on first use because btime is
// constant for the process lifetime.
func (s *systemSource) bootTime() (float64, error) {
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

// workers returns the current worker process list. Full worker discovery
// requires raylet gRPC; this task reserves the field with an empty result,
// to be wired up when the raylet client is integrated.
func (s *systemSource) workers() ([]interface{}, error) {
	return []interface{}{}, nil
}

// raylet returns process info for the raylet process by scanning /proc,
// mirroring Python _get_raylet.
func (s *systemSource) raylet() (map[string]interface{}, error) {
	pid := s.findProcessByCmdline("raylet")
	if pid <= 0 {
		return nil, nil
	}
	return s.procAttrs(pid), nil
}

// gcs returns process info for the GCS server on head nodes, mirroring
// Python _get_gcs.
func (s *systemSource) gcs() (map[string]interface{}, error) {
	pid := s.findProcessByCmdline("gcs_server")
	if pid <= 0 {
		return map[string]interface{}{}, nil
	}
	return s.procAttrs(pid), nil
}

// agent returns process info for the current process, mirroring Python
// _get_agent.
func (s *systemSource) agent() (map[string]interface{}, error) {
	return s.procAttrs(os.Getpid()), nil
}

// additional returns an empty additional-process map. Matching additional Ray
// processes requires matcher definitions; this task reserves the field.
func (s *systemSource) additional() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// findProcessByCmdline scans /proc for a process whose cmdline contains the
// given substring, returning its pid or 0. The full /proc walk is cached per
// systemSource instance so repeated lookups within a collect cycle (raylet and
// gcs) reuse a single scan instead of re-reading every process cmdline.
func (s *systemSource) findProcessByCmdline(substr string) int {
	if s.procCache == nil {
		s.procCache = scanProcessCmdlines()
	}
	return s.procCache[substr]
}

// scanProcessCmdlines walks /proc once and returns a map of cmdline-substring
// to pid for the first process matching each substring.
func scanProcessCmdlines() map[string]int {
	matches := map[string]int{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return matches
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		cmd, err := readProcFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		cmdline := strings.ReplaceAll(cmd, "\x00", " ")
		for _, substr := range []string{"raylet", "gcs_server"} {
			if _, seen := matches[substr]; seen {
				continue
			}
			if strings.Contains(cmdline, substr) {
				matches[substr] = pid
			}
		}
	}
	return matches
}

// procAttrs returns basic process info for a pid, or an empty map if the
// process cannot be inspected. The pid is always present even on failure so
// callers can identify the intended target. The returned map uses snake_case
// keys (pid, create_time, cpu_percent, cmdline, name) matching the Python
// PSUTIL_PROCESS_ATTRS keys; toGoogleStyle converts them to the camelCase
// ProcessInfo fields expected by the Python StatsPayload model.
func (s *systemSource) procAttrs(pid int) map[string]interface{} {
	info := map[string]interface{}{
		"pid":         pid,
		"create_time": s.processCreateTime(pid),
		"cpu_percent": 0.0,
	}
	cmd, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err == nil {
		info["cmdline"] = splitCmdline(cmd)
	}
	name, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "comm"))
	if err == nil {
		info["name"] = name
	}
	return info
}

// processCreateTime returns the epoch-second start time of a process by reading
// its starttime (field 22) from /proc/<pid>/stat and adding the boot time,
// mirroring psutil's create_time. HZ (USER_HZ) is 100 clock ticks per second.
// It returns 0 if the process or its starttime cannot be read.
func (s *systemSource) processCreateTime(pid int) float64 {
	raw, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0
	}
	// The comm field (enclosed in parentheses) may contain spaces, so locate
	// the last ')' and parse the numeric fields that follow.
	rp := strings.LastIndex(raw, ")")
	if rp < 0 || rp+1 >= len(raw) {
		return 0
	}
	fields := strings.Fields(raw[rp+1:])
	// fields[0] is field 3 (state); starttime is field 22 overall, which is
	// index 19 within the post-comm fields (22 - 3).
	if len(fields) < 20 {
		return 0
	}
	startTicks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0
	}
	boot, err := s.bootTime()
	if err != nil {
		return 0
	}
	return boot + float64(startTicks)/100.0
}

func splitCmdline(raw string) []string {
	if raw == "" {
		return []string{}
	}
	return strings.Split(raw, "\x00")
}
