package reporter

import (
	"sort"
	"strconv"
)

// gaugeRecord is one gauge value with its tag set, aligned with the Python
// metrics_agent Record namedtuple (gauge, value, tags). Tags are raw key
// values; they are projected onto the gauge's declared label keys when the
// record is written to the Prometheus registry.
type gaugeRecord struct {
	Gauge gaugeDef
	Value float64
	Tags  map[string]string
}

// recordsState tracks the component names seen in the previous collection
// cycle so metrics of components that disappeared can be reset to zero,
// mirroring the Python ReporterAgent _latest_worker_proc_names /
// _latest_gpu_worker_proc_names / _latest_additional_component_names sets. The
// zero value is usable; names seen for the first time simply have no stale
// predecessors to reset.
type recordsState struct {
	latestWorkerProcNames          map[string]struct{}
	latestGPUWorkerProcNames       map[string]struct{}
	latestAdditionalComponentNames map[string]struct{}
}

// gcsServerComponent is the component name used for the GCS server process,
// aligned with the Python _to_records call for stats["gcs"].
const gcsServerComponent = "gcs"

// Component names emitted for the node's own processes, aligned with the
// Python _to_records component section.
const (
	componentRaylet = "raylet"
	componentAgent  = "agent"
)

// nodeTags builds the per-node tags attached to every node-level record,
// aligned with the Python _to_records node_tags. RayNodeType marks head/worker
// and IsHeadNode is retained for backwards compatibility with user dashboards.
func nodeTags(ip string, isHead bool) map[string]string {
	rayNodeType := "worker"
	isHeadNode := "false"
	if isHead {
		rayNodeType = "head"
		isHeadNode = "true"
	}
	return map[string]string{
		"ip":          ip,
		"RayNodeType": rayNodeType,
		"IsHeadNode":  isHeadNode,
	}
}

// toRecords converts a collected stats snapshot into Prometheus gauge records,
// mirroring the Python ReporterAgent._to_records. The sections are emitted in
// the same order as Python: cluster autoscaler metrics (head node only), the
// per-device GPU and TPU metrics, the per-component resource metrics with their
// stale-component resets, and finally the node-level metrics. The global tags
// (Version, SessionName) are merged into every record, matching the Python
// record_and_export global_tags.
func (s *recordsState) toRecords(stats, clusterStats map[string]interface{}, isHead bool, globalTags map[string]string) []gaugeRecord {
	ip := asString(stats["ip"])
	tags := nodeTags(ip, isHead)

	records := make([]gaugeRecord, 0, 64)
	records = append(records, clusterRecords(clusterStats, isHead)...)
	records = append(records, gpuRecords(stats, tags)...)
	records = append(records, tpuRecords(stats, tags)...)
	if isHead {
		records = append(records, componentProcessRecords(stats, "gcs", gcsServerComponent, ip)...)
	}
	records = append(records, componentProcessRecords(stats, "raylet", componentRaylet, ip)...)
	records = append(records, s.workerRecords(stats, ip)...)
	records = append(records, componentProcessRecords(stats, "agent", componentAgent, ip)...)
	records = append(records, s.additionalRecords(stats, ip)...)
	records = append(records, nodeRecords(stats, tags)...)

	for i := range records {
		records[i].Tags = mergeTags(records[i].Tags, globalTags)
	}
	return records
}

// clusterRecords builds the cluster autoscaler gauges, mirroring the Python
// _to_records autoscaler_report block: active nodes per node type, failed nodes
// summed per node type, and pending nodes summed per node type. Cluster stats
// are published by the head node and only once the autoscaler has stored an
// autoscaler_report.
func clusterRecords(clusterStats map[string]interface{}, isHead bool) []gaugeRecord {
	if !isHead {
		return nil
	}
	report, ok := clusterStats["autoscaler_report"].(map[string]interface{})
	if !ok {
		return nil
	}

	records := make([]gaugeRecord, 0, 3)

	active := countsByNodeType(report["active_nodes"])
	for _, nodeType := range sortedKeys(active) {
		records = append(records, gaugeRecord{
			Gauge: gaugeOf(GaugeClusterActiveNodes),
			Value: float64(active[nodeType]),
			Tags:  map[string]string{"node_type": nodeType},
		})
	}

	failed := countsOfPairs(report["failed_nodes"])
	for _, nodeType := range sortedKeys(failed) {
		records = append(records, gaugeRecord{
			Gauge: gaugeOf(GaugeClusterFailedNodes),
			Value: float64(failed[nodeType]),
			Tags:  map[string]string{"node_type": nodeType},
		})
	}

	pending := countsOfPairs(report["pending_nodes"])
	for _, nodeType := range sortedKeys(pending) {
		records = append(records, gaugeRecord{
			Gauge: gaugeOf(GaugeClusterPendingNodes),
			Value: float64(pending[nodeType]),
			Tags:  map[string]string{"node_type": nodeType},
		})
	}
	return records
}

// countsByNodeType sums a node-type -> count mapping as published by the
// autoscaler (autoscaler_report.active_nodes).
func countsByNodeType(v interface{}) map[string]int {
	out := map[string]int{}
	if m, ok := v.(map[string]interface{}); ok {
		for nodeType, count := range m {
			out[nodeType] = asInt(count)
		}
	}
	return out
}

// countsOfPairs counts node types in a list of tuples whose second element is
// the node type. autoscaler_report.failed_nodes entries are [ip, node_type]
// and pending_nodes entries are [ip, node_type, status_message].
func countsOfPairs(v interface{}) map[string]int {
	out := map[string]int{}
	list, ok := v.([]interface{})
	if !ok {
		return out
	}
	for _, e := range list {
		pair, ok := e.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		out[asString(pair[1])]++
	}
	return out
}

// sortedKeys returns the sorted keys of a count map so the emitted records are
// deterministic across runs.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// nodeRecords builds the node-level records (CPU, memory, disk IO, disk usage
// and network) in the same order as the Python _to_records list.
func nodeRecords(stats map[string]interface{}, tags map[string]string) []gaugeRecord {
	records := make([]gaugeRecord, 0, 28)

	add := func(name string, value float64) {
		records = append(records, gaugeRecord{Gauge: gaugeOf(name), Value: value, Tags: tags})
	}

	cpuCount := 0
	if cpus, ok := stats["cpus"].([]int); ok && len(cpus) > 0 {
		cpuCount = cpus[0]
	}

	add(GaugeNodeCPUUtilization, asFloat(stats["cpu"]))
	add(GaugeNodeCPUCount, float64(cpuCount))

	if mem, ok := stats["mem"].([]interface{}); ok && len(mem) == 4 {
		add(GaugeNodeMemUsed, asFloat(mem[3]))
		add(GaugeNodeMemAvailable, asFloat(mem[1]))
		add(GaugeNodeMemTotal, asFloat(mem[0]))
	}
	if shm := asFloat(stats["shm"]); shm != 0 {
		add(GaugeNodeMemSharedBytes, shm)
	}
	if hostMem, ok := stats["host_mem"].([]interface{}); ok && len(hostMem) == 2 {
		add(GaugeNodeMemUsedHost, asFloat(hostMem[0]))
		add(GaugeNodeMemTotalHost, asFloat(hostMem[1]))
	}
	// Swap is only reported when the flag asks for it and the node has swap;
	// a constant zero series on every node is noise on the dashboards.
	if swap, ok := stats["swap"].([]interface{}); ok && len(swap) == 3 {
		add(GaugeNodeSwapTotal, asFloat(swap[0]))
		add(GaugeNodeSwapUsed, asFloat(swap[1]))
		add(GaugeNodeSwapUtilization, asFloat(swap[2]))
	}
	// The container figures only exist where a cgroup memory controller does.
	if cgroup, ok := stats["cgroup_mem"].([]interface{}); ok && len(cgroup) == 2 {
		add(GaugeNodeCgroupMemUsed, asFloat(cgroup[0]))
		add(GaugeNodeCgroupMemTotal, asFloat(cgroup[1]))
	}

	diskIO, _ := stats["disk_io"].(DiskIOMetrics)
	add(GaugeNodeDiskIORead, float64(diskIO.ReadBytes))
	add(GaugeNodeDiskIOWrite, float64(diskIO.WriteBytes))
	add(GaugeNodeDiskIOReadCount, float64(diskIO.ReadCount))
	add(GaugeNodeDiskIOWriteCount, float64(diskIO.WriteCount))

	speed := asFloatSlice(stats["disk_io_speed"], 4)
	add(GaugeNodeDiskIOReadSpeed, speed[0])
	add(GaugeNodeDiskIOWriteSpeed, speed[1])
	add(GaugeNodeDiskReadIOPS, speed[2])
	add(GaugeNodeDiskWriteIOPS, speed[3])

	if disk, ok := stats["disk"].(map[string]DiskInfo); ok {
		root := disk["/"]
		used := float64(root.Used)
		free := float64(root.Free)
		add(GaugeNodeDiskUsage, used)
		add(GaugeNodeDiskFree, free)
		// Python computes used / (used + free), so the dummy K8s entry
		// (total=1, free=1, used=0) yields 0 instead of dividing by zero.
		utilization := 0.0
		if used+free > 0 {
			utilization = used / (used + free) * 100
		}
		add(GaugeNodeDiskUtilizationPct, utilization)
	}

	net, _ := stats["network"].(IOMetrics)
	add(GaugeNodeNetworkSent, float64(net.Sent))
	add(GaugeNodeNetworkReceived, float64(net.Recv))

	netSpeed := asFloatSlice(stats["network_speed"], 2)
	add(GaugeNodeNetworkSendSpeed, netSpeed[0])
	add(GaugeNodeNetworkReceiveSpeed, netSpeed[1])

	return records
}

// gpuRecords builds the per-device GPU records, mirroring the Python
// _to_records GPU section: availability 1 per device, utilization, used RAM and
// available RAM (total - used), each tagged with the device index and name.
func gpuRecords(stats map[string]interface{}, tags map[string]string) []gaugeRecord {
	gpus, _ := stats["gpus"].([]GpuUtilizationInfo)
	if len(gpus) == 0 {
		return nil
	}

	records := make([]gaugeRecord, 0, len(gpus)*4)
	for _, gpu := range gpus {
		gpuTags := map[string]string{"GpuIndex": strconv.Itoa(gpu.Index)}
		if gpu.Name != "" {
			gpuTags["GpuDeviceName"] = gpu.Name
		}
		if gpu.UUID != "" {
			gpuTags["GpuUuid"] = gpu.UUID
		}
		dev := mergeTags(tags, gpuTags)

		// There is only one GPU per index, so availability is recorded as 1.
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGPUsAvailable), Value: 1, Tags: dev})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGPUsUtilization), Value: float64(gpu.UtilizationGPU), Tags: dev})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGRAMUsed), Value: float64(gpu.MemoryUsed), Tags: dev})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGRAMAvaliable), Value: float64(gpu.MemoryTotal - gpu.MemoryUsed), Tags: dev})
		// Power and temperature are only reported by devices that expose them,
		// mirroring the Python provider's optional None: a series of zeros for
		// every unsupported card would be worse than no series.
		if gpu.HasPower {
			records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGPUPower), Value: float64(gpu.PowerMW), Tags: dev})
		}
		if gpu.HasTemperature {
			records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeNodeGPUTemperature), Value: float64(gpu.TemperatureC), Tags: dev})
		}
	}
	return records
}

// tpuRecords builds the per-device TPU records, mirroring the Python
// _to_records TPU section.
func tpuRecords(stats map[string]interface{}, tags map[string]string) []gaugeRecord {
	tpus, _ := stats["tpus"].([]TpuUtilizationInfo)
	if len(tpus) == 0 {
		return nil
	}

	records := make([]gaugeRecord, 0, len(tpus)*5)
	for _, tpu := range tpus {
		tpuTags := mergeTags(tags, map[string]string{
			"TpuIndex":      strconv.Itoa(tpu.Index),
			"TpuDeviceName": tpu.Name,
			"TpuType":       tpu.TPUType,
			"TpuTopology":   tpu.TPUTopology,
		})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeTpuTensorcoreUtilization), Value: tpu.TensorcoreUtilization, Tags: tpuTags})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeTpuMemoryBandwidth), Value: tpu.HBMUtilization, Tags: tpuTags})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeTpuDutyCycle), Value: tpu.DutyCycle, Tags: tpuTags})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeTpuMemoryUsed), Value: float64(tpu.MemoryUsed), Tags: tpuTags})
		records = append(records, gaugeRecord{Gauge: gaugeOf(GaugeTpuMemoryTotal), Value: float64(tpu.MemoryTotal), Tags: tpuTags})
	}
	return records
}

// componentProcessRecords builds the component records of one named process of
// the current node (gcs, raylet or agent), mirroring one
// _generate_system_stats_record call of the Python _to_records component
// section. An absent or empty process map yields no records.
func componentProcessRecords(stats map[string]interface{}, key, component, ip string) []gaugeRecord {
	procStats, ok := stats[key].(map[string]interface{})
	if !ok || len(procStats) == 0 {
		return nil
	}
	return systemStatsRecords([]map[string]interface{}{procStats}, component, ip, pidTag(procStats))
}

// pidTag renders the pid of a process stats map as the string the Python
// reporter uses for the component pid label. A missing pid yields the empty
// string, which systemStatsRecords treats as "no pid label".
func pidTag(stat map[string]interface{}) string {
	pid, ok := stat["pid"]
	if !ok {
		return ""
	}
	return strconv.Itoa(asInt(pid))
}

// workerRecords groups worker processes by their command name and emits one
// component record set per group, mirroring the Python
// generate_worker_stats_record. It also resets the metrics of worker names that
// disappeared since the previous cycle and of names that stopped using a GPU.
func (s *recordsState) workerRecords(stats map[string]interface{}, ip string) []gaugeRecord {
	workers := asMapSlice(stats["workers"])

	byName := map[string][]map[string]interface{}{}
	order := make([]string, 0, len(workers))
	gpuNames := map[string]struct{}{}
	for _, w := range workers {
		cmdline := asStringSlice(w["cmdline"])
		if len(cmdline) == 0 {
			continue
		}
		name := cmdline[0]
		if _, seen := byName[name]; !seen {
			order = append(order, name)
		}
		byName[name] = append(byName[name], w)
		if asFloat(w["gpu_memory_usage"]) > 0 || asFloat(w["gpu_utilization"]) > 0 {
			gpuNames[name] = struct{}{}
		}
	}

	var records []gaugeRecord
	for _, name := range order {
		records = append(records, systemStatsRecords(byName[name], name, ip, "")...)
	}

	current := nameSet(order)
	records = append(records, resetComponentRecords(difference(s.latestWorkerProcNames, current), ip)...)
	s.latestWorkerProcNames = current

	records = append(records, resetGPUComponentRecords(difference(s.latestGPUWorkerProcNames, gpuNames), ip)...)
	s.latestGPUWorkerProcNames = gpuNames

	return records
}

// additionalRecords emits the component records of the additional Ray processes
// (log_monitor, runtime_env_agent) and resets the metrics of the components
// that disappeared since the previous cycle, mirroring the Python _to_records
// additional block.
func (s *recordsState) additionalRecords(stats map[string]interface{}, ip string) []gaugeRecord {
	additional, _ := stats["additional"].(map[string]interface{})

	names := make([]string, 0, len(additional))
	for name := range additional {
		names = append(names, name)
	}
	sort.Strings(names)

	current := map[string]struct{}{}
	var records []gaugeRecord
	for _, name := range names {
		statsList := asMapSlice(additional[name])
		if len(statsList) == 0 {
			continue
		}
		current[name] = struct{}{}
		records = append(records, systemStatsRecords(statsList, name, ip, pidTag(statsList[0]))...)
	}

	records = append(records, resetComponentRecords(difference(s.latestAdditionalComponentNames, current), ip)...)
	s.latestAdditionalComponentNames = current
	return records
}

// systemStatsRecords aggregates the process stats of one component into its
// gauge records, mirroring the Python _generate_system_stats_record. RSS, USS
// and PSS are converted from bytes to MB (divided by 1e6) while the shared
// memory figure stays in bytes, matching the Python units exactly. The USS and
// PSS gauges are only emitted when their totals are positive, as are the two
// component GPU gauges.
//
// pid is an explicit parameter rather than read from the stats, matching the
// Python signature: the component processes (gcs, raylet, agent, additional)
// pass their pid while the worker grouping deliberately leaves it unset, so a
// worker record carries no pid label even though its stats do.
func systemStatsRecords(statsList []map[string]interface{}, component, ip, pid string) []gaugeRecord {
	var totalCPU, totalGPUPct, totalGPUMem, totalRSS, totalRSSBytes, totalUSS, totalUSSBytes, totalShm float64
	var totalNumFDs int

	for _, stat := range statsList {
		totalCPU += asFloat(stat["cpu_percent"])
		totalGPUPct += asFloat(stat["gpu_utilization"])
		totalGPUMem += asFloat(stat["gpu_memory_usage"])
		if mem, ok := stat["memory_info"].(map[string]interface{}); ok {
			totalRSSBytes += asFloat(mem["rss"])
			totalRSS = totalRSSBytes / 1.0e6
			totalShm += asFloat(mem["shared"])
		}
		if mem, ok := stat["memory_full_info"].(map[string]interface{}); ok {
			totalUSSBytes += asFloat(mem["uss"])
			totalUSS = totalUSSBytes / 1.0e6
		}
		totalNumFDs += asInt(stat["num_fds"])
	}

	tags := map[string]string{"ip": ip, "Component": component}
	if pid != "" {
		tags["pid"] = pid
	}

	records := []gaugeRecord{
		{Gauge: gaugeOf(ComponentGaugeCPUPercentage), Value: totalCPU, Tags: tags},
		{Gauge: gaugeOf(ComponentGaugeSharedBytes), Value: totalShm, Tags: tags},
		{Gauge: gaugeOf(ComponentGaugeRSSMB), Value: totalRSS, Tags: tags},
		{Gauge: gaugeOf(ComponentGaugeRSSBytes), Value: totalRSSBytes, Tags: tags},
	}
	if totalUSS > 0 {
		records = append(records, gaugeRecord{Gauge: gaugeOf(ComponentGaugeUSSMB), Value: totalUSS, Tags: tags})
		records = append(records, gaugeRecord{Gauge: gaugeOf(ComponentGaugeUSSBytes), Value: totalUSSBytes, Tags: tags})
	}
	records = append(records, gaugeRecord{Gauge: gaugeOf(ComponentGaugeNumFDs), Value: float64(totalNumFDs), Tags: tags})

	if totalGPUMem > 0 {
		records = append(records, gaugeRecord{Gauge: gaugeOf(ComponentGaugeGPUMemoryMB), Value: totalGPUMem, Tags: tags})
	}
	if totalGPUPct > 0 {
		records = append(records, gaugeRecord{Gauge: gaugeOf(ComponentGaugeGPUPercentage), Value: totalGPUPct, Tags: tags})
	}
	return records
}

// resetComponentRecords zeroes the six component gauges of the given component
// names, mirroring the Python _generate_reseted_stats_record.
func resetComponentRecords(names []string, ip string) []gaugeRecord {
	gauges := []string{
		ComponentGaugeCPUPercentage,
		ComponentGaugeSharedBytes,
		ComponentGaugeRSSMB,
		ComponentGaugeRSSBytes,
		ComponentGaugeUSSMB,
		ComponentGaugeUSSBytes,
		ComponentGaugeNumFDs,
	}
	var records []gaugeRecord
	for _, name := range names {
		tags := map[string]string{"ip": ip, "Component": name}
		for _, g := range gauges {
			records = append(records, gaugeRecord{Gauge: gaugeOf(g), Value: 0, Tags: tags})
		}
	}
	return records
}

// resetGPUComponentRecords zeroes the two component GPU gauges of the given
// component names, mirroring the Python _generate_reseted_gpu_stats_record.
func resetGPUComponentRecords(names []string, ip string) []gaugeRecord {
	gauges := []string{ComponentGaugeGPUMemoryMB, ComponentGaugeGPUPercentage}
	var records []gaugeRecord
	for _, name := range names {
		tags := map[string]string{"ip": ip, "Component": name}
		for _, g := range gauges {
			records = append(records, gaugeRecord{Gauge: gaugeOf(g), Value: 0, Tags: tags})
		}
	}
	return records
}

// gaugeOf returns the definition of a gauge by name. Names passed here are the
// constants of metrics_gauges.go, so the lookup always succeeds; an unknown
// name degrades to a label-less definition instead of panicking.
func gaugeOf(name string) gaugeDef {
	for _, g := range metricsGauges {
		if g.Name == name {
			return g
		}
	}
	return gaugeDef{Name: name}
}

// difference returns the names present in have but absent from notHave, sorted
// so the emitted reset records are deterministic.
func difference(have, notHave map[string]struct{}) []string {
	out := make([]string, 0, len(have))
	for name := range have {
		if _, ok := notHave[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// mergeTags returns the union of the given tag maps; a key defined by several
// inputs takes the value of the last map that defines it, matching the Python
// `{**tags, **global_tags}` merge order.
func mergeTags(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// nameSet converts a name slice to a set.
func nameSet(names []string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out
}

// asFloat coerces a stats value to float64, tolerating the concrete numeric
// types the collector and JSON decoding produce.
func asFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	}
	return 0
}

// asInt coerces a stats value to int.
func asInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	}
	return 0
}

// asString coerces a stats value to string.
func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// asFloatSlice returns the first n values of a []float64 stats field, padding
// with zeros when the field is missing or shorter than n.
func asFloatSlice(v interface{}, n int) []float64 {
	out := make([]float64, n)
	s, _ := v.([]float64)
	for i := 0; i < n && i < len(s); i++ {
		out[i] = s[i]
	}
	return out
}

// asStringSlice coerces a cmdline value to []string.
func asStringSlice(v interface{}) []string {
	if s, ok := v.([]string); ok {
		return s
	}
	return nil
}

// asMapSlice coerces a process-list value to []map[string]interface{},
// tolerating both the collector's []interface{} of maps and a JSON-decoded
// []interface{}.
func asMapSlice(v interface{}) []map[string]interface{} {
	switch t := v.(type) {
	case []map[string]interface{}:
		return t
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(t))
		for _, e := range t {
			if m, ok := e.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}
