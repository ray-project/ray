package reporter

// Prometheus label key sets, aligned with python/ray/dashboard/consts.py
// (NODE_TAG_KEYS / GPU_TAG_KEYS / TPU_TAG_KEYS / CLUSTER_TAG_KEYS /
// COMPONENT_METRICS_TAG_KEYS / COMPONENT_GPU_TAG_KEYS). They are the label
// names a gauge declares, in declaration order.
var (
	nodeTagKeys      = []string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType"}
	gpuTagKeys       = append(append([]string{}, nodeTagKeys...), "GpuDeviceName", "GpuIndex", "GpuUuid")
	tpuTagKeys       = append(append([]string{}, nodeTagKeys...), "TpuDeviceName", "TpuIndex", "TpuType", "TpuTopology")
	clusterTagKeys   = []string{"node_type", "Version", "SessionName"}
	componentTagKeys = []string{"ip", "pid", "Version", "Component", "SessionName"}
	// componentGPUTagKeys labels the two component GPU gauges. Python declares
	// COMPONENT_GPU_TAG_KEYS = GPU_TAG_KEYS + COMPONENT_METRICS_TAG_KEYS for
	// them, but that concatenation repeats "Version" and "SessionName", which a
	// Prometheus label set cannot represent (label names must be unique), and
	// the reporter never attaches the node/GPU tags to component records: its
	// _generate_system_stats_record tags are {ip, Component[, pid]} merged with
	// the global {Version, SessionName}. Declaring COMPONENT_METRICS_TAG_KEYS
	// therefore describes exactly the label set Python emits for these gauges
	// instead of adding permanently empty node/GPU labels.
	componentGPUTagKeys = componentTagKeys
)

// gaugeDef describes one Prometheus gauge, aligned with a Python Gauge entry of
// METRICS_GAUGES (reporter_agent.py:124-381). Help is the Python description.
type gaugeDef struct {
	Name       string
	Help       string
	LabelNames []string
}

// Gauge names of the 21 node-level metrics, aligned with the METRICS_GAUGES
// keys in reporter_agent.py.
const (
	GaugeNodeCPUUtilization      = "node_cpu_utilization"
	GaugeNodeCPUCount            = "node_cpu_count"
	GaugeNodeMemUsed             = "node_mem_used"
	GaugeNodeMemAvailable        = "node_mem_available"
	GaugeNodeMemTotal            = "node_mem_total"
	GaugeNodeMemSharedBytes      = "node_mem_shared_bytes"
	GaugeNodeMemUsedHost         = "node_mem_used_host"
	GaugeNodeMemTotalHost        = "node_mem_total_host"
	GaugeNodeSwapUsed            = "node_swap_used"
	GaugeNodeSwapTotal           = "node_swap_total"
	GaugeNodeSwapUtilization     = "node_swap_utilization"
	GaugeNodeCgroupMemUsed       = "node_cgroup_mem_used"
	GaugeNodeCgroupMemTotal      = "node_cgroup_mem_total"
	GaugeNodeDiskIORead          = "node_disk_io_read"
	GaugeNodeDiskIOWrite         = "node_disk_io_write"
	GaugeNodeDiskIOReadCount     = "node_disk_io_read_count"
	GaugeNodeDiskIOWriteCount    = "node_disk_io_write_count"
	GaugeNodeDiskIOReadSpeed     = "node_disk_io_read_speed"
	GaugeNodeDiskIOWriteSpeed    = "node_disk_io_write_speed"
	GaugeNodeDiskReadIOPS        = "node_disk_read_iops"
	GaugeNodeDiskWriteIOPS       = "node_disk_write_iops"
	GaugeNodeDiskUsage           = "node_disk_usage"
	GaugeNodeDiskFree            = "node_disk_free"
	GaugeNodeDiskUtilizationPct  = "node_disk_utilization_percentage"
	GaugeNodeNetworkSent         = "node_network_sent"
	GaugeNodeNetworkReceived     = "node_network_received"
	GaugeNodeNetworkSendSpeed    = "node_network_send_speed"
	GaugeNodeNetworkReceiveSpeed = "node_network_receive_speed"
)

// Gauge names of the 4 GPU-level and 5 TPU-level metrics.
const (
	GaugeNodeGPUsAvailable   = "node_gpus_available"
	GaugeNodeGPUsUtilization = "node_gpus_utilization"
	GaugeNodeGRAMUsed        = "node_gram_used"
	GaugeNodeGRAMAvaliable   = "node_gram_available"
	GaugeNodeGPUPower        = "node_gpu_power_milliwatts"
	GaugeNodeGPUTemperature  = "node_gpu_temperature_celsius"

	GaugeTpuTensorcoreUtilization = "tpu_tensorcore_utilization"
	GaugeTpuMemoryBandwidth       = "tpu_memory_bandwidth_utilization"
	GaugeTpuDutyCycle             = "tpu_duty_cycle"
	GaugeTpuMemoryUsed            = "tpu_memory_used"
	GaugeTpuMemoryTotal           = "tpu_memory_total"
)

// Gauge names of the 3 cluster-level metrics.
const (
	GaugeClusterActiveNodes  = "cluster_active_nodes"
	GaugeClusterIdleNodes    = "cluster_idle_nodes"
	GaugeClusterFailedNodes  = "cluster_failed_nodes"
	GaugeClusterPendingNodes = "cluster_pending_nodes"
)

// metricsGauges is the full list of gauges recorded and exported, aligned
// one-to-one with the Python METRICS_GAUGES mapping (name, description, label
// keys). Keeping the description text identical to Python means the exported
// Prometheus HELP lines match too.
var metricsGauges = []gaugeDef{
	{GaugeNodeCPUUtilization, "Total CPU usage on a ray node", nodeTagKeys},
	{GaugeNodeCPUCount, "Total CPUs available on a ray node", nodeTagKeys},
	{GaugeNodeMemUsed, "Memory usage on a ray node", nodeTagKeys},
	{GaugeNodeMemAvailable, "Memory available on a ray node", nodeTagKeys},
	{GaugeNodeMemTotal, "Total memory on a ray node", nodeTagKeys},
	{GaugeNodeMemSharedBytes, "Total shared memory usage on a ray node", nodeTagKeys},
	{GaugeNodeMemUsedHost, "Host memory usage on a ray node", nodeTagKeys},
	{GaugeNodeMemTotalHost, "Total host memory on a ray node", nodeTagKeys},
	{GaugeNodeSwapUsed, "Swap usage on a ray node (cgroup-aware when in a container)", nodeTagKeys},
	{GaugeNodeSwapTotal, "Total swap on a ray node (cgroup-aware when in a container)", nodeTagKeys},
	{GaugeNodeSwapUtilization, "Swap utilization on a ray node", nodeTagKeys},
	{GaugeNodeCgroupMemUsed, "Container memory usage on a ray node", nodeTagKeys},
	{GaugeNodeCgroupMemTotal, "Container memory limit on a ray node", nodeTagKeys},
	{GaugeNodeGPUsAvailable, "Total GPUs available on a ray node", gpuTagKeys},
	{GaugeNodeGPUsUtilization, "Total GPUs usage on a ray node", gpuTagKeys},
	{GaugeNodeGRAMUsed, "Total GPU RAM usage on a ray node", gpuTagKeys},
	{GaugeNodeGRAMAvaliable, "Total GPU RAM available on a ray node", gpuTagKeys},
	{GaugeNodeGPUPower, "Current GPU power draw in milliwatts", gpuTagKeys},
	{GaugeNodeGPUTemperature, "Current GPU temperature in Celsius", gpuTagKeys},
	{GaugeTpuTensorcoreUtilization, "Percentage TPU tensorcore utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuMemoryBandwidth, "Percentage TPU memory bandwidth utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuDutyCycle, "Percentage of time during which the TPU was actively processing, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuMemoryUsed, "Total memory used by the accelerator in bytes", tpuTagKeys},
	{GaugeTpuMemoryTotal, "Total memory allocatable by the accelerator in bytes", tpuTagKeys},
	{GaugeNodeDiskIORead, "Total read from disk", nodeTagKeys},
	{GaugeNodeDiskIOWrite, "Total written to disk", nodeTagKeys},
	{GaugeNodeDiskIOReadCount, "Total read ops from disk", nodeTagKeys},
	{GaugeNodeDiskIOWriteCount, "Total write ops to disk", nodeTagKeys},
	{GaugeNodeDiskIOReadSpeed, "Disk read speed", nodeTagKeys},
	{GaugeNodeDiskIOWriteSpeed, "Disk write speed", nodeTagKeys},
	{GaugeNodeDiskReadIOPS, "Disk read iops", nodeTagKeys},
	{GaugeNodeDiskWriteIOPS, "Disk write iops", nodeTagKeys},
	{GaugeNodeDiskUsage, "Total disk usage (bytes) on a ray node", nodeTagKeys},
	{GaugeNodeDiskFree, "Total disk free (bytes) on a ray node", nodeTagKeys},
	{GaugeNodeDiskUtilizationPct, "Total disk utilization (percentage) on a ray node", nodeTagKeys},
	{GaugeNodeNetworkSent, "Total network sent", nodeTagKeys},
	{GaugeNodeNetworkReceived, "Total network received", nodeTagKeys},
	{GaugeNodeNetworkSendSpeed, "Network send speed", nodeTagKeys},
	{GaugeNodeNetworkReceiveSpeed, "Network receive speed", nodeTagKeys},
	{"component_cpu_percentage", "Total CPU usage of the components on a node.", componentTagKeys},
	{"component_shared_bytes", "SHM usage of all components of the node. It is equivalent to the top command's SHR column.", componentTagKeys},
	{"component_rss_mb", "RSS usage of all components on the node.", componentTagKeys},
	{"component_rss_bytes", "RSS usage of all components on the node.", componentTagKeys},
	{"component_uss_mb", "USS usage of all components on the node.", componentTagKeys},
	{"component_uss_bytes", "USS usage of all components on the node.", componentTagKeys},
	{"component_num_fds", "Number of open fds of all components on the node (Not available on Windows).", componentTagKeys},
	{GaugeClusterActiveNodes, "Active nodes on the cluster", clusterTagKeys},
	{GaugeClusterIdleNodes, "Idle nodes on the cluster", clusterTagKeys},
	{GaugeClusterFailedNodes, "Failed nodes on the cluster", clusterTagKeys},
	{GaugeClusterPendingNodes, "Pending nodes on the cluster", clusterTagKeys},
	{"component_gpu_percentage", "GPU usage of all components on the node.", componentGPUTagKeys},
	{"component_gpu_memory_mb", "GPU memory usage of all components on the node.", componentGPUTagKeys},
}
