package reporter

import (
	"reflect"
	"sort"
	"testing"
)

// pythonGaugeTable is the authoritative transcription of the Python
// METRICS_GAUGES mapping (reporter_agent.py): name, description ("help") and
// tag key list, in declaration order. It is the reference the Go metricsGauges
// table is checked against, so a missing, renamed, reordered or
// differently-labelled gauge fails the test instead of silently dropping a
// metric from the Prometheus endpoint.
type pythonGauge struct {
	Name       string
	Help       string
	LabelNames []string
}

func pythonGaugeTable() []pythonGauge {
	return []pythonGauge{
		{"node_cpu_utilization", "Total CPU usage on a ray node", nodeTagKeys},
		{"node_cpu_count", "Total CPUs available on a ray node", nodeTagKeys},
		{"node_mem_used", "Memory usage on a ray node", nodeTagKeys},
		{"node_mem_available", "Memory available on a ray node", nodeTagKeys},
		{"node_mem_total", "Total memory on a ray node", nodeTagKeys},
		{"node_mem_shared_bytes", "Total shared memory usage on a ray node", nodeTagKeys},
		{"node_mem_used_host", "Host memory usage on a ray node", nodeTagKeys},
		{"node_mem_total_host", "Total host memory on a ray node", nodeTagKeys},
		{"node_swap_used", "Swap usage on a ray node (cgroup-aware when in a container)", nodeTagKeys},
		{"node_swap_total", "Total swap on a ray node (cgroup-aware when in a container)", nodeTagKeys},
		{"node_swap_utilization", "Swap utilization on a ray node", nodeTagKeys},
		{"node_cgroup_mem_used", "Container memory usage on a ray node", nodeTagKeys},
		{"node_cgroup_mem_total", "Container memory limit on a ray node", nodeTagKeys},
		{"node_gpus_available", "Total GPUs available on a ray node", gpuTagKeys},
		{"node_gpus_utilization", "Total GPUs usage on a ray node", gpuTagKeys},
		{"node_gram_used", "Total GPU RAM usage on a ray node", gpuTagKeys},
		{"node_gram_available", "Total GPU RAM available on a ray node", gpuTagKeys},
		{"node_gpu_power_milliwatts", "Current GPU power draw in milliwatts", gpuTagKeys},
		{"node_gpu_temperature_celsius", "Current GPU temperature in Celsius", gpuTagKeys},
		{"tpu_tensorcore_utilization", "Percentage TPU tensorcore utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
		{"tpu_memory_bandwidth_utilization", "Percentage TPU memory bandwidth utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
		{"tpu_duty_cycle", "Percentage of time during which the TPU was actively processing, value should be between 0 and 100", tpuTagKeys},
		{"tpu_memory_used", "Total memory used by the accelerator in bytes", tpuTagKeys},
		{"tpu_memory_total", "Total memory allocatable by the accelerator in bytes", tpuTagKeys},
		{"node_disk_io_read", "Total read from disk", nodeTagKeys},
		{"node_disk_io_write", "Total written to disk", nodeTagKeys},
		{"node_disk_io_read_count", "Total read ops from disk", nodeTagKeys},
		{"node_disk_io_write_count", "Total write ops to disk", nodeTagKeys},
		{"node_disk_io_read_speed", "Disk read speed", nodeTagKeys},
		{"node_disk_io_write_speed", "Disk write speed", nodeTagKeys},
		{"node_disk_read_iops", "Disk read iops", nodeTagKeys},
		{"node_disk_write_iops", "Disk write iops", nodeTagKeys},
		{"node_disk_usage", "Total disk usage (bytes) on a ray node", nodeTagKeys},
		{"node_disk_free", "Total disk free (bytes) on a ray node", nodeTagKeys},
		{"node_disk_utilization_percentage", "Total disk utilization (percentage) on a ray node", nodeTagKeys},
		{"node_network_sent", "Total network sent", nodeTagKeys},
		{"node_network_received", "Total network received", nodeTagKeys},
		{"node_network_send_speed", "Network send speed", nodeTagKeys},
		{"node_network_receive_speed", "Network receive speed", nodeTagKeys},
		{"component_cpu_percentage", "Total CPU usage of the components on a node.", componentTagKeys},
		{"component_shared_bytes", "SHM usage of all components of the node. It is equivalent to the top command's SHR column.", componentTagKeys},
		{"component_rss_mb", "RSS usage of all components on the node.", componentTagKeys},
		{"component_rss_bytes", "RSS usage of all components on the node.", componentTagKeys},
		{"component_uss_mb", "USS usage of all components on the node.", componentTagKeys},
		{"component_uss_bytes", "USS usage of all components on the node.", componentTagKeys},
		{"component_num_fds", "Number of open fds of all components on the node (Not available on Windows).", componentTagKeys},
		{"cluster_active_nodes", "Active nodes on the cluster", clusterTagKeys},
		{"cluster_idle_nodes", "Idle nodes on the cluster", clusterTagKeys},
		{"cluster_failed_nodes", "Failed nodes on the cluster", clusterTagKeys},
		{"cluster_pending_nodes", "Pending nodes on the cluster", clusterTagKeys},
		{"component_gpu_percentage", "GPU usage of all components on the node.", componentGPUTagKeys},
		{"component_gpu_memory_mb", "GPU memory usage of all components on the node.", componentGPUTagKeys},
	}
}

// TestMetricsGaugesMatchPythonTable asserts the Go gauge table is identical to
// pythonGaugeTable, a snapshot transcribed from the Python METRICS_GAUGES
// mapping: same count, same order, and for every entry the same name, help text
// and label keys.
//
// The snapshot is hand-maintained, so it catches the Go table drifting away from
// it but not the Python table moving away from both. Re-transcribe it whenever
// the Python mapping changes; the count is asserted separately below so a
// dropped entry cannot pass unnoticed.
func TestMetricsGaugesMatchPythonTable(t *testing.T) {
	want := pythonGaugeTable()
	if len(metricsGauges) != len(want) {
		t.Fatalf("metricsGauges has %d entries, want %d", len(metricsGauges), len(want))
	}
	for i, w := range want {
		got := metricsGauges[i]
		if got.Name != w.Name {
			t.Errorf("entry %d name = %q, want %q", i, got.Name, w.Name)
		}
		if got.Help != w.Help {
			t.Errorf("%s help = %q, want %q", w.Name, got.Help, w.Help)
		}
		if !reflect.DeepEqual(got.LabelNames, w.LabelNames) {
			t.Errorf("%s labels = %v, want %v", w.Name, got.LabelNames, w.LabelNames)
		}
	}
}

// TestMetricsGaugesHaveUniqueNames verifies no gauge name is declared twice,
// which Prometheus would otherwise reject at registration time and would
// silently disable the whole metrics endpoint.
func TestMetricsGaugesHaveUniqueNames(t *testing.T) {
	seen := map[string]int{}
	for _, g := range metricsGauges {
		seen[g.Name]++
	}
	dups := make([]string, 0)
	for name, n := range seen {
		if n > 1 {
			dups = append(dups, name)
		}
	}
	sort.Strings(dups)
	if len(dups) != 0 {
		t.Errorf("duplicate gauge names declared: %v", dups)
	}
}

// TestMetricsGaugesLabelsAreUnique verifies every gauge declares unique label
// names. Prometheus panics on a GaugeVec with duplicate label names, so a
// duplicated key (such as the repeated Version/SessionName of the Python
// COMPONENT_GPU_TAG_KEYS concatenation) would crash the exporter at runtime.
func TestMetricsGaugesLabelsAreUnique(t *testing.T) {
	for _, g := range metricsGauges {
		seen := map[string]bool{}
		for _, l := range g.LabelNames {
			if seen[l] {
				t.Errorf("%s declares duplicate label %q (labels: %v)", g.Name, l, g.LabelNames)
			}
			seen[l] = true
		}
	}
}

// TestMetricsGaugesDeclareExpectedLabels verifies the tag key sets themselves
// match Python's consts.py: node tags carry the head/worker markers, GPU tags
// add the device name and index, TPU tags the device/type/topology, cluster
// tags the node type, and component tags the pid and component name.
func TestMetricsGaugesDeclareExpectedLabels(t *testing.T) {
	cases := []struct {
		gauge  string
		labels []string
	}{
		{GaugeNodeCPUUtilization, []string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType"}},
		{GaugeNodeGPUsUtilization, []string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType", "GpuDeviceName", "GpuIndex", "GpuUuid"}},
		{GaugeTpuDutyCycle, []string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType", "TpuDeviceName", "TpuIndex", "TpuType", "TpuTopology"}},
		{GaugeClusterActiveNodes, []string{"node_type", "Version", "SessionName"}},
		{ComponentGaugeRSSMB, []string{"ip", "pid", "Version", "Component", "SessionName"}},
		{ComponentGaugeGPUMemoryMB, []string{"ip", "pid", "Version", "Component", "SessionName"}},
	}
	for _, c := range cases {
		got := gaugeOf(c.gauge)
		if !reflect.DeepEqual(got.LabelNames, c.labels) {
			t.Errorf("%s labels = %v, want %v", c.gauge, got.LabelNames, c.labels)
		}
	}
}

// TestGaugeOfReturnsDeclaredDefinition verifies the name lookup used by the
// record generator resolves every declared gauge, so no record can silently
// lose its help text or labels.
func TestGaugeOfReturnsDeclaredDefinition(t *testing.T) {
	for _, g := range metricsGauges {
		got := gaugeOf(g.Name)
		if got.Name != g.Name || got.Help != g.Help {
			t.Errorf("gaugeOf(%q) = %+v, want %+v", g.Name, got, g)
		}
	}
}

// TestGaugeOfUnknownNameDegrades verifies an undeclared name degrades to a
// label-less definition instead of panicking, so a typo cannot crash the
// collection loop.
func TestGaugeOfUnknownNameDegrades(t *testing.T) {
	got := gaugeOf("not_a_declared_gauge")
	if got.Name != "not_a_declared_gauge" {
		t.Errorf("gaugeOf(unknown).Name = %q, want the input name", got.Name)
	}
	if len(got.LabelNames) != 0 || got.Help != "" {
		t.Errorf("gaugeOf(unknown) = %+v, want a zero-value definition", got)
	}
}
