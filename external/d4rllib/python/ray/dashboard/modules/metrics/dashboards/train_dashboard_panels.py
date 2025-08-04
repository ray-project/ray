# flake8: noqa E501
from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
    Row,
)


class PanelId:
    """
    A class to generate unique panel IDs.
    """

    id = 0

    @staticmethod
    def next():
        PanelId.id += 1
        return PanelId.id


# Ray Train Metrics (Controller)
CONTROLLER_STATE_PANEL = Panel(
    id=PanelId.next(),
    title="Controller State",
    description="Current state of the train controller.",
    unit="",
    targets=[
        Target(
            expr='sum(ray_train_controller_state{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", {global_filters}}}) by (ray_train_run_name, ray_train_controller_state)',
            legend="Run Name: {{ray_train_run_name}}, Controller State: {{ray_train_controller_state}}",
        ),
    ],
)

CONTROLLER_OPERATION_TIME_PANEL = Panel(
    id=PanelId.next(),
    title="Controller Operation Time",
    description="Time taken by the controller for worker group operations.",
    unit="seconds",
    targets=[
        Target(
            expr='sum(ray_train_worker_group_start_total_time_s{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", {global_filters}}}) by (ray_train_run_name)',
            legend="Run Name: {{ray_train_run_name}}, Worker Group Start Time",
        ),
        Target(
            expr='sum(ray_train_worker_group_shutdown_total_time_s{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", {global_filters}}}) by (ray_train_run_name)',
            legend="Run Name: {{ray_train_run_name}}, Worker Group Shutdown Time",
        ),
    ],
    fill=0,
    stack=False,
)

# Ray Train Metrics (Worker)
WORKER_CHECKPOINT_REPORT_TIME_PANEL = Panel(
    id=PanelId.next(),
    title="Checkpoint Report Time",
    description="Time taken to report a checkpoint to storage.",
    unit="seconds",
    targets=[
        Target(
            expr='sum(ray_train_report_total_blocked_time_s{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", ray_train_worker_world_rank=~"$TrainWorkerWorldRank", ray_train_worker_actor_id=~"$TrainWorkerActorId", {global_filters}}}) by (ray_train_run_name, ray_train_worker_world_rank, ray_train_worker_actor_id)',
            legend="Run Name: {{ray_train_run_name}}, World Rank: {{ray_train_worker_world_rank}}",
        )
    ],
    fill=0,
    stack=False,
)

# Core System Resources
CPU_UTILIZATION_PANEL = Panel(
    id=PanelId.next(),
    title="CPU Usage",
    description="CPU core utilization across all workers.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_node_cpu_utilization{{instance=~"$Instance", {global_filters}}} * ray_node_cpu_count{{instance=~"$Instance", {global_filters}}} / 100) by (instance)',
            legend="CPU Usage: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_cpu_count{{instance=~"$Instance", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

MEMORY_UTILIZATION_PANEL = Panel(
    id=PanelId.next(),
    title="Total Memory Usage",
    description="Total physical memory used vs total available memory.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_mem_used{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Memory Used: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_mem_total{{instance=~"$Instance", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

MEMORY_DETAILED_PANEL = Panel(
    id=PanelId.next(),
    title="Memory Allocation Details",
    description="Memory allocation details including available and shared memory.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_mem_available{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Available Memory: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_mem_shared_bytes{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Shared Memory: {{instance}}",
        ),
    ],
)

# GPU Resources
# TODO: Add GPU Device/Index as a filter.
GPU_UTILIZATION_PANEL = Panel(
    id=PanelId.next(),
    title="GPU Usage",
    description="GPU utilization across all workers.",
    unit="GPUs",
    targets=[
        Target(
            expr='sum(ray_node_gpus_utilization{{instance=~"$Instance", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}} / 100) by (instance, GpuIndex, GpuDeviceName)',
            legend="GPU Usage: {{instance}}, gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='sum(ray_node_gpus_available{{instance=~"$Instance", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

GPU_MEMORY_UTILIZATION_PANEL = Panel(
    id=PanelId.next(),
    title="GPU Memory Usage",
    description="GPU memory usage across all workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_gram_used{{instance=~"$Instance", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}} * 1024 * 1024) by (instance, GpuIndex, GpuDeviceName)',
            legend="Used GRAM: {{instance}}, gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='(sum(ray_node_gram_available{{instance=~"$Instance", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}}) + sum(ray_node_gram_used{{instance=~"$Instance", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}})) * 1024 * 1024',
            legend="MAX",
        ),
    ],
)

# Storage Resources
DISK_UTILIZATION_PANEL = Panel(
    id=PanelId.next(),
    title="Disk Space Usage",
    description="Disk space usage across all workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_disk_usage{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Disk Used: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_disk_free{{instance=~"$Instance", {global_filters}}}) + sum(ray_node_disk_usage{{instance=~"$Instance", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

DISK_THROUGHPUT_PANEL = Panel(
    id=PanelId.next(),
    title="Disk Throughput",
    description="Current disk read/write throughput.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(ray_node_disk_io_read_speed{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Read Speed: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_disk_io_write_speed{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Write Speed: {{instance}}",
        ),
    ],
)

DISK_OPERATIONS_PANEL = Panel(
    id=PanelId.next(),
    title="Disk Operations",
    description="Current disk read/write operations per second.",
    unit="ops/s",
    targets=[
        Target(
            expr='sum(ray_node_disk_read_iops{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Read IOPS: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_disk_write_iops{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Write IOPS: {{instance}}",
        ),
    ],
)

# Network Resources
NETWORK_THROUGHPUT_PANEL = Panel(
    id=PanelId.next(),
    title="Network Throughput",
    description="Current network send/receive throughput.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(ray_node_network_receive_speed{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Receive Speed: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_network_send_speed{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Send Speed: {{instance}}",
        ),
    ],
)

NETWORK_TOTAL_PANEL = Panel(
    id=PanelId.next(),
    title="Network Total Traffic",
    description="Total network traffic sent/received.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_network_sent{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Total Sent: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_network_received{{instance=~"$Instance", {global_filters}}}) by (instance)',
            legend="Total Received: {{instance}}",
        ),
    ],
)

TRAIN_GRAFANA_PANELS = []

TRAIN_GRAFANA_ROWS = [
    # Train Metrics Row
    Row(
        title="Train Metrics",
        id=PanelId.next(),
        panels=[
            # Ray Train Metrics (Controller)
            CONTROLLER_STATE_PANEL,
            CONTROLLER_OPERATION_TIME_PANEL,
            # Ray Train Metrics (Worker)
            WORKER_CHECKPOINT_REPORT_TIME_PANEL,
        ],
        collapsed=False,
    ),
    # System Resources Row
    Row(
        title="Resource Utilization",
        id=PanelId.next(),
        panels=[
            CPU_UTILIZATION_PANEL,
            MEMORY_UTILIZATION_PANEL,
            MEMORY_DETAILED_PANEL,
            # GPU Resources
            GPU_UTILIZATION_PANEL,
            GPU_MEMORY_UTILIZATION_PANEL,
            # Storage Resources
            DISK_UTILIZATION_PANEL,
            DISK_THROUGHPUT_PANEL,
            DISK_OPERATIONS_PANEL,
            # Network Resources
            NETWORK_THROUGHPUT_PANEL,
            NETWORK_TOTAL_PANEL,
        ],
        collapsed=True,
    ),
]

TRAIN_RUN_PANELS = [
    # Ray Train Metrics (Controller)
    CONTROLLER_STATE_PANEL,
    CONTROLLER_OPERATION_TIME_PANEL,
    # Ray Train Metrics (Worker)
    WORKER_CHECKPOINT_REPORT_TIME_PANEL,
]

TRAIN_WORKER_PANELS = [
    # Ray Train Metrics (Worker)
    WORKER_CHECKPOINT_REPORT_TIME_PANEL,
    # Core System Resources
    CPU_UTILIZATION_PANEL,
    MEMORY_UTILIZATION_PANEL,
    # GPU Resources
    GPU_UTILIZATION_PANEL,
    GPU_MEMORY_UTILIZATION_PANEL,
    # Storage Resources
    DISK_UTILIZATION_PANEL,
    # Network Resources
    NETWORK_THROUGHPUT_PANEL,
]

# Get all panel IDs from both top-level panels and panels within rows
all_panel_ids = [panel.id for panel in TRAIN_GRAFANA_PANELS]
for row in TRAIN_GRAFANA_ROWS:
    all_panel_ids.append(row.id)
    all_panel_ids.extend(panel.id for panel in row.panels)

assert len(all_panel_ids) == len(
    set(all_panel_ids)
), f"Duplicated id found. Use unique id for each panel. {all_panel_ids}"

train_dashboard_config = DashboardConfig(
    name="TRAIN",
    default_uid="rayTrainDashboard",
    rows=TRAIN_GRAFANA_ROWS,
    standard_global_filters=['SessionName=~"$SessionName"'],
    base_json_file_name="train_grafana_dashboard_base.json",
)
