# flake8: noqa E501
from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Row,
    Target,
)

# Ray Train Metrics (Controller)
CONTROLLER_STATE_PANEL = Panel(
    id=1,
    title="Controller State",
    description="Current state of the Ray Train controller.",
    unit="",
    targets=[
        Target(
            expr='sum(ray_train_controller_state{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", {global_filters}}}) by (ray_train_run_name, ray_train_controller_state)',
            legend="Run Name: {{ray_train_run_name}}, Controller State: {{ray_train_controller_state}}",
        ),
    ],
)

CONTROLLER_OPERATION_TIME_PANEL = Panel(
    id=2,
    title="Cumulative Worker Group Start/Shutdown Time",
    description="Cumulative time the controller spends starting and shutting down worker groups (re-created on worker failures and resizes).",
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
WORKER_TRAIN_REPORT_TIME_PANEL = Panel(
    id=3,
    title="Cumulative Time in ray.train.report",
    description="Cumulative time workers spend blocked inside `ray.train.report()`. This includes the cross-rank checkpoint directory sync barrier, the checkpoint file transfer to storage, and the time waiting for the report queue ordering. See the Checkpoint Sync and Checkpoint Transfer panels for a breakdown.",
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
WORKER_CHECKPOINT_SYNC_TIME_PANEL = Panel(
    id=16,
    title="Cumulative Checkpoint Sync Time",
    description="Cumulative time spent in the cross-rank barrier that synchronizes the checkpoint directory name across all workers. High values indicate workers are spending significant time waiting for each other to reach the synchronization point.",
    unit="seconds",
    targets=[
        Target(
            expr='sum(ray_train_checkpoint_sync_total_time_s{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", ray_train_worker_world_rank=~"$TrainWorkerWorldRank", ray_train_worker_actor_id=~"$TrainWorkerActorId", {global_filters}}}) by (ray_train_run_name, ray_train_worker_world_rank, ray_train_worker_actor_id)',
            legend="Run Name: {{ray_train_run_name}}, World Rank: {{ray_train_worker_world_rank}}",
        )
    ],
    fill=0,
    stack=False,
)

WORKER_CHECKPOINT_TRANSFER_TIME_PANEL = Panel(
    id=17,
    title="Cumulative Checkpoint Transfer Time",
    description="Cumulative time spent transferring checkpoint files to storage. High values indicate slow storage throughput or large checkpoint sizes.",
    unit="seconds",
    targets=[
        Target(
            expr='sum(ray_train_checkpoint_transfer_total_time_s{{ray_train_run_name=~"$TrainRunName", ray_train_run_id=~"$TrainRunId", ray_train_worker_world_rank=~"$TrainWorkerWorldRank", ray_train_worker_actor_id=~"$TrainWorkerActorId", {global_filters}}}) by (ray_train_run_name, ray_train_worker_world_rank, ray_train_worker_actor_id)',
            legend="Run Name: {{ray_train_run_name}}, World Rank: {{ray_train_worker_world_rank}}",
        )
    ],
    fill=0,
    stack=False,
)

# Core System Resources
CPU_UTILIZATION_PANEL = Panel(
    id=4,
    title="CPU Usage",
    description="CPU core utilization across all workers.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_node_cpu_utilization{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}} * ray_node_cpu_count{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}} / 100) by (instance, RayNodeType)',
            legend="CPU Usage: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_cpu_count{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

MEMORY_UTILIZATION_PANEL = Panel(
    id=5,
    title="Total Memory Usage",
    description="Total physical memory used vs total available memory.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_mem_used{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Memory Used: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_mem_total{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

MEMORY_DETAILED_PANEL = Panel(
    id=6,
    title="Memory Allocation Details",
    description="Memory allocation details including available and shared memory.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_mem_available{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Available Memory: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_mem_shared_bytes{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Shared Memory: {{instance}} ({{RayNodeType}})",
        ),
    ],
)

# GPU Resources
# TODO: Add GPU Device/Index as a filter.
GPU_UTILIZATION_PANEL = Panel(
    id=7,
    title="GPU Usage",
    description="GPU utilization across all workers.",
    unit="GPUs",
    targets=[
        Target(
            expr='sum(ray_node_gpus_utilization{{instance=~"$Instance", RayNodeType=~"$RayNodeType", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}} / 100) by (instance, RayNodeType, GpuIndex, GpuDeviceName)',
            legend="GPU Usage: {{instance}} ({{RayNodeType}}), gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='sum(ray_node_gpus_available{{instance=~"$Instance", RayNodeType=~"$RayNodeType", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

GPU_MEMORY_UTILIZATION_PANEL = Panel(
    id=8,
    title="GPU Memory Usage",
    description="GPU memory usage across all workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_gram_used{{instance=~"$Instance", RayNodeType=~"$RayNodeType", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}} * 1024 * 1024) by (instance, RayNodeType, GpuIndex, GpuDeviceName)',
            legend="Used GRAM: {{instance}} ({{RayNodeType}}), gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='(sum(ray_node_gram_available{{instance=~"$Instance", RayNodeType=~"$RayNodeType", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}}) + sum(ray_node_gram_used{{instance=~"$Instance", RayNodeType=~"$RayNodeType", GpuIndex=~"$GpuIndex", GpuDeviceName=~"$GpuDeviceName", {global_filters}}})) * 1024 * 1024',
            legend="MAX",
        ),
    ],
)

# Storage Resources
DISK_UTILIZATION_PANEL = Panel(
    id=9,
    title="Disk Space Usage",
    description="Disk space usage across all workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_disk_usage{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Disk Used: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_disk_free{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) + sum(ray_node_disk_usage{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}})',
            legend="MAX",
        ),
    ],
)

DISK_THROUGHPUT_PANEL = Panel(
    id=10,
    title="Disk Throughput",
    description="Current disk read/write throughput.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(ray_node_disk_io_read_speed{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Read Speed: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_disk_io_write_speed{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Write Speed: {{instance}} ({{RayNodeType}})",
        ),
    ],
)

DISK_OPERATIONS_PANEL = Panel(
    id=11,
    title="Disk Operations",
    description="Current disk read/write operations per second.",
    unit="ops/s",
    targets=[
        Target(
            expr='sum(ray_node_disk_read_iops{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Read IOPS: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_disk_write_iops{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Write IOPS: {{instance}} ({{RayNodeType}})",
        ),
    ],
)

# Network Resources
NETWORK_THROUGHPUT_PANEL = Panel(
    id=12,
    title="Network Throughput",
    description="Current network send/receive throughput.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(ray_node_network_receive_speed{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Receive Speed: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_network_send_speed{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Send Speed: {{instance}} ({{RayNodeType}})",
        ),
    ],
)

NETWORK_TOTAL_PANEL = Panel(
    id=13,
    title="Network Total Traffic",
    description="Total network traffic sent/received.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_network_sent{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Total Sent: {{instance}} ({{RayNodeType}})",
        ),
        Target(
            expr='sum(ray_node_network_received{{instance=~"$Instance", RayNodeType=~"$RayNodeType", {global_filters}}}) by (instance, RayNodeType)',
            legend="Total Received: {{instance}} ({{RayNodeType}})",
        ),
    ],
)

# Data Loading Metrics
#
# These panels are backed by Ray Data iteration metrics emitted while the
# training loop consumes batches (e.g. via `iter_torch_batches`).
# NOTE: The exprs group by (dataset, rank). The `rank` label is not emitted
# yet (see the TODO on `iter_tag_keys` in ray/data/_internal/stats.py); until
# it lands, series group under an empty rank and the panels remain valid.
DATA_LOADING_AVG_BATCH_LATENCY_PANEL = Panel(
    id=18,
    title="Avg Batch Latency by Stage",
    description="Average wall-clock time spent in each data loading stage per batch, per dataset and consumer rank. This is the raw cost of each stage, including time hidden by pipelining, so it does not necessarily indicate a training bottleneck. See the Training Thread Blocked Time panel for stall attribution.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Production Wait: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Data Transfer: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Batching: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Format: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Collate: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[1m])) by (dataset, rank) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Finalize: {{dataset}}, rank {{rank}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_TIME_BREAKDOWN_PANEL = Panel(
    id=19,
    title="Training Thread Blocked Time by Stage",
    description="Seconds per second the training thread was blocked waiting on each data loading stage, per dataset and consumer rank. A value of 1 means the training thread was fully stalled on that stage. The dominant stage identifies the data loading bottleneck; near-zero everywhere means training is not data loading bound.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_production_wait_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Production Wait: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_blocked_data_transfer_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Data Transfer: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_blocked_batching_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Batching: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_blocked_format_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Format: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_blocked_collate_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Collate: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_blocked_finalize_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Finalize: {{dataset}}, rank {{rank}}",
        ),
    ],
)

DATA_LOADING_BLOCKED_VS_TOTAL_PANEL = Panel(
    id=20,
    title="Training Thread Blocked vs Total Iteration Time",
    description="Seconds per second the training thread spent blocked waiting for the next batch, compared against total iteration time (blocked + user code), per dataset and consumer rank. Blocked time approaching total time means the training loop is dominated by data loading.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_total_blocked_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Blocked: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_total_seconds{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Total: {{dataset}}, rank {{rank}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_THROUGHPUT_PANEL = Panel(
    id=22,
    title="Data Loading Throughput",
    description="Rows and batches delivered to the training thread per second, per dataset and consumer rank.",
    unit="",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_rows_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Rows/s: {{dataset}}, rank {{rank}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, rank)",
            legend="Batches/s: {{dataset}}, rank {{rank}}",
        ),
    ],
    fill=0,
    stack=False,
)

TRAIN_GRAFANA_PANELS = []

TRAIN_GRAFANA_ROWS = [
    # Train Metrics Row
    Row(
        title="Train Metrics",
        id=14,
        panels=[
            # Ray Train Metrics (Controller)
            CONTROLLER_STATE_PANEL,
            CONTROLLER_OPERATION_TIME_PANEL,
            # Ray Train Metrics (Worker)
            WORKER_TRAIN_REPORT_TIME_PANEL,
            WORKER_CHECKPOINT_SYNC_TIME_PANEL,
            WORKER_CHECKPOINT_TRANSFER_TIME_PANEL,
        ],
        collapsed=False,
    ),
    # System Resources Row
    Row(
        title="Resource Utilization",
        id=15,
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
    # Data Loading Row
    Row(
        title="Data Loading",
        id=21,
        panels=[
            DATA_LOADING_AVG_BATCH_LATENCY_PANEL,
            DATA_LOADING_BLOCKED_TIME_BREAKDOWN_PANEL,
            DATA_LOADING_BLOCKED_VS_TOTAL_PANEL,
            DATA_LOADING_THROUGHPUT_PANEL,
        ],
        collapsed=False,
    ),
]

TRAIN_RUN_PANELS = [
    # Ray Train Metrics (Controller)
    CONTROLLER_STATE_PANEL,
    CONTROLLER_OPERATION_TIME_PANEL,
    # Ray Train Metrics (Worker)
    WORKER_TRAIN_REPORT_TIME_PANEL,
]

TRAIN_WORKER_PANELS = [
    # Ray Train Metrics (Worker)
    WORKER_TRAIN_REPORT_TIME_PANEL,
    WORKER_CHECKPOINT_SYNC_TIME_PANEL,
    WORKER_CHECKPOINT_TRANSFER_TIME_PANEL,
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

all_panel_ids.sort()

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
