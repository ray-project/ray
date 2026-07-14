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
# training loop consumes batches (e.g. via `iter_torch_batches`). Each series
# is grouped by (dataset, split): `dataset` identifies the dataset and `split`
# identifies the per-consumer split introduced in #64608 (`split_0`, `split_1`,
# ...; plain non-stream-split iterators report `split="no_split"`). For the Ray
# Train path there is one split per worker, so "one line per rank" is
# `by (dataset, split)`.
DATA_LOADING_RATIO_PANEL = Panel(
    id=18,
    title="Data Loading Ratio",
    description="Ratio of time the training thread spent blocked on data loading vs. time spent in training/user code, one line per rank. A value >= 1 means the loop waited on data at least as long as it spent training. The headline signal for whether the training loop is data loading bound.",
    unit="",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_total_blocked_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_user_seconds{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_TIME_BY_STAGE_PANEL = Panel(
    id=19,
    title="Blocked Time by Stage (fleet avg)",
    description="Average per-batch stall time attributed to each data loading stage, averaged across all ranks. Six series (one per stage) in one panel; the tallest stage is the current bottleneck. Use this to localize which stage is eating the time at a glance.",
    unit="seconds",
    targets=[
        Target(
            expr="avg(rate(ray_data_iter_blocked_production_wait_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Production Wait: {{dataset}}",
        ),
        Target(
            expr="avg(rate(ray_data_iter_blocked_data_transfer_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Data Transfer: {{dataset}}",
        ),
        Target(
            expr="avg(rate(ray_data_iter_blocked_batching_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Batching: {{dataset}}",
        ),
        Target(
            expr="avg(rate(ray_data_iter_blocked_format_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Format: {{dataset}}",
        ),
        Target(
            expr="avg(rate(ray_data_iter_blocked_collate_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Collate: {{dataset}}",
        ),
        Target(
            expr="avg(rate(ray_data_iter_blocked_finalize_seconds{{{global_filters}}}[1m]) / rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset)",
            legend="Finalize: {{dataset}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_PRODUCTION_WAIT_PANEL = Panel(
    id=20,
    title="Blocked: Production Wait by Rank",
    description="Per-batch stall waiting on upstream block production, one line per rank. Use this to tell whether upstream production is slow for one rank or all.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_production_wait_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_DATA_TRANSFER_PANEL = Panel(
    id=22,
    title="Blocked: Data Transfer by Rank",
    description="Per-batch stall on cross-node ray.get() block transfer, one line per rank. Use this to isolate a rank with slow/remote object fetches.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_data_transfer_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_BATCHING_PANEL = Panel(
    id=23,
    title="Blocked: Batching by Rank",
    description="Per-batch stall building batches (slice/shuffle), one line per rank. Use this to spot uneven batching cost across ranks.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_batching_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_FORMAT_PANEL = Panel(
    id=24,
    title="Blocked: Format by Rank",
    description="Per-batch stall converting blocks to batch format, one line per rank. Use this to find a rank with disproportionate format cost.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_format_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_COLLATE_PANEL = Panel(
    id=25,
    title="Blocked: Collate by Rank",
    description="Per-batch stall in the user collate_fn, one line per rank. Use this to detect a rank where collate is the bottleneck.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_collate_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BLOCKED_FINALIZE_PANEL = Panel(
    id=26,
    title="Blocked: Finalize by Rank",
    description="Per-batch stall in the user finalize_fn, one line per rank. Use this to detect a rank where finalize is the bottleneck.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_blocked_finalize_seconds{{{global_filters}}}[1m])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_THROUGHPUT_PANEL = Panel(
    id=27,
    title="Iterator Throughput by Rank",
    description="Average rows/sec delivered to the training thread, one line per rank. A rank with a low delivery rate is starving; cross-check against the stall panels.",
    unit="",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_rows_total{{{global_filters}}}[1m])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
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
    # Data Ingestion Row
    Row(
        title="Data Ingestion",
        id=21,
        panels=[
            DATA_LOADING_RATIO_PANEL,
            DATA_LOADING_BLOCKED_TIME_BY_STAGE_PANEL,
            DATA_LOADING_BLOCKED_PRODUCTION_WAIT_PANEL,
            DATA_LOADING_BLOCKED_DATA_TRANSFER_PANEL,
            DATA_LOADING_BLOCKED_BATCHING_PANEL,
            DATA_LOADING_BLOCKED_FORMAT_PANEL,
            DATA_LOADING_BLOCKED_COLLATE_PANEL,
            DATA_LOADING_BLOCKED_FINALIZE_PANEL,
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
