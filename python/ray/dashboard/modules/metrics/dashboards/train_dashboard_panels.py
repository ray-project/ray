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
EXPOSED_DATA_LOADING_TIME_PANEL = Panel(
    id=18,
    title="Max Exposed Data Loading Time",
    description="Per-batch data loading time exposed as training stall, not hidden behind data loader pipelining, taken as the max across ranks so it reflects the slowest rank, per dataset, over the last ${window}. Use to identify if training is stalled on data loading; ideally this value is 0, and any non-zero value indicates stall.",
    unit="ms/batch",
    targets=[
        Target(
            expr="max by (dataset) (1000 * sum(rate(ray_data_iter_total_blocked_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split))",
            legend="{{dataset}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_THROUGHPUT_PANEL = Panel(
    id=27,
    title="Data Ingest Throughput by Rank",
    description="Rows per second consumed by the training loop, one line per rank, averaged over the last ${window}.",
    unit="rowsps",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_rows_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_PRODUCTION_THROUGHPUT_PANEL = Panel(
    id=29,
    title="Data Production Throughput",
    description="Rows/sec the Ray Data pipeline delivers to the training workers, over the last ${window}; only reported for datasets split across workers (DataConfig `datasets_to_split`), so non-split datasets show no data here.",
    unit="rowsps",
    targets=[
        Target(
            expr='sum(rate(ray_data_output_rows{{operator=~"split.*", {global_filters}}}[$window])) by (dataset)',
            legend="{{dataset}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_LOADING_BREAKDOWN_PANEL = Panel(
    id=19,
    title="Percentage Data Loading Breakdown by Stage",
    description="Share of per-batch data loading time spent in each stage (production wait -> data transfer -> batching -> format -> collate -> finalize), across ranks, over the last ${window}. If exposed data loading time is 0, the stages do not contribute to training stall and are hidden by data loading pipelining; otherwise, use this graph to identify the offending stages.",
    unit="percentunit",
    targets=[
        Target(
            expr="sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Production Wait: {{dataset}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Data Transfer: {{dataset}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Batching: {{dataset}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Format: {{dataset}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Collate: {{dataset}}",
        ),
        Target(
            expr="sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset) / (sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset) + sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset))",
            legend="Finalize: {{dataset}}",
        ),
    ],
    fill=10,
    stack=True,
)

PRODUCTION_WAIT_TIME_PANEL = Panel(
    id=30,
    title="Production Wait Time by Rank",
    description="Average per-batch time the data loader spends waiting for the Ray Data pipeline to produce the next block, one line per rank, over the last ${window}. Use to identify data production stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

DATA_TRANSFER_TIME_PANEL = Panel(
    id=22,
    title="Data Transfer Time by Rank",
    description="Average per-batch time spent resolving and transferring data blocks, one line per rank, over the last ${window}. Use to identify data transfer stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_get_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

BATCHING_TIME_PANEL = Panel(
    id=23,
    title="Batching Time by Rank",
    description="Average per-batch time spent building batches (slicing, local shuffle), one line per rank, over the last ${window}. Use to identify batching stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_next_batch_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

FORMAT_TIME_PANEL = Panel(
    id=24,
    title="Format Time by Rank",
    description="Average per-batch time spent converting blocks to the batch format, one line per rank, over the last ${window}. Use to identify formatting stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_format_batch_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

COLLATE_TIME_PANEL = Panel(
    id=25,
    title="Collate Time by Rank",
    description="Average per-batch time spent in the user collate function, one line per rank, over the last ${window}. Use to identify collation stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_collate_batch_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
            legend="{{dataset}}, {{split}}",
        ),
    ],
    fill=0,
    stack=False,
)

FINALIZE_TIME_PANEL = Panel(
    id=26,
    title="Finalize Time by Rank",
    description="Average per-batch time spent in finalize (e.g. host-to-device transfer), one line per rank, over the last ${window}. Use to identify finalization stragglers.",
    unit="ms/batch",
    targets=[
        Target(
            expr="1000 * sum(rate(ray_data_iter_finalize_batch_seconds{{{global_filters}}}[$window])) by (dataset, split) / sum(rate(ray_data_iter_batches_total{{{global_filters}}}[$window])) by (dataset, split)",
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
            EXPOSED_DATA_LOADING_TIME_PANEL,
            DATA_LOADING_BREAKDOWN_PANEL,
            DATA_LOADING_THROUGHPUT_PANEL,
            DATA_PRODUCTION_THROUGHPUT_PANEL,
            PRODUCTION_WAIT_TIME_PANEL,
            DATA_TRANSFER_TIME_PANEL,
            BATCHING_TIME_PANEL,
            FORMAT_TIME_PANEL,
            COLLATE_TIME_PANEL,
            FINALIZE_TIME_PANEL,
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
