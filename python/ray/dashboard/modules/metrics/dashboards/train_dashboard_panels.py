# flake8: noqa E501
from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)

WORKER_CHECKPOINT_REPORT_TIME_PANEL = Panel(
    id=1,
    title="Checkpoint Report Time",
    description="Time taken to report a checkpoint to storage.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_train_report_total_blocked_time_s{{ray_train_worker_world_rank=~'$TrainWorkerWorldRank', ray_train_worker_actor_id=~'$TrainWorkerActorId', {global_filters}}}) by (ray_train_run_name, ray_train_worker_world_rank, ray_train_worker_actor_id)",
            legend="Run Name: {{ray_train_run_name}}, World Rank: {{ray_train_worker_world_rank}}",
        )
    ],
    fill=0,
    stack=False,
)

CONTROLLER_OPERATION_TIME_PANEL = Panel(
    id=2,
    title="Train Controller Operation Time",
    description="Time taken by the controller to perform various operations.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_train_worker_group_start_total_time_s{{{global_filters}}}) by (ray_train_run_name)",
            legend="Run Name: {{ray_train_run_name}}, Worker Group Start Time",
        ),
        Target(
            expr="sum(ray_train_worker_group_shutdown_total_time_s{{{global_filters}}}) by (ray_train_run_name)",
            legend="Run Name: {{ray_train_run_name}}, Worker Group Shutdown Time",
        ),
    ],
    fill=0,
    stack=False,
)

CONTROLLER_STATE_PANEL = Panel(
    id=3,
    title="Train Controller State",
    description="State of the train controller.",
    unit="",
    targets=[
        Target(
            expr="sum(ray_train_controller_state{{{global_filters}}}) by (ray_train_run_name, ray_train_controller_state)",
            legend="Run Name: {{ray_train_run_name}}, Controller State: {{ray_train_controller_state}}",
        ),
    ],
)

# IOPS
IOPS_PANEL = Panel(
    id=4,
    title="Train Worker IOPS",
    description="Input/Output operations per second for train workers.",
    unit="ops/s",
    targets=[
        Target(
            expr='sum(ray_node_disk_io_read_speed{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Read IOPS: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_disk_io_write_speed{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Write IOPS: {{instance}}",
        ),
    ],
)

# Network Utilization
NETWORK_UTILIZATION_PANEL = Panel(
    id=5,
    title="Train Worker Network Utilization",
    description="Network utilization for train workers.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(ray_node_network_receive_speed{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Receive: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_network_send_speed{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Send: {{instance}}",
        ),
    ],
)

# Disk Utilization
DISK_UTILIZATION_PANEL = Panel(
    id=6,
    title="Train Worker Disk Utilization",
    description="Disk utilization for train workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_disk_usage{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Disk Used: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_disk_free{{instance=~"$Instance",{global_filters}}}) + sum(ray_node_disk_usage{{instance=~"$Instance",{global_filters}}})',
            legend="MAX",
        ),
    ],
)

# CPU Utilization
CPU_UTILIZATION_PANEL = Panel(
    id=7,
    title="Train Worker CPU Utilization",
    description="CPU utilization for train workers.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_node_cpu_utilization{{instance=~"$Instance",{global_filters}}} * ray_node_cpu_count{{instance=~"$Instance",{global_filters}}} / 100) by (instance)',
            legend="CPU Usage: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_cpu_count{{instance=~"$Instance",{global_filters}}})',
            legend="MAX",
        ),
    ],
)

# Memory Utilization
MEMORY_UTILIZATION_PANEL = Panel(
    id=8,
    title="Train Worker Memory Utilization",
    description="Memory utilization for train workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_mem_used{{instance=~"$Instance",{global_filters}}}) by (instance)',
            legend="Memory Used: {{instance}}",
        ),
        Target(
            expr='sum(ray_node_mem_total{{instance=~"$Instance",{global_filters}}})',
            legend="MAX",
        ),
    ],
)

# GPU Utilization
GPU_UTILIZATION_PANEL = Panel(
    id=9,
    title="Train Worker GPU Utilization",
    description="GPU utilization for train workers.",
    unit="GPUs",
    targets=[
        Target(
            expr='sum(ray_node_gpus_utilization{{instance=~"$Instance",{global_filters}}} / 100) by (instance, GpuIndex, GpuDeviceName)',
            legend="GPU Usage: {{instance}}, gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='sum(ray_node_gpus_available{{instance=~"$Instance",{global_filters}}})',
            legend="MAX",
        ),
    ],
)

# GPU Memory Utilization
GPU_MEMORY_UTILIZATION_PANEL = Panel(
    id=10,
    title="Train Worker GPU Memory Utilization",
    description="GPU memory utilization for train workers.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_node_gram_used{{instance=~"$Instance",{global_filters}}} * 1024 * 1024) by (instance, GpuIndex, GpuDeviceName)',
            legend="Used GRAM: {{instance}}, gpu.{{GpuIndex}}, {{GpuDeviceName}}",
        ),
        Target(
            expr='(sum(ray_node_gram_available{{instance=~"$Instance",{global_filters}}}) + sum(ray_node_gram_used{{instance=~"$Instance",{global_filters}}})) * 1024 * 1024',
            legend="MAX",
        ),
    ],
)

TRAIN_GRAFANA_PANELS = [
    # Ray Train Metrics (Worker)
    WORKER_CHECKPOINT_REPORT_TIME_PANEL,
    # Ray Train Metrics (Controller)
    CONTROLLER_OPERATION_TIME_PANEL,
    CONTROLLER_STATE_PANEL,
    # Resource Utilization Metrics
    IOPS_PANEL,
    NETWORK_UTILIZATION_PANEL,
    DISK_UTILIZATION_PANEL,
    CPU_UTILIZATION_PANEL,
    MEMORY_UTILIZATION_PANEL,
    GPU_UTILIZATION_PANEL,
    GPU_MEMORY_UTILIZATION_PANEL,
]


ids = [panel.id for panel in TRAIN_GRAFANA_PANELS]
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

train_dashboard_config = DashboardConfig(
    name="TRAIN",
    default_uid="rayTrainDashboard",
    panels=TRAIN_GRAFANA_PANELS,
    standard_global_filters=['SessionName=~"$SessionName"'],
    base_json_file_name="train_grafana_dashboard_base.json",
)
