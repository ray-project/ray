# flake8: noqa E501
from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)

TRAIN_GRAFANA_PANELS = [
    # Ray Train Metrics (Worker)
    Panel(
        id=1,
        title="Checkpoint Report Time",
        description="Time taken to report a checkpoint to storage.",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_train_report_total_blocked_time_s{{{global_filters}}}) by (ray_train_run_name, ray_train_worker_world_rank)",
                legend="Run Name: {{ray_train_run_name}}, World Rank: {{ray_train_worker_world_rank}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    # Ray Train Metrics (Controller)
    Panel(
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
    ),
]

ids = [panel.id for panel in TRAIN_GRAFANA_PANELS]
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

train_dashboard_config = DashboardConfig(
    name="TRAIN",
    default_uid="rayTrainDashboard",
    panels=TRAIN_GRAFANA_PANELS,
    standard_global_filters=[
        'SessionName=~"$SessionName"',
        'ray_train_run_name=~"$TrainRunName"',
    ],
    base_json_file_name="train_grafana_dashboard_base.json",
)
