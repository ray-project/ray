# flake8: noqa E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)

DATA_GRAFANA_PANELS = [
    Panel(
        id=1,
        title="Bytes Spilled",
        description="Amount spilled by dataset operators. DataContext.enable_get_object_locations_for_metrics must be set to True to report this metric",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_spilled_bytes{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Spilled: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=2,
        title="Bytes Allocated",
        description="Amount allocated by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_allocated_bytes{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Allocated: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=3,
        title="Bytes Freed",
        description="Amount freed by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_freed_bytes{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Freed: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=4,
        title="Object Store Memory",
        description="Amount of memory store used by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_current_bytes{{{global_filters}}}) by (dataset, operator)",
                legend="Current Usage: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=5,
        title="CPUs (logical slots)",
        description="Logical CPUs allocated to dataset operators.",
        unit="cores",
        targets=[
            Target(
                expr="sum(ray_data_cpu_usage_cores{{{global_filters}}}) by (dataset, operator)",
                legend="CPU Usage: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=6,
        title="GPUs (logical slots)",
        description="Logical GPUs allocated to dataset operators.",
        unit="cores",
        targets=[
            Target(
                expr="sum(ray_data_gpu_usage_cores{{{global_filters}}}) by (dataset, operator)",
                legend="GPU Usage: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=7,
        title="Bytes Outputted",
        description="Total bytes outputted by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_output_bytes{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Outputted: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=11,
        title="Rows Outputted",
        description="Total rows outputted by dataset operators.",
        unit="rows",
        targets=[
            Target(
                expr="sum(ray_data_output_rows{{{global_filters}}}) by (dataset, operator)",
                legend="Rows Outputted: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=8,
        title="Block Generation Time",
        description="Time spent generating blocks.",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_block_generation_seconds{{{global_filters}}}) by (dataset, operator)",
                legend="Block Generation Time: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=9,
        title="Iteration Blocked Time",
        description="Seconds user thread is blocked by iter_batches()",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_iter_total_blocked_seconds{{{global_filters}}}) by (dataset)",
                legend="Seconds: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=10,
        title="Iteration User Time",
        description="Seconds spent in user code",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_iter_user_seconds{{{global_filters}}}) by (dataset)",
                legend="Seconds: {{dataset}}",
            )
        ],
    ),
]

ids = []
for panel in DATA_GRAFANA_PANELS:
    ids.append(panel.id)
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

data_dashboard_config = DashboardConfig(
    name="DATA",
    default_uid="rayDataDashboard",
    panels=DATA_GRAFANA_PANELS,
    standard_global_filters=['dataset=~"$DatasetID"', 'SessionName=~"$SessionName"'],
    base_json_file_name="data_grafana_dashboard_base.json",
)
