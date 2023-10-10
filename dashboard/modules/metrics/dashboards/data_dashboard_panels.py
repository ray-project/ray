# flake8: noqa E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)

DATA_GRAFANA_PANELS = [
    Panel(
        id=45,
        title="Bytes Spilled",
        description="Amount spilled by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_spilled_bytes{{{global_filters}}}) by (dataset)",
                legend="Bytes Spilled: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=46,
        title="Bytes Allocated",
        description="Amount allocated by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_allocated_bytes{{{global_filters}}}) by (dataset)",
                legend="Bytes Allocated: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=47,
        title="Bytes Freed",
        description="Amount freed by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_freed_bytes{{{global_filters}}}) by (dataset)",
                legend="Bytes Freed: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=48,
        title="Memory Store Usage",
        description="Amount of memory store used by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_current_bytes{{{global_filters}}}) by (dataset)",
                legend="Current Usage: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=49,
        title="CPU Usage",
        description="Logical CPU usage of dataset operators.",
        unit="cores",
        targets=[
            Target(
                expr="sum(ray_data_cpu_usage_cores{{{global_filters}}}) by (dataset)",
                legend="CPU Usage: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=50,
        title="GPU Usage",
        description="Logical GPU usage of dataset operators.",
        unit="cores",
        targets=[
            Target(
                expr="sum(ray_data_gpu_usage_cores{{{global_filters}}}) by (dataset)",
                legend="GPU Usage: {{dataset}}",
            )
        ],
    ),
    Panel(
        id=51,
        title="Bytes Outputted",
        description="Total bytes outputted by dataset operators.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_output_bytes{{{global_filters}}}) by (dataset)",
                legend="Bytes Outputted: {{dataset}}",
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
    standard_global_filters=[],
    base_json_file_name="default_grafana_dashboard_base.json",
)
