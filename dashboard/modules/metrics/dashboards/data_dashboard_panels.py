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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
    ),
    # Inputs-related metrics
    Panel(
        id=17,
        title="Input Blocks Received by Operator",
        description="Number of input blocks received by operator.",
        unit="blocks",
        targets=[
            Target(
                expr="sum(ray_data_num_inputs_received{{{global_filters}}}) by (dataset, operator)",
                legend="Blocks Received: {{dataset}}, {{operator}}",
            )
        ],
    ),
    Panel(
        id=19,
        title="Input Blocks Processed by Tasks",
        description="Number of input blocks processed by tasks.",
        unit="blocks",
        targets=[
            Target(
                expr="sum(ray_data_num_task_inputs_processed{{{global_filters}}}) by (dataset, operator)",
                legend="Blocks Processed: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=20,
        title="Input Bytes Processed by Tasks",
        description="Total size in bytes of processed input blocks.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_bytes_task_inputs_processed{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Processed: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=21,
        title="Input Bytes Submitted to Tasks",
        description="Total size in bytes of input blocks passed to submitted tasks.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_bytes_inputs_of_submitted_tasks{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Submitted: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
    ),
    # Input queue metrics
    Panel(
        id=13,
        title="Operator Internal Inqueue Size (Blocks)",
        description="Number of blocks in operator's internal input queue",
        unit="blocks",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_internal_inqueue_blocks{{{global_filters}}}) by (dataset, operator)",
                legend="Number of Blocks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=14,
        title="Operator Internal Inqueue Size (Bytes)",
        description="Total byte size of blocks in operator's internal input queue",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_internal_inqueue{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Size: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
    ),
    Panel(
        id=15,
        title="Operator Internal Outqueue Size (Blocks)",
        description="Number of blocks in operator's internal output queue",
        unit="blocks",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_internal_outqueue_blocks{{{global_filters}}}) by (dataset, operator)",
                legend="Number of Blocks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=16,
        title="Operator Internal Outqueue Size (Bytes)",
        description="Total byte size of blocks in operator's internal output queue",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_internal_outqueue{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Size: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
    ),
    # Iteration metrics
    Panel(
        id=12,
        title="Iteration Initialization Time",
        description="Seconds spent in iterator initialization code",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_iter_initialize_seconds{{{global_filters}}}) by (dataset)",
                legend="Seconds: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
        fill=0,
        stack=False,
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
