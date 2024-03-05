# flake8: noqa E501
import dataclasses
from typing import Dict

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics

RUNTIME_METRIC_FIELDS: Dict[
    str, dataclasses.Field
] = OpRuntimeMetrics.__dataclass_fields__


def _generate_grafana_panel_from_metric(
    field_name: str,
    panel_id: int,
    title: str,
    unit: str,
    legend: str,
    fill: int = 0,
    stack: bool = False,
) -> Panel:
    """Generate a Grafana Panel object from the given OpRuntimeMetrics field name,
    using the args provided. By default, Ray Data charts are unstacked
    line charts."""
    metric = RUNTIME_METRIC_FIELDS[field_name]
    md = metric.metadata

    return Panel(
        title=title,
        description=md.get("description"),
        id=panel_id,
        unit=unit,
        targets=[
            Target(
                expr=f"sum(ray_data_{metric.name}"
                + "{{{global_filters}}}) by (dataset, operator)",
                legend=legend,
            )
        ],
        fill=fill,
        stack=stack,
    )


DATA_GRAFANA_PANELS = [
    # Ray Data Metrics (Overview)
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
    # Ray Data Metrics (Inputs)
    _generate_grafana_panel_from_metric(
        field_name="num_inputs_received",
        panel_id=17,
        title="Input Blocks Received by Operator",
        unit="blocks",
        legend="Blocks Received: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_task_inputs_processed",
        panel_id=19,
        title="Input Blocks Processed by Tasks",
        unit="blocks",
        legend="Blocks Processed: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="bytes_task_inputs_processed",
        panel_id=20,
        title="Input Bytes Processed by Tasks",
        unit="bytes",
        legend="Bytes Processed: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="bytes_inputs_of_submitted_tasks",
        panel_id=21,
        title="Input Bytes Submitted to Tasks",
        unit="bytes",
        legend="Bytes Submitted: {{dataset}}, {{operator}}",
    ),
    # Ray Data Metrics (Outputs)
    _generate_grafana_panel_from_metric(
        field_name="num_task_outputs_generated",
        panel_id=22,
        title="Blocks Generated by Tasks",
        unit="blocks",
        legend="Blocks Generated: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="bytes_task_outputs_generated",
        panel_id=23,
        title="Bytes Generated by Tasks",
        unit="bytes",
        legend="Bytes Generated: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="rows_task_outputs_generated",
        panel_id=24,
        title="Rows Generated by Tasks",
        unit="rows",
        legend="Rows Generated: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_outputs_taken",
        panel_id=25,
        title="Output Blocks Taken by Downstream Operators",
        unit="blocks",
        legend="Blocks Taken: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="bytes_outputs_taken",
        panel_id=26,
        title="Output Bytes Taken by Downstream Operators",
        unit="bytes",
        legend="Bytes Taken: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_outputs_of_finished_tasks",
        panel_id=27,
        title="Output Blocks from Finished Tasks",
        unit="blocks",
        legend="Blocks Taken: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="bytes_outputs_of_finished_tasks",
        panel_id=28,
        title="Output Bytes from Finished Tasks",
        unit="bytes",
        legend="Bytes Taken: {{dataset}}, {{operator}}",
    ),
    # Ray Data Metrics (Tasks)
    _generate_grafana_panel_from_metric(
        field_name="num_tasks_submitted",
        panel_id=29,
        title="Submitted Tasks",
        unit="tasks",
        legend="Submitted Tasks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_tasks_running",
        panel_id=30,
        title="Running Tasks",
        unit="tasks",
        legend="Running Tasks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_tasks_have_outputs",
        panel_id=31,
        title="Tasks with output blocks",
        unit="tasks",
        legend="Tasks with output blocks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_tasks_finished",
        panel_id=32,
        title="Finished Tasks",
        unit="tasks",
        legend="Finished Tasks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="num_tasks_failed",
        panel_id=33,
        title="Failed Tasks",
        unit="tasks",
        legend="Failed Tasks: {{dataset}}, {{operator}}",
    ),
    Panel(
        id=8,
        title="Block Generation Time",
        description="Time spent generating blocks.",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_block_generation_time{{{global_filters}}}) by (dataset, operator)",
                legend="Block Generation Time: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    _generate_grafana_panel_from_metric(
        field_name="task_submission_backpressure_time",
        panel_id=37,
        title="Task Submission Backpressure Time",
        unit="seconds",
        legend="Backpressure Time: {{dataset}}, {{operator}}",
    ),
    # Ray Data Metrics (Object Store Memory)
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_internal_inqueue_blocks",
        panel_id=13,
        title="Operator Internal Inqueue Size (Blocks)",
        unit="blocks",
        legend="Number of Blocks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_internal_inqueue",
        panel_id=14,
        title="Operator Internal Inqueue Size (Bytes)",
        unit="bytes",
        legend="Bytes Size: {{dataset}}, {{operator}}",
        stack=True,
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_internal_outqueue_blocks",
        panel_id=15,
        title="Operator Internal Outqueue Size (Blocks)",
        unit="blocks",
        legend="Number of Blocks: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_internal_outqueue",
        panel_id=16,
        title="Operator Internal Outqueue Size (Bytes)",
        unit="bytes",
        legend="Bytes Size: {{dataset}}, {{operator}}",
        stack=True,
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_pending_task_inputs",
        panel_id=34,
        title="Size of Blocks used in Pending Tasks (Bytes)",
        unit="bytes",
        legend="Bytes Size: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_freed",
        panel_id=35,
        title="Freed Memory in Object Store (Bytes)",
        unit="bytes",
        legend="Bytes Size: {{dataset}}, {{operator}}",
    ),
    _generate_grafana_panel_from_metric(
        field_name="obj_store_mem_spilled",
        panel_id=36,
        title="Spilled Memory in Object Store (Bytes)",
        unit="bytes",
        legend="Bytes Size: {{dataset}}, {{operator}}",
    ),
    # Ray Data Metrics (Iteration)
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
    # Ray Data Metrics (Miscellaneous)
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
