# flake8: noqa E501
import dataclasses
from typing import Dict

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    Target,
)

# When adding a new panels for an OpRuntimeMetric, follow this format:
# Panel(
#     title=title,
#     description=metric.metadata.get("description"),
#     id=panel_id,
#     unit=unit,
#     targets=[
#         Target(
#             expr=f"sum(ray_data_{metric.name}"
#             + "{{{global_filters}}}) by (dataset, operator)",
#             legend=legend,
#         )
#     ],
#     fill=fill,
#     stack=stack,
# )


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
        title="Bytes Output / Second",
        description="Bytes output per second by dataset operators.",
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_output_bytes{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Output / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=11,
        title="Rows Output / Second",
        description="Total rows output per second by dataset operators.",
        unit="rows/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_output_rows{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Rows Output / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    # Ray Data Metrics (Inputs)
    Panel(
        id=17,
        title="Input Blocks Received by Operator / Second",
        description="Number of input blocks received by operator per second.",
        unit="blocks/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_num_inputs_received{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Blocks Received / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=18,
        title="Input Bytes Received by Operator / Second",
        description="Byte size of input blocks received by operator per second.",
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_bytes_inputs_received{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Received / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=19,
        title="Input Blocks Processed by Tasks / Second",
        description=(
            "Number of input blocks that operator's tasks have finished processing per second."
        ),
        unit="blocks/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_num_task_inputs_processed{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Blocks Processed / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=20,
        title="Input Bytes Processed by Tasks / Second",
        description=(
            "Byte size of input blocks that operator's tasks have finished processing per second."
        ),
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_bytes_task_inputs_processed{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Processed / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=21,
        title="Input Bytes Submitted to Tasks / Second",
        description="Byte size of input blocks passed to submitted tasks per second.",
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_bytes_inputs_of_submitted_tasks{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Submitted / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=22,
        title="Blocks Generated by Tasks / Second",
        description="Number of output blocks generated by tasks per second.",
        unit="blocks/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_num_task_outputs_generated{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Blocks Generated / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=23,
        title="Bytes Generated by Tasks / Second",
        description="Byte size of output blocks generated by tasks per second.",
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_bytes_task_outputs_generated{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Generated / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=24,
        title="Rows Generated by Tasks / Second",
        description="Number of rows in generated output blocks from finished tasks per second.",
        unit="rows/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_rows_task_outputs_generated{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Rows Generated / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=25,
        title="Output Blocks Taken by Downstream Operators / Second",
        description="Number of output blocks taken by downstream operators per second.",
        unit="blocks/sec",
        targets=[
            Target(
                expr="sum(rate(ray_data_num_outputs_taken{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Blocks Taken / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=26,
        title="Output Bytes Taken by Downstream Operators / Second",
        description=(
            "Byte size of output blocks taken by downstream operators per second."
        ),
        unit="Bps",
        targets=[
            Target(
                expr="sum(rate(ray_data_bytes_outputs_taken{{{global_filters}}}[1m])) by (dataset, operator)",
                legend="Bytes Taken / Second: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    # Ray Data Metrics (Tasks)
    Panel(
        id=29,
        title="Submitted Tasks",
        description="Number of submitted tasks.",
        unit="tasks",
        targets=[
            Target(
                expr="sum(ray_data_num_tasks_submitted{{{global_filters}}}) by (dataset, operator)",
                legend="Submitted Tasks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=30,
        title="Running Tasks",
        description="Number of running tasks.",
        unit="tasks",
        targets=[
            Target(
                expr="sum(ray_data_num_tasks_running{{{global_filters}}}) by (dataset, operator)",
                legend="Running Tasks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=31,
        title="Tasks with output blocks",
        description="Number of tasks that already have output.",
        unit="tasks",
        targets=[
            Target(
                expr="sum(ray_data_num_tasks_have_outputs{{{global_filters}}}) by (dataset, operator)",
                legend="Tasks with output blocks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=32,
        title="Finished Tasks",
        description="Number of finished tasks.",
        unit="tasks",
        targets=[
            Target(
                expr="sum(ray_data_num_tasks_finished{{{global_filters}}}) by (dataset, operator)",
                legend="Finished Tasks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=33,
        title="Failed Tasks",
        description="Number of failed tasks.",
        unit="tasks",
        targets=[
            Target(
                expr="sum(ray_data_num_tasks_failed{{{global_filters}}}) by (dataset, operator)",
                legend="Failed Tasks: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=False,
    ),
    Panel(
        id=8,
        title="Block Generation Time",
        description="Time spent generating blocks in tasks.",
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
    Panel(
        id=37,
        title="Task Submission Backpressure Time",
        description="Time spent in task submission backpressure.",
        unit="seconds",
        targets=[
            Target(
                expr="sum(ray_data_task_submission_backpressure_time{{{global_filters}}}) by (dataset, operator)",
                legend="Backpressure Time: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
    ),
    # Ray Data Metrics (Object Store Memory)
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
        description="Byte size of input blocks in the operator's internal input queue.",
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
        description=(
            "Byte size of output blocks in the operator's internal output queue."
        ),
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
    Panel(
        id=34,
        title="Size of Blocks used in Pending Tasks (Bytes)",
        description="Byte size of input blocks used by pending tasks.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_pending_task_inputs{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Size: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
    ),
    Panel(
        id=35,
        title="Freed Memory in Object Store (Bytes)",
        description="Byte size of freed memory in object store.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_freed{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Size: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
    ),
    Panel(
        id=36,
        title="Spilled Memory in Object Store (Bytes)",
        description="Byte size of spilled memory in object store.",
        unit="bytes",
        targets=[
            Target(
                expr="sum(ray_data_obj_store_mem_spilled{{{global_filters}}}) by (dataset, operator)",
                legend="Bytes Size: {{dataset}}, {{operator}}",
            )
        ],
        fill=0,
        stack=True,
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
