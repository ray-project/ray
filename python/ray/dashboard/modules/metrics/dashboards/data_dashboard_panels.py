# ruff: noqa: E501

from dataclasses import replace
from typing import List

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    IdGenerator,
    Panel,
    PanelTemplate,
    Row,
    Target,
    TargetTemplate,
)

# Panel and row ids only have to be unique within this dashboard, so they are
# handed out by `_ids` in declaration order rather than picked by hand. Nothing
# outside this module refers to a Ray Data panel by id; if that ever changes,
# pin that panel with `_ids.reserve(<id>)` so its id stops moving.
_ids = IdGenerator()

# When adding a new panels for an OpRuntimeMetric, follow this format:
# Panel(
#     title=title,
#     description=metric.metadata.get("description"),
#     id=_ids.next(),
#     unit=unit,
#     targets=[
#         Target(
#             expr=f"sum(ray_data_{metric.name}"
#             + "{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)",
#             legend=legend,
#         )
#     ],
#     fill=fill,
#     stack=stack,
# )


# Ray Data Metrics (Overview)
BYTES_SPILLED_PANEL = Panel(
    id=_ids.next(),
    title="Bytes Spilled from Object Store",
    description="Total memory (in bytes) of data blocks moved from the Ray object store to local disk due to memory pressure. This metric is only reported if `DataContext.enable_get_object_locations_for_metrics` is set to True.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_spilled_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Spilled: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

BYTES_FREED_PANEL = Panel(
    id=_ids.next(),
    title="Bytes Freed from Object Store",
    description="Total memory (in bytes) that's been released by dataset operators. This indicates memory that was previously used by blocks and is now available for reuse.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_freed_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Freed: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OBJECT_STORE_MEMORY_PANEL = Panel(
    id=_ids.next(),
    title="Object Store Memory",
    description="Current amount of memory (in bytes) actively consumed within the Ray object store by dataset operators. Use this metric to monitor in-memory data footprint and identify potential memory bottlenecks.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_current_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Current Usage: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

CPU_USAGE_PANEL = Panel(
    id=_ids.next(),
    title="Logical Slots Being Used (CPU)",
    description="Current number of logical CPU cores assigned to running tasks per dataset operator. This tracks logical resource allocation, not actual physical CPU usage.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_data_cpu_usage_cores{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="CPU Usage: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

GPU_USAGE_PANEL = Panel(
    id=_ids.next(),
    title="Logical Slots Being Used (GPU)",
    description="Current number of logical GPU cores allocated and actively consumed by running tasks within operators. This graph is active when specifying num_gpus>0 in Ray remote args. This tracks logical resource allocation, not actual physical GPU usage.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_data_gpu_usage_cores{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="GPU Usage: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

MEMORY_USAGE_PANEL = Panel(
    id=_ids.next(),
    title="Logical Slots Being Used (Memory)",
    description="Current amount of logical heap memory in bytes allocated to running tasks per dataset operator. This tracks logical resource allocation, not actual physical memory usage.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_memory_usage_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Memory Usage: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

BYTES_OUTPUT_PER_SECOND_PANEL = Panel(
    id=_ids.next(),
    title="Bytes Output / Second",
    description="Measures the average rate (in bytes per second) at which operators emit output data (processed results sent to downstream operators). This measures throughput and shows how quickly processed data is produced by operators.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_output_bytes{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Output / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ROWS_OUTPUT_PER_SECOND_PANEL = Panel(
    id=_ids.next(),
    title="Rows Output / Second",
    description="Measures the average rate (in rows per second) at which operators emit output rows (processed results sent to downstream operators). This provides a logical view of throughput, independent of byte size.",
    unit="rows/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_output_rows{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Rows Output / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Inputs)
INPUT_BLOCKS_RECEIVED_PANEL = Panel(
    id=_ids.next(),
    title="Input Blocks Received by Operator / Second",
    description="Measures the average rate (in blocks per second) at which operators receive input blocks from upstream operators or external data sources. This measures the ingress rate of data into an operator.",
    unit="blocks/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_num_inputs_received{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Blocks Received / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INPUT_BYTES_RECEIVED_PANEL = Panel(
    id=_ids.next(),
    title="Input Bytes Received by Operator / Second",
    description="Measures the average rate (in bytes per second) at which operators receive input data. This quantifies data transfer throughput from upstream operators or external data sources.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_inputs_received{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Received / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INPUT_BLOCKS_PROCESSED_PANEL = Panel(
    id=_ids.next(),
    title="Input Blocks Processed by Tasks / Second",
    description=(
        "Measures the average rate (in blocks per second) at which tasks consume input blocks within an operator. This block-level throughput metric measures how quickly tasks read through their assigned input."
    ),
    unit="blocks/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_num_task_inputs_processed{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Blocks Processed / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INPUT_BYTES_PROCESSED_PANEL = Panel(
    id=_ids.next(),
    title="Input Bytes Processed by Tasks / Second",
    description=(
        "Measures the average rate (in bytes per second) at which tasks consume input data within an operator. This byte-level throughput metric measures how quickly tasks read through their assigned input."
    ),
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_task_inputs_processed{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Processed / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INPUT_BYTES_SUBMITTED_PANEL = Panel(
    id=_ids.next(),
    title="Input Bytes Submitted to Tasks / Second",
    description="Measures the average rate (in bytes per second) at which input blocks are passed to newly submitted tasks for execution. This measures how quickly data's being dispatched from an operator's internal queues to worker tasks.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_inputs_of_submitted_tasks{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Submitted / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Outputs)
BLOCKS_GENERATED_PANEL = Panel(
    id=_ids.next(),
    title="Blocks Generated by Tasks / Second",
    description="Measures the raw throughput of block generation across all concurrent tasks submitted by an operator. Tracks the average rate (in blocks per second) at which tasks emit new output blocks.",
    unit="blocks/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Blocks Generated / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

BYTES_GENERATED_PANEL = Panel(
    id=_ids.next(),
    title="Bytes Generated by Tasks / Second",
    description="Measures the raw data throughput of output block generation across all concurrent tasks submitted by an operator. Tracks the average rate (in bytes per second) at which tasks emit new output blocks.",
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Generated / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ROWS_GENERATED_PANEL = Panel(
    id=_ids.next(),
    title="Rows Generated by Tasks / Second",
    description="Measures the logical throughput of output row generation across all concurrent tasks submitted by an operator. Tracks the average rate (in rows per second) at which tasks emit output rows.",
    unit="rows/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_rows_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Rows Generated / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OUTPUT_BLOCKS_TAKEN_PANEL = Panel(
    id=_ids.next(),
    title="Output Blocks Taken by Downstream Operators / Second",
    description="Measures the average rate (in blocks per second) at which downstream operators consume output blocks from this operator. This metric helps identify bottlenecks in data flow between operators.",
    unit="blocks/sec",
    targets=[
        Target(
            expr='sum(rate(ray_data_num_outputs_taken{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Blocks Taken / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OUTPUT_BYTES_TAKEN_PANEL = Panel(
    id=_ids.next(),
    title="Output Bytes Taken by Downstream Operators / Second",
    description=(
        "Measures the average rate (in bytes per second) at which downstream operators consume output data from this operator. This provides a byte-level view of inter-operator data transfer throughput."
    ),
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_outputs_taken{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Bytes Taken / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

AVERAGE_BYTES_PER_BLOCK_PANEL = Panel(
    id=_ids.next(),
    title="Average Bytes Generated / Output Block",
    description="Measures the average byte size of output blocks generated by tasks over a recent 5-minute window. This metric helps understand the granularity of data blocks being produced, which can impact performance and memory usage.",
    unit="bytes",
    targets=[
        Target(
            expr='increase(ray_data_bytes_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Average Bytes Generated / Output Block: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

AVERAGE_BLOCKS_PER_TASK_PANEL = Panel(
    id=_ids.next(),
    title="Average Number of Output Blocks / Task",
    description="Measures the average number of output blocks generated per task over a recent 5-minute window. This indicates how many distinct output blocks each task typically produces upon completion.",
    unit="blocks",
    targets=[
        Target(
            expr='increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_tasks_finished{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Average Number of Output Blocks / Task: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OUTPUT_BYTES_BY_NODE_PANEL = Panel(
    id=_ids.next(),
    title="Output Bytes from Finished Tasks / Second (by Node)",
    description=(
        "Measures the average rate (in bytes per second) of output produced by finished tasks, grouped by the node where tasks completed. This provides a per-node perspective on output throughput."
    ),
    unit="Bps",
    targets=[
        Target(
            expr="sum(rate(ray_data_bytes_outputs_of_finished_tasks_per_node{{{global_filters}}}[1m])) by (dataset, node_ip)",
            legend="Bytes output / Second: {{dataset}}, {{node_ip}}",
        )
    ],
    fill=0,
    stack=False,
)

BLOCKS_BY_NODE_PANEL = Panel(
    id=_ids.next(),
    title="Blocks from Finished Tasks / Second (by Node)",
    description=(
        "Measures the average rate (in blocks per second) of output blocks produced by finished tasks, grouped by the node where tasks completed. This offers a per-node view of logical block throughput."
    ),
    unit="blocks/s",
    targets=[
        Target(
            expr="sum(rate(ray_data_blocks_outputs_of_finished_tasks_per_node{{{global_filters}}}[1m])) by (dataset, node_ip)",
            legend="Blocks output / Second: {{dataset}}, {{node_ip}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Tasks)
SUBMITTED_TASKS_PANEL = Panel(
    id=_ids.next(),
    title="Submitted Tasks",
    description="Cumulative count of tasks submitted to the Ray cluster for execution by dataset operators. This metric indicates the total workload generated by the pipeline.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_submitted{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Submitted Tasks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTIVE_TASKS_PANEL = Panel(
    id=_ids.next(),
    title="Active Tasks",
    description="Tracks the current number of tasks that have been submitted and have not completed (pending or running) across operators. Provides insight into the degree of parallelism currently utilized for data processing.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_running{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Active Tasks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

TASKS_WITH_OUTPUT_PANEL = Panel(
    id=_ids.next(),
    title="Tasks with output blocks",
    description="Current cumulative count of tasks that successfully generated at least one output block, even if the task hasn't yet fully completed. This metric signals early progress in output generation.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_have_outputs{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Tasks with output blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

FINISHED_TASKS_PANEL = Panel(
    id=_ids.next(),
    title="Finished Tasks",
    description="Cumulative count of tasks that completed their execution, either successfully or with failure. This offers a high-level overview of task completion progress.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_finished{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Finished Tasks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

FAILED_TASKS_PANEL = Panel(
    id=_ids.next(),
    title="Failed Tasks",
    description="Cumulative count of tasks that terminated with an error or encountered a failure during execution. This metric is useful for identifying and debugging stability issues within the data pipeline.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_failed{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Failed Tasks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

MAX_USS_PER_TASK_PANEL = Panel(
    id=_ids.next(),
    title="Max Task USS Memory per Operator",
    description="Maximum unique set size (USS) memory usage in bytes among completed tasks for each operator.",
    unit="bytes",
    targets=[
        Target(
            expr='max(ray_data_max_uss_bytes_max{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Max Task USS: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

TASK_THROUGHPUT_BY_NODE_PANEL = Panel(
    id=_ids.next(),
    title="Task Throughput (by Node)",
    description="Average rate (in finished tasks per second) grouped by the node where tasks completed. This metric shows how efficiently different nodes are contributing to overall task processing.",
    unit="tasks/s",
    targets=[
        Target(
            expr="sum(rate(ray_data_num_tasks_finished_per_node{{{global_filters}}}[1m])) by (dataset, node_ip)",
            legend="Finished Tasks: {{dataset}}, {{node_ip}}",
        )
    ],
    fill=0,
    stack=False,
)

BLOCK_GENERATION_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Block Generation Time",
    description="Average time (in seconds) spent by tasks generating their output blocks over a recent 5-minute window. This metric helps pinpoint performance bottlenecks related to data serialization, transformation, or computation of output blocks within tasks.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_block_generation_time{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Block Generation Time: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

BLOCK_TRANSFORM_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Block Transform Time",
    description="Average time (in seconds) a map, read, or write operator spent transforming data per output block over a recent 5-minute window. This covers the whole transform chain: forming the batches or rows the operator's stages consume, running the stage bodies, and building the output blocks. Block Generation Time measures the same blocks as wall clock over everything between them, so it is always the larger of the two and the gap is Ray Data's own per-block work rather than yours. Neither includes the object store write, which ray_data_block_serialization_time_s reports. Only map, read, and write operators report this; shuffles and aggregations don't.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_block_transform_time_s{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Block Transform Time: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

BLOCK_TRANSFORM_TIME_BY_PHASE_PANEL = Panel(
    id=_ids.next(),
    title="Block Transform Time by Phase",
    description="The Block Transform Time panel broken into the three phases that sum to it, averaged per output block over a recent 5-minute window. Input prep is turning input blocks into the batches or rows your functions receive; Function body is the stage bodies themselves, both yours and the ones Ray Data supplies; Output block build is assembling what they return back into blocks. This panel is empty for row-based transforms such as map and filter unless DataContext.accurate_map_phase_timing is set, because timing each row individually costs more than the breakdown reports; the Block Transform Time panel still plots the total.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_input_prep_time_s{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Input Prep: {{dataset}}, {{operator}}",
        ),
        Target(
            expr='increase(ray_data_function_body_time_s{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Function Body: {{dataset}}, {{operator}}",
        ),
        Target(
            expr='increase(ray_data_output_build_time_s{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Output Block Build: {{dataset}}, {{operator}}",
        ),
    ],
    fill=10,
    stack=True,
)

TASK_SUBMISSION_BACKPRESSURE_PANEL = Panel(
    id=_ids.next(),
    title="Task Submission Backpressure Time",
    description="Average time (in seconds) that tasks spend waiting due to backpressure during submission over a recent 5-minute window. High values can indicate saturation of task scheduling resources or insufficient downstream processing capacity to accept new work.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_task_submission_backpressure_time{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_tasks_submitted{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Backpressure Time: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

# Task Completion Time Percentiles
TASK_COMPLETION_TIME_P50_PANEL = Panel(
    id=_ids.next(),
    title="P50 Task Completion Time",
    description="P50 time (in seconds) tasks spend running to completion, including backpressure.",
    targets=[
        Target(
            expr='histogram_quantile(0.5, sum by (operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P90_PANEL = Panel(
    id=_ids.next(),
    title="P90 Task Completion Time",
    description="P90 time (in seconds) tasks spend running to completion, including backpressure.",
    targets=[
        Target(
            expr='histogram_quantile(0.9, sum by (operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P99_PANEL = Panel(
    id=_ids.next(),
    title="P99 Task Completion Time",
    description="P99 time (in seconds) tasks spend running to completion, including backpressure.",
    targets=[
        Target(
            expr='histogram_quantile(0.99, sum by (operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

BLOCK_COMPLETION_TIME_P50_PANEL = Panel(
    id=_ids.next(),
    title="P50 Block Completion Time",
    description="P50 time (in seconds) spent processing blocks to completion. If multiple blocks are generated per task, this is approximated by dividing task time equally among blocks.",
    targets=[
        Target(
            expr='histogram_quantile(0.5, sum by (operator, le) (rate(ray_data_block_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

BLOCK_COMPLETION_TIME_P90_PANEL = Panel(
    id=_ids.next(),
    title="P90 Block Completion Time",
    description="P90 time (in seconds) spent processing blocks to completion. If multiple blocks are generated per task, this is approximated by dividing task time equally among blocks.",
    targets=[
        Target(
            expr='histogram_quantile(0.9, sum by (operator, le) (rate(ray_data_block_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

BLOCK_COMPLETION_TIME_P99_PANEL = Panel(
    id=_ids.next(),
    title="P99 Block Completion Time",
    description="P99 time (in seconds) spent processing blocks to completion. If multiple blocks are generated per task, this is approximated by dividing task time equally among blocks.",
    targets=[
        Target(
            expr='histogram_quantile(0.99, sum by (operator, le) (rate(ray_data_block_completion_time_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="s",
    fill=0,
    stack=False,
)

BLOCK_SIZE_BYTES_P50_PANEL = Panel(
    id=_ids.next(),
    title="P50 Block Size (Bytes)",
    description="P50 block size in bytes.",
    targets=[
        Target(
            expr='histogram_quantile(0.5, sum by (operator, le) (rate(ray_data_block_size_bytes_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="bytes",
    fill=0,
    stack=False,
)

BLOCK_SIZE_BYTES_P90_PANEL = Panel(
    id=_ids.next(),
    title="P90 Block Size (Bytes) Histogram",
    description="P90 block size in bytes. This provides insights into block granularity, which can significantly influence memory usage, I/O efficiency, and task scheduling.",
    targets=[
        Target(
            expr='histogram_quantile(0.9, sum by (operator, le) (rate(ray_data_block_size_bytes_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="bytes",
    fill=0,
    stack=False,
)

BLOCK_SIZE_BYTES_P99_PANEL = Panel(
    id=_ids.next(),
    title="P99 Block Size (Bytes)",
    description="P99 block size in bytes.",
    targets=[
        Target(
            expr='histogram_quantile(0.99, sum by (operator, le) (rate(ray_data_block_size_bytes_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="bytes",
    fill=0,
    stack=False,
)

BLOCK_SIZE_ROWS_P50_PANEL = Panel(
    id=_ids.next(),
    title="P50 Block Size (Rows)",
    description="P50 block size in rows.",
    targets=[
        Target(
            expr='histogram_quantile(0.5, sum by (operator, le) (rate(ray_data_block_size_rows_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="rows",
    fill=0,
    stack=False,
)

BLOCK_SIZE_ROWS_P90_PANEL = Panel(
    id=_ids.next(),
    title="P90 Block Size (Rows) Histogram",
    description="P90 block size in rows. This is useful for understanding the logical size and composition of data units, impacting processing logic and batching strategies.",
    targets=[
        Target(
            expr='histogram_quantile(0.9, sum by (operator, le) (rate(ray_data_block_size_rows_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="rows",
    fill=0,
    stack=False,
)

BLOCK_SIZE_ROWS_P99_PANEL = Panel(
    id=_ids.next(),
    title="P99 Block Size (Rows)",
    description="P99 block size in rows. This is useful for understanding the logical size and composition of data units, impacting processing logic and batching strategies.",
    targets=[
        Target(
            expr='histogram_quantile(0.99, sum by (operator, le) (rate(ray_data_block_size_rows_bucket{{{global_filters}, operator=~"$Operator"}}[$__rate_interval])))',
            legend="{{operator}}",
        ),
    ],
    unit="rows",
    fill=0,
    stack=False,
)

TASK_OUTPUT_BACKPRESSURE_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Task Output Backpressure Time",
    description=(
        "Average time (in seconds) tasks spend waiting due to backpressure when attempting to "
        "output results over a recent 5-minute window. High values indicate operators produce "
        "large outputs or many blocks that consume object store capacity. This may starve "
        "downstream operators waiting for output."
    ),
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_task_output_backpressure_time{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_tasks_finished{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Task Output Backpressure Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_WITHOUT_BACKPRESSURE_PANEL = Panel(
    id=_ids.next(),
    title="Task Completion Time Without Backpressure",
    description="Average time (in seconds) tasks spend executing their core logic, excluding backpressure, over a recent 5-minute window. This metric helps isolate actual computation time from delays caused by data flow bottlenecks, aiding in differentiating between computation-bound and data-flow-bound performance issues.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_task_completion_time_excl_backpressure_s{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_tasks_finished{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Task Completion Time w/o Backpressure: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Object Store Memory)
INTERNAL_INQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator Internal Input Queue Size (Blocks)",
    description="Current number of blocks held within an operator's internal input queue. A continuously growing queue can indicate the operator's processing inputs slower than they're being received, potentially leading to increased memory consumption.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_inqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INTERNAL_INQUEUE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Operator Internal Input Queue Size (Bytes)",
    description="Current total byte size of input blocks stored in an operator's internal input queue. This quantifies the memory footprint of pending input data awaiting processing by the operator's tasks.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_inqueue{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

INTERNAL_OUTQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator Internal Output Queue Size (Blocks)",
    description="Current number of blocks waiting in an operator's internal output queue to be consumed by downstream operators.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_outqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

INTERNAL_OUTQUEUE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Operator Internal Output Queue Size (Bytes)",
    description="Current total byte size of output blocks in an operator's internal output queue. This helps understand memory consumption attributed to buffered output data awaiting transfer to subsequent pipeline operators.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_outqueue{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

EXTERNAL_INQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator External Input Queue Size (Blocks)",
    description="Current number of blocks in an operator's external input queue. This queue holds bundles of blocks dispatched to the operator but not yet fully processed by its tasks, providing an external view of pending work.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_num_external_inqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

EXTERNAL_INQUEUE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Operator External Input Queue Size (bytes)",
    description="Current total byte size of blocks in an operator's external input queue. This quantifies the memory footprint of externally buffered input data, representing data that's assigned to the operator but not yet internally queued.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_num_external_inqueue_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Bytes: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

EXTERNAL_OUTQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator External Output Queue Size (Blocks)",
    description="Current number of blocks in an operator's external output queue. This queue typically stores references to results produced by the operator's tasks and awaiting collection by downstream operators. A large output queue suggests downstream operators aren't consuming data as quickly as it's being produced.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_num_external_outqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

EXTERNAL_OUTQUEUE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Operator External Output Queue Size (bytes)",
    description="Current total byte size of blocks in an operator's external output queue. This helps understand the memory footprint of results produced but still awaiting consumption or transfer. A large output queue suggests downstream operators aren't consuming data as quickly as it's being produced.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_num_external_outqueue_bytes{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Number of Bytes: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# Combined Input Queue and Output Queue Blocks Panel
COMBINED_INQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator Combined Internal + External Input Queue Size (Blocks)",
    description="Total number of blocks across both the operator's internal and external input queues. This provides a comprehensive view of all pending input blocks that are either being held internally or awaiting processing by the operator.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_inqueue_blocks{{{global_filters}, operator=~"$Operator"}} + ray_data_num_external_inqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Combined Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

COMBINED_OUTQUEUE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Operator Combined Internal + External Output Queue Size (Blocks)",
    description="Total number of blocks across both the operator's internal and external output queues. This gives a complete picture of all produced buffered output blocks and awaiting consumption by downstream operators.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_internal_outqueue_blocks{{{global_filters}, operator=~"$Operator"}} + ray_data_num_external_outqueue_blocks{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Combined Blocks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

PENDING_TASK_INPUTS_PANEL = Panel(
    id=_ids.next(),
    title="Size of Blocks used in Pending Tasks (Bytes)",
    description="Current total byte size of input blocks referenced by submitted tasks not yet running or in a pending state. This represents memory that's conceptually 'reserved' for upcoming task execution.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_pending_task_inputs{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

FREED_MEMORY_PANEL = Panel(
    id=_ids.next(),
    title="Freed Memory in Object Store (Bytes)",
    description="Total byte size of memory that's been deallocated from the Ray object store by operators. This reflects the efficiency of memory recycling within the pipeline and indicates memory no longer in use.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_freed{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

SPILLED_MEMORY_PANEL = Panel(
    id=_ids.next(),
    title="Spilled Memory in Object Store (Bytes)",
    description="Total byte size of memory from the Ray object store that's been written to external storage (spilled to disk). This directly indicates instances of memory pressure where data couldn't be held entirely in-memory.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_obj_store_mem_spilled{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

# Ray Data Metrics (Iteration)
ITERATION_INITIALIZATION_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Initialization Time",
    description="Total time (in seconds) spent setting up and initializing the data iterator before it begins yielding batches. This includes overhead such as establishing connections, resolving data sources, and preparing internal structures.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_initialize_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKED_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Blocked Time",
    description="Total time (in seconds) that the user's application thread is blocked while waiting for `iter_batches()` to produce data. High values indicate the data pipeline isn't generating batches fast enough to keep up with consumption rate, pointing to upstream bottlenecks.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_total_blocked_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_USER_PANEL = Panel(
    id=_ids.next(),
    title="Iteration User Time",
    description="Total time (in seconds) spent executing user-defined code during data iteration. This includes time spent in UDFs (User-Defined Functions) and custom batch processing logic, useful for profiling user code performance.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_user_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_GET_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Get Time",
    description="Total time (in seconds) spent performing `ray.get()` calls to resolve Ray object references into actual data blocks during iteration. This indicates latency associated with fetching data from the Ray object store, potentially across the network.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_get_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_NEXT_BATCH_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Next Batch Time",
    description="Total time (in seconds) spent retrieving the next batch of data from the internal block buffer of the iterator. This is a fine-grained measure of the efficiency of the batching mechanism before formatting or collation.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_next_batch_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_FORMAT_BATCH_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Format Batch Time",
    description="Total time (in seconds) spent converting raw data blocks into the desired output format (e.g., Pandas DataFrame, PyArrow Table, NumPy array) for consumption by the user or a machine learning framework. This reflects the cost of data marshalling.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_format_batch_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_COLLATE_BATCH_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Collate Batch Time",
    description="Total time (in seconds) spent applying a `CollateFn` to batches, typically for deep learning frameworks like PyTorch. This includes operations such as stacking tensors, padding, or moving data to a specific device like a GPU.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_collate_batch_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_FINALIZE_BATCH_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Finalize Batch Time",
    description="Total time (in seconds) spent in any final processing steps applied to a batch before it's yielded to the user, as defined by a `finalize_fn`. This can include last-minute transformations or device transfers.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_finalize_batch_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_LOCAL_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Blocks Local",
    description="Cumulative count of blocks found on the local node (same node as the consuming application) during iteration. Accessing local blocks is generally faster and more efficient as it avoids network transfer.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_blocks_local{{{global_filters}}}) by (dataset, split)",
            legend="Blocks: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_REMOTE_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Blocks Remote",
    description="Cumulative count of blocks that needed to be fetched from a remote node (different node in the Ray cluster) during iteration. A high number of remote blocks can indicate significant network transfer overhead, potentially bottlenecking iteration performance.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_blocks_remote{{{global_filters}}}) by (dataset, split)",
            legend="Blocks: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_UNKNOWN_LOCATION_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Blocks Unknown Location",
    description="Cumulative count of blocks for which the location (local or remote) couldn't be determined during iteration. This might suggest issues with the Ray object store's metadata tracking or liveness of relevant Ray nodes.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_unknown_location{{{global_filters}}}) by (dataset, split)",
            legend="Blocks: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_PREFETCHED_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Prefetched Bytes",
    description="Current bytes of prefetched blocks in the iterator",
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_iter_prefetched_bytes{{{global_filters}}}) by (dataset, split)",
            legend="Prefetched Bytes: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_TIME_TO_FIRST_BATCH_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Time to First Batch",
    description="Seconds spent waiting for the first batch after starting iteration",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_time_to_first_batch_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_GET_REF_BUNDLES_PANEL = Panel(
    id=_ids.next(),
    title="Iteration Get Ref Bundles Time",
    description="Seconds spent getting RefBundles from the dataset iterator",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}) by (dataset, split)",
            legend="Seconds: {{dataset}}, {{split}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Miscellaneous)
SCHEDULING_LOOP_DURATION_PANEL = Panel(
    id=_ids.next(),
    title="Scheduling Loop Duration",
    description="Average duration (in seconds) of the Ray Data scheduling loop over a recent 5-minute window. This loop is responsible for managing task submission, resource allocation, and overall execution flow. Longer durations may indicate scheduling overhead or contention within the system.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_sched_loop_duration_s{{{global_filters}}}) by (dataset)",
            legend="Scheduling Loop Duration: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

MAX_BYTES_TO_READ_PANEL = Panel(
    id=_ids.next(),
    title="Max Bytes to Read",
    description="Maximum number of bytes that the streaming generator buffer's configured to read. This helps manage memory usage and apply backpressure for streaming data sources.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_max_bytes_to_read{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Max Bytes to Read: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Cluster Autoscaler)
# Default threshold for scaling up is 75%.
# TODO: This should depend on what the cluster autoscaler is configured to use, rather
# than hardcoded here.
DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD = 75

CLUSTER_CPU_UTILIZATION_PANEL = Panel(
    id=_ids.next(),
    title="Cluster utilization % (CPU)",
    description="Average cluster CPU utilization percentage used by Ray Data. When utilization exceeds the scaling threshold (default 75%), the cluster autoscaler may request additional resources.",
    unit="percent",
    targets=[
        Target(
            expr="sum(ray_data_cluster_cpu_utilization{{{global_filters}}}) by (dataset)",
            legend="CPU Utilization %: {{dataset}}",
        ),
        # Constant threshold line rendered as dotted via seriesOverrides for legend "MAX"
        Target(
            expr=str(DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD),
            legend="MAX",
        ),
    ],
    fill=0,
    stack=False,
    template=PanelTemplate.GRAPH,
)

CLUSTER_GPU_UTILIZATION_PANEL = Panel(
    id=_ids.next(),
    title="Cluster utilization % (GPU)",
    description="Average cluster GPU utilization percentage used by Ray Data. When utilization exceeds the scaling threshold (default 75%), the cluster autoscaler may request additional resources.",
    unit="percent",
    targets=[
        Target(
            expr="sum(ray_data_cluster_gpu_utilization{{{global_filters}}}) by (dataset)",
            legend="GPU Utilization %: {{dataset}}",
        ),
        Target(
            expr=str(DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD),
            legend="MAX",
        ),
    ],
    fill=0,
    stack=False,
    template=PanelTemplate.GRAPH,
)

CLUSTER_MEMORY_UTILIZATION_PANEL = Panel(
    id=_ids.next(),
    title="Cluster utilization % (Memory)",
    description="Average cluster memory utilization percentage used by Ray Data. When utilization exceeds the scaling threshold (default 75%), the cluster autoscaler may request additional resources.",
    unit="percent",
    targets=[
        Target(
            expr="sum(ray_data_cluster_mem_utilization{{{global_filters}}}) by (dataset)",
            legend="Memory Utilization %: {{dataset}}",
        ),
        Target(
            expr=str(DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD),
            legend="MAX",
        ),
    ],
    fill=0,
    stack=False,
    template=PanelTemplate.GRAPH,
)

CLUSTER_OBJECT_STORE_MEMORY_UTILIZATION_PANEL = Panel(
    id=_ids.next(),
    title="Cluster utilization % (Object Store Memory)",
    description="Average cluster object store memory utilization percentage used by Ray Data. When utilization exceeds the scaling threshold (default 75%), the cluster autoscaler may request additional resources.",
    unit="percent",
    targets=[
        Target(
            expr="sum(ray_data_cluster_object_store_memory_utilization{{{global_filters}}}) by (dataset)",
            legend="Object Store Memory Utilization %: {{dataset}}",
        ),
        Target(
            expr=str(DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD),
            legend="MAX",
        ),
    ],
    fill=0,
    stack=False,
    template=PanelTemplate.GRAPH,
)

# Budget Panels
CPU_BUDGET_PANEL = Panel(
    id=_ids.next(),
    title="Budget (CPU)",
    description="Displays the allocated CPU budget (maximum CPU cores) for an operator. Ray Data uses this internal mechanism to manage resource allocation, control concurrency across operators, and prevent overload.",
    unit="cpu",
    targets=[
        Target(
            expr='sum(ray_data_cpu_budget{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Budget (CPU): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

GPU_BUDGET_PANEL = Panel(
    id=_ids.next(),
    title="Budget (GPU)",
    description="Displays the allocated GPU budget (maximum GPU resources) for an operator. Ray Data uses this to control GPU resource consumption across operators.",
    unit="gpu",
    targets=[
        Target(
            expr='sum(ray_data_gpu_budget{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Budget (GPU): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

MEMORY_BUDGET_PANEL = Panel(
    id=_ids.next(),
    title="Budget (Memory)",
    description="Displays the allocated total memory budget (object store and heap memory combined) for an operator. Ray Data uses this to manage overall memory consumption and apply backpressure to prevent out-of-memory errors.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_memory_budget{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Budget (Memory): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OBJECT_STORE_MEMORY_BUDGET_PANEL = Panel(
    id=_ids.next(),
    title="Budget (Object Store Memory)",
    description=(
        "Displays the allocated object store memory budget for an operator. This represents "
        "the portion of the total memory budget dedicated to storing data blocks in the Ray "
        "object store. The budget breaks down into pending task inputs, pending task outputs, "
        "and input and output queues."
    ),
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_object_store_memory_budget{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Budget (Object Store Memory): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ALL_RESOURCES_UTILIZATION_PANEL = Panel(
    id=_ids.next(),
    title="All logical resources utilization",
    description="Displays combined CPU and GPU resource utilization across operators. Filter to a specific operator to understand its overall resource consumption pattern.",
    unit="cores",
    targets=[
        Target(
            expr='sum(ray_data_cpu_usage_cores{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="CPU: {{dataset}}, {{operator}}",
        ),
        Target(
            expr='sum(ray_data_gpu_usage_cores{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="GPU: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

OPERATOR_TASK_COMPLETION_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Task Completion Time Histogram (s)",
    description="Time (in seconds) spent running tasks to completion, including backpressure. Larger bars mean more tasks finished within that duration range.",
    targets=[
        Target(
            expr='sum by (le) (max_over_time(ray_data_task_completion_time_bucket{{{global_filters}, operator=~"$Operator", le!="+Inf"}}[$__range]))',
            legend="{{le}} s",
            template=TargetTemplate.HISTOGRAM_BAR_CHART,
        ),
    ],
    unit="short",
    fill=0,
    stack=False,
    template=PanelTemplate.BAR_CHART,
)

OPERATOR_BLOCK_COMPLETION_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Block Completion Time Histogram (s)",
    description="Time (in seconds) spent processing blocks to completion. If multiple blocks are generated per task, this is approximated by dividing task time equally among blocks. Larger bars mean more blocks finished within that duration range.",
    targets=[
        Target(
            expr='sum by (le) (max_over_time(ray_data_block_completion_time_bucket{{{global_filters}, operator=~"$Operator", le!="+Inf"}}[$__range]))',
            legend="{{le}} s",
            template=TargetTemplate.HISTOGRAM_BAR_CHART,
        ),
    ],
    unit="short",
    fill=0,
    stack=False,
    template=PanelTemplate.BAR_CHART,
)

OPERATOR_BLOCK_SIZE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Block Size (Bytes) Histogram",
    description="Block size in bytes. Larger bars mean more blocks are within that size range.",
    targets=[
        Target(
            expr='sum by (le) (max_over_time(ray_data_block_size_bytes_bucket{{{global_filters}, operator=~"$Operator", le!="+Inf"}}[$__range]))',
            legend="{{le}} bytes",
            template=TargetTemplate.HISTOGRAM_BAR_CHART,
        ),
    ],
    unit="short",
    fill=0,
    stack=False,
    template=PanelTemplate.BAR_CHART,
    # We hide the X axis because the values are too large to fit and they are not useful.
    # We also cannot format it to higher units so it has too many digits.
    hideXAxis=True,
)

OPERATOR_BLOCK_SIZE_ROWS_PANEL = Panel(
    id=_ids.next(),
    title="Block Size (Rows) Histogram",
    description="Block size in rows. Larger bars mean more blocks are within that row count range.",
    targets=[
        Target(
            expr='sum by (le) (max_over_time(ray_data_block_size_rows_bucket{{{global_filters}, operator=~"$Operator", le!="+Inf"}}[$__range]))',
            legend="{{le}} rows",
            template=TargetTemplate.HISTOGRAM_BAR_CHART,
        ),
    ],
    unit="short",
    fill=0,
    stack=False,
    template=PanelTemplate.BAR_CHART,
    # We hide the X axis because the values are too large to fit and they are not useful.
    # We also cannot format it to higher units so it has too many digits.
    hideXAxis=True,
)

# Actor draining: a preempted node shows up
# as N independent actor drains. A draining actor takes no new tasks and is
# torn down only once it has no in-flight tasks and no unconsumed outputs.
# Outcomes: completed (torn down empty), interrupted (died still owing work),
# cancelled (revived by a reclaim/core restart). The reason tag records why
# the drain STARTED and never changes, so e.g. interrupted[resize] means an
# actor draining for a resize died mid-drain (typically its node was lost).
ACTOR_DRAINING_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Draining Actors",
    description="Actors currently draining, per operator.",
    unit="actors",
    targets=[
        Target(
            expr='sum(ray_data_num_draining_actors{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Draining: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_DRAIN_OUTCOMES_PANEL = Panel(
    id=_ids.next(),
    title="Actor Drain Outcomes",
    description=(
        "Actors whose drain ended over the last 5m, by outcome. completed: "
        "torn down with nothing left to serve | interrupted: the actor died "
        "mid-drain (e.g. its node was torn down under it) | cancelled: "
        "rejoined the pool (cause: reclaimed by an upscale). The reason says "
        "why the drain started, not why it ended."
    ),
    unit="actors",
    targets=[
        Target(
            expr='sum(increase(ray_data_actor_drains_completed_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason)',
            legend="completed [{{reason}}]: {{dataset}}, {{operator}}",
        ),
        Target(
            expr='sum(increase(ray_data_actor_drains_interrupted_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason)',
            legend="interrupted [{{reason}}]: {{dataset}}, {{operator}}",
        ),
        Target(
            expr='sum(increase(ray_data_actor_drains_cancelled_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason, cause)',
            legend="cancelled [{{reason}}/{{cause}}]: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_DRAIN_STARTS_BY_REASON_PANEL = Panel(
    id=_ids.next(),
    title="Actor Drains Started by Reason",
    description=(
        "Actors that started draining over the last 5m, split by why the "
        "drain began. resize: a downscale or a completed operator's release "
        "| preemption: the node is being taken away (e.g. spot) | "
        "alloc_shrink: the dataset's allocation shrank and the actor was "
        "evicted from a reclaimed node | unknown: the caller did not say."
    ),
    unit="actors",
    targets=[
        Target(
            expr='sum(increase(ray_data_actor_drains_started_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason)',
            legend="{{reason}}: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_DRAIN_COMPLETED_TIME_PANEL = Panel(
    id=_ids.next(),
    title="Actor Drain Duration (completed)",
    description=(
        "Time from drain start to graceful teardown. Quantiles are cumulative "
        "over the run (drains are rare events, so windowed rates would render "
        "as disconnected dashes) and are bucket-quantized: with few samples "
        "they pin to a bucket boundary rather than a true percentile."
    ),
    unit="s",
    targets=[
        Target(
            expr='histogram_quantile(0.5, sum by (dataset, reason, le) (ray_data_actor_drain_completed_time_bucket{{{global_filters}, operator=~"$Operator"}}))',
            legend="p50 [{{reason}}]: {{dataset}}",
        ),
        Target(
            expr='histogram_quantile(0.9, sum by (dataset, reason, le) (ray_data_actor_drain_completed_time_bucket{{{global_filters}, operator=~"$Operator"}}))',
            legend="p90 [{{reason}}]: {{dataset}}",
        ),
        Target(
            expr='histogram_quantile(0.99, sum by (dataset, reason, le) (ray_data_actor_drain_completed_time_bucket{{{global_filters}, operator=~"$Operator"}}))',
            legend="p99 [{{reason}}]: {{dataset}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_DRAINING_PANELS = [
    ACTOR_DRAINING_ACTORS_PANEL,
    ACTOR_DRAIN_OUTCOMES_PANEL,
    ACTOR_DRAIN_STARTS_BY_REASON_PANEL,
    ACTOR_DRAIN_COMPLETED_TIME_PANEL,
]

# Input locality. Recorded by actor pools under both backends (the standard
# operator records it too), so it sits in the Inputs row; the id is in the
# range this tree uses for its own panels.
CROSS_NODE_INPUT_TRANSFER_PANEL = Panel(
    id=_ids.next(),
    title="Cross-Node Input Transfer / Second",
    description=(
        "Rate of input bytes an operator's actors read from a node other than "
        "their own (fetched over the network). A sustained high rate means "
        "tasks are being scheduled away from the nodes that hold their input "
        "blocks. Block counts are in ray_data_num_remote_blocks_read."
    ),
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_remote_inputs_read{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, operator)',
            legend="Remote Input Bytes / Second: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# --- Actor-only backend -------------------------------------------------------
# Metrics below are emitted only under RAY_DATA_ACTOR_ONLY_BACKEND (see
# experimental/execution/actor_only_metrics.py); the rows stay empty otherwise.

ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Object Store Live by Inputs/Outputs (Bytes)",
    description="Live object-store usage in bytes on each node attributed to dataset inputs/outputs (actor-only ResourceBank).",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="bytes", breakdown=~"inputs|outputs"}}) by (dataset, node, breakdown)',
            legend="{{breakdown}}: {{dataset}}, {{node}}",
        ),
        # inputs + outputs = total live object store per node.
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="bytes", breakdown=~"inputs|outputs"}}) by (dataset, node)',
            legend="total: {{dataset}}, {{node}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Object Store Live by Inputs/Outputs (Blocks)",
    description="Live object-store usage in blocks on each node attributed to dataset inputs/outputs (actor-only ResourceBank).",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="blocks", breakdown=~"inputs|outputs"}}) by (dataset, node, breakdown)',
            legend="{{breakdown}}: {{dataset}}, {{node}}",
        ),
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="blocks", breakdown=~"inputs|outputs"}}) by (dataset, node)',
            legend="total: {{dataset}}, {{node}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_NODE_OBJ_STORE_FREED_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Freed Output (Bytes)",
    description="Cumulative output bytes freed from the live object store on each node (outputs ever pulled minus outputs still live).",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="bytes", breakdown="freed"}}) by (dataset, node)',
            legend="Freed: {{dataset}}, {{node}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_NODE_OBJ_STORE_FREED_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Freed Output (Blocks)",
    description="Cumulative output blocks freed from the live object store on each node (outputs ever pulled minus outputs still live).",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="blocks", breakdown="freed"}}) by (dataset, node)',
            legend="Freed: {{dataset}}, {{node}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_NODE_LOCALITY_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Locality Bytes",
    description="Cumulative local vs remote bytes on each node. Remote applies only to task inputs; outputs are always local to the producing task.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="bytes", breakdown=~"local|remote"}}) by (dataset, node, breakdown)',
            legend="{{breakdown}}: {{dataset}}, {{node}}",
        )
    ],
    fill=1,
    stack=True,
)

ACTOR_ONLY_NODE_NUM_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Actors per Node by Status",
    description="Number of actors on each node by lifecycle status (pending/idle/active/restarting/draining), aggregated across actor-pool operators.",
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_num_actors{{{global_filters}, node=~"$NodeID"}}) by (dataset, node, status)',
            legend="{{status}}: {{dataset}}, {{node}}",
        )
    ],
    fill=1,
    stack=True,
)

ACTOR_ONLY_NODE_OP_NUM_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Actors per Node/Operator by Status",
    description="Number of actors on each node for each operator by lifecycle status.",
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_operator_num_actors{{{global_filters}, node=~"$NodeID", operator=~"$Operator"}}) by (dataset, node, operator, status)',
            legend="{{status}}: {{dataset}}, {{operator}}, {{node}}",
        )
    ],
    fill=1,
    stack=True,
)

ACTOR_ONLY_OP_NUM_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Actors per Operator by Status",
    description="Number of actors for each operator by lifecycle status, summed across all nodes.",
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_operator_num_actors{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator, status)',
            legend="{{status}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=1,
    stack=True,
)

# Input locality: five panels over one metric,
# ``ray_data_actor_only_node_operator_input_locality{node, operator, unit,
# breakdown}``: two cumulative roll-ups (bytes, blocks) and three windowed
# percentages at each grouping.
_LOCALITY_METRIC = "ray_data_actor_only_node_operator_input_locality"
_LOCALITY_WINDOW = "1m"


def _locality_selector(group_by: str, unit: str) -> str:
    """Label matchers for a locality query at the given grouping.

    Only the dimensions the panel groups by are filtered: a roll-up over nodes
    covers every node, so scoping it to $NodeID would silently make it a
    partial roll-up.
    """
    selector = f'unit="{unit}"'
    if "node" in group_by:
        selector += ', node=~"$NodeID"'
    if "operator" in group_by:
        selector += ', operator=~"$Operator"'
    return selector


def _locality_cumulative_expr(unit: str) -> str:
    """Cumulative local/remote input for each operator, split by breakdown."""
    # Concatenated rather than an f-string: the expression must survive
    # `.format(global_filters=...)`, so PromQL braces stay doubled while
    # `{global_filters}` stays single.
    selector = _locality_selector("dataset, operator", unit)
    return (
        "sum("
        + _LOCALITY_METRIC
        + "{{{global_filters}, "
        + selector
        + "}}) by (dataset, operator, breakdown)"
    )


def _locality_pct_expr(group_by: str) -> str:
    """Percent of input bytes that were node-local over the last window.

    Cumulative sums answer "how did the run go"; this answers "what is
    scheduling deciding right now".

    The underlying Gauge carries run-long cumulatives, and an (operator, node)
    pair keeps being written for the rest of the execution once it first
    appears, so each series only grows within a run: it is a counter in all
    but type. ``increase()`` is therefore the right window function; a drop to
    0 only happens when a series restarts (e.g. a new driver process) and is
    treated as a counter reset rather than a large negative ``delta()``.
    """
    selector = _locality_selector(group_by, "bytes")

    def _windowed_sum(breakdown_matcher: str) -> str:
        return (
            "sum(increase("
            + _LOCALITY_METRIC
            + "{{{global_filters}, "
            + selector
            + ", "
            + breakdown_matcher
            + "}}["
            + _LOCALITY_WINDOW
            + "])) by ("
            + group_by
            + ")"
        )

    # No clamp on the denominator: an idle window gives 0/0 -> NaN -> a gap, so
    # "nothing submitted" stays visually distinct from "0% local".
    return (
        "100 * "
        + _windowed_sum('breakdown="local"')
        + " / "
        + _windowed_sum('breakdown=~"local|remote"')
    )


ACTOR_ONLY_OP_INPUT_LOCALITY_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Input Locality (Bytes)",
    description=(
        "Cumulative input bytes submitted to each operator, split by whether the "
        "block was already on the node of the actor running the task."
    ),
    unit="bytes",
    targets=[
        Target(
            expr=_locality_cumulative_expr("bytes"),
            legend="{{breakdown}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=1,
    stack=True,
)

ACTOR_ONLY_OP_INPUT_LOCALITY_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Input Locality (Blocks)",
    description=(
        "Cumulative input blocks submitted to each operator, split by whether the "
        "block was already on the node of the actor running the task."
    ),
    unit="blocks",
    targets=[
        Target(
            expr=_locality_cumulative_expr("blocks"),
            legend="{{breakdown}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=1,
    stack=True,
)

ACTOR_ONLY_NODE_OP_INPUT_LOCALITY_PCT_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Input Locality % (1m, per Node/Operator)",
    description=(
        "Share of input bytes submitted in the last minute that were already on "
        "the actor's node, for each operator on each node. Shows whether "
        "placement and dispatch improve locality over a run. Gaps mean no input "
        "was submitted for that pair during the window."
    ),
    unit="percent",
    targets=[
        Target(
            expr=_locality_pct_expr("dataset, node, operator"),
            legend="Local %: {{dataset}}, {{operator}}, {{node}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_INPUT_LOCALITY_PCT_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Input Locality % (1m, per Operator)",
    description=(
        "Share of input bytes submitted in the last minute that were already on "
        "the actor's node, per operator (summed across nodes). Source operators "
        "sit near 0% by construction: their inputs are read tasks or file "
        "manifests, not the file data itself."
    ),
    unit="percent",
    targets=[
        Target(
            expr=_locality_pct_expr("dataset, operator"),
            legend="Local %: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_NODE_INPUT_LOCALITY_PCT_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Input Locality % (1m, per Node)",
    description=(
        "Share of input bytes submitted in the last minute that were already on "
        "the actor's node, per node (summed across operators). A node that stays "
        "low is pulling most of its actors' inputs over the network. Counts task "
        "inputs only, unlike the 'Node Locality Bytes' panel."
    ),
    unit="percent",
    targets=[
        Target(
            expr=_locality_pct_expr("dataset, node"),
            legend="Local %: {{dataset}}, {{node}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_ESCAPE_HATCH_GRANTS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Escape Hatch Grants by Reason",
    description="Times an escape hatch raised output limits to break a deadlock, per 5m. upgrade: raised an operator's per-actor output limit because its consumer was starved | withheld: gave one actor more room because it was deadlocked on outputs withheld by lineage reconstruction.",
    unit="short",
    targets=[
        Target(
            expr='sum(increase(ray_data_actor_only_escape_hatch_grants_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason)',
            legend="{{reason}}: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_ESCAPE_HATCH_GRANTED_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Escape Hatch Granted Bytes",
    description="Output-limit bytes the escape hatches granted, per 5m, by reason and by where the bytes came from. free_space: free object store on the node | lent: other actors' unused budget on the node, whose limits go down by as much | unbacked: beyond both, so the node is oversubscribed and may spill.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(increase(ray_data_actor_only_escape_hatch_granted_bytes_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, reason, cause)',
            legend="{{reason}} {{cause}}: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=True,
)

ACTOR_ONLY_OP_OUTPUT_LIMIT_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Output Limit (Bytes)",
    description="Snapshot distribution (min/p50/p90/max/mean) of per-actor output byte limits for each operator. Unbounded (inf) limits are excluded.",
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_output_limit{{{global_filters}, unit="bytes", operator=~"$Operator"}}) by (dataset, operator, stat)',
            legend="{{stat}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_OUTPUT_LIMIT_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Output Limit (Blocks)",
    description="Snapshot distribution (min/p50/p90/max/mean) of per-actor output block limits for each operator. Unbounded (inf) limits are excluded.",
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_output_limit{{{global_filters}, unit="blocks", operator=~"$Operator"}}) by (dataset, operator, stat)',
            legend="{{stat}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_OUTPUT_BACKPRESSURED_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Output-Backpressured Actors",
    description="Number of an operator's actors whose output pull budget is exhausted (output-backpressured).",
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_output_backpressured_actors{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Backpressured Actors: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_OUTPUT_OVERSHOOTS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Operator Output-Limit Overshoots",
    description=(
        "Cumulative count, per operator, of output-limit overshoots by kind: "
        "'block_size' (a produced block exceeded 150% of its target block "
        "size) and 'output_bytes' (an actor's outstanding output exceeded 150% "
        "of its output-bytes limit)."
    ),
    unit="short",
    targets=[
        Target(
            expr='sum(increase(ray_data_actor_only_operator_output_overshoots_total{{{global_filters}, operator=~"$Operator"}}[5m])) by (dataset, operator, kind)',
            legend="Overshoots / 5m: {{kind}}, {{dataset}}, {{operator}}",
        ),
        Target(
            expr='sum(ray_data_actor_only_operator_output_overshoots_total{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator, kind)',
            legend="Overshoots Total: {{kind}}, {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_WITHHELD_OUTPUTS_BYTES_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Withheld Outputs (Bytes)",
    description=(
        "Snapshot distribution (min/p50/p90/max/mean) of per-actor output bytes "
        "withheld by lineage reconstruction until their child's whole input set "
        "is re-made. They count against their actors' output budgets, but no "
        "consumer can drain them yet."
    ),
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_withheld_outputs{{{global_filters}, unit="bytes", operator=~"$Operator"}}) by (dataset, operator, stat)',
            legend="{{stat}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_WITHHELD_OUTPUTS_BLOCKS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Withheld Outputs (Blocks)",
    description=(
        "Snapshot distribution (min/p50/p90/max/mean) of per-actor blocks "
        "withheld by lineage reconstruction until their child's whole input set "
        "is re-made."
    ),
    unit="blocks",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_withheld_outputs{{{global_filters}, unit="blocks", operator=~"$Operator"}}) by (dataset, operator, stat)',
            legend="{{stat}}: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_WITHHELD_DEADLOCKED_ACTORS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Actors Deadlocked on Withheld Outputs",
    description=(
        "Actors whose output budgets are full of withheld outputs and that no "
        "amount of waiting frees. The escape hatch grants one of them more "
        "room per pass, so this should drop back to 0 quickly."
    ),
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_operator_withheld_deadlocked_actors{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Deadlocked Actors: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)


# Overview: totals only. The detailed rows keep the breakdowns.
ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BYTES_TOTALS_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Node Object Store Live Total (Bytes)",
    description=(
        "Total live object-store usage in bytes on each node attributed to "
        "dataset inputs + outputs (actor-only ResourceBank). See Object Store "
        "Usage for the inputs/outputs breakdown."
    ),
    unit="bytes",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_obj_store{{{global_filters}, node=~"$NodeID", unit="bytes", breakdown=~"inputs|outputs"}}) by (dataset, node)',
            legend="total: {{dataset}}, {{node}}",
        ),
    ],
    fill=0,
    stack=False,
)

ACTOR_ONLY_OP_NUM_ACTORS_TOTAL_PANEL = Panel(
    id=_ids.next(),
    title="Actor-Only Total Actors per Operator",
    description=(
        "Total actors for each operator (pending + idle + active + restarting + "
        "draining), summed across all nodes. See Actor Status for the "
        "per-status breakdown."
    ),
    unit="short",
    targets=[
        Target(
            expr='sum(ray_data_actor_only_node_operator_num_actors{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Total Actors: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# --- Actor Only Sizer -------------------------------------------------------

# Fixed colors keyed on the operator's position suffix (Ray Data names
# operators `<name>_<position>`), so an operator's up (positive) and down
# (negative) series share a color for any pipeline, and colors are consistent
# across the sizer panels. `/_0$/` cannot match `..._10`: the character before
# the trailing 0 must be the underscore.
SIZER_OPERATOR_COLOR_OVERRIDES = [
    {
        "matcher": {"id": "byRegexp", "options": f"/_{position}$/"},
        "properties": [
            {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
        ],
    }
    for position, color in enumerate(
        [
            "blue",
            "#FADE2A",
            "orange",
            "purple",
            "red",
            "#dbe7d9",
            "green",
            "#8AB8FF",
        ]
    )
]

# Appended to every sizer actor-delta panel: those panels count only what the
# sizer's own passes did, so a pool that changed size because the optimizer
# moved actors shows no bar here. Point readers at the row that does show it.
SIZER_OPTIMIZER_NOTE = (
    " Actor moves made by the pipeline optimizer (capacity transfers, "
    "silent-bottleneck grants) are not counted here -- see the Actor Only "
    "Pipeline Optimizer row."
)

# Coarse buckets keep per-interval bars readable, with the change printed on
# each bar.
SIZER_PER_INTERVAL_TEMPLATE_OVERRIDES = {
    "maxDataPoints": 30,
    "options": {"showValue": "auto", "text": {"valueSize": 10}},
}


def _sizer_delta_selector(phase: str, direction: str) -> str:
    return (
        "ray_data_sizer_actor_delta_total{{{global_filters}, "
        f'operator=~"$Operator", phase="{phase}", direction="{direction}"}}}}'
    )


def _sizer_delta_total(phase: str, direction: str, offset: str = "") -> str:
    return (
        "sum("
        + _sizer_delta_selector(phase, direction)
        + offset
        + ") by (dataset, operator)"
    )


def _per_interval_expr(phase: str, direction: str) -> str:
    """Change per $__interval of a cumulative counter, counting a series' first
    sample as a jump from zero. increase() drops the birth value, which hides
    scaling that finishes before the series' first export (e.g. fixed-size
    pools placed at bootstrap).
    """
    total = _sizer_delta_total(phase, direction)
    prev = _sizer_delta_total(phase, direction, offset=" offset $__interval")
    return "round(" + total + " - (" + prev + " or 0 * " + total + "))"


def _sizer_up_down_targets(phase: str, per_interval: bool) -> List[Target]:
    """Up as positive bars, down as negative bars, per operator."""
    if per_interval:
        up_expr = _per_interval_expr(phase, "up")
        down_expr = "-1 * " + _per_interval_expr(phase, "down")
        interval = "1m"
    else:
        up_expr = _sizer_delta_total(phase, "up")
        down_expr = "-1 * " + _sizer_delta_total(phase, "down")
        interval = None
    return [
        Target(
            expr=expr,
            legend="{{operator}}",
            template=TargetTemplate.TIME_SERIES_BAR_CHART,
            interval=interval,
        )
        for expr in (up_expr, down_expr)
    ]


SIZER_HOW_MANY_UP_DOWN_PANEL = Panel(
    id=_ids.next(),
    title="Sizer How Many Up/Down",
    description=(
        "Cumulative actors the OperatorSizer's scale_how_many requested adding "
        "(up, positive bars) and removing (down, negative bars), per operator."
        + SIZER_OPTIMIZER_NOTE
    ),
    unit="short",
    targets=_sizer_up_down_targets("per_op_how_many", per_interval=False),
    template=PanelTemplate.TIME_SERIES_BAR_CHART,
    field_config_overrides=SIZER_OPERATOR_COLOR_OVERRIDES,
)

SIZER_HOW_MANY_UP_DOWN_PER_INTERVAL_PANEL = Panel(
    id=_ids.next(),
    title="Sizer How Many Up/Down (change per interval)",
    description=(
        "Actors the OperatorSizer's scale_how_many requested adding (up, "
        "positive bars) and removing (down, negative bars) in each interval, "
        "per operator." + SIZER_OPTIMIZER_NOTE
    ),
    unit="short",
    targets=_sizer_up_down_targets("per_op_how_many", per_interval=True),
    template=PanelTemplate.TIME_SERIES_BAR_CHART,
    field_config_overrides=SIZER_OPERATOR_COLOR_OVERRIDES,
    template_overrides=SIZER_PER_INTERVAL_TEMPLATE_OVERRIDES,
)

SIZER_WHERE_UP_DOWN_PANEL = Panel(
    id=_ids.next(),
    title="Sizer Where Up/Down",
    description=(
        "Cumulative actors the OperatorSizer's scale_where placed (up, positive "
        "bars) and removed (down, negative bars), per operator." + SIZER_OPTIMIZER_NOTE
    ),
    unit="short",
    targets=_sizer_up_down_targets("per_op_where", per_interval=False),
    template=PanelTemplate.TIME_SERIES_BAR_CHART,
    field_config_overrides=SIZER_OPERATOR_COLOR_OVERRIDES,
)

SIZER_WHERE_UP_DOWN_PER_INTERVAL_PANEL = Panel(
    id=_ids.next(),
    title="Sizer Where Up/Down (change per interval)",
    description=(
        "Actors the OperatorSizer's scale_where placed (up, positive bars) and "
        "removed (down, negative bars) in each interval, per operator."
        + SIZER_OPTIMIZER_NOTE
    ),
    unit="short",
    targets=_sizer_up_down_targets("per_op_where", per_interval=True),
    template=PanelTemplate.TIME_SERIES_BAR_CHART,
    field_config_overrides=SIZER_OPERATOR_COLOR_OVERRIDES,
    template_overrides=SIZER_PER_INTERVAL_TEMPLATE_OVERRIDES,
)

SIZER_TICK_DURATION_PANEL = Panel(
    id=_ids.next(),
    title="Sizer Tick Duration by Phase",
    description=(
        "OperatorSizer control-loop wall time per phase, as percentiles over "
        "the scrape window (every scheduling-loop step is sampled). prepare: "
        "signal sampling plus the tick's cluster snapshot | cross_op_optimize: "
        "the pipeline optimizer rules | per_op_how_many: per-operator sizing | "
        "per_op_where: placement | apply: applying the requests | e2e: one "
        "whole perform_sizing_decision call (all sizer phases end to end)."
    ),
    unit="s",
    targets=[
        Target(
            expr="histogram_quantile(0.5, sum by (dataset, phase, le) (rate(ray_data_sizer_tick_duration_s_bucket{{{global_filters}}}[$__rate_interval])))",
            legend="p50 {{phase}}: {{dataset}}",
        ),
        Target(
            expr="histogram_quantile(0.99, sum by (dataset, phase, le) (rate(ray_data_sizer_tick_duration_s_bucket{{{global_filters}}}[$__rate_interval])))",
            legend="p99 {{phase}}: {{dataset}}",
        ),
    ],
    fill=0,
    stack=False,
)

# Each cumulative panel sits beside its per-interval variant in the two-column
# layout; the tick-duration panel is per-dataset (no operator tag), so it sits
# on its own at the end rather than breaking a pair.
ACTOR_ONLY_SIZER_PANELS = [
    SIZER_HOW_MANY_UP_DOWN_PANEL,
    SIZER_HOW_MANY_UP_DOWN_PER_INTERVAL_PANEL,
    SIZER_WHERE_UP_DOWN_PANEL,
    SIZER_WHERE_UP_DOWN_PER_INTERVAL_PANEL,
    SIZER_TICK_DURATION_PANEL,
]

# --- Actor Only Pipeline Optimizer (counter-only; views via increase()) -----

OPTIMIZER_ACTIONS_RATE_PANEL = Panel(
    id=_ids.next(),
    title="Optimizer Actions (rate)",
    description="Optimizer actions per 5m. capacity_transfer: fired -> landed | lost | timeout (victim-backed). silent_bottleneck: granted (from free capacity). Series rise and fall because this is a sliding window; the underlying counters never decrease.",
    unit="short",
    targets=[
        Target(
            expr="sum(increase(ray_data_pipeline_optimizer_actions_total{{{global_filters}}}[5m])) by (dataset, optimization, recipient, result)",
            legend="{{optimization}} {{result}} -> {{recipient}}: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

OPTIMIZER_CORRECTIVE_TRANSFERS_PANEL = Panel(
    id=_ids.next(),
    title="Corrective Transfers",
    description="Actors moved per 5m as donor -> recipient edges; spikes align with fired actions, so per-transfer counts are readable. Grants never appear here (no victims).",
    unit="short",
    targets=[
        Target(
            expr="sum(increase(ray_data_pipeline_optimizer_capacity_transfers_total{{{global_filters}}}[5m])) by (dataset, donor, recipient)",
            legend="{{donor}} -> {{recipient}}: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

OPTIMIZER_REFUSALS_PANEL = Panel(
    id=_ids.next(),
    title="Optimizer Refusals by Reason",
    description="Why an optimization refused to act, per 5m. warmup: sizer bootstrapping | cluster_settling: allocation still changing (transfers only) | no_donor_surplus: ops are starved but nothing has provable surplus (normal during an autoscaling ramp) | no_free_capacity: silent grant wanted actors but no node had room (the want is bridged to capacity_transfer) | capacity_already_free: normal sizing will grant | victims_insufficient: no nodes could assemble the shape | thrashing: would invert a recent transfer.",
    unit="short",
    targets=[
        Target(
            expr="sum(increase(ray_data_pipeline_optimizer_refusals_total{{{global_filters}}}[5m])) by (dataset, optimization, reason)",
            legend="{{optimization}} {{reason}}: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

PIPELINE_OPTIMIZER_PANELS = [
    OPTIMIZER_ACTIONS_RATE_PANEL,
    OPTIMIZER_CORRECTIVE_TRANSFERS_PANEL,
    OPTIMIZER_REFUSALS_PANEL,
]

# Grafana panel ids must be unique across the dashboard, so the overview
# re-uses detailed panels as copies with fresh ids.
ACTOR_ONLY_OVERVIEW_PANELS = [
    ACTOR_ONLY_OP_NUM_ACTORS_TOTAL_PANEL,
    ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BYTES_TOTALS_PANEL,
    replace(ACTOR_ONLY_NODE_OBJ_STORE_FREED_BYTES_PANEL, id=_ids.next()),
    replace(ACTOR_ONLY_NODE_LOCALITY_BYTES_PANEL, id=_ids.next()),
    replace(ACTOR_ONLY_OP_INPUT_LOCALITY_BYTES_PANEL, id=_ids.next()),
    replace(ACTOR_ONLY_OP_OUTPUT_BACKPRESSURED_ACTORS_PANEL, id=_ids.next()),
]

ACTOR_ONLY_ACTOR_STATUS_PANELS = [
    ACTOR_ONLY_NODE_NUM_ACTORS_PANEL,
    ACTOR_ONLY_NODE_OP_NUM_ACTORS_PANEL,
    ACTOR_ONLY_OP_NUM_ACTORS_PANEL,
    ACTOR_ONLY_OP_OUTPUT_BACKPRESSURED_ACTORS_PANEL,
]

ACTOR_ONLY_OBJECT_STORE_USAGE_PANELS = [
    ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BYTES_PANEL,
    ACTOR_ONLY_NODE_OBJ_STORE_LIVE_BLOCKS_PANEL,
    ACTOR_ONLY_NODE_OBJ_STORE_FREED_BYTES_PANEL,
    ACTOR_ONLY_NODE_OBJ_STORE_FREED_BLOCKS_PANEL,
]

ACTOR_ONLY_INPUT_LOCALITY_PANELS = [
    ACTOR_ONLY_NODE_LOCALITY_BYTES_PANEL,
    ACTOR_ONLY_OP_INPUT_LOCALITY_BYTES_PANEL,
    ACTOR_ONLY_OP_INPUT_LOCALITY_BLOCKS_PANEL,
    ACTOR_ONLY_NODE_OP_INPUT_LOCALITY_PCT_PANEL,
    ACTOR_ONLY_OP_INPUT_LOCALITY_PCT_PANEL,
    ACTOR_ONLY_NODE_INPUT_LOCALITY_PCT_PANEL,
]

ACTOR_ONLY_OUTPUT_LIMITS_PANELS = [
    ACTOR_ONLY_ESCAPE_HATCH_GRANTS_PANEL,
    ACTOR_ONLY_ESCAPE_HATCH_GRANTED_BYTES_PANEL,
    ACTOR_ONLY_OP_OUTPUT_LIMIT_BYTES_PANEL,
    ACTOR_ONLY_OP_OUTPUT_LIMIT_BLOCKS_PANEL,
    ACTOR_ONLY_OP_OUTPUT_OVERSHOOTS_PANEL,
    ACTOR_ONLY_OP_WITHHELD_OUTPUTS_BYTES_PANEL,
    ACTOR_ONLY_OP_WITHHELD_OUTPUTS_BLOCKS_PANEL,
    ACTOR_ONLY_OP_WITHHELD_DEADLOCKED_ACTORS_PANEL,
]

OPERATOR_PANELS = [
    ROWS_OUTPUT_PER_SECOND_PANEL,
    ALL_RESOURCES_UTILIZATION_PANEL,
    COMBINED_INQUEUE_BLOCKS_PANEL,
    OPERATOR_TASK_COMPLETION_TIME_PANEL,
    OPERATOR_BLOCK_COMPLETION_TIME_PANEL,
    OPERATOR_BLOCK_SIZE_BYTES_PANEL,
    OPERATOR_BLOCK_SIZE_ROWS_PANEL,
]

DATA_GRAFANA_ROWS = [
    # Overview Row
    Row(
        title="Overview",
        id=_ids.next(),
        panels=[
            BYTES_GENERATED_PANEL,
            BLOCKS_GENERATED_PANEL,
            ROWS_GENERATED_PANEL,
            OBJECT_STORE_MEMORY_PANEL,
            ACTIVE_TASKS_PANEL,
            COMBINED_INQUEUE_BLOCKS_PANEL,
            COMBINED_OUTQUEUE_BLOCKS_PANEL,
        ],
        collapsed=False,
    ),
    # Pending Inputs Row
    Row(
        title="Pending Inputs",
        id=_ids.next(),
        panels=[
            INTERNAL_INQUEUE_BLOCKS_PANEL,
            INTERNAL_INQUEUE_BYTES_PANEL,
            EXTERNAL_INQUEUE_BLOCKS_PANEL,
            EXTERNAL_INQUEUE_BYTES_PANEL,
            PENDING_TASK_INPUTS_PANEL,
        ],
        collapsed=True,
    ),
    # Inputs Row
    Row(
        title="Inputs",
        id=_ids.next(),
        panels=[
            INPUT_BLOCKS_RECEIVED_PANEL,
            INPUT_BYTES_RECEIVED_PANEL,
            INPUT_BLOCKS_PROCESSED_PANEL,
            INPUT_BYTES_PROCESSED_PANEL,
            INPUT_BYTES_SUBMITTED_PANEL,
            CROSS_NODE_INPUT_TRANSFER_PANEL,
        ],
        collapsed=True,
    ),
    # Pending Outputs Row
    Row(
        title="Pending Outputs",
        id=_ids.next(),
        panels=[
            INTERNAL_OUTQUEUE_BLOCKS_PANEL,
            INTERNAL_OUTQUEUE_BYTES_PANEL,
            EXTERNAL_OUTQUEUE_BLOCKS_PANEL,
            EXTERNAL_OUTQUEUE_BYTES_PANEL,
            MAX_BYTES_TO_READ_PANEL,
        ],
        collapsed=True,
    ),
    # Outputs Row
    Row(
        title="Outputs",
        id=_ids.next(),
        panels=[
            BLOCK_SIZE_BYTES_P50_PANEL,
            BLOCK_SIZE_BYTES_P90_PANEL,
            BLOCK_SIZE_BYTES_P99_PANEL,
            BLOCK_SIZE_ROWS_P50_PANEL,
            BLOCK_SIZE_ROWS_P90_PANEL,
            BLOCK_SIZE_ROWS_P99_PANEL,
            OUTPUT_BLOCKS_TAKEN_PANEL,
            OUTPUT_BYTES_TAKEN_PANEL,
            OUTPUT_BYTES_BY_NODE_PANEL,
            BLOCKS_BY_NODE_PANEL,
            BYTES_OUTPUT_PER_SECOND_PANEL,
            ROWS_OUTPUT_PER_SECOND_PANEL,
            AVERAGE_BYTES_PER_BLOCK_PANEL,
            AVERAGE_BLOCKS_PER_TASK_PANEL,
            BLOCK_GENERATION_TIME_PANEL,
            BLOCK_TRANSFORM_TIME_PANEL,
            BLOCK_TRANSFORM_TIME_BY_PHASE_PANEL,
        ],
        collapsed=True,
    ),
    # Tasks
    Row(
        title="Tasks",
        id=_ids.next(),
        panels=[
            TASK_COMPLETION_TIME_P50_PANEL,
            TASK_COMPLETION_TIME_P90_PANEL,
            TASK_COMPLETION_TIME_P99_PANEL,
            BLOCK_COMPLETION_TIME_P50_PANEL,
            BLOCK_COMPLETION_TIME_P90_PANEL,
            BLOCK_COMPLETION_TIME_P99_PANEL,
            TASK_COMPLETION_TIME_WITHOUT_BACKPRESSURE_PANEL,
            TASK_OUTPUT_BACKPRESSURE_TIME_PANEL,
            TASK_SUBMISSION_BACKPRESSURE_PANEL,
            MAX_USS_PER_TASK_PANEL,
            TASK_THROUGHPUT_BY_NODE_PANEL,
            TASKS_WITH_OUTPUT_PANEL,
            SUBMITTED_TASKS_PANEL,
            FINISHED_TASKS_PANEL,
            FAILED_TASKS_PANEL,
        ],
        collapsed=True,
    ),
    # Resource Budget / Usage Row
    Row(
        title="Resource Budget / Usage",
        id=_ids.next(),
        panels=[
            CPU_USAGE_PANEL,
            GPU_USAGE_PANEL,
            MEMORY_USAGE_PANEL,
            CPU_BUDGET_PANEL,
            GPU_BUDGET_PANEL,
            MEMORY_BUDGET_PANEL,
            OBJECT_STORE_MEMORY_BUDGET_PANEL,
            FREED_MEMORY_PANEL,
            SPILLED_MEMORY_PANEL,
            BYTES_SPILLED_PANEL,
            BYTES_FREED_PANEL,
        ],
        collapsed=True,
    ),
    # Scheduling Loop Row
    Row(
        title="Scheduling Loop",
        id=_ids.next(),
        panels=[
            SCHEDULING_LOOP_DURATION_PANEL,
        ],
        collapsed=True,
    ),
    # Cluster Autoscaler Row
    Row(
        title="Cluster Autoscaler",
        id=_ids.next(),
        panels=[
            CLUSTER_CPU_UTILIZATION_PANEL,
            CLUSTER_GPU_UTILIZATION_PANEL,
            CLUSTER_MEMORY_UTILIZATION_PANEL,
            CLUSTER_OBJECT_STORE_MEMORY_UTILIZATION_PANEL,
        ],
        collapsed=True,
    ),
    # Iteration Row
    Row(
        title="Iteration",
        id=_ids.next(),
        panels=[
            ITERATION_INITIALIZATION_PANEL,
            ITERATION_BLOCKED_PANEL,
            ITERATION_USER_PANEL,
            ITERATION_GET_PANEL,
            ITERATION_NEXT_BATCH_PANEL,
            ITERATION_FORMAT_BATCH_PANEL,
            ITERATION_COLLATE_BATCH_PANEL,
            ITERATION_FINALIZE_BATCH_PANEL,
            ITERATION_BLOCKS_LOCAL_PANEL,
            ITERATION_BLOCKS_REMOTE_PANEL,
            ITERATION_BLOCKS_UNKNOWN_LOCATION_PANEL,
            ITERATION_PREFETCHED_BYTES_PANEL,
            ITERATION_TIME_TO_FIRST_BATCH_PANEL,
            ITERATION_GET_REF_BUNDLES_PANEL,
        ],
        collapsed=True,
    ),
    # Operator Panels Row (these graphs should only be viewed when filtering down to a single operator)
    Row(
        title="Operator Panels",
        id=_ids.next(),
        panels=[
            ALL_RESOURCES_UTILIZATION_PANEL,
            OPERATOR_TASK_COMPLETION_TIME_PANEL,
            OPERATOR_BLOCK_COMPLETION_TIME_PANEL,
            OPERATOR_BLOCK_SIZE_BYTES_PANEL,
            OPERATOR_BLOCK_SIZE_ROWS_PANEL,
        ],
        collapsed=True,
    ),
    # Actor-only rows: populated only when the actor-only backend
    # (RAY_DATA_ACTOR_ONLY_BACKEND) is enabled.
    Row(
        title="Actor Only Overview",
        id=_ids.next(),
        panels=ACTOR_ONLY_OVERVIEW_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Actor Status",
        id=_ids.next(),
        panels=ACTOR_ONLY_ACTOR_STATUS_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Object Store Usage",
        id=_ids.next(),
        panels=ACTOR_ONLY_OBJECT_STORE_USAGE_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Input Locality",
        id=_ids.next(),
        panels=ACTOR_ONLY_INPUT_LOCALITY_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Output Limits",
        id=_ids.next(),
        panels=ACTOR_ONLY_OUTPUT_LIMITS_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Sizer",
        id=_ids.next(),
        panels=ACTOR_ONLY_SIZER_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Pipeline Optimizer",
        id=_ids.next(),
        panels=PIPELINE_OPTIMIZER_PANELS,
        collapsed=True,
    ),
    Row(
        title="Actor Only Draining",
        id=_ids.next(),
        panels=ACTOR_DRAINING_PANELS,
        collapsed=True,
    ),
]

# Get all panel IDs from both top-level panels and panels within rows
all_panel_ids = []
for row in DATA_GRAFANA_ROWS:
    all_panel_ids.append(row.id)
    all_panel_ids.extend(panel.id for panel in row.panels)

all_panel_ids.sort()

assert len(all_panel_ids) == len(
    set(all_panel_ids)
), f"Duplicated id found. Use unique id for each panel. {all_panel_ids}"

data_dashboard_config = DashboardConfig(
    name="DATA",
    default_uid="rayDataDashboard",
    rows=DATA_GRAFANA_ROWS,
    standard_global_filters=[
        'dataset=~"$DatasetID"',
        'SessionName=~"$SessionName"',
        'ray_io_cluster=~"$Cluster"',
    ],
    base_json_file_name="data_grafana_dashboard_base.json",
)
