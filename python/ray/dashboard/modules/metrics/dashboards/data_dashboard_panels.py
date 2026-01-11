# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    PanelTemplate,
    Row,
    Target,
    TargetTemplate,
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
#             + "{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)",
#             legend=legend,
#         )
#     ],
#     fill=fill,
#     stack=stack,
# )


# Ray Data Metrics (Overview)
BYTES_SPILLED_PANEL = Panel(
    id=1,
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
    id=3,
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
    id=4,
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
    id=5,
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
    id=6,
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

BYTES_OUTPUT_PER_SECOND_PANEL = Panel(
    id=7,
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
    id=11,
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
    id=17,
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
    id=18,
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
    id=19,
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
    id=20,
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
    id=21,
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
    id=22,
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
    id=23,
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
    id=24,
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
    id=25,
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
    id=26,
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
    id=49,
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
    id=50,
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
    id=43,
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
    id=48,
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
    id=29,
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

RUNNING_TASKS_PANEL = Panel(
    id=30,
    title="Running Tasks",
    description="Tracks the current number of tasks actively running across operators. Provides insight into the degree of parallelism currently utilized for data processing.",
    unit="tasks",
    targets=[
        Target(
            expr='sum(ray_data_num_tasks_running{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Running Tasks: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

TASKS_WITH_OUTPUT_PANEL = Panel(
    id=31,
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
    id=32,
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
    id=33,
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

TASK_THROUGHPUT_BY_NODE_PANEL = Panel(
    id=46,
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
    id=8,
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

TASK_SUBMISSION_BACKPRESSURE_PANEL = Panel(
    id=37,
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
    id=38,
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
    id=82,
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
    id=83,
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
    id=84,
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
    id=61,
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
    id=85,
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
    id=86,
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
    id=62,
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
    id=87,
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
    id=88,
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
    id=63,
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
    id=89,
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
    id=39,
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
    id=40,
    title="Task Completion Time Without Backpressure",
    description="Average time (in seconds) tasks spend executing their core logic, excluding backpressure, over a recent 5-minute window. This metric helps isolate actual computation time from delays caused by data flow bottlenecks, aiding in differentiating between computation-bound and data-flow-bound performance issues.",
    unit="s",
    targets=[
        Target(
            expr='increase(ray_data_task_completion_time_without_backpressure{{{global_filters}, operator=~"$Operator"}}[5m]) / increase(ray_data_num_tasks_finished{{{global_filters}, operator=~"$Operator"}}[5m])',
            legend="Task Completion Time w/o Backpressure: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Object Store Memory)
INTERNAL_INQUEUE_BLOCKS_PANEL = Panel(
    id=13,
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
    id=14,
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
    id=15,
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
    id=16,
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
    id=2,
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
    id=27,
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
    id=58,
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
    id=59,
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
    id=56,
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
    id=60,
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
    id=34,
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
    id=35,
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
    id=36,
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
    id=12,
    title="Iteration Initialization Time",
    description="Total time (in seconds) spent setting up and initializing the data iterator before it begins yielding batches. This includes overhead such as establishing connections, resolving data sources, and preparing internal structures.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_initialize_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKED_PANEL = Panel(
    id=9,
    title="Iteration Blocked Time",
    description="Total time (in seconds) that the user's application thread is blocked while waiting for `iter_batches()` to produce data. High values indicate the data pipeline isn't generating batches fast enough to keep up with consumption rate, pointing to upstream bottlenecks.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_total_blocked_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_USER_PANEL = Panel(
    id=10,
    title="Iteration User Time",
    description="Total time (in seconds) spent executing user-defined code during data iteration. This includes time spent in UDFs (User-Defined Functions) and custom batch processing logic, useful for profiling user code performance.",
    unit="s",
    targets=[
        Target(
            expr="sum(ray_data_iter_user_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_GET_PANEL = Panel(
    id=70,
    title="Iteration Get Time",
    description="Total time (in seconds) spent performing `ray.get()` calls to resolve Ray object references into actual data blocks during iteration. This indicates latency associated with fetching data from the Ray object store, potentially across the network.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_get_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_NEXT_BATCH_PANEL = Panel(
    id=71,
    title="Iteration Next Batch Time",
    description="Total time (in seconds) spent retrieving the next batch of data from the internal block buffer of the iterator. This is a fine-grained measure of the efficiency of the batching mechanism before formatting or collation.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_next_batch_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_FORMAT_BATCH_PANEL = Panel(
    id=72,
    title="Iteration Format Batch Time",
    description="Total time (in seconds) spent converting raw data blocks into the desired output format (e.g., Pandas DataFrame, PyArrow Table, NumPy array) for consumption by the user or a machine learning framework. This reflects the cost of data marshalling.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_format_batch_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_COLLATE_BATCH_PANEL = Panel(
    id=73,
    title="Iteration Collate Batch Time",
    description="Total time (in seconds) spent applying a `CollateFn` to batches, typically for deep learning frameworks like PyTorch. This includes operations such as stacking tensors, padding, or moving data to a specific device like a GPU.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_collate_batch_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_FINALIZE_BATCH_PANEL = Panel(
    id=74,
    title="Iteration Finalize Batch Time",
    description="Total time (in seconds) spent in any final processing steps applied to a batch before it's yielded to the user, as defined by a `finalize_fn`. This can include last-minute transformations or device transfers.",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_finalize_batch_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_LOCAL_PANEL = Panel(
    id=75,
    title="Iteration Blocks Local",
    description="Cumulative count of blocks found on the local node (same node as the consuming application) during iteration. Accessing local blocks is generally faster and more efficient as it avoids network transfer.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_blocks_local{{{global_filters}}}) by (dataset)",
            legend="Blocks: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_REMOTE_PANEL = Panel(
    id=76,
    title="Iteration Blocks Remote",
    description="Cumulative count of blocks that needed to be fetched from a remote node (different node in the Ray cluster) during iteration. A high number of remote blocks can indicate significant network transfer overhead, potentially bottlenecking iteration performance.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_blocks_remote{{{global_filters}}}) by (dataset)",
            legend="Blocks: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_BLOCKS_UNKNOWN_LOCATION_PANEL = Panel(
    id=77,
    title="Iteration Blocks Unknown Location",
    description="Cumulative count of blocks for which the location (local or remote) couldn't be determined during iteration. This might suggest issues with the Ray object store's metadata tracking or liveness of relevant Ray nodes.",
    unit="blocks",
    targets=[
        Target(
            expr="sum(ray_data_iter_unknown_location{{{global_filters}}}) by (dataset)",
            legend="Blocks: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_PREFETCHED_BYTES_PANEL = Panel(
    id=90,
    title="Iteration Prefetched Bytes",
    description="Current bytes of prefetched blocks in the iterator",
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_iter_prefetched_bytes{{{global_filters}}}) by (dataset)",
            legend="Prefetched Bytes: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_TIME_TO_FIRST_BATCH_PANEL = Panel(
    id=120,
    title="Iteration Time to First Batch",
    description="Seconds spent waiting for the first batch after starting iteration",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_time_to_first_batch_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

ITERATION_GET_REF_BUNDLES_PANEL = Panel(
    id=121,
    title="Iteration Get Ref Bundles Time",
    description="Seconds spent getting RefBundles from the dataset iterator",
    unit="seconds",
    targets=[
        Target(
            expr="sum(ray_data_iter_get_ref_bundles_seconds{{{global_filters}}}) by (dataset)",
            legend="Seconds: {{dataset}}",
        )
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Miscellaneous)
SCHEDULING_LOOP_DURATION_PANEL = Panel(
    id=47,
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
    id=55,
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

# Budget Panels
CPU_BUDGET_PANEL = Panel(
    id=51,
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
    id=52,
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
    id=53,
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
    id=54,
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
    id=57,
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
    id=78,
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
    id=79,
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
    id=80,
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
    id=81,
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
        id=99,
        panels=[
            BYTES_GENERATED_PANEL,
            BLOCKS_GENERATED_PANEL,
            ROWS_GENERATED_PANEL,
            OBJECT_STORE_MEMORY_PANEL,
            RUNNING_TASKS_PANEL,
            COMBINED_INQUEUE_BLOCKS_PANEL,
            COMBINED_OUTQUEUE_BLOCKS_PANEL,
        ],
        collapsed=False,
    ),
    # Pending Inputs Row
    Row(
        title="Pending Inputs",
        id=100,
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
        id=101,
        panels=[
            INPUT_BLOCKS_RECEIVED_PANEL,
            INPUT_BYTES_RECEIVED_PANEL,
            INPUT_BLOCKS_PROCESSED_PANEL,
            INPUT_BYTES_PROCESSED_PANEL,
            INPUT_BYTES_SUBMITTED_PANEL,
        ],
        collapsed=True,
    ),
    # Pending Outputs Row
    Row(
        title="Pending Outputs",
        id=102,
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
        id=103,
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
        ],
        collapsed=True,
    ),
    # Tasks
    Row(
        title="Tasks",
        id=104,
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
        id=105,
        panels=[
            CPU_USAGE_PANEL,
            GPU_USAGE_PANEL,
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
        id=106,
        panels=[
            SCHEDULING_LOOP_DURATION_PANEL,
        ],
        collapsed=True,
    ),
    # Iteration Row
    Row(
        title="Iteration",
        id=107,
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
        id=108,
        panels=[
            ALL_RESOURCES_UTILIZATION_PANEL,
            OPERATOR_TASK_COMPLETION_TIME_PANEL,
            OPERATOR_BLOCK_COMPLETION_TIME_PANEL,
            OPERATOR_BLOCK_SIZE_BYTES_PANEL,
            OPERATOR_BLOCK_SIZE_ROWS_PANEL,
        ],
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
