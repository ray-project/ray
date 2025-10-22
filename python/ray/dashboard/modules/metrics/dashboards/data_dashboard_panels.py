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
    title="Bytes Spilled",
    description="Amount spilled by dataset operators. DataContext.enable_get_object_locations_for_metrics must be set to True to report this metric",
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
    title="Bytes Freed",
    description="Amount freed by dataset operators.",
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
    description="Amount of memory store used by dataset operators.",
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
    description="Logical CPUs currently being used by dataset operators.",
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
    description="Logical GPUs currently being used by dataset operators.",
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
    description="Bytes output per second by dataset operators.",
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
    description="Total rows output per second by dataset operators.",
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
    description="Number of input blocks received by operator per second.",
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
    description="Byte size of input blocks received by operator per second.",
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
        "Number of input blocks that operator's tasks have finished processing per second."
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
        "Byte size of input blocks that operator's tasks have finished processing per second."
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
    description="Byte size of input blocks passed to submitted tasks per second.",
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
    description="Number of output blocks generated by tasks per second.",
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
    description="Byte size of output blocks generated by tasks per second.",
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
    description="Number of rows in generated output blocks from finished tasks per second.",
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
    description="Number of output blocks taken by downstream operators per second.",
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
        "Byte size of output blocks taken by downstream operators per second."
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
    description="Average byte size of output blocks generated by tasks.",
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
    description="Average number of output blocks generated by tasks.",
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
        "Byte size of output blocks from finished tasks per second, grouped by node."
    ),
    unit="Bps",
    targets=[
        Target(
            expr='sum(rate(ray_data_bytes_outputs_of_finished_tasks_per_node{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, node_ip)',
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
        "Number of output blocks from finished tasks per second, grouped by node."
    ),
    unit="blocks/s",
    targets=[
        Target(
            expr='sum(rate(ray_data_blocks_outputs_of_finished_tasks_per_node{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, node_ip)',
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
    description="Number of submitted tasks.",
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
    description="Number of running tasks.",
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
    description="Number of tasks that already have output.",
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
    description="Number of finished tasks.",
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
    description="Number of failed tasks.",
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
    description="Number of finished tasks per second, grouped by node.",
    unit="tasks/s",
    targets=[
        Target(
            expr='sum(rate(ray_data_num_tasks_finished_per_node{{{global_filters}, operator=~"$Operator"}}[1m])) by (dataset, node_ip)',
            legend="Finished Tasks: {{dataset}}, {{node_ip}}",
        )
    ],
    fill=0,
    stack=False,
)

BLOCK_GENERATION_TIME_PANEL = Panel(
    id=8,
    title="Block Generation Time",
    description="Time spent generating blocks in tasks.",
    unit="s",
    targets=[
        Target(
            expr='sum(ray_data_block_generation_time{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Block Generation Time: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

TASK_SUBMISSION_BACKPRESSURE_PANEL = Panel(
    id=37,
    title="Task Submission Backpressure Time",
    description="Time spent in task submission backpressure.",
    unit="s",
    targets=[
        Target(
            expr='sum(ray_data_task_submission_backpressure_time{{{global_filters}, operator=~"$Operator"}}) by (dataset, operator)',
            legend="Backpressure Time: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

# Task Completion Time Percentiles
TASK_COMPLETION_TIME_PANEL = Panel(
    id=38,
    title="Task Completion Time Histogram (s)",
    description="Time (in seconds) spent (including backpressure) running tasks to completion. Larger bars means more tasks finished within that duration range.",
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

BLOCK_COMPLETION_TIME_PANEL = Panel(
    id=61,
    title="Block Completion Time Histogram (s)",
    description="Time (in seconds) spent processing blocks to completion. If multiple blocks are generated per task, this is approximated by assuming each block took an equal amount of time to process. Larger bars means more blocks finished within that duration range.",
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

BLOCK_SIZE_BYTES_PANEL = Panel(
    id=62,
    title="Block Size (Bytes) Histogram",
    description="Size (in bytes) per block. Larger bars means more blocks are within that size range.",
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
)

BLOCK_SIZE_ROWS_PANEL = Panel(
    id=63,
    title="Block Size (Rows) Histogram",
    description="Number of rows per block. Larger bars means more blocks are within that number of rows range.",
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
)

TASK_OUTPUT_BACKPRESSURE_TIME_PANEL = Panel(
    id=39,
    title="Task Output Backpressure Time",
    description="Time spent in output backpressure.",
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
    description="Time spent running tasks to completion w/o backpressure.",
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
    description="Number of blocks in operator's internal input queue",
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
    description="Byte size of input blocks in the operator's internal input queue.",
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
    description="Number of blocks in operator's internal output queue",
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
    description=("Byte size of output blocks in the operator's internal output queue."),
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
    description="Number of blocks in operator's external input queue",
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
    description="Byte size of blocks in operator's external input queue",
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
    description="Number of blocks in operator's external output queue",
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
    description="Byte size of blocks in operator's external output queue",
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
    description="Total number of blocks in operator's internal + external input queue.",
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
    description="Total number of blocks in operator's internal + external output queue.",
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
    description="Byte size of input blocks used by pending tasks.",
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
    description="Byte size of freed memory in object store.",
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
    description="Byte size of spilled memory in object store.",
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
    description="Seconds spent in iterator initialization code",
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
    description="Seconds user thread is blocked by iter_batches()",
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
    description="Seconds spent in user code",
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
    description="Seconds spent in ray.get() while resolving block references",
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
    description="Seconds spent getting the next batch from the block buffer",
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
    description="Seconds spent formatting the batch",
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
    description="Seconds spent collating the batch",
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
    description="Seconds spent finalizing the batch",
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
    description="Number of blocks already on the local node",
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
    description="Number of blocks that require fetching from another node",
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
    description="Number of blocks that have unknown locations",
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

# Ray Data Metrics (Miscellaneous)
SCHEDULING_LOOP_DURATION_PANEL = Panel(
    id=47,
    title="Scheduling Loop Duration",
    description=("Duration of the scheduling loop in seconds."),
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
    description="Maximum bytes to read from streaming generator buffer.",
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
    description=("Budget (CPU) for the operator."),
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
    description=("Budget (GPU) for the operator."),
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
    description=("Budget (Memory) for the operator."),
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
    description=("Budget (Object Store Memory) for the operator."),
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
    description=(
        "Shows all logical resources utilization on a single graph. Filtering by operator is recommended."
    ),
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

OPERATOR_PANELS = [
    ROWS_OUTPUT_PER_SECOND_PANEL,
    ALL_RESOURCES_UTILIZATION_PANEL,
    COMBINED_INQUEUE_BLOCKS_PANEL,
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
            BLOCK_SIZE_BYTES_PANEL,
            BLOCK_SIZE_ROWS_PANEL,
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
            TASK_COMPLETION_TIME_PANEL,
            BLOCK_COMPLETION_TIME_PANEL,
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
        ],
        collapsed=True,
    ),
    # Operator Panels Row (these graphs should only be viewed when filtering down to a single operator)
    Row(
        title="Operator Panels",
        id=108,
        panels=[ALL_RESOURCES_UTILIZATION_PANEL],
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
