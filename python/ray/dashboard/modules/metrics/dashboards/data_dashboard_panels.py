# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    PanelIdGenerator,
    Target,
    Row,
)


panel_id_gen = PanelIdGenerator()

# Ray Data Metrics (Overview)
BYTES_SPILLED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

BYTES_FREED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

OBJECT_STORE_MEMORY_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Ray Data Metrics (Resource Usage)
CPU_USAGE_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

GPU_USAGE_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Ray Data Metrics (Throughput)
BYTES_OUTPUT_PER_SECOND_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

ROWS_OUTPUT_PER_SECOND_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Ray Data Metrics (Blocks)
INPUT_BLOCKS_RECEIVED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INPUT_BYTES_RECEIVED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INPUT_BLOCKS_PROCESSED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INPUT_BYTES_PROCESSED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INPUT_BYTES_SUBMITTED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

BLOCKS_GENERATED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

BYTES_GENERATED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

AVERAGE_BYTES_PER_BLOCK_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Average Bytes Generated / Output Block",
    description="Average byte size of output blocks generated by tasks.",
    unit="bytes",
    targets=[
        Target(
            expr="increase(ray_data_bytes_task_outputs_generated{{{global_filters}}}[5m]) / increase(ray_data_num_task_outputs_generated{{{global_filters}}}[5m])",
            legend="Average Bytes Generated / Output Block: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

AVERAGE_BLOCKS_PER_TASK_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Average Number of Output Blocks / Task",
    description="Average number of output blocks generated by tasks.",
    unit="blocks",
    targets=[
        Target(
            expr="increase(ray_data_num_task_outputs_generated{{{global_filters}}}[5m]) / increase(ray_data_num_tasks_finished{{{global_filters}}}[5m])",
            legend="Average Number of Output Blocks / Task: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

ROWS_GENERATED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

OUTPUT_BLOCKS_TAKEN_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

OUTPUT_BYTES_TAKEN_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

OUTPUT_BYTES_BY_NODE_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Output Bytes from Finished Tasks / Second (by Node)",
    description=(
        "Byte size of output blocks from finished tasks per second, grouped by node."
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
    id=panel_id_gen.next(),
    title="Blocks from Finished Tasks / Second (by Node)",
    description=(
        "Number of output blocks from finished tasks per second, grouped by node."
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
    id=panel_id_gen.next(),
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
)

RUNNING_TASKS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

TASKS_WITH_OUTPUT_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

FINISHED_TASKS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

TASK_THROUGHPUT_BY_NODE_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Task Throughput (by Node)",
    description="Number of finished tasks per second, grouped by node.",
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

FAILED_TASKS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

BLOCK_GENERATION_TIME_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

TASK_SUBMISSION_BACKPRESSURE_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Task Completion Time Percentiles
TASK_COMPLETION_TIME_P00_PANEL = Panel(
    id=panel_id_gen.next(),
    title="(p00) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p00) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P05_PANEL = Panel(
    id=panel_id_gen.next(),
    title="(p05) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0.05, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p05) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P50_PANEL = Panel(
    id=panel_id_gen.next(),
    title="(p50) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0.50, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p50) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P75_PANEL = Panel(
    id=panel_id_gen.next(),
    title="(p75) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0.75, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p75) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P90_PANEL = Panel(
    id=panel_id_gen.next(),
    title="(p90) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0.9, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p90) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P99_PANEL = Panel(
    id=panel_id_gen.next(),
    title="p(99) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(0.99, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p99) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

TASK_COMPLETION_TIME_P100_PANEL = Panel(
    id=panel_id_gen.next(),
    title="p(100) Task Completion Time",
    description="Time spent running tasks to completion.",
    unit="seconds",
    targets=[
        Target(
            expr="histogram_quantile(1, sum by (dataset, operator, le) (rate(ray_data_task_completion_time_bucket{{{global_filters}}}[5m])))",
            legend="(p100) Completion Time: {{dataset}}, {{operator}}",
        ),
    ],
    fill=0,
    stack=False,
)

# Ray Data Metrics (Object Store Memory)
INTERNAL_INQUEUE_BLOCKS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INTERNAL_INQUEUE_BYTES_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INTERNAL_OUTQUEUE_BLOCKS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

INTERNAL_OUTQUEUE_BYTES_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Operator Internal Outqueue Size (Bytes)",
    description=("Byte size of output blocks in the operator's internal output queue."),
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_obj_store_mem_internal_outqueue{{{global_filters}}}) by (dataset, operator)",
            legend="Bytes Size: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=True,
)

PENDING_TASK_INPUTS_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

FREED_MEMORY_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

SPILLED_MEMORY_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Ray Data Metrics (Iteration)
ITERATION_INITIALIZATION_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

ITERATION_BLOCKED_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

ITERATION_USER_PANEL = Panel(
    id=panel_id_gen.next(),
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
)

# Ray Data Metrics (Miscellaneous)
SCHEDULING_LOOP_DURATION_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Scheduling Loop Duration",
    description=("Duration of the scheduling loop in seconds."),
    unit="seconds",
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
    id=panel_id_gen.next(),
    title="Max Bytes to Read",
    description="Maximum bytes to read from streaming generator buffer.",
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_max_bytes_to_read{{{global_filters}}}) by (dataset, operator)",
            legend="Max Bytes to Read: {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

# Budget Panels
CPU_BUDGET_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Budget (CPU)",
    description=("Budget (CPU) for the operator."),
    unit="cpu",
    targets=[
        Target(
            expr="sum(ray_data_cpu_budget{{{global_filters}}}) by (dataset, operator)",
            legend="Budget (CPU): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

GPU_BUDGET_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Budget (GPU)",
    description=("Budget (GPU) for the operator."),
    unit="gpu",
    targets=[
        Target(
            expr="sum(ray_data_gpu_budget{{{global_filters}}}) by (dataset, operator)",
            legend="Budget (GPU): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

MEMORY_BUDGET_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Budget (Memory)",
    description=("Budget (Memory) for the operator."),
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_memory_budget{{{global_filters}}}) by (dataset, operator)",
            legend="Budget (Memory): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

OBJECT_STORE_MEMORY_BUDGET_PANEL = Panel(
    id=panel_id_gen.next(),
    title="Budget (Object Store Memory)",
    description=("Budget (Object Store Memory) for the operator."),
    unit="bytes",
    targets=[
        Target(
            expr="sum(ray_data_object_store_memory_budget{{{global_filters}}}) by (dataset, operator)",
            legend="Budget (Object Store Memory): {{dataset}}, {{operator}}",
        )
    ],
    fill=0,
    stack=False,
)

DATA_GRAFANA_ROWS = [
    # Inputs Row
    Row(
        title="Inputs",
        id=panel_id_gen.next(),
        panels=[
            INPUT_BLOCKS_RECEIVED_PANEL,
            INPUT_BYTES_RECEIVED_PANEL,
            INPUT_BLOCKS_PROCESSED_PANEL,
            INPUT_BYTES_PROCESSED_PANEL,
            INPUT_BYTES_SUBMITTED_PANEL,
        ],
        collapsed=False,
    ),
    # Pending Inputs Row
    Row(
        title="Pending Inputs",
        id=panel_id_gen.next(),
        panels=[
            INTERNAL_INQUEUE_BLOCKS_PANEL,
            INTERNAL_INQUEUE_BYTES_PANEL,
            PENDING_TASK_INPUTS_PANEL,
        ],
        collapsed=True,
    ),
    # Pending Outputs Row
    Row(
        title="Pending Outputs",
        id=panel_id_gen.next(),
        panels=[
            INTERNAL_OUTQUEUE_BLOCKS_PANEL,
            INTERNAL_OUTQUEUE_BYTES_PANEL,
            MAX_BYTES_TO_READ_PANEL,
        ],
        collapsed=True,
    ),
    # Outputs Row
    Row(
        title="Outputs",
        id=panel_id_gen.next(),
        panels=[
            BLOCKS_GENERATED_PANEL,
            BYTES_GENERATED_PANEL,
            ROWS_GENERATED_PANEL,
            OUTPUT_BLOCKS_TAKEN_PANEL,
            OUTPUT_BYTES_TAKEN_PANEL,
            OUTPUT_BYTES_BY_NODE_PANEL,
            BLOCKS_BY_NODE_PANEL,
            BYTES_OUTPUT_PER_SECOND_PANEL,
            ROWS_OUTPUT_PER_SECOND_PANEL,
            AVERAGE_BYTES_PER_BLOCK_PANEL,
            AVERAGE_BLOCKS_PER_TASK_PANEL,
        ],
        collapsed=False,
    ),
    # Resource Budget / Usage Row
    Row(
        title="Resource Budget / Usage",
        id=panel_id_gen.next(),
        panels=[
            CPU_USAGE_PANEL,
            GPU_USAGE_PANEL,
            CPU_BUDGET_PANEL,
            GPU_BUDGET_PANEL,
            MEMORY_BUDGET_PANEL,
            OBJECT_STORE_MEMORY_BUDGET_PANEL,
            OBJECT_STORE_MEMORY_PANEL,
            FREED_MEMORY_PANEL,
            SPILLED_MEMORY_PANEL,
            BYTES_SPILLED_PANEL,
            BYTES_FREED_PANEL,
        ],
        collapsed=True,
    ),
    # Scheduling Loop Duration Row
    Row(
        title="Scheduling Loop Duration",
        id=panel_id_gen.next(),
        panels=[
            SCHEDULING_LOOP_DURATION_PANEL,
        ],
        collapsed=True,
    ),
    # Tasks Row
    Row(
        title="Tasks",
        id=panel_id_gen.next(),
        panels=[
            SUBMITTED_TASKS_PANEL,
            RUNNING_TASKS_PANEL,
            TASKS_WITH_OUTPUT_PANEL,
            FINISHED_TASKS_PANEL,
            TASK_THROUGHPUT_BY_NODE_PANEL,
            FAILED_TASKS_PANEL,
            BLOCK_GENERATION_TIME_PANEL,
            TASK_SUBMISSION_BACKPRESSURE_PANEL,
            TASK_COMPLETION_TIME_P00_PANEL,
            TASK_COMPLETION_TIME_P05_PANEL,
            TASK_COMPLETION_TIME_P50_PANEL,
            TASK_COMPLETION_TIME_P75_PANEL,
            TASK_COMPLETION_TIME_P90_PANEL,
            TASK_COMPLETION_TIME_P99_PANEL,
            TASK_COMPLETION_TIME_P100_PANEL,
        ],
        collapsed=True,
    ),
    # Iteration Row
    Row(
        title="Iteration",
        id=panel_id_gen.next(),
        panels=[
            ITERATION_INITIALIZATION_PANEL,
            ITERATION_BLOCKED_PANEL,
            ITERATION_USER_PANEL,
        ],
        collapsed=True,
    ),
]

# Get all panel IDs from both top-level panels and panels within rows
all_panel_ids = []
for row in DATA_GRAFANA_ROWS:
    all_panel_ids.append(row.id)
    all_panel_ids.extend(panel.id for panel in row.panels)

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
