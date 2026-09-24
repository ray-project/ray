---
myst:
  html_meta:
    description: "Monitor Ray Data execution with progress bars, the Ray Data dashboard, and Prometheus metrics for inputs, operators, and resource use."
---

(monitoring-your-workload)=

# Monitoring your workload

This page describes how to debug and monitor the execution of your {class}`~ray.data.Dataset` with the following tools:

* {ref}`Ray Data progress bars <ray-data-progress-bars>`
* {ref}`Ray Data dashboard <ray-data-dashboard>`
* {ref}`Ray Data logs <ray-data-logs>`
* {ref}`Ray Data stats <ray-data-stats>`

(ray-data-progress-bars)=

## Ray Data progress bars

When you execute a {class}`~ray.data.Dataset`, Ray Data displays a set of progress bars in the console. The progress bars show execution and progress metrics, including the number of rows completed and remaining, resource usage, and task and actor status. The following annotated image breaks down how to read the progress bar output.

```{image} images/dataset-progress-bar.png
:align: center
```

Keep the following in mind when you read the progress bars:

* The progress bars update every second. Resource usage, metrics, and task and actor status can take up to 5 seconds to update.
* When the tasks section shows the `[backpressure]` label, the operator is *backpressured*. A backpressured operator doesn't submit more tasks until the downstream operator is ready to accept more data.
* The global resource usage is the sum of the resources that all operators use, both active and requested. Requested resources include resources pending scheduling and resources pending node assignment.

(configuring-the-progress-bar)=

### Configure the progress bar

To reduce the progress bar output or turn the progress bars off entirely, use one of the following three settings:

* Disable operator-level progress bars: Set `DataContext.get_current().enable_operator_progress_bars = False`. Ray Data then shows only the global progress bar.
* Disable all progress bars: Set `DataContext.get_current().enable_progress_bars = False`. This setting disables all Ray Data progress bars for dataset execution.
* Disable `ray_tqdm`: Set `DataContext.get_current().use_ray_tqdm = False`. Ray Data then uses the base `tqdm` library instead of its custom distributed `tqdm` implementation. This setting can help when you debug logging issues in a distributed setting.

By default, Ray Data truncates operator names longer than 100 characters, so that long names don't make the progress bar too wide to fit on the screen. To change this behavior, do one of the following:

* To turn off this behavior and show the full operator name, set `DataContext.get_current().enable_progress_bar_name_truncation = False`.
* To change the truncation threshold, update the constant `ray.data._internal.progress_bar.ProgressBar.MAX_NAME_LENGTH = 42`.

:::{tip}
To use the experimental console UI for progress bars, set `DataContext.get_current().enable_rich_progress_bars = True` or set the `RAY_DATA_ENABLE_RICH_PROGRESS_BARS=1` environment variable.
:::

(ray-data-dashboard)=

## Ray Data dashboard

Ray Data emits Prometheus metrics in real time while a Dataset executes. Ray Data tags these metrics by dataset and operator, and the Ray dashboard displays them in several views.

:::{note}
Most metrics are available only for physical operators that use the map operation, such as the physical operators that {meth}`~ray.data.Dataset.map_batches`, {meth}`~ray.data.Dataset.map`, and {meth}`~ray.data.Dataset.flat_map` create.
:::

### Jobs: Ray Data overview

For an overview of every dataset that has run or is running on your cluster, see the **Ray Data Overview** table in the {ref}`jobs view <dash-jobs-view>`. The table appears once the first dataset starts executing on the cluster and shows dataset details such as the following:

* Execution progress, measured in blocks
* Execution state, such as running, failed, or finished
* Dataset start and end time
* Dataset-level metrics, such as the sum of rows processed over all operators

```{image} images/data-overview-table.png
:align: center
```

For a finer-grained view, expand a dataset row in the table to see the same details for each of its operators.

```{image} images/data-overview-table-expanded.png
:align: center
```

:::{tip}
When summing the values of all the individual operators doesn't produce a meaningful dataset-level metric, the operator-level metrics of the last operator might be more useful. For example, to calculate a dataset's throughput, use the **Rows Outputted** value of the dataset's last operator, because the dataset-level metric sums the rows outputted over all operators.
:::

### Ray dashboard metrics

For a time-series view of these metrics, see the Ray Data section in the {ref}`Metrics view <dash-metrics-view>`. This section contains time-series graphs of all metrics that Ray Data emits. The dashboard groups execution metrics by dataset and operator, and iteration metrics by dataset.

The recorded metrics include the following:

* Bytes spilled by objects from object store to disk
* Bytes of objects allocated in object store
* Bytes of objects freed in object store
* Current total bytes of objects in object store
* Logical CPUs allocated to dataset operators
* Logical GPUs allocated to dataset operators
* Bytes outputted by dataset operators
* Rows outputted by dataset operators
* Input blocks received by data operators
* Input blocks and bytes processed in tasks by data operators
* Input bytes submitted to tasks by data operators
* Output blocks, bytes, and rows generated in tasks by data operators
* Output blocks and bytes taken by downstream operators
* Output blocks and bytes from finished tasks
* Submitted tasks
* Running tasks
* Tasks with at least one output block
* Finished tasks
* Failed tasks
* Operator internal inqueue size in blocks and bytes
* Operator internal outqueue size in blocks and bytes
* Size of blocks used in pending tasks
* Freed memory in object store
* Spilled memory in object store
* Time spent generating blocks
* Time spent transforming data, and its breakdown into input prep, function body, and output block build
* Time spent in task submission backpressure
* Time spent to initialize iteration
* Time user code is blocked during iteration
* Time spent in user code during iteration

```{image} images/data-dashboard.png
:align: center
```

For more about the Ray dashboard, including setup instructions, see {ref}`Ray dashboard <observability-getting-started>`.

### Prometheus metrics

Ray Data emits Prometheus metrics that you can use to monitor dataset execution. Ray Data tags the metrics with `dataset` and `operator` labels, so you can identify which dataset and operator each metric comes from.

To access these metrics, query the Prometheus server running on the Ray head node. The default Prometheus server URL is `http://<head-node-ip>:9090`.

The following tables list all Ray Data metrics, grouped by category.

#### Overview metrics

These metrics provide high-level information about dataset execution and resource usage.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `data_spilled_bytes`
  - Bytes spilled by dataset operators. Set `DataContext.enable_get_object_locations_for_metrics` to `True` to report this metric.
* - `data_freed_bytes`
  - Bytes freed by dataset operators.
* - `data_current_bytes`
  - Bytes of object store memory used by dataset operators.
* - `data_cpu_usage_cores`
  - CPUs allocated to dataset operators.
* - `data_gpu_usage_cores`
  - GPUs allocated to dataset operators.
* - `data_memory_usage_bytes`
  - Heap memory allocated to dataset operators.
* - `data_output_bytes`
  - Bytes outputted by dataset operators.
* - `data_output_rows`
  - Rows outputted by dataset operators.
```

#### Input metrics

These metrics track input data flowing into operators.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `num_inputs_received`
  - Number of input blocks received by the operator
* - `num_row_inputs_received`
  - Number of input rows received by the operator
* - `bytes_inputs_received`
  - Byte size of input blocks received by the operator
* - `num_task_inputs_processed`
  - Number of input blocks that the operator's tasks finished processing
* - `bytes_task_inputs_processed`
  - Byte size of input blocks that the operator's tasks finished processing
* - `bytes_inputs_of_submitted_tasks`
  - Byte size of input blocks passed to submitted tasks
* - `rows_inputs_of_submitted_tasks`
  - Number of rows in the input blocks passed to submitted tasks
* - `average_num_inputs_per_task`
  - Average number of input blocks per task, or `None` if no task finished
* - `average_bytes_inputs_per_task`
  - Average size in bytes of ref bundles passed to tasks, or `None` if no tasks submitted
* - `average_rows_inputs_per_task`
  - Average number of rows in input blocks per task, or `None` if no task submitted
```

#### Output metrics

These metrics track output data generated by operators.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `num_task_outputs_generated`
  - Number of output blocks generated by tasks
* - `bytes_task_outputs_generated`
  - Byte size of output blocks generated by tasks
* - `rows_task_outputs_generated`
  - Number of output rows generated by tasks
* - `row_outputs_taken`
  - Number of rows that downstream operators have taken
* - `block_outputs_taken`
  - Number of blocks that downstream operators have taken
* - `num_outputs_taken`
  - Number of output blocks that downstream operators have taken
* - `bytes_outputs_taken`
  - Byte size of output blocks that downstream operators have taken
* - `num_outputs_of_finished_tasks`
  - Number of generated output blocks that are from finished tasks
* - `bytes_outputs_of_finished_tasks`
  - Total byte size of generated output blocks produced by finished tasks
* - `rows_outputs_of_finished_tasks`
  - Number of rows generated by finished tasks
* - `num_external_inqueue_blocks`
  - Number of blocks in the external inqueue
* - `num_external_inqueue_bytes`
  - Byte size of blocks in the external inqueue
* - `num_external_outqueue_blocks`
  - Number of blocks in the external outqueue
* - `num_external_outqueue_bytes`
  - Byte size of blocks in the external outqueue
* - `average_num_outputs_per_task`
  - Average number of output blocks per task, or `None` if no task finished
* - `average_bytes_per_output`
  - Average size in bytes of output blocks
* - `average_bytes_outputs_per_task`
  - Average total output size of task in bytes, or `None` if no task finished
* - `average_rows_outputs_per_task`
  - Average number of rows produced per task, or `None` if no task finished
* - `num_output_blocks_per_task_s`
  - Average number of output blocks per task per second
```

#### Task metrics

These metrics track task execution and scheduling.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `num_tasks_submitted`
  - Number of submitted tasks.
* - `num_tasks_running`
  - Number of running tasks.
* - `num_tasks_have_outputs`
  - Number of tasks with at least one output.
* - `num_tasks_finished`
  - Number of finished tasks.
* - `num_tasks_failed`
  - Number of failed tasks.
* - `block_generation_time`
  - Time spent generating blocks in tasks.
* - `block_transform_time_s`
  - Time spent transforming one output block, summed over the operator's blocks. Only map operators report this metric.
* - `input_prep_time_s`
  - Part of `block_transform_time_s` spent turning input blocks into batches or rows. This metric is absent when the operator measures only the total, which a row transform does by default.
* - `function_body_time_s`
  - Part of `block_transform_time_s` spent inside the stage bodies. This metric is absent under the same condition as `input_prep_time_s`.
* - `output_build_time_s`
  - Part of `block_transform_time_s` spent assembling stage output back into blocks. This metric is absent under the same condition as `input_prep_time_s`.
* - `task_submission_backpressure_time`
  - Time spent in task submission backpressure.
* - `task_output_backpressure_time`
  - Time spent in task output backpressure.
* - `task_completion_time`
  - Histogram of time spent running tasks to completion.
* - `block_completion_time`
  - Histogram of time spent running a single block to completion. If a task generates multiple blocks, Ray Data approximates this value by assuming each block took the same amount of time to process.
* - `task_completion_time_s`
  - Time spent running tasks to completion.
* - `task_completion_time_excl_backpressure_s`
  - Time spent running tasks to completion without backpressure.
* - `block_size_bytes`
  - Histogram of block sizes in bytes generated by tasks.
* - `block_size_rows`
  - Histogram of number of rows in blocks generated by tasks.
* - `average_total_task_completion_time_s`
  - Average task completion time in seconds including throttling. This includes Ray Core and Ray Data backpressure.
* - `average_task_completion_excl_backpressure_time_s`
  - Average task completion time in seconds excluding throttling.
* - `average_max_uss_per_task`
  - Average Unique Set Size (USS) memory usage of tasks. USS is the amount of memory unique to a process, which is freed when the process terminates.
```

#### Actor metrics

These metrics track the actor lifecycle for operations that use actors.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `num_alive_actors`
  - Number of alive actors
* - `num_restarting_actors`
  - Number of restarting actors
* - `num_pending_actors`
  - Number of pending actors
```

#### Object store memory metrics

These metrics track memory usage in the Ray object store.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `obj_store_mem_internal_inqueue_blocks`
  - Number of blocks in the operator's internal input queue
* - `obj_store_mem_internal_outqueue_blocks`
  - Number of blocks in the operator's internal output queue
* - `obj_store_mem_freed`
  - Byte size of freed memory in object store
* - `obj_store_mem_spilled`
  - Byte size of spilled memory in object store
* - `obj_store_mem_used`
  - Byte size of used memory in object store
* - `obj_store_mem_internal_inqueue`
  - Byte size of input blocks in the operator's internal input queue
* - `obj_store_mem_internal_outqueue`
  - Byte size of output blocks in the operator's internal output queue
* - `obj_store_mem_pending_task_inputs`
  - Byte size of input blocks used by pending tasks
```

#### Scheduling and resource metrics

These metrics track resource allocation and scheduling behavior in the streaming executor.

```{list-table}
:header-rows: 1
:widths: 30 70

* - Metric name
  - Description
* - `data_sched_loop_duration_s`
  - Duration of the scheduling loop in seconds
* - `data_cpu_budget`
  - CPU budget allocated per operator
* - `data_gpu_budget`
  - GPU budget allocated per operator
* - `data_memory_budget`
  - Memory budget allocated per operator
* - `data_object_store_memory_budget`
  - Object store memory budget allocated per operator
* - `data_max_bytes_to_read`
  - Maximum bytes to read from streaming generator buffer per operator
```

(ray-data-logs)=

## Ray Data logs

During execution, Ray Data periodically logs updates to `ray-data.log`.

Every five seconds, Ray Data logs the execution progress of every operator in the dataset. For more frequent updates, set `RAY_DATA_TRACE_SCHEDULING=1` so that Ray Data logs the progress after it dispatches each task.

```text
Execution Progress:
0: - Input: 0 active, 0 queued, 0.0 MiB objects, Blocks Outputted: 200/200
1: - ReadRange->MapBatches(<lambda>): 10 active, 190 queued, 381.47 MiB objects, Blocks Outputted: 100/200
```

When an operator completes, Ray Data also logs the metrics for that operator.

```text
Operator InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange->MapBatches(<lambda>)] completed. Operator Metrics:
{'num_inputs_received': 20, 'bytes_inputs_received': 46440, 'num_task_inputs_processed': 20, 'bytes_task_inputs_processed': 46440, 'num_task_outputs_generated': 20, 'bytes_task_outputs_generated': 800, 'rows_task_outputs_generated': 100, 'num_outputs_taken': 20, 'bytes_outputs_taken': 800, 'num_outputs_of_finished_tasks': 20, 'bytes_outputs_of_finished_tasks': 800, 'num_tasks_submitted': 20, 'num_tasks_running': 0, 'num_tasks_have_outputs': 20, 'num_tasks_finished': 20, 'obj_store_mem_freed': 46440, 'obj_store_mem_spilled': 0, 'block_generation_time': 1.191296085, 'cpu_usage': 0, 'gpu_usage': 0, 'ray_remote_args': {'num_cpus': 1, 'scheduling_strategy': 'SPREAD'}}
```

You can find this log file locally at `/tmp/ray/{SESSION_NAME}/logs/ray-data/ray-data.log`. You can also find it on the Ray dashboard under the head node's logs in the {ref}`Logs view <dash-logs-view>`.

(ray-data-stats)=

## Ray Data stats

To see detailed stats on a dataset's execution, call the {meth}`~ray.data.Dataset.stats` method.

### Operator stats

The stats output includes a summary of each operator's execution stats. Ray Data calculates this summary across many blocks, so some stats show the min, max, mean, and sum aggregated over all the blocks. The output includes the following stats at the operator level:

* **Remote wall time**: The start-to-finish time for an operator. It includes time when the operator isn't processing data, such as time spent sleeping or waiting for I/O.
* **Remote CPU time**: The process time for an operator, which excludes time slept. It includes both user and system CPU time.
* **Block transform time**: The time an operator spends transforming data, which Ray Data measures per output block. The min, max, and mean are therefore across blocks, and the total is the operator's. This isn't the same as the task's total time, which also covers scheduling and writing blocks to the object store. Read, write, and map operators all report it, because a read and a write run functions too. It covers the functions you pass into Ray Data methods, such as {meth}`~ray.data.Dataset.map`, {meth}`~ray.data.Dataset.map_batches`, and {meth}`~ray.data.Dataset.filter`, plus the work around those calls that feeds them and collects what they return. Set `DataContext.verbose_stats_logs` to break it into the following phases, which sum to this total:

  * **Input prep**: Time spent turning input blocks into the batches or rows your functions receive, including converting them to the `batch_format` you asked for. This can dominate when rows hold Python objects or large tensors.
  * **Function body**: Time spent inside the stage bodies, excluding the prep and build around them. This covers the functions you passed in and the ones Ray Data supplies, such as a read or a write that Ray Data fused into the same operator, because a read or a write is a function like any other.
  * **Output block build**: Time spent assembling what your functions return back into blocks, including materializing Python objects into Arrow. This is separate from the object store write, which Ray Data reports as the `data_block_serialization_time_s` metric.

  Ray Data fuses adjacent operators where it can, and a fused operator reports one figure per phase covering all of its stages. Row-based transforms such as {meth}`~ray.data.Dataset.map` report the breakdown only when you also set `DataContext.accurate_map_phase_timing`, because timing each row individually costs more than the breakdown reports. The figures are always present on the summary object that `Dataset.get_stats_summary()` returns, regardless of either setting.
* **Memory usage**: The output displays memory usage per block in MiB.
* **Output stats**: The output includes the number of output rows and the output size in bytes per block, plus the number of output rows per task. Together, these stats show how much data Ray Data outputs per block and per task.
* **Task stats**: The output shows the scheduling of tasks to nodes. Use it to check whether you're using all of your nodes as expected.
* **Throughput**: The summary calculates the operator's throughput and, for comparison, estimates the throughput of the same task on a single node. This estimate assumes the total time of the work stays the same, but with no concurrency. The overall summary also calculates the dataset-level throughput, including a single-node estimate.

(reading-the-block-transform-time-breakdown)=

### Read the block transform time breakdown

Ray Data fuses adjacent operators into one task, so a single block transform time breakdown often spans several stages, some of them yours and some of them Ray Data's. Consider the following pipeline:

```python
import time
import ray
from ray.data.context import DataContext

DataContext.get_current().verbose_stats_logs = True

def map1(batch):
    time.sleep(0.1)
    return batch

def map2(batch):
    time.sleep(0.2)
    return batch

ds = (
    ray.data.range(2, override_num_blocks=2)
    .select_columns(["id"])
    .map_batches(map1, batch_size=None)
    .map_batches(map2, batch_size=None)
    .materialize()
)
print(ds.stats())
```

Ray Data fuses all four stages into one operator, and the breakdown splits that operator's time as follows:

```text
Operator 1 ReadRange->Project->MapBatches(map1)->MapBatches(map2): 2 tasks executed, 2 blocks produced in 0.67s
* Remote wall time: 308.95ms min, 341.72ms max, 325.33ms mean, 650.67ms total
* Remote cpu time: 3.96ms min, 30.06ms max, 17.01ms mean, 34.02ms total
* Block transform time: 307.77ms min, 341.02ms max, 324.4ms mean, 648.79ms total
    * Input prep: 270.79us min, 1.42ms max, 845.89us mean, 1.69ms total
    * Function body: 306.8ms min, 338.86ms max, 322.83ms mean, 645.66ms total
    * Output block build: 700.34us min, 743.38us max, 721.86us mean, 1.44ms total
...
```

Every figure covers all four stages:

* **Function body** holds all four stage bodies, which are `ReadRange`, `Project`, `map1`, and `map2`. Two tasks each slept 0.1 seconds and then 0.2 seconds, so `map1` and `map2` account for 0.6 seconds of the 645 ms total. The read and the column projection account for the remainder.
* **Input prep** and **Output block build** likewise cover every stage, not only the two you wrote. Each stage forms its own batches and builds its own output blocks, and all four report into the same two figures.

This breakdown tells you which *phase* the time went to, not which stage. Attributing the body time to `map2` rather than to the operator needs a per-stage breakdown, which Ray Data doesn't report yet.

### Iterator stats

When you iterate over the data, Ray Data also generates iteration stats. You might see iteration stats even when you don't iterate over the data directly, such as when you call {meth}`~ray.data.Dataset.take_all`. The iterator-level stats include the following:

* **Iterator initialization**: The time Ray Data spent initializing the iterator. This time is internal to Ray Data.
* **Time user thread is blocked**: The time Ray Data spent producing data in the iterator. If you haven't materialized the dataset before, this time is often the primary execution time of the dataset.
* **Time in user thread**: The time spent in the user thread that iterates over the dataset, outside the Ray Data code. If this time is high, consider optimizing the body of the loop that iterates over the dataset.
* **Batch iteration stats**: The stats for batch prefetching. These times are internal to Ray Data code, but you can still optimize them by tuning the prefetching process.

(verbose-stats)=

### Enable verbose stats

By default, Ray Data logs only the most important high-level stats. To turn on verbose stats output, add the following code to your Ray Data program:

```{testcode}
from ray.data import DataContext

context = DataContext.get_current()
context.verbose_stats_logs = True
```

With verbose stats on, Ray Data adds the following outputs:

* **Extra metrics**: A dictionary of metrics that components such as operators and executors can add to. Some of these stats duplicate the default output, but the dictionary gives advanced users more insight into the dataset's execution.
* **Runtime metrics**: A high-level breakdown of the dataset execution's runtime. For each operator, these stats show the time the operator took to complete and that time as a fraction of the total execution time. Because multiple operators can run concurrently, these percentages don't necessarily sum to 100%. Instead, they show how long each operator runs relative to the full dataset execution.
* **Block transform time breakdown**: Ray Data splits each operator's block transform time, which it measures per output block, into the input prep, function body, and output block build phases, so you can see which part of the transform the time went to.

### Example stats

The following stats output comes from {doc}`Image Classification Batch Inference with PyTorch ResNet18 </data/examples/pytorch_resnet_batch_prediction>`:

```text
Operator 1 ReadImage->Map(preprocess_image): 384 tasks executed, 386 blocks produced in 9.21s
* Remote wall time: 33.55ms min, 2.22s max, 1.03s mean, 395.65s total
* Remote cpu time: 34.93ms min, 3.36s max, 1.64s mean, 632.26s total
* Block transform time: 535.1ms min, 2.16s max, 975.7ms mean, 376.62s total
* Peak heap memory usage (MiB): 556.32 min, 1126.95 max, 655 mean
* Output num rows per block: 4 min, 25 max, 24 mean, 9469 total
* Output size bytes per block: 6060399 min, 105223020 max, 31525416 mean, 12168810909 total
* Output rows per task: 24 min, 25 max, 24 mean, 384 tasks used
* Tasks per node: 32 min, 64 max, 48 mean; 8 nodes used
* Operator throughput:
      * Ray Data throughput: 1028.5218637702708 rows/s
      * Estimated single node throughput: 23.932674100499128 rows/s

Operator 2 MapBatches(ResnetModel): 14 tasks executed, 48 blocks produced in 27.43s
* Remote wall time: 523.93us min, 7.01s max, 1.82s mean, 87.18s total
* Remote cpu time: 523.23us min, 6.23s max, 1.76s mean, 84.61s total
* Block transform time: 4.49s min, 17.81s max, 10.52s mean, 505.08s total
* Peak heap memory usage (MiB): 4025.42 min, 7920.44 max, 5803 mean
* Output num rows per block: 84 min, 334 max, 197 mean, 9469 total
* Output size bytes per block: 72317976 min, 215806447 max, 134739694 mean, 6467505318 total
* Output rows per task: 319 min, 720 max, 676 mean, 14 tasks used
* Tasks per node: 3 min, 4 max, 3 mean; 4 nodes used
* Operator throughput:
      * Ray Data throughput: 345.1533728632648 rows/s
      * Estimated single node throughput: 108.62003864820711 rows/s

Dataset iterator time breakdown:
* Total time overall: 38.53s
   * Total time in Ray Data iterator initialization code: 16.86s
   * Total time user thread is blocked by Ray Data iter_batches: 19.76s
   * Total execution time for user thread: 1.9s
* Batch iteration time breakdown (summed across prefetch threads):
   * In ray.get(): 70.49ms min, 2.16s max, 272.8ms avg, 13.09s total
   * In batch creation: 3.6us min, 5.95us max, 4.26us avg, 204.41us total
   * In batch formatting: 4.81us min, 7.88us max, 5.5us avg, 263.94us total

Dataset throughput:
      * Ray Data throughput: 1026.5318925757008 rows/s
      * Estimated single node throughput: 19.611578909587674 rows/s
```

With verbose stats on, the same example produces the following stats output:

```text
Operator 1 ReadImage->Map(preprocess_image): 384 tasks executed, 387 blocks produced in 9.49s
* Remote wall time: 22.81ms min, 2.5s max, 999.95ms mean, 386.98s total
* Remote cpu time: 24.06ms min, 3.36s max, 1.63s mean, 629.93s total
* Block transform time: 552.79ms min, 2.41s max, 956.84ms mean, 370.3s total
* Peak heap memory usage (MiB): 550.95 min, 1186.28 max, 651 mean
* Output num rows per block: 4 min, 25 max, 24 mean, 9469 total
* Output size bytes per block: 4444092 min, 105223020 max, 31443955 mean, 12168810909 total
* Output rows per task: 24 min, 25 max, 24 mean, 384 tasks used
* Tasks per node: 39 min, 60 max, 48 mean; 8 nodes used
* Operator throughput:
      * Ray Data throughput: 997.9207015895857 rows/s
      * Estimated single node throughput: 24.46899945870273 rows/s
* Extra metrics: {'num_inputs_received': 384, 'bytes_inputs_received': 1104723940, 'num_task_inputs_processed': 384, 'bytes_task_inputs_processed': 1104723940, 'bytes_inputs_of_submitted_tasks': 1104723940, 'num_task_outputs_generated': 387, 'bytes_task_outputs_generated': 12168810909, 'rows_task_outputs_generated': 9469, 'num_outputs_taken': 387, 'bytes_outputs_taken': 12168810909, 'num_outputs_of_finished_tasks': 387, 'bytes_outputs_of_finished_tasks': 12168810909, 'num_tasks_submitted': 384, 'num_tasks_running': 0, 'num_tasks_have_outputs': 384, 'num_tasks_finished': 384, 'num_tasks_failed': 0, 'block_generation_time': 386.97945193799995, 'task_submission_backpressure_time': 7.263684450000142, 'obj_store_mem_internal_inqueue_blocks': 0, 'obj_store_mem_internal_inqueue': 0, 'obj_store_mem_internal_outqueue_blocks': 0, 'obj_store_mem_internal_outqueue': 0, 'obj_store_mem_pending_task_inputs': 0, 'obj_store_mem_freed': 1104723940, 'obj_store_mem_spilled': 0, 'obj_store_mem_used': 12582535566, 'cpu_usage': 0, 'gpu_usage': 0, 'ray_remote_args': {'num_cpus': 1, 'scheduling_strategy': 'SPREAD'}}

Operator 2 MapBatches(ResnetModel): 14 tasks executed, 48 blocks produced in 28.81s
* Remote wall time: 134.84us min, 7.23s max, 1.82s mean, 87.16s total
* Remote cpu time: 133.78us min, 6.28s max, 1.75s mean, 83.98s total
* Block transform time: 4.56s min, 17.78s max, 10.28s mean, 493.48s total
* Peak heap memory usage (MiB): 3925.88 min, 7713.01 max, 5688 mean
* Output num rows per block: 125 min, 259 max, 197 mean, 9469 total
* Output size bytes per block: 75531617 min, 187889580 max, 134739694 mean, 6467505318 total
* Output rows per task: 325 min, 719 max, 676 mean, 14 tasks used
* Tasks per node: 3 min, 4 max, 3 mean; 4 nodes used
* Operator throughput:
      * Ray Data throughput: 328.71474145609153 rows/s
      * Estimated single node throughput: 108.6352856660782 rows/s
* Extra metrics: {'num_inputs_received': 387, 'bytes_inputs_received': 12168810909, 'num_task_inputs_processed': 0, 'bytes_task_inputs_processed': 0, 'bytes_inputs_of_submitted_tasks': 12168810909, 'num_task_outputs_generated': 1, 'bytes_task_outputs_generated': 135681874, 'rows_task_outputs_generated': 252, 'num_outputs_taken': 1, 'bytes_outputs_taken': 135681874, 'num_outputs_of_finished_tasks': 0, 'bytes_outputs_of_finished_tasks': 0, 'num_tasks_submitted': 14, 'num_tasks_running': 14, 'num_tasks_have_outputs': 1, 'num_tasks_finished': 0, 'num_tasks_failed': 0, 'block_generation_time': 7.229860895999991, 'task_submission_backpressure_time': 0, 'obj_store_mem_internal_inqueue_blocks': 13, 'obj_store_mem_internal_inqueue': 413724657, 'obj_store_mem_internal_outqueue_blocks': 0, 'obj_store_mem_internal_outqueue': 0, 'obj_store_mem_pending_task_inputs': 12168810909, 'obj_store_mem_freed': 0, 'obj_store_mem_spilled': 0, 'obj_store_mem_used': 1221136866.0, 'cpu_usage': 0, 'gpu_usage': 4}

Dataset iterator time breakdown:
* Total time overall: 42.29s
   * Total time in Ray Data iterator initialization code: 20.24s
   * Total time user thread is blocked by Ray Data iter_batches: 19.96s
   * Total execution time for user thread: 2.08s
* Batch iteration time breakdown (summed across prefetch threads):
   * In ray.get(): 73.0ms min, 2.15s max, 246.3ms avg, 11.82s total
   * In batch creation: 3.62us min, 6.6us max, 4.39us avg, 210.7us total
   * In batch formatting: 4.75us min, 8.67us max, 5.52us avg, 264.98us total

Dataset throughput:
      * Ray Data throughput: 468.11051989434594 rows/s
      * Estimated single node throughput: 972.8197093015862 rows/s

Runtime Metrics:
* ReadImage->Map(preprocess_image): 9.49s (46.909%)
* MapBatches(ResnetModel): 28.81s (142.406%)
* Scheduling: 6.16s (30.448%)
* Total: 20.23s (100.000%)
```
