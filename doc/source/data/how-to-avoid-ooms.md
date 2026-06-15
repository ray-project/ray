# How to avoid out-of-memory errors (OOMs)

Out-of-memory errors (OOMs) are one of the most common issues Ray Data users encounter.

This guide describes what OOMs look like and provides practical guidance for mitigating 
them.

For a lower-level explanation of how Ray treats memory, read 
{doc}`Ray Core resource isolation </ray-core/resource-isolation-with-cgroupv2>`. 

## What OOMs look like

OOMs show up in several ways. If you see one or more of these error messages, your
job might be using too much memory.

### Ray OOM kills

When the Ray OOM killer proactively kills a task or actor, you might see an error like
this:

```
(raylet) Task _map_task failed due to oom. There are infinite oom retries remaining, so the task will be retried. Error: 1 worker(s) were killed due to the node running low on memory. Memory on the node (IP: 10.0.50.112, ID: 4cb25bc084aeb5a31ca5402ad589ae042a71165a8e2dd8418fecee26) was 28.62GB / 30.00GB (0.954017), which exceeds the memory usage threshold of 0.950000; Object store memory usage: [- objects spillable: 0; - bytes spillable: 0; - objects unsealed: 0; - bytes unsealed: 0; - objects in use: 10; - bytes in use: 42199; - objects evictable: 13; - bytes evictable: 51792; ; - objects created by worker: 0; - bytes created by worker: 0; - objects restored: 0; - bytes restored: 0; - objects received: 23; - bytes received: 93991; - objects errored: 0; - bytes errored: 0; ; Eviction Stats:; (global lru) capacity: 9614230732; (global lru) used: 0.000538701%; (global lru) num objects: 13; (global lru) num evictions: 0; (global lru) bytes evicted: 0]; Ray killed 1 worker(s) based on the killing policy: [(Task: job ID=05000000, lease ID=2f00000005000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=ReadRange->MapBatches(uses_lots_of_memory), required resources={memory: 8, CPU: 1}, pid=3471, actual memory used=2.85GB, worker ID=8150a4b5316a164c2932ec1389b38be165bf0e29465ff65328fa0162)]; To see more information about memory usage on this node, use `ray logs raylet.out -ip 10.0.50.112`; Top 10 memory users: PID        MEM(GB) COMMAND, 2936   4.10    , 2938  4.02    ray::ReadRange->MapBatches(uses_lots_of_memory), 2943  3.99    ray::ReadRange->MapBatches(uses_lots_of_memory), 2939   3.97    ray::ReadRange->MapBatches(uses_lots_of_memory), 2942   3.83    ray::ReadRange->MapBatches(uses_lots_of_memory), 3468  3.18    ray::ReadRange->MapBatches(uses_lots_of_memory), 3471   2.85    ray::ReadRange->MapBatches(uses_lots_of_memory), 3815   2.46    ray::ReadRange->MapBatches(uses_lots_of_memory), 2419  0.09    ray::DashboardAgent, 2421       0.05    ray::RuntimeEnvAgent, Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.
```

You can see the number of Ray OOM kills in the "..." chart in the Ray Core dashboard:

![Ray OOM kills chart](ray-oom-kills-chart.png)

### Kernel OOM kills

When the kernel OOM killer kills a Ray process before the Ray OOM killer, you might see
an error like this:

```
(raylet) Task _map_task failed. There are infinite retries remaining, so the task will be retried. Error:                                                                                      
(raylet) A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker. Lease ID: 2100000005000000ffffffffffffffffffffffffffffffffffffffffffffffff Worker ID: 863d8a6a594d60f8d143462b96cd3bf4270eafe617aaf2d2ca7266cb Node ID: 4cb25bc084aeb5a31ca5402ad589ae042a71165a8e2dd8418fecee26 Worker IP address: 10.0.50.112 Worker port: 10015 Worker PID: 2938 Worker exit type: SYSTEM_ERROR Worker exit detail: Worker unexpectedly exits with a connection error code 2. End of file. Some common causes include: (1) the process was killed by the OOM killer due to high memory usage, (2) ray stop --force was called, or (3) the worker crashed unexpectedly due to SIGSEGV or another unexpected error.
```

You can see the number of unexpected worker deaths in the "..." chart in the Ray Core 
dashboard. Kernel OOM kills often cause unexpected worker death.

![Unexpected system-level worker deaths chart](unexpected-system-level-chart.png)

### Node death

If you're using older versions of Ray without resource isolation, nodes can die under
memory pressure, and you might see an error like this:

```
{"asctime":"2026-03-23 18:24:28,943","levelname":"E","message":":info_message: Attempting to recover 41 lost objects by resubmitting their tasks or setting a new primary location from existing copies. To disable object reconstruction, set @ray.remote(max_retries=0).","filename":"core_worker.cc","lineno":475}
```

You can see node death in the "Node Count" chart in the Ray Core dashboard:

![Unexpected system-level worker deaths chart](node-count.png)

Nodes die for reasons unrelated to memory pressure. If you see node death along with 
other memory-related errors, memory pressure might have caused the death.

## Best practices

### Use ``batch_size="auto"`` or small batch sizes

Choose the smallest batch size that achieves good performance, or use
``batch_size="auto"``.

<!-- We're recommending 16 MiB because we found that it's the smallest batch size
that doesn't degrade throughput on a variety of UDFs.
See https://docs.google.com/document/d/1sw9CVm9cKp1b6voLc5gWIJLJM57NSGQjJ-HxvD_92jQ/edit?tab=t.0 -->

If your UDF runs on CPU and isn't vectorized, use `map` instead. If it is vectorized, a
good rule of thumb is a batch size of about 16 MiB. Unlike GPUs, CPUs have limited 
ability to parallelize work, so they don't benefit from large batches the way a GPU 
does.

<!-- The rule of thumb to use 1/4 of GRAM comes from @stephanie-wang -->

If your UDF runs on GPU, a good rule of thumb is to use about 1/4 of the GPU memory. 
Keep in mind that large GPU batches increase the risk of not only GPU OOMs, but also of 
regular heap OOMs because Ray Data builds the batch in heap memory first.

### Configure ``memory`` for reads and high-memory UDFs

If a task or actor uses more than a few GiB of memory, set ``memory``. This tells Ray 
Data how much memory each task or actor needs so it doesn't launch too many at once.

To pick a value for ``memory``, read the Ray Data log file and look for the 
`max_uss_bytes` field. Ray typically writes the log file to
`/tmp/ray/session-latest/ray-data/ray-data.log`. 

```
ReadRange->MapBatches(uses_lots_of_memory): {'average_num_outputs_per_task': 1.0, ..., 'max_uss_bytes': {'num_samples': 20, 'mean': 4393336422.4, 'variance': 26855731156.89417, 'min': 4393119744, 'max': 4393529344, 'p50': 4393418752.0, 'p90': 4393500672.0, 'p95': 4393529344.0, 'p99': 4393529344.0}, ...}
```

Ray Data also emits the information to STDOUT:

```
Operator 'ReadRange->MapBatches(uses_lots_of_memory)' uses 4.1GiB of
memory per task on average, but Ray only requests 0.0B per task at the
start of the pipeline.

To avoid out-of-memory errors, consider setting `memory=4.1GiB` in the
appropriate function or method call. (This might be unnecessary if the
number of concurrent tasks is low.)

To change the frequency of this warning, set
`DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
or disable the warning by setting value to -1. (current value: 30)
```

### Enable default map memory

Unless you specify a value, Ray Data assumes a UDF needs 0 ``memory``. So even if you've
set ``memory`` correctly for some APIs, Ray Data can still oversubscribe tasks and 
actors for the ones you haven't.

To avoid this, set ``DataContext.get_current().default_map_logical_memory = True``.

### Start Ray with resource isolation


...


### Isolate reads for large files

Ray Data uses PyArrow to implement APIs like `read_parquet`, and PyArrow can allocate 
lots of memory that isn't reclaimed when the read tasks finish. Because Ray reuses 
workers across operators, a downstream operator can schedule tasks onto a worker that's 
still holding that allocation. As a result, downstream operators can appear to be 
consuming far more memory than they actually are.

If you encounter this, try ``DataContext.get_current().isolate_read_workers = True``. 
The flag prevents Ray Data from scheduling downstream operators on the same 
workers as reads. It can improve memory safety at the cost of some performance.

### Don't increase RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION

Older versions of Ray Data emit a warning that suggests you increase 
`RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION`. While this can improve performance for 
some workloads like shuffle, it can also increase the risk of OOMs because it decreases
the amount of memory available for your UDFs.

To improve memory safety, don't configure the knob.

### Configure system memory to cover the raylet and anything outside the container

By default, Ray reserves 10% of physical memory for system use. "System" covers Ray
processes that aren't worker tasks or actors, the OS itself, and anything else on the
node that isn't Ray, including processes outside the container.

If you run large non-Ray processes like Vector, you might need to set this higher
than the default.

The default is usually fine unless you're on tiny nodes, like an m5.xlarge.

## What to expect after tuning

If you do all of the following:

- Start Ray with resource isolation enabled.
- Set system memory large enough to cover everything used outside of Ray worker tasks 
  and actors
- Set logical memory to physical memory minus system memory minus object store memory.
- Set ``memory`` for each API to at least the heap memory that API needs to run.

Then you shouldn't see OOMs or node deaths.

The main limitation of these configurations is performance. When you set ``memory`` 
based on worst-case heap memory use, the system might launch fewer tasks or actors than 
it might be able to, and that can decrease throughput.

If you want to experiment with oversubscription at the risk of potential OOMs, decrease
`memory`.

## Further reading

For a deeper understanding of how Ray handles memory, read 
{doc}`Ray Core resource isolation </ray-core/resource-isolation-with-cgroupv2>`.
