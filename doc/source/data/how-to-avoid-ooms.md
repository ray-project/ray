# How to avoid out-of-memory errors (OOMs)

Out-of-memory errors (OOMs) are one of the most common issues Ray Data users encounter.

This guide describes what OOMs look like and provides practical guidance for mitigating 
them.

For a lower-level explanation of how Ray treats memory, read 
{doc}`Ray Core resource  isolation </ray-core/resource-isolation-with-cgroupv2>`. 

## What OOMs look like

OOMs show up in several ways. If you see one or more of these error messages, your
job might be using too much memory.

### Ray OOM kils

When the Ray OOM killer proactively kills a task or actor, you might see an error like
this:

```
  Task hungry_hippo failed due to oom. There are infinite oom retries remaining, so the task will be retried. Error: 2 worker(s) were killed due to the node running low on memory. Memory on the node (IP: <ip address>, ID: 92edc4e97e4dac3cee61126133ee7ab6d0a2ee73803623d24a02979d) was 110.69GB / 124.35GB (0.890161)
  OOM kill reason: user cgroup memory upper bound was met or exceeded
  Object store memory usage: [- objects spillable: 0
  - bytes spillable: 0
  - objects unsealed: 0
  - bytes unsealed: 0
  - objects in use: 0
  - bytes in use: 0
  - objects evictable: 0
  - bytes evictable: 0
  
  - objects created by worker: 0
  - bytes created by worker: 0
  - objects restored: 0
  - bytes restored: 0
  - objects received: 0
  - bytes received: 0
  - objects errored: 0
  - bytes errored: 0
  
  Eviction Stats:
  (global lru) capacity: 35098657996
  (global lru) used: 0%
  (global lru) num objects: 0
  (global lru) num evictions: 0
  (global lru) bytes evicted: 0]
  Ray killed 2 worker(s) based on the killing policy
  Considered workers: [
  Selected to kill: (Task: job ID=01000000, lease ID=0600000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310152, actual memory used=0.67GB, worker ID=3e3d8f80b70d48b643d79ed2292b5d4f779820a964e55ad65413687d)
  Selected to kill: (Task: job ID=01000000, lease ID=0400000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310153, actual memory used=21.64GB, worker ID=0e5649d39c15609c0db6a5cf95de94befded2ee7da2facbf64b52e6f)
  (Task: job ID=01000000, lease ID=0500000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310151, actual memory used=21.95GB, worker ID=34241048bfb59ac29bd5e32d706c9bd41eafc6972c9bcbada99464e7)
  (Task: job ID=01000000, lease ID=0000000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310149, actual memory used=21.87GB, worker ID=2cc6dbeef4ebc06789de65fb43e04fbe1feebf1e699902ece89a8328)
  (Task: job ID=01000000, lease ID=0200000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310155, actual memory used=21.85GB, worker ID=14d4cc84e2f21ba3edbc9948b780d67013afbffda99dd33e829d56d3)
  (Task: job ID=01000000, lease ID=0100000001000000ffffffffffffffffffffffffffffffffffffffffffffffff, task name=hungry_hippo, required resources={CPU: 1}, pid=3310147, actual memory used=21.53GB, worker ID=c90db9af23d78530d1f848c1301bc4b877925fe9122bc70948ad9489)]
  Total non-selected idle workers: 25
  Total non-selected idle workers USS bytes: 1.00GB
  To see more information about memory usage on this node, use `ray logs raylet.out -ip <ip address>`
  Top 10 memory users: PID  MEM(GB) COMMAND
  3310151   21.95   ray::hungry_hippo
  3310149   21.87   ray::hungry_hippo
  3310155   21.85   ray::hungry_hippo
  3310153   21.64   ray::hungry_hippo
  3310147   21.53   ray::hungry_hippo
  3108574   1.95    bazel
  3180337   1.61    ray::foo_actor
  3310152   0.67    ray::hungry_hippo
  2924839   0.53    ray::idle_worker
  3149737   0.47    ray::idle_worker
  Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero. Since 2.56, Ray updated the oom killing policy to enabling killing multiple workers and selecting workers based on the time since the task start executing. To revert to the legacy policy of determining worker to oom kill based on owner group size or only selecting a single worker to kill at a time, set the environment variable `RAY_worker_killing_policy_by_group` to true before starting Ray. If the idle workers have a non-trivial memory footprint at the time of OOM (check OOM log for non-selected idle workers), consider setting the environment variable `RAY_idle_worker_killing_memory_threshold_bytes` to a lower value to consider idle workers with lower memory footprint for killing.
```

You can see the number of Ray OOM kills in the "..." chart in the Ray Core dashboard:

...

### Kernel OOM kills

When the kernel OOM killer kills a Ray process before the Ray OOM killer, you might see
an error like this:

...

You can see the number of unexpected worker deaths in the "..." chart in the Ray Core 
dashboard. Kernel OOM kills often cause unexpected worker death.

### Node death

If you're using older versions of Ray without resource isolation, nodes can die under
memory pressure, and you might see an error like this:

...

You can see node death in the "..." chart in the Ray Core dashboard:

...

Nodes die for reasons unrelated to memory pressure. If you see node death along with 
other memory-related errors, memory pressure might have caused the death.

## Best practices

### Use ``batch_size="auto"`` or small batch sizes

...


### Configure ``memory`` for reads and high-memory UDFs

If a task or actors uses more than a few GiB of memory, set ``memory``. This tells Ray 
Data how much memory each task or actor needs so it doesn't launch too many at once.

To pick a value for ``memory``, read the Ray Data log file and look for the 
`max_uss_bytes` field. Ray typicaly writes the log file to
`/tmp/ray/session-latest/ray-data/ray-data.log`. 

```
...
```


Ray Data also emits the information to STDOUT:


```
...
```


### Enable default map memory

Unless you specify a value, Ray Data assumes a UDF needs 0 ``memory``. So even if you've
set ``memory`` correctly for some APIs, Ray Data can still oversubscribe tasks and 
actors for the ones you haven't.

To avoid this, set ``DataContext.get_current().default_map_logical_memory = True``.

### Start Ray with resource isolation

If you are encountering kernel OOM kills or memory pressure related node deaths, 
you can enable *resource isolation* to provide enhanced protection for critical system components and eliminate kernel OOMs and node deaths. 

To enable *resource isolation*, simply pass the `--enable-resource-isolation` flag to your ray start command. If you still experience kernel OOMs with resource isolation enabled, this means that we did not allocate enough memory for our system components. In this case, please allocate more memory to critical system processes (default to 10% of total memory) by passing in a custom byte value to `--system-reserved-memory`.

For more details on enabling resource isolation, please see {doc}`Ray Core Resource Isolation </ray-core/resource-isolation-with-cgroupv2>`.
For more details on debugging node failure or kernel OOMs, please see {doc}`Debugging Memory Issues </ray-observability/user-guides/debug-apps/debug-memory>`.

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
some workloads like shuffle, it can also increase the risk of OOMs because it decrease
the amount of  memory available for your UDFs.

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
{ref}`Ray Data Memory Management <data_memory_management>`, 
{ref}`Out-Of-Memory Prevention <ray-oom-prevention>`, 
{doc}`Ray Core Resource Isolation </ray-core/resource-isolation-with-cgroupv2>`.
