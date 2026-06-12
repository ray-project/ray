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

...

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
{doc}`Ray Core resource isolation </ray-core/resource-isolation-with-cgroupv2>`.
