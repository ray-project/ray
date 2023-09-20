(serve-resource-allocation)=

# Resource Allocation

This guide helps you configure Ray Serve to:

- Scale your deployments horizontally by specifying a number of replicas
- Scale up and down automatically to react to changing traffic
- Allocate hardware resources (CPUs, GPUs, etc) for each deployment


(serve-cpus-gpus)=

## Resource management (CPUs, GPUs)

You may want to specify a deployment's resource requirements to reserve cluster resources like GPUs.  To assign hardware resources per replica, you can pass resource requirements to
`ray_actor_options`.
By default, each replica reserves one CPU.
To learn about options to pass in, take a look at the [Resources with Actors guide](actor-resource-guide).

For example, to create a deployment where each replica uses a single GPU, you can do the
following:

```python
@serve.deployment(ray_actor_options={"num_gpus": 1})
def func(*args):
    return do_something_with_my_gpu()
```

(serve-fractional-resources-guide)=

### Fractional CPUs and fractional GPUs

Suppose you have two models and each doesn't fully saturate a GPU.  You might want to have them share a GPU by allocating 0.5 GPUs each.

To do this, the resources specified in `ray_actor_options` can be *fractional*.
For example, if you have two models and each doesn't fully saturate a GPU, you might want to have them share a GPU by allocating 0.5 GPUs each.

```python
@serve.deployment(ray_actor_options={"num_gpus": 0.5})
def func_1(*args):
    return do_something_with_my_gpu()

@serve.deployment(ray_actor_options={"num_gpus": 0.5})
def func_2(*args):
    return do_something_with_my_gpu()
```

In this example, each replica of each deployment will be allocated 0.5 GPUs.  The same can be done to multiplex over CPUs, using `"num_cpus"`.

### Custom resources, accelerator types, and more

You can also specify {ref}`custom resources <cluster-resources>` in `ray_actor_options`, for example to ensure that a deployment is scheduled on a specific node.
For example, if you have a deployment that requires 2 units of the `"custom_resource"` resource, you can specify it like this:

```python
@serve.deployment(ray_actor_options={"resources": {"custom_resource": 2}})
def func(*args):
    return do_something_with_my_custom_resource()
```

You can also specify {ref}`accelerator types <accelerator-types>` via the `accelerator_type` parameter in `ray_actor_options`.

Below is the full list of supported options in `ray_actor_options`; please see the relevant Ray Core documentation for more details about each option:

- `accelerator_type`
- `memory`
- `num_cpus`
- `num_gpus`
- `object_store_memory`
- `resources`
- `runtime_env`

(serve-omp-num-threads)=

## Configuring parallelism with OMP_NUM_THREADS

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the `OMP_NUM_THREADS` environment variable.
Ray sets `OMP_NUM_THREADS=<num_cpus>` by default. To [avoid contention](omp-num-thread-note), Ray sets `OMP_NUM_THREADS=1` if `num_cpus` is not specified on the tasks/actors, to reduce contention between actors/tasks which run in a single thread.
If you *do* want to enable this parallelism in your Serve deployment, just set `num_cpus` (recommended) to the desired value, or manually set the `OMP_NUM_THREADS` environment variable when starting Ray or in your function/class definition.

```bash
OMP_NUM_THREADS=12 ray start --head
OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS
```

```{literalinclude} doc_code/managing_deployments.py
:start-after: __configure_parallism_start__
:end-before: __configure_parallism_end__
:language: python
```

:::{note}
Some other libraries may not respect `OMP_NUM_THREADS` and have their own way to configure parallelism.
For example, if you're using OpenCV, you'll need to manually set the number of threads using `cv2.setNumThreads(num_threads)` (set to 0 to disable multi-threading).
You can check the configuration using `cv2.getNumThreads()` and `cv2.getNumberOfCPUs()`.
:::
