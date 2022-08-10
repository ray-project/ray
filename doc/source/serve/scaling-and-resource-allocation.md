# Scaling and Resource Allocation

This guide helps you to:

- scale your deployments horizontally by specifying a number of replicas
- scale up and down automatically to react to changing traffic
- allocate hardware resources (CPUs, GPUs, etc) for each deployment

(scaling-out-a-deployment)=

## Scaling horizontally with `num_replicas`

Each deployment consists of one or more [replicas](serve-architecture#high-level-view).
The number of replicas is specified by the `num_replicas` field in the deployment options.
By default, `num_replicas` is 1.

```python
# Create with a deployment with ten replicas.
@serve.deployment(num_replicas=10)
def func(*args):
    pass
```

(ray-serve-autoscaling)=

## Autoscaling

Serve also supports a demand-based replica autoscaler. It adjusts to traffic spikes by observing queue sizes and making scaling decisions to add or remove replicas.
To configure it, you can set the `autoscaling_config` field in deployment options.

```python
@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 10,
    })
def func(_):
    time.sleep(1)
    return ""
```

The `min_replicas` and `max_replicas` fields configure the range of replicas which the
Serve autoscaler chooses from.  Deployments will start with `min_replicas` initially.

The `target_num_ongoing_requests_per_replica` configuration specifies how aggressively the
autoscaler should react to traffic. Serve will try to make sure that each replica has roughly that number
of requests being processed and waiting in the queue. For example, if your processing time is `10ms`
and the latency constraint is `100ms`, you can have at most `10` requests ongoing per replica so
the last requests can finish within the latency constraint. We recommend you benchmark your application
code and set this number based on end to end latency objective.

:::{note}
The Ray Serve Autoscaler is an application-level autoscaler that sits on top of the [Ray Autoscaler](cluster-index).
Concretely, this means that the Ray Serve autoscaler asks Ray to start a number of replica actors based on the request demand.
If the Ray Autoscaler determines there aren't enough available CPUs to place these actors, it responds by requesting more Ray nodes.
The underlying cloud provider will then respond by adding more nodes.
Similarly, when Ray Serve scales down and terminates some replica actors, it will try to do so in a way that results in the most nodes having no Ray actors or tasks running on them, at which point the Ray autoscaler will remove those nodes.
:::

To learn about the architecture underlying Ray Serve Autoscaling, see {ref}`serve-autoscaling-architecture`.

### `autoscaling_config` parameters

There are several user-specified parameters the autoscaling algorithm takes into consideration when deciding the target number of replicas for your deployment:

**min_replicas[default_value=1]**: The minimum number of replicas for the deployment. ``min_replicas`` will also be the initial number of replicas when the deployment is deployed.
:::{note}
Ray Serve Autoscaling allows the `min_replicas` to be 0 when starting your deployment; the scale up will be started when you start sending traffic. There will be a cold start time as the Ray ServeHandle waits (blocks) for available replicas to assign the request.
:::
**max_replicas[default_value=1]**: The maximum number of replicas for the deployment. Ray Serve Autoscaling will rely on the Ray Autoscaler to scale up more nodes when the currently available cluster resources (CPUs, GPUs, etc.) are not enough to support more replicas.

**target_num_ongoing_requests_per_replica[default_value=1]**: How many ongoing requests are expected to run concurrently per replica. The autoscaler scales up if the value is lower than the current number of ongoing requests per replica. Similarly, the autoscaler scales down if it's higher than the current number of ongoing requests. Scaling happens quicker if there's a high disparity between this value and the current number of ongoing requests.
:::{note}
- It is always recommended to load test your workloads. For example, if the use case is latency sensitive, you can lower the `target_num_ongoing_requests_per_replica` number to maintain high performance.
- Internally, the autoscaler will decide to scale up or down by comparing `target_num_ongoing_requests_per_replica` to the number of `RUNNING` and `PENDING` tasks on each replica.
- `target_num_ongoing_requests_per_replica` is only a target value used for autoscaling (not a hard limit), the real ongoing requests number can be higher than the config.
:::

**downscale_delay_s[default_value=600.0]**: How long the cluster needs to wait before scaling down replicas.

**upscale_delay_s[default_value=30.0]**: How long the cluster needs to wait before scaling up replicas.

:::{note}
`downscale_delay_s` and `upscale_delay_s` control the frequency of doing autoscaling work. For example, if your application takes a long time to do initialization work, you can increase `downscale_delay_s` to make the downscaling happen slowly.
:::

**smoothing_factor[default_value=1]**: The multiplicative factor to speedup/slowdown each autoscaling step. For example, when the use case has high traffic volume in short period of time, you can increase `smoothing_factor` to scale up the resource quickly.

**metrics_interval_s[default_value=10]**: This is control how often each replica sends metrics to the autoscaler. (Normally you don't need to change this config.)

(serve-cpus-gpus)=

## Resource Management (CPUs, GPUs)

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

### Fractional CPUs and Fractional GPUs

Suppose you have two models and each doesn't fully saturate a GPU.  You might want to have them share a GPU by allocating 0.5 GPUs each.

To do this, the resources specified in `ray_actor_options` can be *fractional*.
This allows you to flexibly share resources between replicas.
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

(serve-omp-num-threads)=

## Configuring Parallelism with OMP_NUM_THREADS

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the `OMP_NUM_THREADS` environment variable.
To [avoid contention](omp-num-thread-note), Ray sets `OMP_NUM_THREADS=1` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve deployment, just set `OMP_NUM_THREADS` to the desired value either when starting Ray or in your function/class definition:

```bash
OMP_NUM_THREADS=12 ray start --head
OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS
```

```python
@serve.deployment
class MyDeployment:
    def __init__(self, parallelism):
        os.environ["OMP_NUM_THREADS"] = parallelism
        # Download model weights, initialize model, etc.

MyDeployment.deploy()
```

:::{note}
Some other libraries may not respect `OMP_NUM_THREADS` and have their own way to configure parallelism.
For example, if you're using OpenCV, you'll need to manually set the number of threads using `cv2.setNumThreads(num_threads)` (set to 0 to disable multi-threading).
You can check the configuration using `cv2.getNumThreads()` and `cv2.getNumberOfCPUs()`.
:::
