# Scaling and Resource Allocation

This guide helps you to:

- scale your deployments horizontally by specifying a number of replicas
- scale up and down automatically to react to changing traffic
- allocate hardware resources (CPUs, GPUs, etc) for each deployment.

(scaling-out-a-deployment)=

## Scaling horizontally with `num_replicas`

Each deployment consists of one or more [replicas](serve-architecture#high-level-view).
The number of replicas is specified by the `num_replicas` field in the deployment options.
By default, `num_replicas` is 1.

```python
# Create with a ten replicas.
@serve.deployment(num_replicas=10)
def func(*args):
    pass
```

(ray-serve-autoscaling)=

## Autoscaling

Serve also supports a demand-based replica autoscaler. It adjusts to traffic spikes by observing queue sizes and making scaling decisions to add or remove replicas.
To configure it, you can set the `autoscaling` field in deployment options.

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

(serve-cpus-gpus)=

## Resource Management (CPUs, GPUs)

You may want to specify a deployment's resource requirements, for example if a deployment requires a GPU.  To assign hardware resources per replica, you can pass resource requirements to
`ray_actor_options`.
By default, each replica requires one CPU.
To learn about options to pass in, take a look at [Resources with Actors guide](actor-resource-guide).

For example, to create a deployment where each replica uses a single GPU, you can do the
following:

```python
@serve.deployment(ray_actor_options={"num_gpus": 1})
def func(*args):
    return do_something_with_my_gpu()
```

(serve-fractional-resources-guide)=

### Fractional Resources

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
