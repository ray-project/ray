# Managing Deployments

This section should help you:

- create, query, update and configure deployments
- configure resources of your deployments
- specify different Python dependencies across different deployment using Runtime Environments

:::{tip}
Get in touch with us if you're using or considering using [Ray Serve](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU).
:::

```{contents}
```

## Updating a Deployment

Often you want to be able to update your code or configuration options for a deployment over time.
Deployments can be updated simply by updating the code or configuration options and calling `deploy()` again.

```python
@serve.deployment(name="my_deployment", num_replicas=1)
class SimpleDeployment:
    pass

# Creates one initial replica.
SimpleDeployment.deploy()

# Re-deploys, creating an additional replica.
# This could be the SAME Python script, modified and re-run.
@serve.deployment(name="my_deployment", num_replicas=2)
class SimpleDeployment:
    pass

SimpleDeployment.deploy()

# You can also use Deployment.options() to change options without redefining
# the class. This is useful for programmatically updating deployments.
SimpleDeployment.options(num_replicas=2).deploy()
```

By default, each call to `.deploy()` will cause a redeployment, even if the underlying code and options didn't change.
This could be detrimental if you have many deployments in a script and and only want to update one: if you re-run the script, all of the deployments will be redeployed, not just the one you updated.
To prevent this, you may provide a `version` string for the deployment as a keyword argument in the decorator or `Deployment.options()`.
If provided, the replicas will only be updated if the value of `version` is updated; if the value of `version` is unchanged, the call to `.deploy()` will be a no-op.
When a redeployment happens, Serve will perform a rolling update, bringing down at most 20% of the replicas at any given time.

(configuring-a-deployment)=

## Configuring a Deployment

There are a number of things you'll likely want to do with your serving application including
scaling out or configuring the maximum number of in-flight requests for a deployment.
All of these options can be specified either in {mod}`@serve.deployment <ray.serve.api.deployment>` or in `Deployment.options()`.

To update the config options for a running deployment, simply redeploy it with the new options set.

### Scaling Out

To scale out a deployment to many processes, simply configure the number of replicas.

```python
# Create with a single replica.
@serve.deployment(num_replicas=1)
def func(*args):
    pass

func.deploy()

# Scale up to 10 replicas.
func.options(num_replicas=10).deploy()

# Scale back down to 1 replica.
func.options(num_replicas=1).deploy()
```

#### Autoscaling

Serve also has experimental support for a demand-based replica autoscaler.
It reacts to traffic spikes via observing queue sizes and making scaling decisions.
To configure it, you can set the `_autoscaling` field in deployment options.

:::{warning}
The API is experimental and subject to change. We welcome you to test it out
and leave us feedback through [Github Issues](https://github.com/ray-project/ray/issues) or our [discussion forum](https://discuss.ray.io/)!
:::

```python
@serve.deployment(
    _autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 10,
    },
    version="v1")
def func(_):
    time.sleep(1)
    return ""

func.deploy() # The func deployment will now autoscale based on requests demand.
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
The `version` field is required for autoscaling. We are actively working on removing
this limitation.
:::

:::{note}
The Ray Serve Autoscaler is an application-level autoscaler that sits on top of the [Ray Autoscaler](cluster-index).
Concretely, this means that the Ray Serve autoscaler asks Ray to start a number of replica actors based on the request demand.
If the Ray Autoscaler determines there aren't enough available CPUs to place these actors, it responds by adding more nodes.
Similarly, when Ray Serve scales down and terminates some replica actors, it may result in some nodes being empty, at which point the Ray autoscaler will remove those nodes.
:::

(serve-cpus-gpus)=

### Resource Management (CPUs, GPUs)

To assign hardware resources per replica, you can pass resource requirements to
`ray_actor_options`.
By default, each replica requires one CPU.
To learn about options to pass in, take a look at [Resources with Actor](actor-resource-guide) guide.

For example, to create a deployment where each replica uses a single GPU, you can do the
following:

```python
@serve.deployment(ray_actor_options={"num_gpus": 1})
def func(*args):
    return do_something_with_my_gpu()
```

### Fractional Resources

The resources specified in `ray_actor_options` can also be *fractional*.
This allows you to flexibly share resources between replicas.
For example, if you have two models and each doesn't fully saturate a GPU, you might want to have them share a GPU by allocating 0.5 GPUs each.
The same could be done to multiplex over CPUs.

```python
@serve.deployment(name="deployment1", ray_actor_options={"num_gpus": 0.5})
def func(*args):
    return do_something_with_my_gpu()

@serve.deployment(name="deployment2", ray_actor_options={"num_gpus": 0.5})
def func(*args):
    return do_something_with_my_gpu()
```

### Configuring Parallelism with OMP_NUM_THREADS

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the OMP_NUM_THREADS environment variable.
To [avoid contention](omp-num-thread-note), Ray sets `OMP_NUM_THREADS=1` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve deployment, just set OMP_NUM_THREADS to the desired value either when starting Ray or in your function/class definition:

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

### User Configuration (Experimental)

Suppose you want to update a parameter in your model without needing to restart
the replicas in your deployment.  You can do this by writing a `reconfigure` method
for the class underlying your deployment.  At runtime, you can then pass in your
new parameters by setting the `user_config` option.

The following simple example will make the usage clear:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_reconfigure.py
```

The `reconfigure` method is called when the class is created if `user_config`
is set.  In particular, it's also called when new replicas are created in the
future if scale up your deployment later.  The `reconfigure` method is also  called
each time `user_config` is updated.

