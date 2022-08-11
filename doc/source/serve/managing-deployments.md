(serve-managing-deployments-guide)=

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
Deployments can be updated simply by updating the code or configuration options and calling `serve.run()` again.

```{literalinclude} ../serve/doc_code/managing_deployments.py
:start-after: __updating_a_deployment_start__
:end-before: __updating_a_deployment_end__
:language: python
```

By default, each call to `serve.run()` will cause a redeployment, even if the underlying code and options didn't change.
This could be detrimental if you have many deployments in a script and and only want to update one: if you re-run the script, all of the deployments will be redeployed, not just the one you updated.
To prevent this, you may provide a `version` string for the deployment as a keyword argument in the decorator or `Deployment.options()`.
When a redeployment happens, Serve will perform a rolling update, bringing down at most 20% of the replicas at any given time.

(configuring-a-deployment)=

## Configuring a Deployment

There are a number of things you'll likely want to do with your serving application including
scaling out or configuring the maximum number of in-flight requests for a deployment.
All of these options can be specified either in {mod}`@serve.deployment <ray.serve.api.deployment>` or in `Deployment.options()`.

To update the config options for a running deployment, simply redeploy it with the new options set.

(scaling-out-a-deployment)=

### Scaling Out

To scale out a deployment to many processes, simply configure the number of replicas.

```{literalinclude} ../serve/doc_code/managing_deployments.py
:start-after: __scaling_out_start__
:end-before: __scaling_out_end__
:language: python
```

(ray-serve-autoscaling)=

#### Autoscaling

Serve also has the support for a demand-based replica autoscaler.
It reacts to traffic spikes via observing queue sizes and making scaling decisions.
To configure it, you can set the `autoscaling` field in deployment options.


```{literalinclude} ../serve/doc_code/managing_deployments.py
:start-after: __autoscaling_start__
:end-before: __autoscaling_end__
:language: python
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
If the Ray Autoscaler determines there aren't enough available CPUs to place these actors, it responds by requesting more nodes.
The underlying cloud provider will then respond by adding more nodes.
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

(serve-fractional-resources-guide)=

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

(serve-omp-num-threads)=
### Configuring Parallelism with OMP_NUM_THREADS

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the OMP_NUM_THREADS environment variable.
To [avoid contention](omp-num-thread-note), Ray sets `OMP_NUM_THREADS=1` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve deployment, just set OMP_NUM_THREADS to the desired value either when starting Ray or in your function/class definition:

```bash
OMP_NUM_THREADS=12 ray start --head
OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS
```

```{literalinclude} ../serve/doc_code/managing_deployments.py
:start-after: __configure_parallism_start__
:end-before: __configure_parallism_end__
:language: python
```

:::{note}
Some other libraries may not respect `OMP_NUM_THREADS` and have their own way to configure parallelism.
For example, if you're using OpenCV, you'll need to manually set the number of threads using `cv2.setNumThreads(num_threads)` (set to 0 to disable multi-threading).
You can check the configuration using `cv2.getNumThreads()` and `cv2.getNumberOfCPUs()`.
:::

(managing-deployments-user-configuration)=

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

:::{note}
The `user_config` and its contents must be JSON-serializable.
:::
