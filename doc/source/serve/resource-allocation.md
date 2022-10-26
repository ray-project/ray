(serve-cpus-gpus)=

# CPU & GPU support and Customized resource

This guide helps you to:
- allocate hardware resources for each deployment
- allocate fractional CPU & GPU resources
- allocate customized resources

## Allocate resources

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

## Fractional CPUs and Fractional GPUs

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

## Custom Resources, Accelerator types, and more

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