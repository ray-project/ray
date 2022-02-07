import ray
from ray.util.dask import enable_dask_on_ray
import dask
import dask.array as da

# Start Ray.
# Tip: If connecting to an existing cluster, use ray.init(address="auto").
ray.init()

# Use our Dask config helper to set the scheduler to ray_dask_get globally,
# without having to specify it on each compute call.
enable_dask_on_ray()

# All Ray tasks that underly the Dask operations performed in an annotation
# context will require the indicated resources: 2 CPUs and 0.01 of the custom
# resource.
with dask.annotate(
    ray_remote_args=dict(num_cpus=2, resources={"custom_resource": 0.01})
):
    d_arr = da.ones(100)

# Operations on the same collection can have different annotations.
with dask.annotate(ray_remote_args=dict(resources={"other_custom_resource": 0.01})):
    d_arr = 2 * d_arr

# This happens outside of the annotation context, so no resource constraints
# will be attached to the underlying Ray tasks for the sum() operation.
sum_ = d_arr.sum()

# Compute the result, passing in a default resource request that will be
# applied to all operations that aren't already annotated with a resource
# request. In this case, only the sum() operation will get this default
# resource request.
# We also give ray_remote_args, which will be given to every Ray task that
# Dask-on-Ray submits; note that this can also be overridden for individual
# Dask operations via the dask.annotate API.
# NOTE: We disable graph optimization since it can break annotations,
# see this issue: https://github.com/dask/dask/issues/7036.
result = sum_.compute(
    ray_remote_args=dict(max_retries=5, resources={"another_custom_resource": 0.01}),
    optimize_graph=False,
)
print(result)
# 200

ray.shutdown()
