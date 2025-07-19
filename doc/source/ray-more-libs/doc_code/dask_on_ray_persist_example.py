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

d_arr = da.ones(100)

# Print the internal Dask graph. Replace this with `print(dask.base.collections_to_dsk([d_arr]))` when dask>=2024.11.0,<2025.4.0.
print(dask.base.collections_to_expr([d_arr]).dask)
# {('ones_like-5902a58f37d3b639948dee893f5c4f4a', 0):
# <Task ('ones_like-5902a58f37d3b639948dee893f5c4f4a', 0)
# ones_like(...)>}

# This submits all underlying Ray tasks to the cluster and returns
# a Dask array with the Ray futures inlined.
d_arr_p = d_arr.persist()

# Notice that the Ray ObjectRef is inlined. The dask.ones() task has
# been submitted to and is running on the Ray cluster.
# Replace this in a similar way when dask>=2024.11.0,<2025.4.0.
print(dask.base.collections_to_expr([d_arr_p]).dask)
# {('ones_like-5902a58f37d3b639948dee893f5c4f4a', 0):
# DataNode(ObjectRef(2c329aa28fcae64affffffffffffffffffffffff2c00000001000000))}

# Future computations on this persisted Dask Array will be fast since we
# already started computing d_arr_p in the background.
d_arr_p.sum().compute()
d_arr_p.min().compute()
d_arr_p.max().compute()

ray.shutdown()
