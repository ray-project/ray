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
print(dask.base.collections_to_dsk([d_arr]))
# {('ones-c345e6f8436ff9bcd68ddf25287d27f3',
#   0): (functools.partial(<function _broadcast_trick_inner at 0x7f27f1a71f80>,
#   dtype=dtype('float64')), (5,))}

# This submits all underlying Ray tasks to the cluster and returns
# a Dask array with the Ray futures inlined.
d_arr_p = d_arr.persist()

# Notice that the Ray ObjectRef is inlined. The dask.ones() task has
# been submitted to and is running on the Ray cluster.
dask.base.collections_to_dsk([d_arr_p])
# {('ones-c345e6f8436ff9bcd68ddf25287d27f3',
#   0): ObjectRef(8b4e50dc1ddac855ffffffffffffffffffffffff0100000001000000)}

# Future computations on this persisted Dask Array will be fast since we
# already started computing d_arr_p in the background.
d_arr_p.sum().compute()
d_arr_p.min().compute()
d_arr_p.max().compute()

ray.shutdown()
