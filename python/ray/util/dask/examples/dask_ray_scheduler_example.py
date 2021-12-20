import ray
from ray.util.dask import ray_dask_get, enable_dask_on_ray, disable_dask_on_ray
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd

# Start Ray.
# Tip: If connecting to an existing cluster, use ray.init(address="auto").
ray.init()

d_arr = da.from_array(np.random.randint(0, 1000, size=(256, 256)))

# The Dask scheduler submits the underlying task graph to Ray.
d_arr.mean().compute(scheduler=ray_dask_get)

# Use our Dask config helper to set the scheduler to ray_dask_get globally,
# without having to specify it on each compute call.
enable_dask_on_ray()

df = dd.from_pandas(
    pd.DataFrame(
        np.random.randint(0, 100, size=(1024, 2)), columns=["age", "grade"]),
    npartitions=2)
df.groupby(["age"]).mean().compute()

disable_dask_on_ray()

# The Dask config helper can be used as a context manager, limiting the scope
# of the Dask-on-Ray scheduler to the context.
with enable_dask_on_ray():
    d_arr.mean().compute()

ray.shutdown()
