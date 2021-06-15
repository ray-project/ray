import ray
from ray.util.dask import ray_dask_get
import dask
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

# Set the scheduler to ray_dask_get in your config so you don't have to
# specify it on each compute call.
dask.config.set(scheduler=ray_dask_get)

df = dd.from_pandas(
    pd.DataFrame(
        np.random.randint(0, 100, size=(1024, 2)), columns=["age", "grade"]),
    npartitions=2)
df.groupby(["age"]).mean().compute()

ray.shutdown()
