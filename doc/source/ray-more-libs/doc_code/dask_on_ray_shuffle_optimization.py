import ray
from ray.util.dask import dataframe_optimize, ray_dask_get
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

# Start Ray.
# Tip: If connecting to an existing cluster, use ray.init(address="auto").
ray.init()

# Set the Dask DataFrame optimizer to
# our custom optimization function, this time using the config setter as a
# context manager.
with dask.config.set(scheduler=ray_dask_get, dataframe_optimize=dataframe_optimize):
    npartitions = 100
    df = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(0, 100, size=(10000, 2)), columns=["age", "grade"]
        ),
        npartitions=npartitions,
    )
    # We set max_branch to infinity in order to ensure that the task-based
    # shuffle happens in a single stage, which is required in order for our
    # optimization to work.
    df.set_index(["age"], shuffle="tasks", max_branch=float("inf")).head(
        10, npartitions=-1
    )

ray.shutdown()
