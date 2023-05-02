# flake8: noqa
# isort: skip_file
# fmt: off

# __simple_map_function_start__
import ray

ds = ray.data.read_csv("example://iris.csv")

def map_function(data):
    return data[data["sepal.length"] < 5]

batch = ds.take_batch(10)
mapped_batch = map_function(batch)

transformed = ds.map_batches(map_function, batch_size=10)
# __simple_map_function_end__

# __simple_pandas_start__
import ray
import pandas as pd

ds = ray.data.read_csv("example://iris.csv")
ds.show(1)
# -> {'sepal.length': 5.1, ..., 'petal.width': 0.2, 'variety': 'Setosa'}

# pandas.core.frame.DataFrame

def transform_pandas(df_batch: pd.DataFrame) -> pd.DataFrame:
    df_batch = df_batch[df_batch["variety"] == "Versicolor"]
    df_batch.loc[:, "normalized.sepal.length"] = df_batch["sepal.length"] / df_batch["sepal.length"].max()
    df_batch = df_batch.drop(columns=["sepal.length"])
    return df_batch

ds.map_batches(transform_pandas, batch_format="pandas").show(1)
# -> {..., 'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# __simple_pandas_end__

# __simple_numpy_start__
from typing import Dict

import ray
import numpy as np


ds = ray.data.range_tensor(1000, shape=(2, 2))

def transform_numpy(arr: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    arr["data"] = arr["data"] * 2
    return arr


# test map function on a batch
batch = ds.take_batch(1)
mapped_batch = transform_numpy(batch)

ds.map_batches(transform_numpy)
# __simple_numpy_end__


# __simple_pyarrow_start__
import ray
import pyarrow as pa
import pyarrow.compute as pac

ds = ray.data.read_csv("example://iris.csv")


def transform_pyarrow(batch: pa.Table) -> pa.Table:
    batch = batch.filter(pac.equal(batch["variety"], "Versicolor"))
    return batch.drop(["sepal.length"])


# test map function on a batch
batch = ds.take_batch(1)
mapped_batch = transform_pyarrow(batch)

ds.map_batches(transform_pyarrow, batch_format="pyarrow").show(1)
# -> {'sepal.width': 3.2, ..., 'variety': 'Versicolor'}
# __simple_pyarrow_end__
# fmt: on
