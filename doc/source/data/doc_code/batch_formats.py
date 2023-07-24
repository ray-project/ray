# flake8: noqa
# isort: skip_file
# fmt: off

# __simple_map_function_start__
import ray

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

def map_function(data):
    return data[data["sepal length (cm)"] < 5]

batch = ds.take_batch(10, batch_format="pandas")
mapped_batch = map_function(batch)

transformed = ds.map_batches(map_function, batch_format="pandas", batch_size=10)
# __simple_map_function_end__

# __simple_pandas_start__
import ray
import pandas as pd

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
ds.show(1)
# -> {'sepal length (cm)': 5.1, ..., 'petal width (cm)': 0.2, 'target': 0}

def transform_pandas(df_batch: pd.DataFrame) -> pd.DataFrame:
    df_batch = df_batch[df_batch["target"] == 2]
    df_batch.loc[:, "normalized.sepal length (cm)"] = df_batch["sepal length (cm)"] / df_batch["sepal length (cm)"].max()
    df_batch = df_batch.drop(columns=["sepal length (cm)"])
    return df_batch

ds.map_batches(transform_pandas, batch_format="pandas").show(1)
# -> {..., 'target': 2, 'normalized.sepal length (cm)': 1.0}
# __simple_pandas_end__

# __simple_numpy_start__
from typing import Dict

import ray
import numpy as np
from typing import Dict


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

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")


def transform_pyarrow(batch: pa.Table) -> pa.Table:
    batch = batch.filter(pac.equal(batch["target"], 1))
    return batch.drop(["sepal length (cm)"])


# test map function on a batch
batch = ds.take_batch(1, batch_format="pyarrow")
mapped_batch = transform_pyarrow(batch)

ds.map_batches(transform_pyarrow, batch_format="pyarrow").show(1)
# -> {'sepal width (cm)': 3.2, ..., 'target': 1}
# __simple_pyarrow_end__
# fmt: on
