# flake8: noqa
# isort: skip_file
import ray
import pandas as pd


ds = ray.data.read_csv("example://iris.csv")
ds.show(1)
# -> {'sepal.length': 5.1, ..., 'petal.width': 0.2, 'variety': 'Setosa'}

ds.default_batch_format()
# pandas.core.frame.DataFrame


def transform_pandas(df_batch: pd.DataFrame) -> pd.DataFrame:
    df_batch = df_batch[df_batch["variety"] == "Versicolor"]
    df_batch.loc[:, "normalized.sepal.length"] = df_batch["sepal.length"] / df_batch["sepal.length"].max()
    df_batch = df_batch.drop(columns=["sepal.length"])
    return df_batch

ds.map_batches(transform_pandas).show(1)
# -> {..., 'variety': 'Versicolor', 'normalized.sepal.length': 1.0}


import ray
import numpy as np

ds = ray.data.range_tensor(1000, shape=(2, 2))
ds.default_batch_format()
# 'numpy.ndarray'

def transform_numpy(arr: np.ndarray) -> np.ndarray:
    return arr * 2

ds.map_batches(transform_numpy)
