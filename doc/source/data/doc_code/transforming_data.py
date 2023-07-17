# flake8: noqa
# fmt: off

# __map_batches_begin__
import ray
import numpy as np
from typing import Dict

# Load data.
ds = ray.data.from_items(["Test", "String", "Test String"])
# -> Dataset(num_blocks=1, num_rows=3, schema={item: string})

# Define the transform function.
def to_lowercase(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    lowercase_batch = [b.lower() for b in batch["item"]]
    return {"text": lowercase_batch}

ds.map_batches(to_lowercase).show()
# -> {'text': 'test'}
# -> {'text': 'string'}
# -> {'text': 'test string'}
# __map_batches_end__


# __map_begin__
import ray
from typing import Dict, Any

# Load data.
ds = ray.data.from_items(["Test", "String", "Test String"])
# -> Dataset(num_blocks=1, num_rows=3, schema={item: string})

# Define the transform function.
def to_lowercase(row: Dict[str, Any]) -> Dict[str, Any]:
    lowercase = row["item"].lower()
    return {"text": lowercase}

ds.map(to_lowercase).show()
# -> {'text': 'test'}
# -> {'text': 'string'}
# -> {'text': 'test string'}
# __map_end__

# __writing_numpy_udfs_begin__
import ray
import numpy as np
from typing import Dict

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

def numpy_transform(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    new_col = batch["sepal length (cm)"] / np.max(batch["sepal length (cm)"])
    batch["normalized.sepal length (cm)"] = new_col
    del batch["sepal length (cm)"]
    return batch

ds.map_batches(numpy_transform, batch_format="numpy").show(2)
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.7, 'petal width (cm)': 1.4,
#     'target': 0, 'normalized.sepal length (cm)': 1.0}
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.5, 'petal width (cm)': 1.5,
#     'target': 0, 'normalized.sepal length (cm)': 0.9142857142857144}
# __writing_numpy_udfs_end__

# __writing_pandas_udfs_begin__
import ray
import pandas as pd

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

def pandas_transform(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "normalized.sepal length (cm)"] = df["sepal length (cm)"] / df["sepal length (cm)"].max()
    df = df.drop(columns=["sepal length (cm)"])
    return df

ds.map_batches(pandas_transform, batch_format="pandas").show(2)
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.7, 'petal width (cm)': 1.4,
#     'target': 0, 'normalized.sepal length (cm)': 1.0}
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.5, 'petal width (cm)': 1.5,
#     'target': 0, 'normalized.sepal length (cm)': 0.9142857142857144}
# __writing_pandas_udfs_end__

# __writing_arrow_udfs_begin__
import ray
import pyarrow as pa
import pyarrow.compute as pac

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

def pyarrow_transform(batch: pa.Table) -> pa.Table:
    batch = batch.append_column(
        "normalized.sepal length (cm)",
        pac.divide(batch["sepal length (cm)"], pac.max(batch["sepal length (cm)"])),
    )
    return batch.drop(["sepal length (cm)"])

ds.map_batches(pyarrow_transform, batch_format="pyarrow").show(2)
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.7, 'petal width (cm)': 1.4,
#     'target': 0, 'normalized.sepal length (cm)': 1.0}
# -> {'sepal width (cm)': 3.2, 'petal length (cm)': 4.5, 'petal width (cm)': 1.5,
#     'target': 0, 'normalized.sepal length (cm)': 0.9142857142857144}
# __writing_arrow_udfs_end__

# __dataset_compute_strategy_begin__
import ray
import pandas as pd
import numpy as np
from ray.data import ActorPoolStrategy

# Dummy model to predict Iris target.
def predict_iris(df: pd.DataFrame) -> pd.DataFrame:
    conditions = [
        (df["sepal length (cm)"] < 5.0),
        (df["sepal length (cm)"] >= 5.0) & (df["sepal length (cm)"] < 6.0),
        (df["sepal length (cm)"] >= 6.0)
    ]
    values = ["Setosa", "0", "Virginica"]
    return pd.DataFrame({"predicted_variety": np.select(conditions, values)})

class IrisInferModel:
    # Do any expensive model setup in the __init__ function.
    def __init__(self):
        self._model = predict_iris

    # This method is called repeatedly by Ray Data to process batches.
    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        return self._model(batch)

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv").repartition(10)

# Batch inference processing with Ray tasks (the default compute strategy).
predicted = ds.map_batches(predict_iris, batch_format="pandas")

# Batch inference processing with Ray actors (pool of size 5).
predicted = ds.map_batches(
    IrisInferModel, compute=ActorPoolStrategy(size=5), batch_size=10)
# __dataset_compute_strategy_end__

# __writing_generator_udfs_begin__
import ray
from typing import Iterator

# Load iris data.
ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

# UDF to repeat the dataframe 100 times, in chunks of 20.
def repeat_dataframe(df: pd.DataFrame) -> Iterator[pd.DataFrame]:
    for _ in range(5):
        yield pd.concat([df]*20)

ds.map_batches(repeat_dataframe, batch_format="pandas").show(2)
# -> {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
# -> {'sepal length (cm)': 4.9, 'sepal width (cm)': 3.0, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
# __writing_generator_udfs_end__

# __shuffle_begin__
import ray

# The dataset starts off with 1000 blocks.
ds = ray.data.range(10000, parallelism=1000)
# -> Dataset(num_blocks=1000, num_rows=10000, schema={id: int64})

# Repartition the data into 100 blocks. Since shuffle=False, Ray Data will minimize
# data movement during this operation by merging adjacent blocks.
ds = ds.repartition(100, shuffle=False).materialize()
# -> MaterializedDataset(num_blocks=100, num_rows=10000, schema={id: int64})

# Repartition the data into 200 blocks, and force a full data shuffle.
# This operation will be more expensive
ds = ds.repartition(200, shuffle=True).materialize()
# -> MaterializedDataset(num_blocks=200, num_rows=10000, schema={id: int64})
# __shuffle_end__

# __map_groups_begin__
import ray
import numpy as np
from typing import Dict

# Load iris data.
ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

# The user function signature for `map_groups` is the same as that of `map_batches`.
# It takes in a batch representing the grouped data, and must return a batch of
# zero or more records as the result.
def custom_count(group: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    # Since we are grouping by target, all elements in this batch are equal.
    target = group["target"][0]
    count = len(group["target"])
    # Here we return a batch of a single record for the group (array of len 1).
    return {
        "target": np.array([target]),
        "count": np.array([count]),
    }

ds = ds.groupby("target").map_groups(custom_count)
ds.show()
# -> {'target': 0, 'count': 50}
#    {'target': 1, 'count': 50}
#    {'target': 2, 'count': 50}
# __map_groups_end__

# fmt: on
