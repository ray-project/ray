# flake8: noqa

# fmt: off
# __writing_default_udfs_tabular_begin__
import ray
import pandas as pd

# Load datastream.
ds = ray.data.read_csv("example://iris.csv")

# UDF as a function on Pandas DataFrame batches.
def pandas_transform(df_batch: pd.DataFrame) -> pd.DataFrame:
    # Filter rows.
    df_batch = df_batch[df_batch["variety"] == "Versicolor"]
    # Add derived column.
    # Notice here that `df["sepal.length"].max()` is only the max value of the column
    # within a given batch (instead of globally)!!
    df_batch.loc[:, "normalized.sepal.length"] = df_batch["sepal.length"] / df_batch["sepal.length"].max()
    # Drop column.
    df_batch = df_batch.drop(columns=["sepal.length"])
    return df_batch

ds.map_batches(pandas_transform, batch_format="pandas").show(2)
# -> {'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# -> {'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'normalized.sepal.length': 0.9142857142857144}
# __writing_default_udfs_tabular_end__
# fmt: on

# fmt: off
# __datastream_transformation_begin__
import ray
import pandas

# Create a datastream from file with Iris data.
# Tip: "example://" is a convenient protocol to access the
# python/ray/data/examples/data directory.
ds = ray.data.read_csv("example://iris.csv")
# Datastream(num_blocks=1, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})
ds.show(3)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.7, 'sepal.width': 3.2,
#     'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

# Repartition the datastream to 5 blocks.
ds = ds.repartition(5)
# -> Repartition
#    +- Datastream(num_blocks=1, num_rows=150,
#               schema={sepal.length: float64, sepal.width: float64,
#                       petal.length: float64, petal.width: float64, variety: object})

# Find rows with sepal.length < 5.5 and petal.length > 3.5.
def transform_batch(df: pandas.DataFrame) -> pandas.DataFrame:
    return df[(df["sepal.length"] < 5.5) & (df["petal.length"] > 3.5)]

# Map processing the datastream.
ds.map_batches(transform_batch, batch_format="pandas").show()
# -> {'sepal.length': 5.2, 'sepal.width': 2.7,
#     'petal.length': 3.9, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 5.4, 'sepal.width': 3.0,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 4.9, 'sepal.width': 2.5,
#     'petal.length': 4.5, 'petal.width': 1.7, 'variety': 'Virginica'}

# Split the datastream into 2 disjoint iterators.
ds.streaming_split(2)
# -> [<StreamSplitDataIterator at 0x7fa5b5c99070>,
#     <StreamSplitDataIterator at 0x7fa5b5c990d0>]

# Sort the datastream by sepal.length.
ds = ds.sort("sepal.length")
ds.show(3)
# -> {'sepal.length': 4.3, 'sepal.width': 3.0,
#     'petal.length': 1.1, 'petal.width': 0.1, 'variety': 'Setosa'}
# -> {'sepal.length': 4.4, 'sepal.width': 2.9,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.4, 'sepal.width': 3.0,
#     'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

# Shuffle the datastream.
ds = ds.random_shuffle()
ds.show(3)
# -> {'sepal.length': 6.7, 'sepal.width': 3.1,
#     'petal.length': 4.4, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.7, 'sepal.width': 3.3,
#     'petal.length': 5.7, 'petal.width': 2.1, 'variety': 'Virginica'}
# -> {'sepal.length': 4.5, 'sepal.width': 2.3,
#     'petal.length': 1.3, 'petal.width': 0.3, 'variety': 'Setosa'}

# Group by the variety.
ds.groupby("variety").count().show()
# -> {'variety': 'Setosa', 'count()': 50}
# -> {'variety': 'Versicolor', 'count()': 50}
# -> {'variety': 'Virginica', 'count()': 50}
# __datastream_transformation_end__
# fmt: on

# fmt: off
# __writing_pandas_udfs_begin__
import ray
import pandas as pd

# Load datastream.
ds = ray.data.read_csv("example://iris.csv")

# UDF as a function on Pandas DataFrame batches.
def pandas_transform(df: pd.DataFrame) -> pd.DataFrame:
    # Filter rows.
    df = df[df["variety"] == "Versicolor"]
    # Add derived column.
    df.loc[:, "normalized.sepal.length"] = df["sepal.length"] / df["sepal.length"].max()
    # Drop column.
    df = df.drop(columns=["sepal.length"])
    return df

ds.map_batches(pandas_transform, batch_format="pandas").show(2)
# -> {'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# -> {'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'normalized.sepal.length': 0.9142857142857144}
# __writing_pandas_udfs_end__
# fmt: on

# fmt: off
# __writing_arrow_udfs_begin__
import ray
import pyarrow as pa
import pyarrow.compute as pac

# Load datastream.
ds = ray.data.read_csv("example://iris.csv")

# UDF as a function on Arrow Table batches.
def pyarrow_transform(batch: pa.Table) -> pa.Table:
    batch = batch.filter(pac.equal(batch["variety"], "Versicolor"))
    batch = batch.append_column(
        "normalized.sepal.length",
        pac.divide(batch["sepal.length"], pac.max(batch["sepal.length"])),
    )
    return batch.drop(["sepal.length"])

ds.map_batches(pyarrow_transform, batch_format="pyarrow").show(2)
# -> {'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# -> {'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'normalized.sepal.length': 0.9142857142857144}
# __writing_arrow_udfs_end__
# fmt: on

# fmt: off
# __writing_numpy_udfs_begin__
import ray
import numpy as np
from typing import Dict

# Load datastream.
ds = ray.data.read_numpy("example://mnist_subset.npy")

# UDF as a function on NumPy ndarray batches.
def normalize(arr: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    arr = arr["data"]
    # Normalizes each image to [0, 1] range.
    mins = arr.min((1, 2))[:, np.newaxis, np.newaxis]
    maxes = arr.max((1, 2))[:, np.newaxis, np.newaxis]
    range_ = maxes - mins
    idx = np.where(range_ == 0)
    mins[idx] = 0
    range_[idx] = 1
    return {"data": (arr - mins) / range_}

ds = ds.map_batches(normalize, batch_format="numpy")
# -> MapBatches(normalize)
#    +- Datastream(num_blocks=1,
#               num_rows=3,
#               schema={data: numpy.ndarray(shape=(28, 28), dtype=uint8)}
#       )
# __writing_numpy_udfs_end__
# fmt: on

# fmt: off
# __writing_callable_classes_udfs_begin__
import ray

# Load datastream.
ds = ray.data.read_csv("example://iris.csv")

# UDF as a function on Pandas DataFrame batches.
class ModelUDF:
    def __init__(self):
        self.model = lambda df: df["sepal.length"] > 0.65

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        # Filter rows.
        df = df[df["variety"] == "Versicolor"]
        # Apply model.
        df["output"] = self.model(df)
        return df

ds.map_batches(ModelUDF, batch_format="pandas", compute=ray.data.ActorPoolStrategy(size=2)).show(2)
# -> {'sepal.length': 7.0, 'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'output': True}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'output': False}`
# __writing_callable_classes_udfs_end__
# fmt: on

# fmt: off
# __writing_generator_udfs_begin__
import ray
from typing import Iterator

# Load datastream.
ds = ray.data.read_csv("example://iris.csv")

# UDF to repeat the dataframe 100 times, in chunks of 20.
def repeat_dataframe(df: pd.DataFrame) -> Iterator[pd.DataFrame]:
    for _ in range(5):
        yield pd.concat([df]*20)

ds.map_batches(repeat_dataframe, batch_format="pandas", ).show(2)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# __writing_generator_udfs_end__
# fmt: on

# fmt: off
# __writing_pandas_out_udfs_begin__
import ray
import numpy as np
import pandas as pd
from typing import Dict

# Load datastream.
ds = ray.data.from_items(["test", "string", "teststring"])
# -> Datastream(num_blocks=1, num_rows=3, schema={item: string})

def rename_column(batch: Dict[str, np.ndarray]) -> pd.DataFrame:
    return pd.DataFrame({"text": batch["item"]})

ds = ds.map_batches(rename_column)
# -> MapBatches(rename_column)
#    +- Datastream(num_blocks=3, num_rows=3, schema={item: string})

ds.show(2)
# -> {'text': 'test'}
# -> {'text': 'string'}

print(ds)
# -> Datastream(num_blocks=3, num_rows=3, schema={text: string})
# __writing_pandas_out_udfs_end__
# fmt: on

# fmt: off
# __writing_arrow_out_udfs_begin__
import ray
import numpy as np
import pyarrow as pa
from typing import Dict

# Load datastream.
ds = ray.data.from_items(["test", "string", "teststring"])
# -> Datastream(num_blocks=1, num_rows=3, schema={item: string})

def rename_column(batch: Dict[str, np.ndarray]) -> pa.Table:
    return pa.table({"text": batch["item"]})

ds = ds.map_batches(rename_column)
# -> MapBatches(rename_column)
#    +- Datastream(num_blocks=1, num_rows=3, schema={text: string})

ds.show(2)
# -> {'text': 'test'}
# -> {'text': 'string'}

print(ds)
# -> Datastream(num_blocks=3, num_rows=3, schema={text: string})
# __writing_arrow_out_udfs_end__
# fmt: on

# fmt: off
# __writing_numpy_out_udfs_begin__
import ray
import numpy as np
from typing import Dict

# Load datastream.
ds = ray.data.from_items(["test", "string", "teststring"])
# -> Datastream(num_blocks=1, num_rows=3, schema={item: string})

def rename_column(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    return {"text": batch["item"]}

ds = ds.map_batches(rename_column)
# -> MapBatches(rename_column)
#    +- Datastream(num_blocks=1, num_rows=3, schema={text: string})

ds.show(2)
# -> {'text': 'test'}
# -> {'text': 'string'}

print(ds)
# -> Datastream(num_blocks=3, num_rows=3, schema={text: string})
# __writing_numpy_out_udfs_end__
# fmt: on

# fmt: off
# __writing_dict_out_row_udfs_begin__
import ray
import pandas as pd
from typing import Dict, Any

# Load datastream.
ds = ray.data.range(10)
# -> Datastream(num_blocks=10, num_rows=10, schema={id: int64})

def rename_column(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"foo": row["id"]}

ds = ds.map(rename_column)
# -> Map
#    +- Datastream(num_blocks=10, num_rows=10, schema={foo: int64})

ds.show(2)
# -> {'foo': 0}
# -> {'foo': 1}
# __writing_dict_out_row_udfs_end__
# fmt: on

# fmt: off
# __datastream_compute_strategy_begin__
import ray
import pandas
import numpy
from ray.data import ActorPoolStrategy

# Dummy model to predict Iris variety.
def predict_iris(df: pandas.DataFrame) -> pandas.DataFrame:
    conditions = [
        (df["sepal.length"] < 5.0),
        (df["sepal.length"] >= 5.0) & (df["sepal.length"] < 6.0),
        (df["sepal.length"] >= 6.0)
    ]
    values = ["Setosa", "Versicolor", "Virginica"]
    return pandas.DataFrame({"predicted_variety": numpy.select(conditions, values)})

class IrisInferModel:
    def __init__(self):
        self._model = predict_iris

    def __call__(self, batch: pandas.DataFrame) -> pandas.DataFrame:
        return self._model(batch)

ds = ray.data.read_csv("example://iris.csv").repartition(10)

# Batch inference processing with Ray tasks (the default compute strategy).
predicted = ds.map_batches(predict_iris)

# Batch inference processing with Ray actors (pool of size 5).
predicted = ds.map_batches(
    IrisInferModel, compute=ActorPoolStrategy(size=5), batch_size=10)
# __datastream_compute_strategy_end__
# fmt: on
