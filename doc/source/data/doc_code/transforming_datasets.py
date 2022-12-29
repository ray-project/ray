# flake8: noqa

# fmt: off
# __dataset_transformation_begin__
import ray
import pandas

# Create a dataset from file with Iris data.
# Tip: "example://" is a convenient protocol to access the
# python/ray/data/examples/data directory.
ds = ray.data.read_csv("example://iris.csv")
# Dataset(num_blocks=1, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})
ds.show(3)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.7, 'sepal.width': 3.2,
#     'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

# Repartition the dataset to 5 blocks.
ds = ds.repartition(5)
# Dataset(num_blocks=5, num_rows=150,
#         schema={sepal.length: double, sepal.width: double,
#                 petal.length: double, petal.width: double, variety: string})

# Find rows with sepal.length < 5.5 and petal.length > 3.5.
def transform_batch(df: pandas.DataFrame) -> pandas.DataFrame:
    return df[(df["sepal.length"] < 5.5) & (df["petal.length"] > 3.5)]

# Map processing the dataset.
ds.map_batches(transform_batch).show()
# -> {'sepal.length': 5.2, 'sepal.width': 2.7,
#     'petal.length': 3.9, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 5.4, 'sepal.width': 3.0,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 4.9, 'sepal.width': 2.5,
#     'petal.length': 4.5, 'petal.width': 1.7, 'variety': 'Virginica'}

# Split the dataset into 2 datasets
ds.split(2)
# -> [Dataset(num_blocks=3, num_rows=90,
#             schema={sepal.length: double, sepal.width: double,
#                     petal.length: double, petal.width: double, variety: string}),
#     Dataset(num_blocks=2, num_rows=60,
#             schema={sepal.length: double, sepal.width: double,
#                     petal.length: double, petal.width: double, variety: string})]

# Sort the dataset by sepal.length.
ds = ds.sort("sepal.length")
ds.show(3)
# -> {'sepal.length': 4.3, 'sepal.width': 3.0,
#     'petal.length': 1.1, 'petal.width': 0.1, 'variety': 'Setosa'}
# -> {'sepal.length': 4.4, 'sepal.width': 2.9,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.4, 'sepal.width': 3.0,
#     'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

# Shuffle the dataset.
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
# __dataset_transformation_end__
# fmt: on

# fmt: off
# __writing_default_udfs_tabular_begin__
import ray
import pandas as pd

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
print(ds.default_batch_format())
# <class 'pandas.core.frame.DataFrame'>

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

ds.map_batches(pandas_transform).show(2)
# -> {'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# -> {'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'normalized.sepal.length': 0.9142857142857144}
# __writing_default_udfs_tabular_end__
# fmt: on

# fmt: off
# __writing_default_udfs_tensor_begin__
import ray
import numpy as np

# Load dataset.
ds = ray.data.range_tensor(1000, shape=(2, 2))
print(ds.default_batch_format())
# <class 'numpy.ndarray'>

# UDF as a function on NumPy ndarray batches.
def tensor_transform(arr: np.ndarray) -> np.ndarray:
    # Notice here that the ndarray is of shape (batch_size, 2, 2)
    # Multiply each element in the ndarray by a factor of 2
    return arr * 2

ds.map_batches(tensor_transform).show(2)
# [array([[0, 0],
#         [0, 0]]),
# array([[2, 2],
#         [2, 2]])]

# __writing_default_udfs_tensor_end__
# fmt: on

# fmt: off
# __writing_default_udfs_list_begin__
import ray

# Load dataset.
ds = ray.data.range(1000)
print(ds.default_batch_format())
# <class 'list'>

# UDF as a function on Python list batches.
def list_transform(list) -> list:
    # Notice here that the list is of length batch_size
    # Multiply each element in the list by a factor of 2
    return [x * 2 for x in list]

ds.map_batches(list_transform).show(2)
# 0
# 2

# __writing_default_udfs_list_end__
# fmt: on

# fmt: off
# __writing_pandas_udfs_begin__
import ray
import pandas as pd

# Load dataset.
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

ds.map_batches(pandas_transform).show(2)
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

# Load dataset.
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

# Load dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")

# UDF as a function on NumPy ndarray batches.
def normalize(arr: np.ndarray) -> np.ndarray:
    # Normalizes each image to [0, 1] range.
    mins = arr.min((1, 2))[:, np.newaxis, np.newaxis]
    maxes = arr.max((1, 2))[:, np.newaxis, np.newaxis]
    range_ = maxes - mins
    idx = np.where(range_ == 0)
    mins[idx] = 0
    range_[idx] = 1
    return (arr - mins) / range_

ds.map_batches(normalize, batch_format="numpy")
# -> Dataset(
#        num_blocks=1,
#        num_rows=3,
#        schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=double>}
#    )
# __writing_numpy_udfs_end__
# fmt: on

# fmt: off
# __writing_callable_classes_udfs_begin__
import ray

# Load dataset.
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

ds.map_batches(ModelUDF, compute="actors").show(2)
# -> {'sepal.length': 7.0, 'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'output': True}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'output': False}`
# __writing_callable_classes_udfs_end__
# fmt: on

# fmt: off
# __writing_pandas_out_udfs_begin__
import ray
import pandas as pd
from typing import List

# Load dataset.
ds = ray.data.read_text("example://sms_spam_collection_subset.txt")
# -> Dataset(num_blocks=1, num_rows=10, schema=<class 'str'>)

# Convert to Pandas.
def convert_to_pandas(text: List[str]) -> pd.DataFrame:
    return pd.DataFrame({"text": text})

ds = ds.map_batches(convert_to_pandas)
# -> Dataset(num_blocks=1, num_rows=10, schema={text: object})

ds.show(2)
# -> {
#        'text': (
#            'ham\tGo until jurong point, crazy.. Available only in bugis n great '
#            'world la e buffet... Cine there got amore wat...'
#        ),
#    }
# -> {'text': 'ham\tOk lar... Joking wif u oni...'}
# __writing_pandas_out_udfs_end__
# fmt: on

# fmt: off
# __writing_arrow_out_udfs_begin__
import ray
import pyarrow as pa
from typing import List

# Load dataset.
ds = ray.data.read_text("example://sms_spam_collection_subset.txt")
# -> Dataset(num_blocks=1, num_rows=10, schema=<class 'str'>)

# Convert to Arrow.
def convert_to_arrow(text: List[str]) -> pa.Table:
    return pa.table({"text": text})

ds = ds.map_batches(convert_to_arrow)
# -> Dataset(num_blocks=1, num_rows=10, schema={text: object})

ds.show(2)
# -> {
#        'text': (
#            'ham\tGo until jurong point, crazy.. Available only in bugis n great '
#            'world la e buffet... Cine there got amore wat...'
#        ),
#    }
# -> {'text': 'ham\tOk lar... Joking wif u oni...'}
# __writing_arrow_out_udfs_end__
# fmt: on

# fmt: off
# __writing_numpy_out_udfs_begin__
import ray
import pandas as pd
import numpy as np
from typing import Dict

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        },
#   )

# Convert to NumPy.
def convert_to_numpy(df: pd.DataFrame) -> np.ndarray:
    return df[["sepal.length", "sepal.width"]].to_numpy()

ds = ds.map_batches(convert_to_numpy)
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={__value__: <ArrowTensorType: shape=(2,), dtype=double>},
#    )

ds.show(2)
# -> [5.1 3.5]
#    [4.9 3. ]
# __writing_numpy_out_udfs_end__
# fmt: on

# fmt: off
# __writing_numpy_dict_out_udfs_begin__
import ray
import pandas as pd
import numpy as np
from typing import Dict

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        },
#   )

# Convert to dict of NumPy ndarrays.
def convert_to_numpy(df: pd.DataFrame) -> Dict[str, np.ndarray]:
    return {
        "sepal_len_and_width": df[["sepal.length", "sepal.width"]].to_numpy(),
        "petal_len": df["petal.length"].to_numpy(),
        "petal_width": df["petal.width"].to_numpy(),
    }

ds = ds.map_batches(convert_to_numpy)
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal_len_and_width: <ArrowTensorType: shape=(2,), dtype=double>,
#            petal_len: double,
#            petal_width: double,
#        },
#    )

ds.show(2)
# -> {'sepal_len_and_width': array([5.1, 3.5]), 'petal_len': 1.4, 'petal_width': 0.2}
# -> {'sepal_len_and_width': array([4.9, 3. ]), 'petal_len': 1.4, 'petal_width': 0.2}
# __writing_numpy_dict_out_udfs_end__
# fmt: on

# fmt: off
# __writing_simple_out_udfs_begin__
import ray
import pandas as pd
from typing import List

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        },
#   )

# Convert to list of dicts.
def convert_to_list(df: pd.DataFrame) -> List[dict]:
    return df.to_dict("records")

ds = ds.map_batches(convert_to_list)
# -> Dataset(num_blocks=1, num_rows=150, schema=<class 'dict'>)

ds.show(2)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa'}
# __writing_simple_out_udfs_end__
# fmt: on

# fmt: off
# __writing_dict_out_row_udfs_begin__
import ray
import pandas as pd
from typing import Dict

# Load dataset.
ds = ray.data.range(10)
# -> Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)

# Convert row to dict.
def row_to_dict(row: int) -> Dict[str, int]:
    return {"foo": row}

ds = ds.map(row_to_dict)
# -> Dataset(num_blocks=10, num_rows=10, schema={foo: int64})

ds.show(2)
# -> {'foo': 0}
# -> {'foo': 1}
# __writing_dict_out_row_udfs_end__
# fmt: on

# fmt: off
# __writing_table_row_out_row_udfs_begin__
import ray
import pandas as pd
from typing import Dict

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        },
#   )

# Treat row as dict.
def map_row(row: TableRow) -> TableRow:
    row = row.as_pydict()
    row["sepal.area"] = row["sepal.length"] * row["sepal.width"]
    return row

ds = ds.map(map_row)
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#            sepal.area: double,
#        },
#   )

ds.show(2)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa', 'sepal.area': 17.849999999999998}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa', 'sepal.area': 14.700000000000001}
# __writing_table_row_out_row_udfs_end__
# fmt: on

# fmt: off
# __writing_numpy_out_row_udfs_begin__
import ray
import numpy as np
from typing import Dict

# Load dataset.
ds = ray.data.range(10)
# -> Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)

# Convert row to NumPy ndarray.
def row_to_numpy(row: int) -> np.ndarray:
    return np.full(shape=(2, 2), fill_value=row)

ds = ds.map(row_to_numpy)
# -> Dataset(
#        num_blocks=10,
#        num_rows=10,
#        schema={__value__: <ArrowTensorType: shape=(2, 2), dtype=int64>},
#    )

ds.show(2)
# -> [[0 0]
#     [0 0]]
#    [[1 1]
#     [1 1]]
# __writing_numpy_out_row_udfs_end__
# fmt: on

# fmt: off
# __writing_simple_out_row_udfs_begin__
import ray
from typing import List

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        },
#   )

# Convert row to simple (opaque) row.
def map_row(row: TableRow) -> tuple:
    return tuple(row.items())

ds = ds.map(map_row)
# -> Dataset(num_blocks=1, num_rows=150, schema=<class 'tuple'>)

ds.show(2)
# -> (('sepal.length', 5.1), ('sepal.width', 3.5), ('petal.length', 1.4),
#     ('petal.width', 0.2), ('variety', 'Setosa'))
# -> (('sepal.length', 4.9), ('sepal.width', 3.0), ('petal.length', 1.4),
#     ('petal.width', 0.2), ('variety', 'Setosa'))
# __writing_simple_out_row_udfs_end__
# fmt: on

# fmt: off
# __configuring_batch_size_begin__
import ray
import pandas as pd

# Load dataset.
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

# Have each batch that pandas_transform receives contain 10 rows.
ds = ds.map_batches(pandas_transform, batch_size=10)

ds.show(2)
# -> {'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4,
#     'variety': 'Versicolor', 'normalized.sepal.length': 1.0}
# -> {'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5,
#     'variety': 'Versicolor', 'normalized.sepal.length': 0.9142857142857144}
# __configuring_batch_size_end__
# fmt: on

# fmt: off
# __dataset_compute_strategy_begin__
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

# Batch inference processing with Ray actors. Autoscale the actors between 3 and 10.
predicted = ds.map_batches(
    IrisInferModel, compute=ActorPoolStrategy(3, 10), batch_size=10)
# __dataset_compute_strategy_end__
# fmt: on
