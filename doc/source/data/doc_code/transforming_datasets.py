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
# __writing_udfs_begin__
import ray
import pandas
import pyarrow

# Load dataset.
ds = ray.data.read_csv("example://iris.csv")

# UDF as a function on Pandas DataFrame.
def pandas_filter_rows(df: pandas.DataFrame) -> pandas.DataFrame:
    return df[df["variety"] == "Versicolor"]

ds.map_batches(pandas_filter_rows).show(2)
# -> {'sepal.length': 7.0, 'sepal.width': 3.2,
#     'petal.length': 4.7, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}

# UDF as a function on PyArrow Table.
def pyarrow_filter_rows(batch: pyarrow.Table) -> pyarrow.Table:
    return batch.filter(pyarrow.compute.equal(batch["variety"], "Versicolor"))

ds.map_batches(pyarrow_filter_rows, batch_format="pyarrow").show(2)
# -> {'sepal.length': 7.0, 'sepal.width': 3.2,
#     'petal.length': 4.7, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}

# UDF as a callable class on Pandas DataFrame.
class UDFClassOnPandas:
    def __int__(self):
        pass

    def __call__(self, df: pandas.DataFrame) -> pandas.DataFrame:
        return df[df["variety"] == "Versicolor"]

ds.map_batches(UDFClassOnPandas, compute="actors").show(2)
# -> {'sepal.length': 7.0, 'sepal.width': 3.2,
#     'petal.length': 4.7, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}

# More UDF examples: Add a column.
def add_column(df: pandas.DataFrame) -> pandas.DataFrame:
    df["normalized.sepal.length"] =  df["sepal.length"] / df["sepal.length"].max()
    return df

ds.map_batches(add_column).show(2)
# -> {'sepal.length': 5.1, 'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa', 'normalized.sepal.length': 0.6455696202531644}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2,
#     'variety': 'Setosa', 'normalized.sepal.length': 0.620253164556962}

# More UDF examples: Drop a column.
def drop_column(df: pandas.DataFrame) -> pandas.DataFrame:
    return df.drop(columns=["sepal.length"])

ds.map_batches(drop_column).show(2)
# -> {'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# __writing_udfs_end__
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
    IrisInferModel, compute=ActorPoolStrategy(3, 10), batch_size=256)
# __dataset_compute_strategy_end__
# fmt: on
