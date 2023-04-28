# flake8: noqa

# fmt: off
# __create_from_python_begin__
import ray

# Create a Datastream of Python objects.
ds = ray.data.range(10000)
# -> Datastream(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]

ds.schema()
# <class 'int'>

# Create a Datastream from Python objects, which are held as Arrow records.
ds = ray.data.from_items([
        {"sepal.length": 5.1, "sepal.width": 3.5,
         "petal.length": 1.4, "petal.width": 0.2, "variety": "Setosa"},
        {"sepal.length": 4.9, "sepal.width": 3.0,
         "petal.length": 1.4, "petal.width": 0.2, "variety": "Setosa"},
        {"sepal.length": 4.7, "sepal.width": 3.2,
         "petal.length": 1.3, "petal.width": 0.2, "variety": "Setosa"},
     ])
# Datastream(num_blocks=3, num_rows=3,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

ds.show()
# -> {'sepal.length': 5.1, 'sepal.width': 3.5,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0,
#     'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.7, 'sepal.width': 3.2,
#     'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

ds.schema()
# -> sepal.length: double
# -> sepal.width: double
# -> petal.length: double
# -> petal.width: double
# -> variety: string
# __create_from_python_end__
# fmt: on

# fmt: off
# __create_from_files_begin__
# Create from CSV.
ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
# Datastream(num_blocks=1, num_rows=150,
#         schema={sepal length (cm): double, sepal width (cm): double, 
#         petal length (cm): double, petal width (cm): double, target: int64})

# Create from Parquet.
ds = ray.data.read_parquet("s3://anonymous@air-example-data/iris.parquet")
# Datastream(num_blocks=1, num_rows=150,
#         schema={sepal.length: double, sepal.width: double, 
#         petal.length: double, petal.width: double, variety: string})

# __create_from_files_end__
# fmt: on

# fmt: off
# __data_transform_begin__
import pandas

# Create 10 blocks for parallelism.
ds = ds.repartition(10)
# Datastream(num_blocks=10, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

# Find rows with sepal.length < 5.5 and petal.length > 3.5.
def transform_batch(df: pandas.DataFrame) -> pandas.DataFrame:
    return df[(df["sepal.length"] < 5.5) & (df["petal.length"] > 3.5)]

transformed_ds = ds.map_batches(transform_batch, batch_format="pandas")
# Datastream(num_blocks=10, num_rows=3,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

transformed_ds.show()
# -> {'sepal.length': 5.2, 'sepal.width': 2.7,
#     'petal.length': 3.9, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 5.4, 'sepal.width': 3.0,
#     'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 4.9, 'sepal.width': 2.5,
#     'petal.length': 4.5, 'petal.width': 1.7, 'variety': 'Virginica'}
# __data_transform_end__
# fmt: on
