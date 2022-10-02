# flake8: noqa

# fmt: off
# __create_from_python_begin__
import ray

# Create a Dataset of Python objects.
ds = ray.data.range(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]

ds.schema()
# <class 'int'>

# Create a Dataset from Python objects, which are held as Arrow records.
ds = ray.data.from_items([
        {"sepal.length": 5.1, "sepal.width": 3.5,
         "petal.length": 1.4, "petal.width": 0.2, "variety": "Setosa"},
        {"sepal.length": 4.9, "sepal.width": 3.0,
         "petal.length": 1.4, "petal.width": 0.2, "variety": "Setosa"},
        {"sepal.length": 4.7, "sepal.width": 3.2,
         "petal.length": 1.3, "petal.width": 0.2, "variety": "Setosa"},
     ])
# Dataset(num_blocks=3, num_rows=3,
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
# Tip: "example://" is a convenient protocol to access the
# python/ray/data/examples/data directory.
ds = ray.data.read_csv("example://iris.csv")
# Dataset(num_blocks=1, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

# Create from Parquet.
ds = ray.data.read_parquet("example://iris.parquet")
# Dataset(num_blocks=1, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

# __create_from_files_end__
# fmt: on

# fmt: off
# __save_dataset_begin__
# Write to Parquet files in /tmp/iris.
ds.write_parquet("/tmp/iris")
# -> /tmp/iris/data_000000.parquet

# Use repartition to control the number of output files:
ds.repartition(2).write_parquet("/tmp/iris2")
# -> /tmp/iris2/data_000000.parquet
# -> /tmp/iris2/data_000001.parquet
# __save_dataset_end__
# fmt: on

# fmt: off
# __data_transform_begin__
import pandas

# Create 10 blocks for parallelism.
ds = ds.repartition(10)
# Dataset(num_blocks=10, num_rows=150,
#         schema={sepal.length: float64, sepal.width: float64,
#                 petal.length: float64, petal.width: float64, variety: object})

# Find rows with sepal.length < 5.5 and petal.length > 3.5.
def transform_batch(df: pandas.DataFrame) -> pandas.DataFrame:
    return df[(df["sepal.length"] < 5.5) & (df["petal.length"] > 3.5)]

transformed_ds = ds.map_batches(transform_batch)
# Dataset(num_blocks=10, num_rows=3,
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

# fmt: off
# __data_access_begin__
@ray.remote
def consume(data) -> int:
    num_batches = 0
    for batch in data.iter_batches(batch_size=10):
        num_batches += 1
    return num_batches

ray.get(consume.remote(ds))
# -> 15
# __data_access_end__
# fmt: on

# fmt: off
# __dataset_split_begin__
@ray.remote
class Worker:
    def __init__(self, rank: int):
        pass

    def train(self, shard) -> int:
        for batch in shard.iter_batches(batch_size=256):
            pass
        return shard.count()

workers = [Worker.remote(i) for i in range(4)]
# -> [Actor(Worker, ...), Actor(Worker, ...), ...]

shards = ds.split(n=4, locality_hints=workers)
# -> [
#       Dataset(num_blocks=3, num_rows=45,
#               schema={sepal.length: double, sepal.width: double,
#                       petal.length: double, petal.width: double, variety: string}),
#       Dataset(num_blocks=3, num_rows=45,
#               schema={sepal.length: double, sepal.width: double,
#                       petal.length: double, petal.width: double, variety: string}),
#       Dataset(num_blocks=2, num_rows=30,
#               schema={sepal.length: double, sepal.width: double,
#                       petal.length: double, petal.width: double, variety: string}),
#       Dataset(num_blocks=2, num_rows=30,
#               schema={sepal.length: double, sepal.width: double,
#                       petal.length: double, petal.width: double, variety: string}),
#    ]

ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
# -> [45, 45, 30, 30]
# __dataset_split_end__
# fmt: on
