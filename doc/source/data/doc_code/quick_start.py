# flake8: noqa
# fmt: off

# __create_from_python_begin__
import ray

# Create a Dataset from Python objects, which are also held as Python objects.
ds = ray.data.range(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]

ds.schema()
# <class 'int'>

# Create a Dataset from Python objects, which are held as Arrow records.
ds = ray.data.from_items([
        {"sepal.length":5.1,"sepal.width":3.5,"petal.length":1.4,"petal.width":0.2,"variety":"Setosa"},
        {"sepal.length":4.9,"sepal.width":3.0,"petal.length":1.4,"petal.width":0.2,"variety":"Setosa"},
        {"sepal.length":4.7,"sepal.width":3.2,"petal.length":1.3,"petal.width":0.2,"variety":"Setosa"},
     ])
# -> Dataset(num_blocks=3, num_rows=3, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string})

ds.show()
# -> {'sepal.length': 5.1, 'sepal.width': 3.5, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.9, 'sepal.width': 3.0, 'petal.length': 1.4, 'petal.width': 0.2, 'variety': 'Setosa'}
# -> {'sepal.length': 4.7, 'sepal.width': 3.2, 'petal.length': 1.3, 'petal.width': 0.2, 'variety': 'Setosa'}

ds.schema()
# -> sepal.length: double
# -> sepal.width: double
# -> petal.length: double
# -> petal.width: double
# -> variety: string
# __create_from_python_end__


# __create_from_files_begin__
# Create from CSV.
# Tip: "example://" is a convenient protocol to access python/ray/data/examples/data directory.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(num_blocks=1, num_rows=150, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string})

# Create from Parquet.
ds = ray.data.read_parquet("example://iris.parquet")
# -> Dataset(num_blocks=1, num_rows=150, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string})
# __create_from_files_end__


# __save_dataset_begin__
# Write to Parquet files in /tmp/iris.
ds.write_parquet("/tmp/iris")
# -> /tmp/iris/data_000000.parquet

# Use repartition to control the number of output files:
ds.repartition(2).write_parquet("/tmp/iris2")
# -> /tmp/iris2/data_000000.parquet
# -> /tmp/iris2/data_000001.parquet
# __save_dataset_end__


# __data_transform_begin__
import pyarrow as pa

# Create 10 blocks for parallelism.
ds = ds.repartition(10)

# Filter out the Versicolor.
def transform_batch(batch: pa.Table) -> pa.Table:
    return batch.filter(pa.compute.equal(batch["variety"], "Versicolor"))

transformed_ds = ds.map_batches(transform_batch, batch_format="pyarrow")
# -> Map Progress: 100%|███████████████████████████████████████████████████████████████████████████████████████| 10/10 [00:00<00:00, 17.65it/s]
# -> Dataset(num_blocks=10, num_rows=50, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string})

transformed_ds.show()
# -> {'sepal.length': 7.0, 'sepal.width': 3.2, 'petal.length': 4.7, 'petal.width': 1.4, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.4, 'sepal.width': 3.2, 'petal.length': 4.5, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.9, 'sepal.width': 3.1, 'petal.length': 4.9, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 5.5, 'sepal.width': 2.3, 'petal.length': 4.0, 'petal.width': 1.3, 'variety': 'Versicolor'}
# -> {'sepal.length': 6.5, 'sepal.width': 2.8, 'petal.length': 4.6, 'petal.width': 1.5, 'variety': 'Versicolor'}
# -> {'sepal.length': 5.7, 'sepal.width': 2.8, 'petal.length': 4.5, 'petal.width': 1.3, 'variety': 'Versicolor'}
# __data_transform_end__


# __data_access_begin__
@ray.remote
def consume(data) -> int:
    num_batches = 0
    for batch in data.iter_batches():
        num_batches += 1
    return num_batches

ray.get(consume.remote(ds))
# -> 10
# __data_access_end__


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
# -> [Dataset(num_blocks=3, num_rows=45, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string}), Dataset(num_blocks=3, num_rows=45, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string}), Dataset(num_blocks=2, num_rows=30, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string}), Dataset(num_blocks=2, num_rows=30, schema={sepal.length: double, sepal.width: double, petal.length: double, petal.width: double, variety: string})]

ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
# -> [45, 45, 30, 30]
# __dataset_split_end__
