# flake8: noqa

# fmt: off
# __take_begin__
import ray

ds = ray.data.range(10000)

print(ds.take(5))
# -> [0, 1, 2, 3, 4]

# Warning: This will print all of the rows!
print(ds.take_all())

ds.show(5)
# -> 0
#    1
#    2
#    3
#    4
# __take_end__
# fmt: on

# fmt: off
# __iter_rows_begin__
import ray

ds = ray.data.range(10000)
num_rows = 0

# Consume all rows in the Dataset.
for row in ds.iter_rows():
    assert isinstance(row, int)
    num_rows += 1

print(num_rows)
# -> 10000
# __iter_rows_end__
# fmt: on

# fmt: off
# __iter_batches_begin__
import ray
import pandas as pd

ds = ray.data.range(10000)
num_batches = 0

# Consume all batches in the Dataset.
for batch in ds.iter_batches(batch_size=2):
    assert isinstance(batch, list)
    num_batches += 1

print(num_batches)
# -> 5000

# Consume data as Pandas DataFrame batches.
cum_sum = 0
for batch in ds.iter_batches(batch_size=2, batch_format="pandas"):
    assert isinstance(batch, pd.DataFrame)
    # Simple integer Dataset is converted to a single-column Pandas DataFrame.
    cum_sum += batch["value"]
print(cum_sum)
# -> 49995000

# __iter_batches_end__
# fmt: on

# fmt: off
# __remote_iterators_begin__
import ray

@ray.remote
def consume(data: ray.data.Dataset[int]) -> int:
    num_batches = 0
    # Consume data in 2-record batches.
    for batch in data.iter_batches(batch_size=2):
        assert len(batch) == 2
        num_batches += 1
    return num_batches

ds = ray.data.range(10000)
ray.get(consume.remote(ds))
# -> 5000
# __remote_iterators_end__
# fmt: on

# fmt: off
# __split_begin__
# @ray.remote(num_gpus=1)  # Uncomment this to run on GPUs.
@ray.remote
class Worker:
    def __init__(self, rank: int):
        pass

    def train(self, shard: ray.data.Dataset[int]) -> int:
        for batch in shard.iter_torch_batches(batch_size=256):
            pass
        return shard.count()

workers = [Worker.remote(i) for i in range(4)]
# -> [Actor(Worker, ...), Actor(Worker, ...), ...]

ds = ray.data.range(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

shards = ds.split(n=4, locality_hints=workers)
# -> [Dataset(num_blocks=13, num_rows=2500, schema=<class 'int'>),
#     Dataset(num_blocks=13, num_rows=2500, schema=<class 'int'>), ...]

ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
# -> [2500, 2500, 2500, 2500]
# __split_end__
# fmt: on
