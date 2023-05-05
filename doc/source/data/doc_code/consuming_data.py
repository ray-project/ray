# flake8: noqa

# fmt: off
# __take_begin__
import ray

ds = ray.data.range(10000)

# Take up to five records as a batch.
print(ds.take(5))
# -> [{'id': 0}, {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]

# Similar to above but returning in a batch format.
print(ds.take_batch(5))
# -> {'id': array([0, 1, 2, 3, 4])}

# Warning: This will print all of the rows!
print(ds.take_all())

ds.show(5)
# -> {'id': 0}
#    {'id': 1}
#    {'id': 2}
#    {'id': 3}
#    {'id': 4}
# __take_end__
# fmt: on

# fmt: off
# __iter_rows_begin__
import ray

ds = ray.data.range(10000)
num_rows = 0

# Consume all rows in the Datastream.
for row in ds.iter_rows():
    assert isinstance(row, dict)
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

# Consume all batches in the Datastream.
for batch in ds.iter_batches(batch_size=2):
    assert isinstance(batch, dict)
    num_batches += 1

print(num_batches)
# -> 5000

# Consume data as Pandas DataFrame batches.
cum_sum = 0
for batch in ds.iter_batches(batch_size=2, batch_format="pandas"):
    assert isinstance(batch, pd.DataFrame)
    # Simple integer Datastream is converted to a single-column Pandas DataFrame.
    cum_sum += batch["id"]
print(cum_sum)
# -> 49995000

# __iter_batches_end__
# fmt: on

# fmt: off
# __remote_iterators_begin__
import ray

@ray.remote
def consume(data: ray.data.Datastream) -> int:
    num_batches = 0
    # Consume data in 2-record batches.
    for batch in data.iter_batches(batch_size=2):
        assert len(batch["id"]) == 2
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

    def train(self, shard: ray.data.DataIterator) -> int:
        total = 0
        for batch in shard.iter_torch_batches(batch_size=256):
            total += len(batch["id"])
        return total

workers = [Worker.remote(i) for i in range(4)]
# -> [Actor(Worker, ...), Actor(Worker, ...), ...]

ds = ray.data.range(10000)
# -> Datastream(num_blocks=200, num_rows=10000, schema=<class 'int'>)

shards = ds.streaming_split(n=4, equal=True)
# -> [<StreamSplitDataIterator at 0x7fa5b5c99070>,
#     <StreamSplitDataIterator at 0x7fa5b5c990d0>,
#     <StreamSplitDataIterator at 0x7fa5b5c990f0>,
#     <StreamSplitDataIterator at 0x7fa5b5c991a0>]

ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
# -> [2500, 2500, 2500, 2500]
# __split_end__
# fmt: on
