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
# __torch_begin__
import ray
import torch

ds = ray.data.range(10000)

num_batches = 0
for batch in ds.iter_torch_batches(batch_size=2):
    assert isinstance(batch, torch.Tensor)
    assert batch.size(dim=0) == 2
    num_batches += 1

print(num_batches)
# -> 5000
# __torch_end__
# fmt: on

# fmt: off
# __torch_with_label_begin__
import ray
import torch
import pandas as pd

df = pd.DataFrame({
    "feature1": list(range(4)),
    "feature2": [2 * i for i in range(4)],
    "label": [True, False, True, False],
})
ds = ray.data.from_pandas(df)

num_batches = 0
for batch in ds.iter_torch_batches(batch_size=2):
    feature1 = batch["feature1"]
    feature2 = batch["feature2"]
    label = batch["label"]
    assert isinstance(feature1, torch.Tensor)
    assert isinstance(feature2, torch.Tensor)
    assert isinstance(label, torch.Tensor)
    # Batch dimension.
    assert feature1.size(dim=0) == 2
    assert feature2.size(dim=0) == 2
    assert label.size(dim=0) == 2
    num_batches += 1

print(num_batches)
# -> 2
# __torch_with_label_end__
# fmt: on

# fmt: off
# __tf_begin__
import ray
import tensorflow as tf

ds = ray.data.range(10000)

tf_ds = ds.iter_tf_batches(
    batch_size=2,
)

num_batches = 0
for batch in tf_ds:
    assert isinstance(batch, tf.Tensor)
    assert batch.shape[0] == 2, batch.shape
    num_batches += 1

print(num_batches)
# -> 5000
# __tf_end__
# fmt: on

# fmt: off
# __tf_with_label_begin__
import ray
import tensorflow as tf
import pandas as pd

df = pd.DataFrame({
    "feature1": list(range(4)),
    "feature2": [2 * i for i in range(4)],
    "label": [True, False, True, False],
})
ds = ray.data.from_pandas(df)

# Specify the label column; all other columns will be treated as feature columns and
# will be concatenated into the same TensorFlow tensor.
tf_ds = ds.iter_tf_batches(
    batch_size=2,
)

num_batches = 0
for batch in tf_ds:
    feature1 = batch["feature1"]
    feature2 = batch["feature2"]
    label = batch["label"]
    assert isinstance(feature1, tf.Tensor)
    assert isinstance(feature2, tf.Tensor)
    assert isinstance(label, tf.Tensor)
    # Batch dimension.
    assert feature1.shape[0] == 2
    assert feature2.shape[0] == 2
    assert label.shape[0] == 2
    num_batches += 1

print(num_batches)
# -> 2
# __tf_with_label_end__
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

shards = ds.split(n=4)
# -> [Dataset(num_blocks=13, num_rows=2500, schema=<class 'int'>),
#     Dataset(num_blocks=13, num_rows=2500, schema=<class 'int'>), ...]

ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
# -> [2500, 2500, 2500, 2500]
# __split_end__
# fmt: on
