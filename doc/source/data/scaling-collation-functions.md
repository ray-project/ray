---
myst:
  html_meta:
    description: "Move an expensive collate function into Ray Data so it scales across the cluster, with batch size alignment and tensor serialization."
---

(scaling_collation_functions)=

# Advanced: Scaling out expensive collate functions

By default, the collate function executes on the training worker when you call {meth}`ray.data.DataIterator.iter_torch_batches`. This approach has two main drawbacks:

- **Low scalability**: The collate function runs sequentially on each training worker, limiting parallelism.
- **Resource competition**: The collate function uses CPU and memory on the training worker, which can slow down model training.

When you move the collate function into Ray Data, you can scale collation across multiple CPU nodes independently of the training workers. This improves overall pipeline throughput, especially for heavy collate functions.

This optimization is especially effective when the collate function is computationally expensive and you have additional CPU resources available for data preprocessing. Expensive collate functions include tokenization, image augmentation, and complex feature engineering.

(moving-the-collate-function-to-ray-data)=

## Move the collate function to Ray Data

The following example shows a typical collate function that runs on the training worker:

```python
train_dataset = read_parquet().map(...)

def train_func():
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=collate_fn,
        batch_size=BATCH_SIZE
    ):
        # Training logic here
        pass

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

To scale out a time-intensive or compute-intensive collate function, do the following:

- Create a custom collate function that runs in Ray Data and use {meth}`ray.data.Dataset.map_batches` to scale it out.
- Use {meth}`ray.data.Dataset.repartition` to ensure batch size alignment.

(creating-a-custom-collate-function-that-runs-in-ray-data)=

## Create a custom collate function that runs in Ray Data

To scale out, move the `collate_fn` into a Ray Data `map_batches` operation:

```python
def collate_fn(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    return batch

train_dataset = train_dataset.map_batches(collate_fn, batch_size=BATCH_SIZE)

def train_func():
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=None,
        batch_size=BATCH_SIZE,
    ):
        # Training logic here
        pass

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

This example has two key details:

- The `collate_fn` returns a dictionary of NumPy arrays, which is a standard Ray Data batch format.
- The `iter_torch_batches` method uses `collate_fn=None`, which reduces the work that the training worker process does.

(ensuring-batch-size-alignment)=

## Ensure batch size alignment

A collate function typically creates complete batches of data with a target batch size. However, by default, {meth}`ray.data.Dataset.map_batches` doesn't guarantee the batch size for each function call, which matters when you move the collate function into Ray Data.

You might encounter two common problems:

1. The collate function requires a certain number of input rows to work properly.
1. You want to avoid reformatting or rebatching the data on the training worker process.

To solve these problems, use {meth}`ray.data.Dataset.repartition` with `target_num_rows_per_block` to ensure batch size alignment.

Call `repartition` before `map_batches` to ensure that the input blocks contain the desired number of rows:

```python
# Note: If you only use map_batches(batch_size=BATCH_SIZE), you are not guaranteed to get the desired number of rows as an input.
dataset = dataset.repartition(target_num_rows_per_block=BATCH_SIZE).map_batches(collate_fn, batch_size=BATCH_SIZE)
```

Call `repartition` after `map_batches` to ensure that the output blocks contain the desired number of rows. This avoids reformatting or rebatching the data on the training worker process.

```python
dataset = dataset.map_batches(collate_fn, batch_size=BATCH_SIZE).repartition(target_num_rows_per_block=BATCH_SIZE)

def train_func():
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=None,
        batch_size=BATCH_SIZE,
    ):
        # Training logic here
        pass

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

(putting-things-together)=

## Put it all together

The following examples use a mock text dataset to demonstrate the optimization. For the mock dataset implementation, see {ref}`random-text-generator`.

::::{tab-set}

:::{tab-item} Baseline implementation

The following example shows a typical collate function that runs on the training worker:

```{testcode}
:skipif: True

from transformers import AutoTokenizer
import torch
import numpy as np
from typing import Dict
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig
from mock_dataset import create_mock_ray_text_dataset

BATCH_SIZE = 10000

def vanilla_collate_fn(tokenizer: AutoTokenizer, batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
    outputs = tokenizer(
        list(batch["text"]),
        truncation=True,
        padding="longest",
        return_tensors="pt",
    )
    outputs["labels"] = torch.LongTensor(batch["label"])
    return outputs

def train_func():
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    collate_fn = lambda x: vanilla_collate_fn(tokenizer, x)

    # Collate function runs on the training worker
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=collate_fn,
        batch_size=BATCH_SIZE
    ):
        # Training logic here
        pass

train_dataset = create_mock_ray_text_dataset(
    dataset_size=1000000,
    min_len=1000,
    max_len=3000
)

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

:::

:::{tab-item} Optimized implementation

The following example moves the collate function to Ray Data preprocessing:

```{testcode}
:skipif: True

from transformers import AutoTokenizer
import numpy as np
from typing import Dict
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig
from mock_dataset import create_mock_ray_text_dataset
import pyarrow as pa

BATCH_SIZE = 10000

class CollateFnRayData:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def __call__(self, batch: pa.Table) -> Dict[str, np.ndarray]:
        results = self.tokenizer(
            batch["text"].to_pylist(),
            truncation=True,
            padding="longest",
            return_tensors="np",
        )
        results["labels"] = np.array(batch["label"])
        return results

def train_func():
    # Collate function already ran in Ray Data
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=None,
        batch_size=BATCH_SIZE,
    ):
        # Training logic here
        pass

# Apply preprocessing in Ray Data
train_dataset = (
    create_mock_ray_text_dataset(
        dataset_size=1000000,
        min_len=1000,
        max_len=3000
    )
    .map_batches(
        CollateFnRayData,
        batch_size=BATCH_SIZE,
        batch_format="pyarrow",
    )
    .repartition(target_num_rows_per_block=BATCH_SIZE)  # Ensure batch size alignment
)

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

:::

::::

The optimized implementation makes four changes:

- **Preprocessing in Ray Data**: The tokenization logic moves from `train_func` to `CollateFnRayData`, which runs in `map_batches`.
- **NumPy output**: The collate function returns `Dict[str, np.ndarray]` instead of PyTorch tensors. Ray Data natively supports this format.
- **Batch alignment**: `repartition(target_num_rows_per_block=BATCH_SIZE)` after `map_batches` ensures the collate function receives exact batch sizes and output blocks align with the batch size.
- **No `collate_fn` in iterator**: `iter_torch_batches` uses `collate_fn=None` because the preprocessing already ran in Ray Data.

### Benchmark results

The following benchmarks show the performance improvement from scaling out the collate function. The benchmark runs text tokenization with a batch size of 10,000 on a dataset of 1 million rows, with text lengths between 1,000 and 3,000 characters.

```{list-table} Single g4dn.12xlarge node with 48 vCPU, 4 NVIDIA T4 GPUs, and 192 GiB memory
:header-rows: 1

* - Configuration
  - Throughput
* - Collate in iterator (baseline)
  - 1,588 rows/s
* - Collate in Ray Data
  - 3,437 rows/s
```

```{list-table} With 2 additional m5.8xlarge CPU nodes, each with 32 vCPU and 128 GiB memory
:header-rows: 1

* - Configuration
  - Throughput
* - Collate in iterator (baseline)
  - 1,659 rows/s
* - Collate in Ray Data
  - 10,717 rows/s
```

In these benchmarks, scaling out the collate function to Ray Data provides a 2x speedup on a single node and a 6x speedup when you add CPU-only nodes for preprocessing.

(advanced-handling-custom-data-types)=

## Advanced: Handle custom data types

The preceding optimized implementation returns `Dict[str, np.ndarray]`, which Ray Data natively supports. If your collate function must return PyTorch tensors or other custom data types that {meth}`ray.data.Dataset.map_batches` doesn't directly support, serialize them.

(train-tensor-serialization-utility)=
(tensor-serialization-utility)=

### What does the tensor serialization utility do?

The tensor serialization utility serializes PyTorch tensors into PyArrow format. It flattens all tensors in a batch into a single binary buffer, stores metadata about tensor shapes and dtypes, and packs everything into a single-row PyArrow table. On the training side, it deserializes the table back into the original tensor structure.

Serialization and deserialization are typically lightweight compared to the collate function's main work, such as tokenization or image processing. The overhead is therefore usually minimal relative to the performance gains from scaling out the collate function.

Use {ref}`train-collate-utils` as a reference implementation, and adapt it to your needs.

(example-with-tensor-serialization)=

### Serialize tensors in a collate function

The following example uses tensor serialization for a collate function that must return PyTorch tensors. This approach requires `repartition` before `map_batches` because the collate function changes the number of output rows. Each batch becomes a single serialized row.

```{testcode}
:skipif: True

from transformers import AutoTokenizer
import torch
from typing import Dict
from ray.data.collate_fn import ArrowBatchCollateFn
import pyarrow as pa
from collate_utils import serialize_tensors_to_table, deserialize_table_to_tensors
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig
from mock_dataset import create_mock_ray_text_dataset

BATCH_SIZE = 10000

class TextTokenizerCollateFn:
    """Collate function that runs in Ray Data preprocessing."""
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def __call__(self, batch: pa.Table) -> pa.Table:
        # Tokenize the batch
        outputs = self.tokenizer(
            batch["text"].to_pylist(),
            truncation=True,
            padding="longest",
            return_tensors="pt",
        )
        outputs["labels"] = torch.LongTensor(batch["label"].to_numpy())

        # Serialize to single-row table using the utility
        return serialize_tensors_to_table(outputs)

class IteratorCollateFn(ArrowBatchCollateFn):
    """Collate function for iter_torch_batches that deserializes the batch."""
    def __init__(self, pin_memory=False):
        self._pin_memory = pin_memory

    def __call__(self, batch: pa.Table) -> Dict[str, torch.Tensor]:
        # Deserialize from single-row table using the utility
        return deserialize_table_to_tensors(batch, pin_memory=self._pin_memory)

def train_func():
    collate_fn = IteratorCollateFn()

    # Collate function only deserializes on the training worker
    for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
        collate_fn=collate_fn,
        batch_size=1  # Each "row" is actually a full batch
    ):
        # Training logic here
        pass

# Apply preprocessing in Ray Data
# Use repartition BEFORE map_batches because output row count changes
train_dataset = (
    create_mock_ray_text_dataset(
        dataset_size=1000000,
        min_len=1000,
        max_len=3000
    )
    .repartition(target_num_rows_per_block=BATCH_SIZE)
    .map_batches(
        TextTokenizerCollateFn,
        batch_size=BATCH_SIZE,
        batch_format="pyarrow",
    )
)

trainer = TorchTrainer(
    train_func,
    datasets={"train": train_dataset},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```
