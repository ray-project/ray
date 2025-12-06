.. _train-scaling-collation-functions:

Scaling out expensive collation functions
==========================================

This guide shows how to scale out your collate function by moving it from Ray Train workers to Ray Data for improved scalability and performance.

Why scale out collation functions?
-----------------------------------

By default, the collate function executes on the training worker when you call :meth:`ray.data.DataIterator.iter_torch_batches`. This approach has two main drawbacks:

- **Low scalability**: The collate function runs sequentially on each training worker, limiting parallelism.
- **Resource competition**: The collate function consumes CPU and memory resources from the training worker, potentially slowing down model training.

Scaling out the collate function to Ray Data allows you to:

- Scale collation across multiple CPU nodes independently of training workers
- Free up training worker resources for model computation
- Achieve better overall pipeline throughput, especially with heavy collate functions

This optimization is particularly effective when the collate function is computationally expensive (such as tokenization, image augmentation, or complex feature engineering) and you have additional CPU resources available for data preprocessing.

Overview
--------

This guide uses a text tokenization collate function as an example to demonstrate the optimization. You'll learn how to address the technical challenges of scaling out collate functions to Ray Data and see the complete implementation.

Mock dataset for examples
--------------------------

Throughout this guide, we use a mock text dataset to demonstrate the optimization. The following helper functions generate random text samples with labels:

.. testcode::
    :skipif: True

    import random
    import string
    import ray

    def random_text(length: int) -> str:
        """Generate random text of specified length."""
        if length <= 0:
            return ""
        
        if length <= 3:
            return "".join(random.choices(string.ascii_lowercase, k=length))
        
        words = []
        current_length = 0
        
        while current_length < length:
            remaining = length - current_length
            
            if remaining <= 4:
                word_length = remaining
                word = "".join(random.choices(string.ascii_lowercase, k=word_length))
                words.append(word)
                break
            else:
                max_word_length = min(10, remaining - 1)
                if max_word_length >= 3:
                    word_length = random.randint(3, max_word_length)
                else:
                    word_length = remaining
                word = "".join(random.choices(string.ascii_lowercase, k=word_length))
                words.append(word)
                current_length += len(word) + 1
        
        text = " ".join(words)
        return text[:length]

    def random_label() -> int:
        """Pick a random label."""
        labels = [0, 1, 2, 3, 4, 5, 6, 7]
        return random.choice(labels)

    def create_mock_ray_text_dataset(dataset_size: int = 96, min_len: int = 5, max_len: int = 100):
        """Create a mock Ray dataset with random text and labels."""
        numbers = random.choices(range(min_len, max_len + 1), k=dataset_size)
        ray_dataset = ray.data.from_items(numbers)
        
        def map_to_text_and_label(item):
            length = item['item']
            text = random_text(length)
            label = random_label()
            return {
                "length": length,
                "text": text,
                "label": label
            }
        
        text_dataset = ray_dataset.map(map_to_text_and_label)
        return text_dataset

The examples below assume these functions are available.

Ensuring batch size alignment
------------------------------

The collate function needs to process complete batches, but :meth:`ray.data.Dataset.map_batches` doesn't guarantee the batch size for each function call. Use :meth:`ray.data.Dataset.repartition` with ``target_num_rows_per_block`` to align block boundaries with batch boundaries.

There are two modes depending on whether your collate function changes the number of output rows:

**Mode 1: Output row count equals input row count**

Use ``repartition`` after ``map_batches`` when your collate function produces the same number of output rows as input rows:

.. code-block:: python

    dataset = dataset.map_batches(collate_fn, batch_size=BATCH_SIZE).repartition(target_num_rows_per_block=BATCH_SIZE)

This ensures the collate function receives batches of size ``BATCH_SIZE``, and the output blocks also contain ``BATCH_SIZE`` rows.

**Mode 2: Output row count differs from input row count**

Use ``repartition`` before ``map_batches`` when you don't need to ensure output block sizes. See :ref:`Advanced: Handling custom data types <train-tensor-serialization-utility>` for an example where batches are serialized into single rows.

.. code-block:: python

    dataset = dataset.repartition(target_num_rows_per_block=BATCH_SIZE).map_batches(collate_fn, batch_size=BATCH_SIZE)

Implementation
--------------

Baseline implementation
~~~~~~~~~~~~~~~~~~~~~~~

The following example shows a typical collate function that runs on the training worker:

.. testcode::
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

Optimized implementation
~~~~~~~~~~~~~~~~~~~~~~~~~

The following example moves the collate function to Ray Data preprocessing:

.. testcode::
    :skipif: True

    from transformers import AutoTokenizer
    import numpy as np
    from typing import Dict
    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig
    from ray.data.collate_fn import ArrowBatchCollateFn
    from mock_dataset import create_mock_ray_text_dataset
    import pyarrow as pa

    BATCH_SIZE = 10000

    class CollateFnRayData(ArrowBatchCollateFn):
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
            collate_fn=None,    # numpy array is converted to torch.Tensor automatically
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
            CollateFnRayData(),
            batch_size=BATCH_SIZE,
            batch_format="pyarrow",
        )
        .repartition(target_num_rows_per_block=BATCH_SIZE)  # Ensure outputs match batch size
    )

    trainer = TorchTrainer(
        train_func,
        datasets={"train": train_dataset},
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
    )

    result = trainer.fit()

Key differences from baseline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The optimized implementation makes these changes:

1. **Preprocessing in Ray Data**: The tokenization logic moves from ``train_func`` to ``CollateFnRayData``, which runs in ``map_batches``.

2. **NumPy output**: The collate function returns ``Dict[str, np.ndarray]`` instead of PyTorch tensors, which Ray Data natively supports.

3. **Batch alignment**: ``repartition(target_num_rows_per_block=BATCH_SIZE)`` after ``map_batches`` ensures the collate function receives exact batch sizes and output blocks align with the batch size.

4. **No collate_fn in iterator**: ``iter_torch_batches`` uses ``collate_fn=None`` because preprocessing already happened in Ray Data.

Benchmark results
-----------------

The following benchmarks demonstrate the performance improvement from scaling out the collate function. The test uses text tokenization with a batch size of 10,000 on a dataset of 1 million rows with text lengths between 1,000 and 3,000 characters.

**Single node (g4dn.12xlarge: 48 vCPU, 4 NVIDIA T4 GPUs, 192 GiB memory)**

.. list-table::
   :header-rows: 1

   * - Configuration
     - Throughput
   * - Collate in iterator (baseline)
     - 1,588 rows/s
   * - Collate in Ray Data
     - 3,437 rows/s

**With 2 additional CPU nodes (m5.8xlarge: 32 vCPU, 128 GiB memory each)**

.. list-table::
   :header-rows: 1

   * - Configuration
     - Throughput
   * - Collate in iterator (baseline)
     - 1,659 rows/s
   * - Collate in Ray Data
     - 10,717 rows/s

The results show that scaling out the collate function to Ray Data provides a 2x speedup on a single node and a 6x speedup when adding CPU-only nodes for preprocessing.

Advanced: Handling custom data types
-------------------------------------

The optimized implementation above returns ``Dict[str, np.ndarray]``, which Ray Data natively supports. However, if your collate function needs to return PyTorch tensors or other custom data types that :meth:`ray.data.Dataset.map_batches` doesn't directly support, you need to serialize them.

.. _train-tensor-serialization-utility:

Tensor serialization utility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following utility serializes PyTorch tensors into PyArrow format. It flattens all tensors in a batch into a single binary buffer, stores metadata about tensor shapes and dtypes, and packs everything into a single-row PyArrow table. On the training side, it deserializes the table back into the original tensor structure.

The serialization and deserialization operations are typically lightweight compared to the actual collate function work (such as tokenization or image processing), so the overhead is minimal relative to the performance gains from scaling the collate function.

You can use this reference implementation or adapt it to your needs:

.. testcode::
    :skipif: True

    from dataclasses import dataclass
    from typing import Dict, List, Tuple, Union
    import torch
    from ray import cloudpickle as pickle
    import pyarrow as pa

    # (dtype, shape, offset)
    FEATURE_TYPE = Tuple[torch.dtype, torch.Size, int]
    TORCH_BYTE_ELEMENT_TYPE = torch.uint8

    def _create_binary_array_from_buffer(buffer: bytes) -> pa.BinaryArray:
        """Zero-copy create a binary array from a buffer."""
        data_buffer = pa.py_buffer(buffer)
        return pa.Array.from_buffers(
            pa.binary(),
            1,
            [
                None,
                pa.array([0, data_buffer.size], type=pa.int32()).buffers()[1],
                data_buffer,
            ],
        )

    @dataclass
    class _Metadata:
        features: Dict[str, List[FEATURE_TYPE]]
        total_buffer_size: int

    @dataclass
    class _TensorBatch:
        """Internal class for serializing/deserializing tensor batches."""
        buffer: torch.Tensor
        metadata: _Metadata

        @classmethod
        def from_batch(cls, batch: Dict[str, Union[List[torch.Tensor], torch.Tensor]]) -> '_TensorBatch':
            """Serialize a batch of tensors into a single buffer."""
            features: Dict[str, List[FEATURE_TYPE]] = {}
            flattened_binary_tensors = []
            total_buffer_size = 0
            
            for name, tensors in batch.items():
                features[name] = []
                if not isinstance(tensors, list):
                    tensors = [tensors]
                for tensor in tensors:
                    flattened_tensor = tensor.flatten().contiguous().view(TORCH_BYTE_ELEMENT_TYPE)
                    flattened_binary_tensors.append(flattened_tensor)
                    features[name].append((tensor.dtype, tensor.shape, total_buffer_size))
                    total_buffer_size += flattened_tensor.shape[0]
            
            buffer = torch.empty(total_buffer_size, dtype=TORCH_BYTE_ELEMENT_TYPE)
            cur_offset = 0
            for flattened_tensor in flattened_binary_tensors:
                buffer[cur_offset:cur_offset + flattened_tensor.shape[0]] = flattened_tensor
                cur_offset += flattened_tensor.shape[0]
            
            return _TensorBatch(
                buffer=buffer,
                metadata=_Metadata(
                    features=features,
                    total_buffer_size=total_buffer_size,
                ),
            )

        def to_table(self) -> pa.Table:
            """Convert to a single-row PyArrow table."""
            buffer_array = _create_binary_array_from_buffer(self.buffer.numpy().data)
            metadata_array = _create_binary_array_from_buffer(pickle.dumps(self.metadata))
            return pa.Table.from_arrays(
                arrays=[buffer_array, metadata_array],
                names=["_buffer", "_metadata"],
            )

        @classmethod
        def from_table(cls, table: pa.Table) -> '_TensorBatch':
            """Deserialize from a single-row PyArrow table."""
            return _TensorBatch(
                buffer=torch.frombuffer(
                    table["_buffer"].chunks[0].buffers()[2],
                    dtype=TORCH_BYTE_ELEMENT_TYPE
                ),
                metadata=pickle.loads(table["_metadata"].chunks[0].buffers()[2]),
            )

        def to_batch(self, pin_memory: bool = False) -> Dict[str, List[torch.Tensor]]:
            """Deserialize back to a batch of tensors."""
            batch = {}
            storage_buffer = self.buffer.untyped_storage()
            offsets = []
            for name, features in self.metadata.features.items():
                for _, _, offset in features:
                    offsets.append(offset)
            offsets.append(self.metadata.total_buffer_size)
            
            offset_id = 0
            for name, features in self.metadata.features.items():
                batch[name] = []
                for dtype, shape, _ in features:
                    # Create a zero-copy view of the byte slice.
                    byte_slice = self.buffer[offsets[offset_id]:offsets[offset_id + 1]]
                    tensor = torch.frombuffer(
                        byte_slice.numpy().data, dtype=dtype
                    ).view(shape)
                    if pin_memory:
                        tensor = tensor.pin_memory()
                    batch[name].append(tensor)
                    offset_id += 1
            return batch

    # Helper functions for use in your code
    def serialize_tensors_to_table(batch: Dict[str, Union[List[torch.Tensor], torch.Tensor]]) -> pa.Table:
        """Serialize a batch of tensors to a PyArrow table."""
        return _TensorBatch.from_batch(batch).to_table()

    def deserialize_table_to_tensors(table: pa.Table, pin_memory: bool = False) -> Dict[str, List[torch.Tensor]]:
        """Deserialize a PyArrow table back to tensors."""
        return _TensorBatch.from_table(table).to_batch(pin_memory=pin_memory)

Example with tensor serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example demonstrates using tensor serialization when your collate function must return PyTorch tensors. This approach requires ``repartition`` before ``map_batches`` because the collate function changes the number of output rows (each batch becomes a single serialized row).

.. testcode::
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
            TextTokenizerCollateFn(),
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
