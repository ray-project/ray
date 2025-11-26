.. _train-move-collate-to-data:

Move collate function to Ray Data
==================================

This guide shows how to move your collate function from Ray Train workers to Ray Data for improved scalability and performance.

Why move collate functions to Ray Data?
----------------------------------------

By default, the collate function executes on the training worker when you call :meth:`ray.data.DataIterator.iter_torch_batches`. This approach has two main drawbacks:

- **Low scalability**: The collate function runs sequentially on each training worker, limiting parallelism.
- **Resource competition**: The collate function consumes CPU and memory resources from the training worker, potentially slowing down model training.

Moving the collate function to Ray Data allows you to:

- Scale collation across multiple CPU nodes independently of training workers
- Free up training worker resources for model computation
- Achieve better overall pipeline throughput, especially with heavy collate functions

Overview
--------

This guide demonstrates how to optimize a text tokenization collate function by moving it from the training worker to Ray Data preprocessing. The optimization is particularly effective when:

- The collate function is computationally expensive (such as tokenization, image augmentation, or complex feature engineering)
- Training is relatively lightweight compared to data preprocessing
- You have additional CPU resources available for data preprocessing

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

Challenges and solutions
------------------------

Moving collate functions to Ray Data introduces two main challenges:

**Challenge 1: Batch boundaries**
  The collate function needs to process complete batches, but :meth:`ray.data.Dataset.map_batches` doesn't guarantee that it won't split or merge data blocks for performance optimization.

  **Solution**: Use :meth:`ray.data.Dataset.repartition` with ``target_num_rows_per_block`` set to your batch size to align block boundaries with batch boundaries.

**Challenge 2: Return type mismatch**
  Collate functions typically return ``Dict[str, torch.Tensor]``, but :meth:`ray.data.Dataset.map_batches` only accepts functions that return ``pyarrow.Table``, ``pandas.DataFrame``, or ``Dict[str, numpy.ndarray]``.

  **Solution**: Serialize PyTorch tensors into a single-row PyArrow table during preprocessing, then deserialize them back to tensors in the training loop. See the :ref:`tensor serialization utility <train-tensor-serialization-utility>` for a reference implementation.

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
            batch_size=1024
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

.. _train-tensor-serialization-utility:

Tensor serialization utility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To address the return type mismatch, you need a way to serialize PyTorch tensors into PyArrow format. The following utility demonstrates one approach: it flattens all tensors in a batch into a single binary buffer, stores metadata about tensor shapes and dtypes, and packs everything into a single-row PyArrow table. On the training side, it deserializes the table back into the original tensor structure.

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

Optimized implementation
~~~~~~~~~~~~~~~~~~~~~~~~~

The following example moves the collate function to Ray Data preprocessing.

First, define the collate functions that will run in Ray Data and on the training worker:

.. testcode::
    :skipif: True

    from transformers import AutoTokenizer
    import torch
    from typing import Dict
    from ray.data.collate_fn import ArrowBatchCollateFn
    import pyarrow as pa
    from collate_utils import serialize_tensors_to_table, deserialize_table_to_tensors

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

Then, update your training function and dataset pipeline:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig
    from mock_dataset import create_mock_ray_text_dataset

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
    train_dataset = (
        create_mock_ray_text_dataset(
            dataset_size=1000000,
            min_len=1000,
            max_len=3000
        )
        .repartition(target_num_rows_per_block=1024)  # Align blocks with batch size
        .map_batches(
            TextTokenizerCollateFn(),
            batch_size=1024,
            batch_format="pyarrow",
            udf_modifying_row_count=True  # Each batch becomes 1 row
        )
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

1. **Preprocessing in Ray Data**: The tokenization logic moves from ``train_func`` to ``TextTokenizerCollateFn``, which runs in ``map_batches``.

2. **Batch alignment**: ``repartition(target_num_rows_per_block=1024)`` ensures data blocks align with the batch size of 1024.

3. **Serialization**: ``TextTokenizerCollateFn`` serializes the tokenized batch into a single-row PyArrow table using the tensor serialization utility.

4. **Deserialization**: ``IteratorCollateFn`` deserializes the single-row table back to tensors in the training loop.

5. **Batch size adjustment**: ``iter_torch_batches`` uses ``batch_size=1`` because each "row" contains a complete batch of 1024 samples.

Expected performance improvement
--------------------------------

The optimized implementation should show substantial speedup from higher collate function scalability, even without additional CPU machines. The speedup is more noticeable when:

- The collate function is computationally expensive (such as tokenization for long sequences)
- You have multiple training workers that would otherwise sequentially process their data
- Ray Data can parallelize preprocessing across multiple CPU cores or nodes

For the text tokenization example, you can expect:

- Better GPU utilization as training workers spend less time on data preprocessing
- Higher overall throughput as collation scales independently
- More efficient resource usage by dedicating CPU nodes to preprocessing

Complete runnable example
--------------------------

The following is a complete, self-contained example that you can copy and run. It includes the tensor serialization utility, mock dataset generation, and the optimized collate function implementation:

.. testcode::
    :skipif: True

    from dataclasses import dataclass
    from typing import Dict, List, Tuple, Union
    import torch
    from ray import cloudpickle as pickle
    import pyarrow as pa
    import random
    import string
    import ray
    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig
    from ray.data.collate_fn import ArrowBatchCollateFn
    from transformers import AutoTokenizer

    # ============================================================================
    # Tensor Serialization Utility
    # ============================================================================

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

    def serialize_tensors_to_table(batch: Dict[str, Union[List[torch.Tensor], torch.Tensor]]) -> pa.Table:
        """Serialize a batch of tensors to a PyArrow table."""
        return _TensorBatch.from_batch(batch).to_table()

    def deserialize_table_to_tensors(table: pa.Table, pin_memory: bool = False) -> Dict[str, List[torch.Tensor]]:
        """Deserialize a PyArrow table back to tensors."""
        return _TensorBatch.from_table(table).to_batch(pin_memory=pin_memory)

    # ============================================================================
    # Mock Dataset Generation
    # ============================================================================

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

    # ============================================================================
    # Collate Functions
    # ============================================================================

    class TextTokenizerCollateFn:
        """Collate function that runs in Ray Data preprocessing."""
        def __init__(self):
            self.tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

        def __call__(self, batch: pa.Table) -> pa.Table:
            outputs = self.tokenizer(
                batch["text"].to_pylist(),
                truncation=True,
                padding="longest",
                return_tensors="pt",
            )
            outputs["labels"] = torch.LongTensor(batch["label"].to_numpy())
            return serialize_tensors_to_table(outputs)

    class IteratorCollateFn(ArrowBatchCollateFn):
        """Collate function for iter_torch_batches that deserializes the batch."""
        def __init__(self, pin_memory=False):
            self._pin_memory = pin_memory

        def __call__(self, batch: pa.Table) -> Dict[str, torch.Tensor]:
            return deserialize_table_to_tensors(batch, pin_memory=self._pin_memory)

    # ============================================================================
    # Training Function and Trainer Setup
    # ============================================================================

    def train_func():
        collate_fn = IteratorCollateFn()
        for batch in ray.train.get_dataset_shard("train").iter_torch_batches(
            collate_fn=collate_fn, 
            batch_size=1
        ):
            # Your training logic here
            pass

    # Create and preprocess dataset
    train_dataset = (
        create_mock_ray_text_dataset(dataset_size=1000000, min_len=1000, max_len=3000)
        .repartition(target_num_rows_per_block=1024)
        .map_batches(
            TextTokenizerCollateFn(),
            batch_size=1024,
            batch_format="pyarrow",
            udf_modifying_row_count=True
        )
    )

    trainer = TorchTrainer(
        train_func,
        datasets={"train": train_dataset},
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
    )

    result = trainer.fit()

Next steps
----------

- Explore :ref:`Ray Data preprocessing operations <transforming_data>` for more complex data pipelines
- Learn about :ref:`Ray Data performance tuning <data_performance_tips>` to optimize your preprocessing
- Review :ref:`Ray Train scaling configurations <train-scaling-config>` to balance training and preprocessing resources

