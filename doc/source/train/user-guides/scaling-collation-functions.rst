.. _train-scaling-collation-functions:

Advanced: Scaling out expensive collate functions
=================================================

By default, the collate function executes on the training worker when you call :meth:`ray.data.DataIterator.iter_torch_batches`. This approach has two main drawbacks:

- **Low scalability**: The collate function runs sequentially on each training worker, limiting parallelism.
- **Resource competition**: The collate function consumes CPU and memory resources from the training worker, potentially slowing down model training.

Scaling out the collate function to Ray Data allows you to scale collation across multiple CPU nodes independently of training workers, improving better overall pipeline throughput, especially with heavy collate functions.

This optimization is particularly effective when the collate function is computationally expensive (such as tokenization, image augmentation, or complex feature engineering) and you have additional CPU resources available for data preprocessing.

Moving the collate function to Ray Data
---------------------------------------

The following example shows a typical collate function that runs on the training worker:

.. code-block:: python

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

If the collate function is time/compute intensive and you'd like to scale it out,you should:

* Create a custom collate function that runs in Ray Data and use :meth:`ray.data.Dataset.map_batches` to scale it out.
* Use :meth:`ray.data.Dataset.repartition` to ensure the batch size alignment.


Creating a custom collate function that runs in Ray Data
--------------------------------------------------------

To scale out, you'll want to move the ``collate_fn`` into a Ray Data ``map_batches`` operation:

.. code-block:: python

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

A couple of things to note:

- The ``collate_fn`` returns a dictionary of NumPy arrays, which is a standard Ray Data batch format.
- The ``iter_torch_batches`` method uses ``collate_fn=None``, which reduces the amount of work is done on the training worker process.

Ensuring batch size alignment
-----------------------------

Typically, collate functions are used to create complete batches of data with a target batch size.
However, if you move the collate function to Ray Data using :meth:`ray.data.Dataset.map_batches`, by default, it will not guarantee the batch size for each function call.

There are two common problems that you may encounter.

1. The collate function requires a certain number of rows provided as an input to work properly.
2. You want to avoid any reformatting / rebatching of the data on the training worker process.

To solve these problems, you can use :meth:`ray.data.Dataset.repartition` with ``target_num_rows_per_block`` to ensure the batch size alignment.

By calling ``repartition`` before ``map_batches``, you ensure that the input blocks contain the desired number of rows.

.. code-block:: python

    # Note: If you only use map_batches(batch_size=BATCH_SIZE), you are not guaranteed to get the desired number of rows as an input.
    dataset = dataset.repartition(target_num_rows_per_block=BATCH_SIZE).map_batches(collate_fn, batch_size=BATCH_SIZE)

By calling ``repartition`` after ``map_batches``, you ensure that the output blocks contain the desired number of rows. This avoids any reformatting / rebatching of the data on the training worker process.

.. code-block:: python

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

Putting things together
-----------------------

Throughout this guide, we use a mock text dataset to demonstrate the optimization. You can find the implementation of the mock dataset in :ref:`random-text-generator`.

.. tab-set::
    .. tab-item:: Baseline implementation

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

    .. tab-item:: Optimized implementation

        The following example moves the collate function to Ray Data preprocessing:

        .. testcode::
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

The optimized implementation makes these changes:

- **Preprocessing in Ray Data**: The tokenization logic moves from ``train_func`` to ``CollateFnRayData``, which runs in ``map_batches``.
- **NumPy output**: The collate function returns ``Dict[str, np.ndarray]`` instead of PyTorch tensors, which Ray Data natively supports.
- **Batch alignment**: ``repartition(target_num_rows_per_block=BATCH_SIZE)`` after ``map_batches`` ensures the collate function receives exact batch sizes and output blocks align with the batch size.
- **No collate_fn in iterator**: ``iter_torch_batches`` uses ``collate_fn=None`` because preprocessing already happened in Ray Data.

Benchmark results
~~~~~~~~~~~~~~~~~

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
------------------------------------

The optimized implementation above returns ``Dict[str, np.ndarray]``, which Ray Data natively supports. However, if your collate function needs to return PyTorch tensors or other custom data types that :meth:`ray.data.Dataset.map_batches` doesn't directly support, you need to serialize them.

.. _train-tensor-serialization-utility:

Tensor serialization utility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following utility serializes PyTorch tensors into PyArrow format. It flattens all tensors in a batch into a single binary buffer, stores metadata about tensor shapes and dtypes, and packs everything into a single-row PyArrow table. On the training side, it deserializes the table back into the original tensor structure.

The serialization and deserialization operations are typically lightweight compared to the actual collate function work (such as tokenization or image processing), so the overhead is minimal relative to the performance gains from scaling the collate function.

You can use :ref:`train-collate-utils` as a reference implementation and adapt it to your needs.

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
