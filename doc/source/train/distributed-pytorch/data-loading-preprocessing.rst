.. _data-ingest-torch:

Data Loading and Preprocessing
==============================

Ray Train supports data ingestion using Ray Data or the :ref:`framework's built-in dataset utilities <data-ingest-framework-utility>`.

This guide mainly introduces how to use `Ray Data <data>`` to load data for distributed training jobs. You may want to use 
Ray Data for training over framework built-in data loading utilities for a few reasons:

1. Leverage the full Ray cluster(both the GPU and CPU nodes) to speed up data preprocessing.
2. Pipeline data preprocessing and model training to improve GPU utilization.
3. Support TB-level huge datasets with features like stream processing and global shuffle.
4. Make data preprocessing agnostic of the training framework.


Overview
--------

:ref:`Ray Data <data>` is the recommended way to work with large datasets in Ray Train. Ray Data provides automatic loading, sharding, and streamed ingest of Data across multiple Train workers.
To get started, pass in one or more datasets under the ``datasets`` keyword argument for Trainer (e.g., ``Trainer(datasets={...})``).

In a nutshell, datasets passed to your :class:`Trainer <ray.train.trainer.BaseTrainer>`
can be accessed from the training function with :meth:`train.get_dataset_shard("train")
<ray.train.get_dataset_shard>` like this:

.. code-block:: python

    from ray import train

    # Datasets can be accessed in your train_func via ``get_dataset_shard``.
    def train_func(config):
        train_data_shard = train.get_dataset_shard("train")
        validation_data_shard = train.get_dataset_shard("validation")
        ...

    # Random split the dataset into 80% training data and 20% validation data.
    dataset = ray.data.read_csv("...")
    train_dataset, validation_dataset = dataset.train_test_split(
        test_size=0.2, shuffle=True,
    )

    trainer = TorchTrainer(
        train_func,
        datasets={"train": train_dataset, "validation": validation_dataset},
        scaling_config=ScalingConfig(num_workers=8),
    )
    trainer.fit()


Basics
------

.. _ingest_basics:

Let's use a single Torch training workload as a running example. A very basic example of using Ray Data with TorchTrainer looks like this:

.. tab-set::

    .. tab-item:: PyTorch

        .. literalinclude:: ../doc_code/data_ingest_torch_new.py
            :language: python
            :start-after: __basic__
            :end-before: __basic_end__

        In this basic example, the `train_ds` object is created in your Ray script before the Trainer is even instantiated. The `train_ds` object is passed to the Trainer via the `datasets` argument, and is accessible to the `train_loop_per_worker` function via the :meth:`train.get_dataset_shard <ray.train.get_dataset_shard>` method.

    .. tab-item:: PyTorch Lightning

        .. code-block:: python
            :emphasize-lines: 9,10,13,14,25,26

            from ray import train
         
            train_data = ray.data.read_csv("./train.csv")
            val_data = ray.data.read_csv("./validation.csv")

            def train_func_per_worker():
                # Access Ray datsets in your train_func via ``get_dataset_shard``.
                # The "train" dataset gets sharded across workers by default
                train_ds = train.get_dataset_shard("train")
                val_ds = train.get_dataset_shard("validation")

                # Create Ray dataset iterables via ``iter_torch_batches``.
                train_dataloader = train_ds.iter_torch_batches(batch_size=16)
                val_dataloader = val_ds.iter_torch_batches(batch_size=16)

                ...

                trainer = pl.Trainer(
                    # ...
                )

                # Feed the Ray dataset iterables to ``pl.Trainer.fit``.
                trainer.fit(
                    model, 
                    train_dataloaders=train_dataloader, 
                    val_dataloaders=val_dataloader
                )

            trainer = TorchTrainer(
                train_func,
                datasets={"train": train_data, "validation": val_data},
                scaling_config=ScalingConfig(num_workers=4),
            )
            trainer.fit()


Splitting data across workers
-----------------------------

By default, Train will split the ``"train"`` dataset across workers using :meth:`Dataset.streaming_split <ray.data.Dataset.streaming_split>`. This means that each worker sees a disjoint subset of the data, instead of iterating over the entire dataset. To customize this, we can pass in a :class:`DataConfig <ray.train.DataConfig>` to the Trainer constructor. For example, the following splits dataset ``"a"`` but not ``"b"``.

.. literalinclude:: ../doc_code/data_ingest_torch_new.py
    :language: python
    :start-after: __custom_split__
    :end-before: __custom_split_end__

Performance
-----------

This section covers common options for improving ingest performance.

Materializing your dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~

Datasets are lazy and their execution is streamed, which means that on each epoch, all preprocessing operations will be re-run. If this loading / preprocessing is expensive, you may benefit from :meth:`materializing <ray.data.Dataset.materialize>` your dataset in memory. This tells Ray Data to compute all the blocks of the dataset fully and pin them in Ray object store memory. This means that when iterating over the dataset repeatedly, the preprocessing operations do not need to be re-run, greatly improving performance. However, the trade-off is that if the preprocessed data is too large to fit into Ray object store memory, this could slow things down because data needs to be spilled to disk.

.. literalinclude:: ../doc_code/data_ingest_torch_new.py
    :language: python
    :start-after: __materialized__
    :end-before: __materialized_end__

Ray Data execution options
~~~~~~~~~~~~~~~~~~~~~~~~~~

Under the hood, Train configures some default Data options for ingest: limiting the data ingest memory usage to 2GB per worker, and telling it to optimize the locality of the output data for ingest. See :meth:`help(DataConfig.default_ingest_options()) <ray.train.DataConfig.default_ingest_options>` if you want to learn more and further customize these settings.

Common options you may want to adjust:

* ``resource_limits.object_store_memory``, which sets the amount of Ray object memory to use for Data ingestion. Increasing this can improve performance up to a point where it can trigger disk spilling and slow things down.
* ``preserve_order``. This is off by default, and lets Ray Data compute blocks out of order. Setting this to True will avoid this source of nondeterminism.

You can pass in custom execution options to the data config, which will apply to all data executions for the Trainer. For example, if you want to adjust the ingest memory size to 10GB per worker:

.. literalinclude:: ../doc_code/data_ingest_torch_new.py
    :language: python
    :start-after: __options__
    :end-before: __options_end__

Other performance tips
~~~~~~~~~~~~~~~~~~~~~~

* Adjust the ``prefetch_batches`` argument for :meth:`DataIterator.iter_batches <ray.data.DataIterator.iter_batches>`. This can be useful if bottlenecked on the network.
* Finally, you can use ``print(ds.stats())`` or ``print(iterator.stats())`` to print detailed timing information about Ray Data performance.


Custom data config (advanced)
-----------------------------

For use cases not covered by the default config class, you can also fully customize exactly how your input datasets are splitted. To do this, you need to define a custom ``DataConfig`` class (DeveloperAPI). The ``DataConfig`` class is responsible for that shared setup and splitting of data across nodes.

.. literalinclude:: ../doc_code/data_ingest_torch_new.py
    :language: python
    :start-after: __custom__
    :end-before: __custom_end__

What do you need to know about this ``DataConfig`` class?

* It must be serializable, since it will be copied from the driver script to the driving actor of the Trainer.
* Its ``configure`` method is called on the main actor of the Trainer group to create the data iterators for each worker.

In general, you can use ``DataConfig`` for any shared setup that has to occur ahead of time before the workers start reading data. The setup will be run at the start of each Trainer run.

Migrating from the legacy DatasetConfig API
-------------------------------------------

Starting from Ray 2.6, the ``DatasetConfig`` API is deprecated, and it will be removed in a future release. If your workloads are still using it, consider migrating to the new :class:`DataConfig <ray.train.DataConfig>` API as soon as possible.

The main difference is that preprocessing is no longer part of the Trainer because Dataset operations are now lazily applied. This means that you can apply any operation to your Datasets before passing them to the Trainer, and the operation will be re-executed before each epoch.

In the following example with the legacy ``DatasetConfig`` API, we pass two Datasets ("train" and "test") to the Trainer and apply an "add_noise" preprocessor per epoch to the "train" Dataset. Also, we will split the "train" Dataset, but not the "test" Dataset.

.. literalinclude:: ../doc_code/data_ingest_torch_migration.py
    :language: python
    :start-after: __legacy_api__
    :end-before: __legacy_api_end__

To migrate this example to the new :class:`DatasetConfig <ray.air.config.DatasetConfig>` API, we apply the "add_noise" preprocesor to the "train" Dataset prior to passing it to the Trainer. Then, we use ``DataConfig(datasets_to_split=["train"])`` to specify which Datasets need to be split. Note that the ``datasets_to_split`` argument is optional. By default, only the "train" Dataset will be split. If you don't want to split the "train" Dataset either, use ``datasets_to_split=[]``.

.. literalinclude:: ../doc_code/data_ingest_torch_migration.py
    :language: python
    :start-after: __new_api__
    :end-before: __new_api_end__

.. _data-ingest-framework-utility:

Using Framework Built-in Data Utilities
---------------------------------------

Some deep learning frameworks provide their own dataloading utilities. For example:

- PyTorch: `PyTorch Dataset <https://pytorch.org/tutorials/beginner/basics/data_tutorial.html>`
- Lightning: `LightningDataModule <https://lightning.ai/docs/pytorch/stable/data/datamodule.html>`
- HuggingFace: `HuggingFace Dataset <https://huggingface.co/docs/datasets/index>`

You can continue to use the above utilities in Ray Train, but be sure to put the dataset initialization logic in ``train_loop_per_worker``.

.. warning:: 

    We do not recommend passing datasets through ``train_loop_config`` or global variables, as it will serialize your 
    dataset objects on the head node and send it to remote workers via object store. This is inefficient for large dataset transfer 
    and may also cause serialization or ``FileNotFound`` errors.
