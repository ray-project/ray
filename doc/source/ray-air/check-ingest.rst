.. _air-ingest:

Configuring Training Datasets
=============================

This guide covers how to leverage :ref:`Ray Data <data>` to load data for distributed training jobs. You may want to use Ray Data for training over framework built-in data loading utilities for a few reasons:

1. To leverage the full Ray cluster to speed up preprocessing of your data.
2. To make data loading agnostic of the underlying framework.
3. Advanced Ray Data features such as global shuffles.

Basics
------

.. _ingest_basics:

Let's use a single Torch training workload as a running example. A very basic example of using Ray Data with TorchTrainer looks like this:

.. literalinclude:: doc_code/air_ingest_new.py
    :language: python
    :start-after: __basic__
    :end-before: __basic_end__

In this basic example, the `train_ds` object is created in your Ray script before the Trainer is even instantiated. The `train_ds` object is passed to the Trainer via the `datasets` argument, and is accessible to the `train_loop_per_worker` function via the :meth:`session.get_dataset_shard <ray.air.session.get_dataset_shard>` method.

Splitting data across workers
-----------------------------

By default, Train will split the ``"train"`` dataset across workers using :meth:`Dataset.streaming_split <ray.data.Dataset.streaming_split>`. This means that each worker sees a disjoint subset of the data, instead of iterating over the entire dataset. To customize this, we can pass in a :class:`DataConfig <ray.train.data_config.DataConfig>` to the Trainer constructor. For example, the following splits dataset ``"a"`` but not ``"b"``.

.. literalinclude:: doc_code/air_ingest_new.py
    :language: python
    :start-after: __custom_split__
    :end-before: __custom_split_end__

Performance
-----------

This section covers common options for improving ingest performance.

Materializing your dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~

Datasets are lazy and their execution is streamed, which means that on epoch, all preprocessing operations will be re-run. If this loading / preprocessing is expensive, you may benefit from :meth:`materializing <ray.data.Dataset.materialize>` your dataset in memory. This tells Ray Data to compute all the blocks of the dataset fully and pin them in Ray object store memory. This means that when iterating over the dataset repeatedly, the preprocessing operations do not need to be re-run, greatly improving performance. However, the trade-off is that if the preprocessed data is too large to fit into Ray object store memory, this could slow things down because data needs to be spilled to disk.

.. literalinclude:: doc_code/air_ingest_new.py
    :language: python
    :start-after: __materialized__
    :end-before: __materialized_end__

Ray Data execution options
~~~~~~~~~~~~~~~~~~~~~~~~~~

Under the hood, Train configures some default Data options for ingest: limiting the data ingest memory usage to 2GB per worker, and telling it to optimize the locality of the output data for ingest. See :meth:`help(DataConfig.default_ingest_options()) <ray.train.data_config.DataConfig.default_ingest_options>` if you want to learn more and further customize these settings. 

Common options you may want to adjust:

* ``resource_limits.object_store_memory``, which sets the amount of Ray object memory to use for Data ingestion. Increasing this can improve performance up to a point where it can trigger disk spilling and slow things down.
* ``preserve_order``. This is off by default, and lets Ray Data compute blocks out of order. Setting this to True will avoid this source of nondeterminism.

You can pass in custom execution options to the data config, which will apply to all data executions for the Trainer. For example, if you want to adjust the ingest memory size to 10GB per worker:

.. literalinclude:: doc_code/air_ingest_new.py
    :language: python
    :start-after: __options__
    :end-before: __options_end__

Other performance tips
~~~~~~~~~~~~~~~~~~~~~~

* Adjust the ``prefetch_batches`` argument for :meth:`DataIterator.iter_batches <ray.data.DataIterator.iter_batches>`. This can be useful if bottlenecked on the network.
* Finally, you can use ``print(ds.stats())`` or ``print(iterator.stats())`` to print detailed timing information about Ray Data performance.


Custom data config (advanced)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For use cases not covered by the default config class, you can also fully customize exactly how your input datasets are splitted. To do this, you need to define a custom ``DataConfig`` class (DeveloperAPI). The ``DataConfig`` class is responsible for that shared setup and splitting of data across nodes.

.. literalinclude:: doc_code/air_ingest_new.py
    :language: python
    :start-after: __custom__
    :end-before: __custom_end__

What do you need to know about this ``DataConfig`` class?

* It must be serializable, since it will be copied from the driver script to the driving actor of the Trainer.
* Its ``configure`` method is called on the main actor of the Trainer group to create the data iterators for each worker.

In general, you can use ``DataConfig`` for any shared setup that has to occur ahead of time before the workers start reading data. The setup will be run at the start of each Trainer run.
