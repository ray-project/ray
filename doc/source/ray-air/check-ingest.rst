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

In this basic example, the `train_ds` object is created in your Ray script before the Trainer is even instantiated. The `train_ds` object is passed to the Trainer via the `datasets` argument, and is accessible to the `train_loop_per_worker` function via the `session.get_dataset_shard` method.

Splitting data across workers
-----------------------------

By default, Train will split the ``"train"`` dataset across workers. This means that each worker sees a disjoint subset of the data, instead of iterating over the entire dataset. To customize this, we can pass in ``dataset_config=ray.train.DataConfig(datasets_to_split=list_of_names)`` to the Trainer constructor. For example, the following splits dataset ``"a"`` but not ``"b"``.

.. code:: python

    dataset_1 = ...
    dataset_2 = ...

    def train_loop_per_worker():
        ...  # As before.

    my_trainer = TorchTrainer(
        ...
        datasets={"a": dataset_1, "b": dataset_2},
        dataset_config=ray.train.DataConfig(
           datasets_to_split=["a"],
        ),
    )

Performance
-----------

This section covers common options for improving ingest performance.

Materializing your dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~

If your data preprocessing / loading is expensive to run, you may benefit from materializing your dataset in memory. This tells Ray Data to compute all the blocks of the dataset fully and pin the in Ray object store memory. This means that when iterating over the dataset repeatedly, the preprocessing operations do not need to be re-run, greatly improving performance. However, the trade-off is that if the preprocessed data is large this could slow things down if data needs to be spilled to disk.

.. code:: python

    train_ds = ...   # As before.

    # Run your preprocessing first.
    ...

    # Compute and materialize the dataset in Ray object store memory.
    train_ds = train_ds.materialize()

    def train_loop_per_worker():
        # Nothing here changes, but it will read the materialized blocks
        # instead of recomputing them each time.
        ...

    ...

Ray Data execution options
~~~~~~~~~~~~~~~~~~~~~~~~~~

Under the hood, Train configures some default Data options for ingest: limiting the data ingest memory usage to 2GB per worker, and telling it to optimize the locality of the output data for ingest. See ``help(DataConfig.default_ingest_options())`` if you want to learn more and further customize these settings. 

Common options you may want to adjust:

* ``resource_limits.object_store_memory``, which sets the amount of Ray object memory to use for Data ingestion. Increasing this can improve performance up to a point where it can trigger disk spilling and slow things down.
* ``preserve_order``. This is off by default, and lets Ray Data compute blocks out of order. Setting this to True will avoid this source of nondeterminism.

You can pass in custom execution options to the data config, which will apply to all data executions for the Trainer. For example, if you want to adjust the ingest memory size to 10GB per worker:

.. code:: python


    from ray.train import DataConfig

    train_ds = ...   # As before.

    def train_loop_per_worker():
        # As before.
        ...

    options = DataConfig.default_ingest_options()
    options.resource_limits.object_store_memory = 10e9

    my_trainer = TorchTrainer(
        dataset_config=ray.train.DataConfig(
           execution_options=options,
        ),
        ...   # Nothing here changes.
    )

Other performance tips
~~~~~~~~~~~~~~~~~~~~~~

* If your dataset is small, or you want to cache preprocessing logic, you can call ``ds.materialize()`` in either your driver script or your setup function. This will tell Ray Data to compute the dataset fully and materialize all its blocks in the Ray object store. This means that workers will be reading data from memory instead of recomputing it each epoch.
* Adjust the ``prefetch_batches`` argument for iter batches. This can be useful if bottlenecked on the network.
* Finally, you can use ``print(ds.stats())`` or ``print(iterator.stats())`` to print detailed timing information about Ray Data performance.


Custom data config (advanced)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For use cases not covered by the default config class, you can also fully customize exactly how your input datasets are splitted. To do this, you need to define a custom ``DataConfig`` class (DeveloperAPI). The ``DataConfig`` class is responsible for that shared setup and splitting of data across nodes.

.. code:: python

    train_ds = ...   # As before.

    # Note this example class is doing the same thing as
    # the basic DataConfig impl included with Train.
    class MyCustomDataConfig(train.DataConfig):
       def get_shards(
            self,
            datasets: Dict[str, Dataset],
            world_size: int,
            worker_node_ids: List[NodeIdStr],
            **kwargs):

          # Configure Ray Data for ingest 
          ctx = ray.data.DataContext.get_current()
          ctx.execution_options = ctx.ingest_default_options()

          # Split the stream into shards.
          iterator_shards = datasets["train"].streaming_split(
               world_size,
               equal=True,
               locality_hints=worker_node_ids)

          # Create shards for each worker and return.
          outputs = {}
          for rank in range(world_size):
              outputs[rank] = {"train": iterator_shards[rank]}
          self.output_splits = outputs

       def get_shard(self, rank: int, name: str) -> DataIterator:
          assert name == "train", "not implemented"
          return self.output_splits[rank][name]

    def train_loop_per_worker():
        # Nothing here changes.
        ...

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": train_ds},
        dataset_config=MyCustomDataConfig(),
    )
    my_trainer.fit()


What do you need to know about this ``DataConfig`` class?

* It must be serializable, since it will be copied from the driver script to the driving actor of the Trainer.
* Its ``configure`` method is called on the main actor of the Trainer group to setup data ingest and iterators.
* Train will get data shards for each worker by calling ``get_shard`` on the instance after setup is called.

In general, you can use ``DataConfig`` for any shared setup that has to occur ahead of time before the workers start reading data. The setup will be run at the start of each Trainer run.
