.. _training_ingest_home:

End-to-end: Data Loading for ML Training
========================================

Data loading and preprocessing is an important step for ML model training. The process involves loading raw data from storage, processing it to the appropriate format, sharding it among training workers, and moving each shard to GPU devices for training.   

:ref:`Ray Data <data>` integrates with :ref:`Ray Train <train-docs>` to efficiently load and preprocess data for distributed training in a streaming fashion, which scales to very large datasets and avoids GPU starvation.

For more advantages with using Ray Data to ingest data for training, and comparisons to alternatives, see :ref:`Ray Data Overview <data_overview>`.

.. figure:: images/train_ingest.png

.. _ingest_quickstart:

Quickstart
----------
Install Ray Data and Ray Train:

.. code-block:: bash

    pip install -U "ray[data,train]"

Using Ray Data and Ray Train for distributed training on large datasets involves four basic steps:

- **Step 1:** Load your data into a Ray Dataset. Ray Data supports many different data sources and formats. For more details, see :ref:`Loading Data <loading_data>`.
- **Step 2:** If required, create a function that defines your training logic and periodically checkpoints your model. For more information, see :ref:`Ray Train user guides <train-userguides>`.
- **Step 3:** Inside your training function, access the dataset shard for the training worker via :meth:`session.get_dataset_shard() <ray.air.session.get_dataset_shard>`. Iterate over the dataset shard to train your model. For more details on how to iterate over your data, see :ref:`Iterating over data <iterating-over-data>`.
- **Step 4:** Create your :ref:`Ray Train Trainer <train-framework-catalog>` and pass in your Ray Dataset. This automatically shards the datasets and passes them to each training worker. For more information on configuring training, see the :ref:`Ray Train user guides <train-userguides>`.

.. tabs::

    .. group-tab:: PyTorch

        .. testcode::

            import torch
            from torch import nn
            import ray
            from ray.air import session, Checkpoint, ScalingConfig
            from ray.train.torch import TorchTrainer

            # Set this to True to use GPU.
            # If False, do CPU training instead of GPU training.
            use_gpu = False

            # Step 1: Create a Ray Dataset from in-memory Python lists.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            train_dataset = ray.data.from_items([{"x": x, "y": 2 * x + 1} for x in range(200)])

            # Step 2: Define your training function. This function contains the logic
            # for creating the model and the training loop to train the model.
            # See the Ray Train user guides for information such as how to report
            # metrics or periodically save model checkpoints.
            def train_func(config):
                model = nn.Sequential(nn.Linear(1, 1), nn.Sigmoid())
                loss_fn = torch.nn.BCELoss()
                optimizer = torch.optim.SGD(model.parameters(), lr=0.001)

                # Step 3: Access the dataset shard for the training worker via
                # ``get_dataset_shard``.
                train_data_shard = session.get_dataset_shard("train")

                for epoch_idx in range(2):
                    # In each epoch, iterate over batches of the dataset shard in torch
                    # format to train the model.
                    for batch in train_data_shard.iter_torch_batches(batch_size=128, dtypes=torch.float32):
                        inputs, labels = torch.unsqueeze(batches["x"], 1), batches["y"]
                        predictions = model(inputs)
                        train_loss = loss_fn(predictions, labels)
                        train_loss.backward()
                        optimizer.step()

                    # Checkpoint the model on each epoch.
                    session.report(
                        {},
                        checkpoint=Checkpoint.from_dict({"model": model.state_dict()})
                    )

            # Step 4: Create a TorchTrainer. Specify the number of training workers and
            # pass in your Ray Dataset.
            # The Ray Dataset is automatically split across all training workers.
            trainer = TorchTrainer(
                train_func,
                datasets={"train": train_dataset},
                scaling_config=ScalingConfig(num_workers=2, use_gpu=use_gpu)
            )
            result = trainer.fit()

            # Extract the model from the checkpoint.
            result.checkpoint.to_dict()["model"]

        .. testoutput::
            :hide:

            ...

    .. group-tab:: TensorFlow

        .. testcode::

            import ray
            import tensorflow as tf

            from ray.air import session, Checkpoint, ScalingConfig
            from ray.train.tensorflow import TensorflowTrainer


            # Set this to True to use GPU.
            # If False, do CPU training instead of GPU training.
            use_gpu = False

            # Step 1: Create a Ray Dataset from in-memory Python lists.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            train_dataset = ray.data.from_items(
                [{"x": x / 200, "y": 2 * x / 200} for x in range(200)]
            )

            # Step 2: Define your training function. 
            # This function contains the logic for
            # creating the model and the training loop to train the model.
            # See the Ray Train user guides for information such as how to report
            # metrics or periodically save model checkpoints.
            def train_func(config):
                strategy = tf.distribute.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    # Model building/compiling needs to be within `strategy.scope()`.
                    model = tf.keras.Sequential([
                        tf.keras.layers.InputLayer(),
                        tf.keras.layers.Flatten(),
                        tf.keras.layers.Dense(1)
                    ])
                    model.compile(
                        optimizer=tf.keras.optimizers.SGD(learning_rate=1e-3),
                        loss=tf.keras.losses.mean_squared_error,
                        metrics=[tf.keras.metrics.mean_squared_error],
                    )

                # Step 3: Access the dataset shard for the training worker via
                # ``get_dataset_shard``.
                dataset = session.get_dataset_shard("train")

                results = []
                for _ in range(3):
                    # In each epoch, iterate over batches of the dataset shard in
                    # TensorFlow format to train the model.
                    tf_dataset = dataset.to_tf(
                        feature_columns="x", label_columns="y", batch_size=32
                    )
                    model.fit(tf_dataset)
                    # Checkpoint the model on each epoch.
                    session.report(
                        {},
                        checkpoint=Checkpoint.from_dict({"model": model.get_weights()})
                    )

            # Step 4: Create a TensorflowTrainer. 
            # Specify the number of training workers and pass in your Ray Dataset.
            # The Ray Dataset is automatically split across all training workers.
            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=use_gpu),
                datasets={"train": train_dataset},
            )
            result = trainer.fit()

            # Extract the model from the checkpoint.
            result.checkpoint.to_dict()["model"]


        .. testoutput::
            :hide:

            ...

    .. group-tab:: XGBoost

        Run `pip install -U xgboost-ray`

        .. testcode::

            import ray
            from ray.train.xgboost import XGBoostTrainer, XGBoostCheckpoint
            from ray.air.config import ScalingConfig

            # Step 1: Create a Ray Dataset from a CSV file.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

            # Split data into train and validation.
            train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

            # XGBoost does not require defining your own training logic, so skip steps
            # 2 and 3

            # Step 4: Create a XGBoostTrainer. 
            # Specify the number of training workers and pass in your Ray Dataset.
            # The Ray Dataset is automatically split across all training workers.
            trainer = XGBoostTrainer(
                scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
                label_column="target",
                num_boost_round=20,
                params={
                    # XGBoost specific params
                    "objective": "binary:logistic",
                    "eval_metric": ["logloss", "error"],
                },
                datasets={"train": train_dataset, "valid": valid_dataset},
            )
            result = trainer.fit()

            # Extract the model from the checkpoint.
            XGBoostCheckpoint.from_checkpoint(result.checkpoint).get_model()

        .. testoutput::
            :hide:

            ...


Related resources
-----------------

* For more in-depth examples for your use case, see :ref:`our distributed training examples<data_train_examples>`.
* For how to configure Ray Datasets for training, see :ref:`the configuration guide <train_datasets_configuration>`.
* For more information on how to configure Ray Train in general, see :ref:`the Ray Train user guides <train-userguides>`

.. _data_train_examples:

More examples
-------------
- :doc:`Fine-tuning PyTorch FasterRCNN_Resnet50 object detection model </ray-air/examples/torch_detection>`
- :doc:`Distributed training with XGBoost </ray-air/examples/xgboost_example>`

.. _train_datasets_configuration:

Customizing how to split datasets
---------------------------------
By default, Ray Train splits the ``"train"`` dataset across workers using :meth:`Dataset.streaming_split <ray.data.Dataset.streaming_split>`. Each worker sees a disjoint subset of the data, instead of iterating over the entire dataset. Unless randomly shuffled, the same splits are used for each iteration of the dataset. 

For all other datasets, Ray Train passes the entire dataset to each worker.

To customize this, pass in a :class:`DataConfig <ray.train.DataConfig>` to the Trainer constructor. For example, to split both the training and validation datasets, do the following:

.. testcode::

    import ray
    from ray.air import ScalingConfig, session
    from ray.train.torch import TorchTrainer

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )
    train_ds, val_ds = dataset.train_test_split(0.3)

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")
        for _ in range(2):
            for batch in it.iter_batches(batch_size=128):
                print("Do some training on batch", batch)

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": train_ds, "val": val_ds},
        dataset_config=ray.train.DataConfig(
            datasets_to_split=["train", "val"],
        ),
    )
    my_trainer.fit()

.. testoutput::
    :hide:

    ...

Full customization (advanced)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For use cases not covered by the default config class, you can also fully customize exactly how your input datasets are split. Define a custom :class:`DataConfig <ray.train.DataConfig>` class (DeveloperAPI). The :class:`DataConfig <ray.train.DataConfig>` class is responsible for that shared setup and splitting of data across nodes.

.. testcode::

    # Note that this example class is doing the same thing as the basic DataConfig
    # implementation included with Ray Train.
    from typing import Optional, Dict, List

    import ray
    from ray.air import ScalingConfig, session
    from ray.train.torch import TorchTrainer
    from ray.data import Dataset, DataIterator, NodeIdStr
    from ray.actor import ActorHandle

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")
        for _ in range(2):
            for batch in it.iter_batches(batch_size=128):
                print("Do some training on batch", batch)


    class MyCustomDataConfig(DataConfig):
        def configure(
            self,
            datasets: Dict[str, Dataset],
            world_size: int,
            worker_handles: Optional[List[ActorHandle]],
            worker_node_ids: Optional[List[NodeIdStr]],
            **kwargs,
        ) -> List[Dict[str, DataIterator]]:
            assert len(datasets) == 1, "This example only handles the simple case"

            # Configure Ray Data for ingest.
            ctx = ray.data.DataContext.get_current()
            ctx.execution_options = DataConfig.default_ingest_options()

            # Split the stream into shards.
            iterator_shards = datasets["train"].streaming_split(
                world_size, equal=True, locality_hints=worker_node_ids
            )

            # Return the assigned iterators for each worker.
            return [{"train": it} for it in iterator_shards]


    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": ds},
        dataset_config=MyCustomDataConfig(),
    )
    my_trainer.fit()

.. testoutput::
    :hide:

    ... 

The subclass must be serializable, since Ray Train copies it from the driver script to the driving actor of the Trainer. Ray Train calls its :meth:`configure <ray.train.DataConfig.configure>` method on the main actor of the Trainer group to create the data iterators for each worker.

In general, you can use :class:`DataConfig <ray.train.DataConfig>` for any shared setup that has to occur ahead of time before the workers start iterating over data. The setup runs at the start of each Trainer run.

Performance tips
----------------

.. _dataset_cache_performance:

Caching the preprocessed Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you're training on GPUs and have an expensive CPU preprocessing operation, this approach may bottleneck training throughput.

If your preprocessed Dataset is small enough to fit in object store memory, *materialize* the preprocessed dataset in Ray's built-in object store, by calling :meth:`materialize() <ray.data.Dataset.materialize>` on the preprocessed dataset. This method tells Ray Data to compute the entire preprocessed and pin it in the Ray object store memory. As a result, when iterating over the dataset repeatedly, the preprocessing operations do not need to be re-run. However, if the preprocessed data is too large to fit into Ray object store memory, this approach will greatly decreases performance as data needs to be spilled to and read back from disk.

Transformations that you want run per-epoch, such as randomization, should go after the materialize call.

.. testcode::

    # Load the data.
    train_ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

    # Preprocess the data. Transformations that are made before the materialize call
    # below are only run once.
    train_ds = train_ds.map_batches(normalize_length)

    # Materialize the dataset in object store memory.
    # Only do this if train_ds is small enough to fit in object store memory.
    train_ds = train_ds.materialize()

    # Add per-epoch preprocessing. Transformations that you want to run per-epoch, such
    # as data augmentation or randomization, should go after the materialize call.
    train_ds = train_ds.map_batches(augment_data)

    # Pass train_ds to the Trainer

Adding CPU-only nodes to your cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you are bottlenecked on expensive CPU preprocessing and the preprocessed Dataset is too large to fit in object store memory, then the above tip doesn't work. In this case, since Ray supports heterogeneous clusters, you can add more CPU-only nodes to your cluster.

For cases where you're bottlenecked by object store memory, adding more CPU-only nodes to your cluster increases total cluster object store memory, allowing more data to be buffered in between preprocessing and training stages.

For cases where you're bottlenecked by preprocessing compute time, adding more CPU-only nodes adds more CPU cores to your cluster, further parallelizing preprocessing. If your preprocessing is still not fast enough to saturate GPUs, then add enough CPU-only nodes to :ref:`cache the preprocessed dataset <dataset_cache_performance>`.

Prefetching batches
~~~~~~~~~~~~~~~~~~~
While iterating over your dataset for training, you can increase ``prefetch_batches`` in :meth:`iter_batches <ray.data.DataIterator.iter_batches>` or :meth:`iter_torch_batches <ray.data.DataIterator.iter_torch_batches>` to further increase performance. While training on the current batch, this launches N background threads to fetch and process the next N batches.

This approach can help if training is bottlenecked on cross-node data transfer or on last-mile preprocessing such as converting batches to tensors or executing ``collate_fn``. However, increasing ``prefetch_batches`` leads to more data that needs to be held in heap memory. By default, ``prefetch_batches`` is set to 1.

For example, the following code prefetches 10 batches at a time for each training worker:

.. testcode::

    import ray
    from ray.air import ScalingConfig, session
    from ray.train.torch import TorchTrainer

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")
        for _ in range(2):
            # Prefetch 10 batches at a time.
            for batch in it.iter_batches(batch_size=128, prefetch_batches=10):
                print("Do some training on batch", batch)

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": ds},
    )
    my_trainer.fit()

.. testoutput::
    :hide:

    ...

Random shuffling
----------------
Randomly shuffling data for each epoch can be important for model quality depending on what model you are training.

Ray Data has two approaches to random shuffling:

1. Shuffling data blocks and local shuffling on each training worker. This requires less communication at the cost of less randomness (i.e. rows that appear in the same data block are more likely to appear near each other in the iteration order).
2. Full global shuffle, which is more expensive. This will fully decorrelate row iteration order from the original dataset order, at the cost of significantly more computation, I/O, and communication.

For most cases, option 1 suffices. 

First, randomize each :ref:`block <dataset_concept>` of your dataset via :meth:`randomize_block_order <ray.data.Dataset.randomize_block_order>`. Then, when iterating over your dataset during training, enable local shuffling by specifying a ``local_shuffle_buffer_size`` to :meth:`iter_batches <ray.data.DataIterator.iter_batches>` or :meth:`iter_torch_batches <ray.data.DataIterator.iter_torch_batches>`.

.. testcode::
    import ray

     ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    # Randomize the blocks of this dataset.
    ds = ds.randomize_block_order()

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")
        for _ in range(2):
            # Use a shuffle buffer size of 10k rows.
            for batch in it.iter_batches(
                local_shuffle_buffer_size=10000, batch_size=128):
                print("Do some training on batch", batch)

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": ds},
    )
    my_trainer.fit()

.. testoutput::
    :hide:

    ...


If your model is sensitive to shuffle quality, call :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` to perform a global shuffle.

.. testcode::

    import ray

     ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    # Do a global shuffle of all rows in this dataset.
    # The dataset will be shuffled on each iteration, unless `.materialize()`
    # is called after the `.random_shuffle()`
    ds = ds.random_shuffle()

For more information on how to optimize shuffling, and which approach to choose, see the :ref:`Optimize shuffling guide <optimizing_shuffles>`.

Reproducibility
---------------
When developing or hyperparameter tuning models, reproducibility is important during data ingest so that data ingest does not affect model quality. Follow these three steps to enable reproducibility:

**Step 1:** Enable deterministic execution in Ray Datasets by setting the `preserve_order` flag in the :class:`DataContext <ray.data.context.DataContext>`.

.. testcode::

    import ray
    from ray.air import ScalingConfig, session
    from ray.train.torch import TorchTrainer

    # Preserve ordering in Ray Datasets for reproducibility.
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )
    train_ds, val_ds = dataset.train_test_split(0.3)

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")
        for _ in range(2):
            for batch in it.iter_batches(batch_size=128):
                print("Do some training on batch", batch)

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": train_ds, "val": val_ds},
    )
    my_trainer.fit()

**Step 2:** Set a seed for any shuffling operations: 

* `seed` argument to :meth:`random_shuffle <ray.data.Dataset.random_shuffle>`
* `seed` argument to :meth:`randomize_block_order <ray.data.Dataset.randomize_block_order>` 
* `local_shuffle_seed` argument to :meth:`iter_batches <ray.data.DataIterator.iter_batches>`

**Step 3:** Follow the best practices for enabling reproducibility for your training framework of choice. For example, see the `Pytorch reproducibility guide <https://pytorch.org/docs/stable/notes/randomness.html>`_.
