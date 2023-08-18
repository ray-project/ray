.. _data-ingest-torch:

Data Loading and Preprocessing
==============================

:ref:`Ray Train <train-docs>` integrates with :ref:`Ray Data <data>` to efficiently load and preprocess data for distributed training in a streaming fashion, which scales to very large datasets and avoids GPU starvation.

This guide covers how to leverage :ref:`Ray Data <data>` to load data for distributed training jobs. For advantages on using Ray Data to ingest data for training, and comparisons to alternatives, see :ref:`Ray Data Overview <data_overview>`.

.. figure:: ../images/train_ingest.png

Quickstart
----------
Install Ray Data and Ray Train:

.. code-block:: bash

    pip install -U "ray[data,train]"

Using Ray Data and Ray Train for distributed training on large datasets involves four basic steps:

- **Step 1:** Load your data into a Ray Dataset. Ray Data supports many different data sources and formats. For more details, see :ref:`Loading Data <loading_data>`.
- **Step 2:** If required, create a function that defines your training logic and periodically checkpoints your model. For more information, see :ref:`Ray Train user guides <train-user-guides>`.
- **Step 3:** Inside your training function, access the dataset shard for the training worker via :meth:`train.get_dataset_shard() <ray.train.get_dataset_shard>`. Iterate over the dataset shard to train your model. For more details on how to iterate over your data, see :ref:`Iterating over data <iterating-over-data>`.
- **Step 4:** Create your :class:`TorchTrainer <ray.train.torch.TorchTrainer>` and pass in your Ray Dataset. This automatically shards the datasets and passes them to each training worker. For more information on configuring training, see the :ref:`Ray Train user guides <train-user-guides>`.

.. tabs::

    .. group-tab:: PyTorch

        .. testcode::

            import torch
            from torch import nn
            import ray
            from ray import train
            from ray.train import Checkpoint, ScalingConfig
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
                train_data_shard = train.get_dataset_shard("train")

                for epoch_idx in range(2):
                    # In each epoch, iterate over batches of the dataset shard in torch
                    # format to train the model.
                    for batch in train_data_shard.iter_torch_batches(batch_size=128, dtypes=torch.float32):
                        inputs, labels = torch.unsqueeze(batch["x"], 1), torch.unsqueeze(batch["y"], 1)
                        predictions = model(inputs)
                        train_loss = loss_fn(predictions, labels)
                        train_loss.backward()
                        optimizer.step()

                    # Checkpoint the model on each epoch.
                    train.report(
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

    .. group-tab:: PyTorch Lightning

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
    
    .. group-tab:: HuggingFace Transformers

        .. code-block:: python
            :emphasize-lines: 12,13,16,17,24,25

            import ray
            import ray.train
         
            ...

            train_data = ray.data.from_huggingface(hf_train_ds)
            eval_data = ray.data.from_huggingface(hf_eval_ds)

            def train_func(config):
                # Access Ray datsets in your train_func via ``get_dataset_shard``.
                # The "train" dataset gets sharded across workers by default
                train_ds = ray.train.get_dataset_shard("train")
                eval_ds = ray.train.get_dataset_shard("evaluation")

                # Create Ray dataset iterables via ``iter_torch_batches``.
                train_iterable_ds = train_ds.iter_torch_batches(batch_size=16)
                eval_iterable_ds = eval_ds.iter_torch_batches(batch_size=16)

                ...

                args = transformers.TrainingArguments(
                    ...,
                    max_steps=max_steps # Required for iterable datasets
                )

                trainer = transformers.Trainer(
                    ...,
                    model=model,
                    train_dataset=train_iterable_ds,
                    eval_dataset=eval_iterable_ds,
                )

                # Prepare your Transformers Trainer
                trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)
                trainer.train()

            trainer = TorchTrainer(
                train_func,
                datasets={"train": train_data, "evaluation": val_data},
                scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
            )
            trainer.fit()


Migrating from PyTorch DataLoader
---------------------------------

Some deep learning frameworks provide their own dataloading utilities. For example:

- PyTorch: `PyTorch Dataset & DataLoader <https://pytorch.org/tutorials/beginner/basics/data_tutorial.html>`
- HuggingFace: `HuggingFace Dataset <https://huggingface.co/docs/datasets/index>`
- PyTorch Lightning: `LightningDataModule <https://lightning.ai/docs/pytorch/stable/data/datamodule.html>`

You can still use the aforementioned utilities in Ray Train. However, for more performant large-scale data ingestion, you should consider migrating to Ray Data.

PyTorch Datasets are replaced by the :class:`Dataset <ray.data.Dataset>` abtraction, and the PyTorch DataLoader is replaced by :meth:`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`.

For more details, see the :ref:`Ray Data PyTorch guide <migrate_pytorch>` and :ref:`Ray Data for HuggingFace and TensorFlow <loading_datasets_from_ml_libraries>`.

.. _train_datasets_configuration:

Customizing how to split datasets
---------------------------------
By default, Ray Train splits the ``"train"`` dataset across workers using :meth:`Dataset.streaming_split <ray.data.Dataset.streaming_split>`. Each worker sees a disjoint subset of the data, instead of iterating over the entire dataset. Unless randomly shuffled, the same splits are used for each iteration of the dataset. 

For all other datasets, Ray Train passes the entire dataset to each worker.

To customize this, pass in a :class:`DataConfig <ray.train.DataConfig>` to the Trainer constructor. For example, to split both the training and validation datasets, do the following:

.. testcode::

    import ray
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )
    train_ds, val_ds = ds.train_test_split(0.3)

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = train.get_dataset_shard("train")
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
    from ray import train
    from ray.train.torch import TorchTrainer
    from ray.train import DataConfig, ScalingConfig
    from ray.data import Dataset, DataIterator, NodeIdStr
    from ray.actor import ActorHandle

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = train.get_dataset_shard("train")
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

If your preprocessed Dataset is small enough to fit in Ray object store memory (by default this is 30% of total cluster RAM), *materialize* the preprocessed dataset in Ray's built-in object store, by calling :meth:`materialize() <ray.data.Dataset.materialize>` on the preprocessed dataset. This method tells Ray Data to compute the entire preprocessed and pin it in the Ray object store memory. As a result, when iterating over the dataset repeatedly, the preprocessing operations do not need to be re-run. However, if the preprocessed data is too large to fit into Ray object store memory, this approach will greatly decreases performance as data needs to be spilled to and read back from disk.

Transformations that you want run per-epoch, such as randomization, should go after the materialize call.

.. testcode::

    from typing import Dict
    import numpy as np
    import ray

    # Load the data.
    train_ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

    # Define a preprocessing function.
    def normalize_length(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        new_col = batch["sepal.length"] / np.max(batch["sepal.length"])
        batch["normalized.sepal.length"] = new_col
        del batch["sepal.length"]
        return batch

    # Preprocess the data. Transformations that are made before the materialize call
    # below are only run once.
    train_ds = train_ds.map_batches(normalize_length)

    # Materialize the dataset in object store memory.
    # Only do this if train_ds is small enough to fit in object store memory.
    train_ds = train_ds.materialize()

    # Dummy augmentation transform.
    def augment_data(batch):
        return batch

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
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = train.get_dataset_shard("train")
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
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    # Randomize the blocks of this dataset.
    ds = ds.randomize_block_order()

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = train.get_dataset_shard("train")
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

Preprocessing Structured Data
-----------------------------

.. note::
    This section is for tabular/structured data. The recommended way for preprocessing unstructured data is to use
    Ray Data operations such as `map_batches`. See the :ref:`Ray Data Working with Pytorch guide <working_with_pytorch>` for more details.

For tabular data, we recommend using Ray Data :ref:`preprocessors <air-preprocessors>`, which implement common data preprocessing operations.
You can use this with Ray Train Trainers by applying them on the dataset before passing the dataset into a Trainer. For example:

.. testcode::

    import numpy as np

    import ray
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.data.preprocessors import Concatenator, Chain, StandardScaler

    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

    # Create a preprocessor to scale some columns and concatenate the result.
    preprocessor = Chain(
        StandardScaler(columns=["mean radius", "mean texture"]),
        Concatenator(exclude=["target"], dtype=np.float32),
    )
    dataset = preprocessor.fit_transform(dataset)  # this will be applied lazily

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = train.get_dataset_shard("train")
        for _ in range(2):
            # Prefetch 10 batches at a time.
            for batch in it.iter_batches(batch_size=128, prefetch_batches=10):
                print("Do some training on batch", batch)

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": dataset},
    )
    my_trainer.fit()



.. testoutput::
    :hide:

    ...


Reproducibility
---------------
When developing or hyperparameter tuning models, reproducibility is important during data ingest so that data ingest does not affect model quality. Follow these three steps to enable reproducibility:

**Step 1:** Enable deterministic execution in Ray Datasets by setting the `preserve_order` flag in the :class:`DataContext <ray.data.context.DataContext>`.

.. testcode::

    import ray

    # Preserve ordering in Ray Datasets for reproducibility.
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

**Step 2:** Set a seed for any shuffling operations: 

* `seed` argument to :meth:`random_shuffle <ray.data.Dataset.random_shuffle>`
* `seed` argument to :meth:`randomize_block_order <ray.data.Dataset.randomize_block_order>` 
* `local_shuffle_seed` argument to :meth:`iter_batches <ray.data.DataIterator.iter_batches>`

**Step 3:** Follow the best practices for enabling reproducibility for your training framework of choice. For example, see the `Pytorch reproducibility guide <https://pytorch.org/docs/stable/notes/randomness.html>`_.
