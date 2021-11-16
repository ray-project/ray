.. _dataset-pipeline:

Dataset Pipelines
=================

Overview
--------

Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <package-ref.html#datasetpipeline-api>`__.

A DatasetPipeline is an unified iterator over a (potentially infinite) sequence of Ray Datasets, each of which represents a *window* over the original data. Conceptually it is similar to a `Spark DStream <https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams>`__, but manages execution over a bounded amount of source data instead of an unbounded stream. Ray computes each dataset window on-demand and stitches their output together into a single logical data iterator. DatasetPipeline implements most of the same transformation and output methods as Datasets (e.g., map, filter, split, iter_rows, to_torch, etc.).

Creating a DatasetPipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~

A DatasetPipeline can be constructed in two ways: either by pipelining the execution of an existing Dataset (via ``Dataset.window``), or generating repeats of an existing Dataset (via ``Dataset.repeat``). Similar to Datasets, you can freely pass DatasetPipelines between Ray tasks, actors, and libraries. Get started with this synthetic data example:

.. code-block:: python

    import ray

    def func1(i: int) -> int:
        return i + 1

    def func2(i: int) -> int:
        return i * 2

    def func3(i: int) -> int:
        return i % 3

    # Create a dataset and then create a pipeline from it.
    base = ray.data.range(1000000)
    print(base)
    # -> Dataset(num_blocks=200, num_rows=1000000, schema=<class 'int'>)
    pipe = base.window(blocks_per_window=10)
    print(pipe)
    # -> DatasetPipeline(num_windows=20, num_stages=1)

    # Applying transforms to pipelines adds more pipeline stages.
    pipe = pipe.map(func1)
    pipe = pipe.map(func2)
    pipe = pipe.map(func3)
    print(pipe)
    # -> DatasetPipeline(num_windows=20, num_stages=4)

    # Output can be pulled from the pipeline concurrently with its execution.
    num_rows = 0
    for row in pipe.iter_rows():
        num_rows += 1
    # ->
    # Stage 0:  55%|█████████████████████████                |11/20 [00:02<00:00,  9.86it/s]
    # Stage 1:  50%|██████████████████████                   |10/20 [00:02<00:01,  9.45it/s]
    # Stage 2:  45%|███████████████████                      | 9/20 [00:02<00:01,  8.27it/s]
    # Stage 3:  35%|████████████████                         | 8/20 [00:02<00:02,  5.33it/s]
    print("Total num rows", num_rows)
    # -> Total num rows 1000000

You can also create a DatasetPipeline from a custom iterator over dataset creators using ``DatasetPipeline.from_iterable``. For example, this is how you would implement ``Dataset.repeat`` and ``Dataset.window`` using ``from_iterable``:

.. code-block:: python

    import ray
    from ray.data.dataset_pipeline import DatasetPipeline

    # Equivalent to ray.data.range(1000).repeat(times=4)
    source = ray.data.range(1000)
    pipe = DatasetPipeline.from_iterable(
        [lambda: source, lambda: source, lambda: source, lambda: source])

    # Equivalent to ray.data.range(1000).window(blocks_per_window=10)
    splits = ray.data.range(1000, parallelism=200).split(20)
    pipe = DatasetPipeline.from_iterable([lambda s=s: s for s in splits])

Handling Epochs
~~~~~~~~~~~~~~~

It's common in ML training to want to divide data ingest into epochs, or repetitions over the original source dataset. DatasetPipeline provides a convenient ``.iter_epochs()`` method that can be used to split up the pipeline into epoch-delimited pipeline segments. Epochs are defined by the last call to ``.repeat()`` in a pipeline, for example:

.. code-block:: python

    pipe = ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(3) \
        .random_shuffle_each_window()
    for i, epoch in enumerate(pipe.iter_epochs()):
        print("Epoch {}", i)
        for row in epoch.iter_rows():
            print(row)
    # ->
    # Epoch 0
    # 2
    # 1
    # 3
    # 4
    # 0
    # Epoch 1
    # 3
    # 4
    # 0
    # 2
    # 1
    # Epoch 2
    # 3
    # 2
    # 4
    # 1
    # 0

Note that while epochs commonly consist of a single window, they can also contain multiple windows if ``.window()`` is used or there are multiple ``.repeat()`` calls.

Per-Window Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~

While most Dataset operations are per-row (e.g., map, filter), some operations apply to the Dataset as a whole (e.g., sort, shuffle). When applied to a pipeline, holistic transforms like shuffle are applied separately to each window in the pipeline:

.. code-block:: python

    # Example of randomly shuffling each window of a pipeline.
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(2) \
        .random_shuffle_each_window() \
        .show_windows()
    # -> 
    # ----- Epoch 0 ------
    # === Window 0 ===
    # 4
    # 3
    # 1
    # 0
    # 2
    # ----- Epoch 1 ------
    # === Window 1 ===
    # 2
    # 1
    # 4
    # 0
    # 3

You can also apply arbitrary transformations to each window using ``DatasetPipeline.foreach_window()``:

.. code-block:: python

    # Equivalent transformation using .foreach_window() 
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(2) \
        .foreach_window(lambda w: w.random_shuffle()) \
        .show_windows()
    # -> 
    # ----- Epoch 0 ------
    # === Window 0 ===
    # 1
    # 0
    # 4
    # 2
    # 3
    # ----- Epoch 1 ------
    # === Window 1 ===
    # 4
    # 2
    # 0
    # 3
    # 1

Example: Pipelined Batch Inference
----------------------------------

In this example, we pipeline the execution of a three-stage Dataset application to minimize GPU idle time. Let's revisit the batch inference example from the previous page:

.. code-block:: python

    def preprocess(image: bytes) -> bytes:
        return image

    class BatchInferModel:
        def __init__(self):
            self.model = ImageNetModel()
        def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
            return self.model(batch)

    # Load data from storage.
    ds: Dataset = ray.data.read_binary_files("s3://bucket/image-dir")

    # Preprocess the data.
    ds = ds.map(preprocess)

    # Apply GPU batch inference to the data.
    ds = ds.map_batches(BatchInferModel, compute="actors", batch_size=256, num_gpus=1)

    # Save the output.
    ds.write_json("/tmp/results")

Ignoring the output, the above script has three separate stages: loading, preprocessing, and inference. Assuming we have a fixed-sized cluster, and that each stage takes 100 seconds each, the cluster GPUs will be idle for the first 200 seconds of execution:

..
  https://docs.google.com/drawings/d/1UMRcpbxIsBRwD8G7hR3IW6DPa9rRSkd05isg9pAEx0I/edit

.. image:: dataset-pipeline-1.svg

Enabling Pipelining
~~~~~~~~~~~~~~~~~~~

We can optimize this by *pipelining* the execution of the dataset with the ``.window()`` call, which returns a DatasetPipeline instead of a Dataset object. The pipeline supports similar transformations to the original Dataset:

.. code-block:: python

    # Convert the Dataset into a DatasetPipeline.
    pipe: DatasetPipeline = ray.data \
        .read_binary_files("s3://bucket/image-dir") \
        .window(blocks_per_window=2)

    # The remainder of the steps do not change.
    pipe = pipe.map(preprocess)
    pipe = pipe.map_batches(BatchInferModel, compute="actors", batch_size=256, num_gpus=1)
    pipe.write_json("/tmp/results")

Here we specified ``blocks_per_window=2``, which means that the Dataset is split into smaller sub-Datasets of two blocks each. Each transformation or *stage* of the pipeline is operating over these two-block Datasets in parallel. This means batch inference processing can start as soon as two blocks are read and preprocessed, greatly reducing the GPU idle time:

.. image:: dataset-pipeline-2.svg

Tuning Parallelism
~~~~~~~~~~~~~~~~~~

Tune the throughput vs latency of your pipeline with the ``blocks_per_window`` setting. As a rule of thumb, higher parallelism settings perform better, however ``blocks_per_window == num_blocks`` effectively disables pipelining, since the DatasetPipeline will only contain a single Dataset. The other extreme is setting ``blocks_per_window=1``, which minimizes the latency to initial output but only allows one concurrent transformation task per stage:

.. image:: dataset-pipeline-3.svg

.. _dataset-pipeline-per-epoch-shuffle:

Example: Per-Epoch Shuffle Pipeline
-----------------------------------
.. tip::

    If you interested in distributed ingest for deep learning, it is
    recommended to use Ray Datasets in conjunction with :ref:`Ray Train <train-docs>`.
    See the :ref:`example below<dataset-pipeline-ray-train>` for more info.

..
  https://docs.google.com/drawings/d/1vWQ-Zfxy2_Gthq8l3KmNsJ7nOCuYUQS9QMZpj5GHYx0/edit

The other method of creating a pipeline is calling ``.repeat()`` on an existing Dataset. This creates a DatasetPipeline over an infinite sequence of the same original Dataset. Readers pulling batches from the pipeline will see the same data blocks repeatedly, which is useful for distributed training.

Pre-repeat vs post-repeat transforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Transformations made prior to the Dataset prior to the call to ``.repeat()`` are executed once. Transformations made to the DatasetPipeline after the repeat will be executed once for each repetition of the Dataset.

For example, in the following pipeline, the datasource read only occurs once. However, the random shuffle is applied to each repetition in the pipeline.

**Code**:

.. code-block:: python

    # Create a pipeline that loops over its source dataset indefinitely.
    pipe: DatasetPipeline = ray.data \
        .read_datasource(...) \
        .repeat() \
        .random_shuffle_each_window()

    @ray.remote(num_gpus=1)
    def train_func(pipe: DatasetPipeline):
        model = MyModel()
        for batch in pipe.to_torch():
            model.fit(batch)

    # Read from the pipeline in a remote training function.
    ray.get(train_func.remote(pipe))


**Pipeline**:

.. image:: dataset-repeat-1.svg

Splitting pipelines for distributed ingest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to how you can ``.split()`` a Dataset, you can also split a DatasetPipeline with the same method call. This returns a number of DatasetPipeline shards that share a common parent pipeline. Each shard can be passed to a remote task or actor.

**Code**:

.. code-block:: python

    # Create a pipeline that loops over its source dataset indefinitely.
    pipe: DatasetPipeline = ray.data \
        .read_parquet("s3://bucket/dir") \
        .repeat() \
        .random_shuffle_each_window()

    @ray.remote(num_gpus=1)
    class TrainingWorker:
        def __init__(self, rank: int, shard: DatasetPipeline):
            self.rank = rank
            self.shard = shard
        ...

    shards: List[DatasetPipeline] = pipe.split(n=3)
    workers = [TrainingWorker.remote(rank, s) for rank, s in enumerate(shards)]
    ...


**Pipeline**:

.. image:: dataset-repeat-2.svg

.. _dataset-pipeline-ray-train:

Distributed Ingest with Ray Train
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Datasets integrates with :ref:`Ray Train <train-docs>`, further simplifying your distributed ingest pipeline.

Ray Train is a lightweight library for scalable deep learning on Ray.

1. It allows you to focus on the training logic and automatically handles distributed setup for your framework of choice (PyTorch, Tensorflow, or Horovod).
2. It has out of the box fault-tolerance and elastic training
3. And it comes with support for standard ML tools and features that practitioners love such as checkpointing and logging.

**Code**

.. code-block:: python

    def train_func():
        # This is a dummy train function just iterating over the dataset shard.
        # You should replace this with your training logic.
        shard = ray.train.get_dataset_shard()
        for row in shard.iter_rows():
            print(row)

    # Create a pipeline that loops over its source dataset indefinitely.
    pipe: DatasetPipeline = ray.data \
        .read_parquet(...) \
        .repeat() \
        .random_shuffle_each_window()


    # Pass in the pipeline to the Trainer.
    # The Trainer will automatically split the DatasetPipeline for you.
    trainer = Trainer(num_workers=8, backend="torch")
    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64, "num_epochs": 2},
        dataset=pipe)

Ray Train is responsible for the orchestration of the training workers and will automatically split the Dataset for you.
See :ref:`the Train User Guide <train-dataset-pipeline>` for more details.

Changing Pipeline Structure
---------------------------

Sometimes, you may want to change the structure of an existing pipeline. For example, after generating a pipeline with ``ds.window(k)``, you may want to repeat that windowed pipeline ``n`` times. This can be done with ``ds.window(k).repeat(n)``. As another example, suppose you have a repeating pipeline generated with ``ds.repeat(n)``. The windowing of that pipeline can be changed with ``ds.repeat(n).rewindow(k)``. Note the subtle difference in the two examples: the former is repeating a windowed pipeline that has a base window size of ``k``, while the latter is re-windowing a pipeline of initial window size of ``ds.num_blocks()``. The latter may produce windows that span multiple copies of the same original data if ``preserve_epoch=False`` is set:

.. code-block:: python

    # Window followed by repeat.
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .window(blocks_per_window=2) \
        .repeat(2) \
        .show_windows()
    # ->
    # ------ Epoch 0 ------
    # === Window 0 ===
    # 0
    # 1
    # === Window 1 ===
    # 2
    # 3
    # === Window 2 ===
    # 4
    # ------ Epoch 1 ------
    # === Window 3 ===
    # 0
    # 1
    # === Window 4 ===
    # 2
    # 3
    # === Window 5 ===
    # 4

    # Repeat followed by window. Since preserve_epoch=True, at epoch boundaries
    # windows may be smaller than the target size. If it was set to False, all
    # windows except the last would be the target size.
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(2) \
        .rewindow(blocks_per_window=2, preserve_epoch=True) \
        .show_windows()
    # ->
    # ------ Epoch 0 ------
    # === Window 0 ===
    # 0
    # 1
    # === Window 1 ===
    # 2
    # 3
    # === Window 2 ===
    # 4
    # ------ Epoch 1 ------
    # === Window 3 ===
    # 0
    # 1
    # === Window 4 ===
    # 2
    # 3
    # === Window 5 ===
    # 4
