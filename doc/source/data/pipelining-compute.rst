.. _pipelining_datasets:

==================
Pipelining Compute 
==================

Creating a DatasetPipeline
==========================

A `DatasetPipeline <package-ref.html#datasetpipeline-api>`__ can be constructed in two ways: either by pipelining the execution of an existing Dataset (via ``Dataset.window``), or generating repeats of an existing Dataset (via ``Dataset.repeat``). Similar to Datasets, you can freely pass DatasetPipelines between Ray tasks, actors, and libraries. Get started with this synthetic data example:

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


Per-Window Transformations
==========================

While most Dataset operations are per-row (e.g., map, filter), some operations apply to the Dataset as a whole (e.g., sort, shuffle). When applied to a pipeline, holistic transforms like shuffle are applied separately to each window in the pipeline:

.. important::

   Windowed shuffle or global shuffle are expensive operations. Use only if you really need them.
   Alternatively, you may consider local shuffle after converting to_tf() or to_torch(), if simple shuffle is sufficient.

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
