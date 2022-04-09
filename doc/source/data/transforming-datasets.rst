.. _transforming_datasets:

=====================
Transforming Datasets
=====================

Datasets can be transformed in parallel using ``.map_batches()``. Ray will transform
batches of records in the Dataset using the given function. The function must return
a batch of records. You are allowed to filter or add additional records to the batch,
which will change the size of the Dataset.

Transformations are executed *eagerly* and block until the operation is finished.

.. code-block:: python

    def transform_batch(df: pandas.DataFrame) -> pandas.DataFrame:
        return df.applymap(lambda x: x * 2)

    ds = ray.data.range_arrow(10000)
    ds = ds.map_batches(transform_batch, batch_format="pandas")
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1927.62it/s]
    ds.take(5)
    # -> [{'value': 0}, {'value': 2}, ...]

The batch format can be specified using ``batch_format`` option, which defaults to "native",
meaning pandas format for Arrow-compatible batches, and Python lists for other types. You
can also specify explicitly "arrow" or "pandas" to force a conversion to that batch format.
The batch size can also be chosen. If not given, the batch size will default to entire blocks.

.. tip::

    Datasets also provides the convenience methods ``map``, ``flat_map``, and ``filter``, which are not vectorized (slower than ``map_batches``), but may be useful for development.

By default, transformations are executed using Ray tasks.
For transformations that require setup, specify ``compute=ray.data.ActorPoolStrategy(min, max)`` and Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your transforms.
For a fixed-size actor pool, specify ``ActorPoolStrategy(n, n)``.
The following is an end-to-end example of reading, transforming, and saving batch inference results using Ray Data:

.. code-block:: python

    from ray.data import ActorPoolStrategy

    # Example of GPU batch inference on an ImageNet model.
    def preprocess(image: bytes) -> bytes:
        return image

    class BatchInferModel:
        def __init__(self):
            self.model = ImageNetModel()
        def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
            return self.model(batch)

    ds = ray.data.read_binary_files("s3://bucket/image-dir")

    # Preprocess the data.
    ds = ds.map(preprocess)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]

    # Apply GPU batch inference with actors, and assign each actor a GPU using
    # ``num_gpus=1`` (any Ray remote decorator argument can be used here).
    ds = ds.map_batches(
        BatchInferModel, compute=ActorPoolStrategy(10, 20),
        batch_size=256, num_gpus=1)
    # -> Map Progress (16 actors 4 pending): 100%|██████| 200/200 [00:07, 27.60it/s]

    # Save the results.
    ds.repartition(1).write_json("s3://bucket/inference-results")
