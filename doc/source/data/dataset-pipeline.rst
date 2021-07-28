Dataset Pipelines
=================

Concepts
--------

Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <package-ref.html#datasetpipeline-api>`__.

A DatasetPipeline is an unified iterator over a (potentially infinite) sequence of Ray Datasets. Conceptually it is similar to a `Spark DStream <https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams>`__. Ray computes each dataset on-demand and stitches their output together into a single iterator where data can be pulled from. DatasetPipeline implements most of the same transformation and output methods as Datasets (e.g., map, filter, split, iter_rows, to_torch, etc.).

A DatasetPipeline can be constructed in two ways: either by pipelining an existing Dataset (via ``Dataset.pipeline``), or generating repeats of an existing Dataset (via ``Dataset.repeat``). Similar to Datasets, you can freely pass DatasetPipelines between Ray tasks, actors, and libraries.

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


We can optimize this by *pipelining* the execution of the dataset.

.. code-block:: python

    # Convert the Dataset into a DatasetPipeline.
    pipe: DatasetPipeline = ray.data \
        .read_binary_files("s3://bucket/image-dir") \
        .pipeline(parallelism=2)

    # The remainder of the steps do not change.
    pipe = pipe.map(preprocess)
    pipe = pipe.map_batches(BatchInferModel, compute="actors", batch_size=256, num_gpus=1)
    pipe.write_json("/tmp/results")

.. image:: dataset-pipeline-2.svg

.. image:: dataset-pipeline-3.svg

Example: Per-Epoch Shuffle Pipeline
-----------------------------------

..
  https://docs.google.com/drawings/d/1vWQ-Zfxy2_Gthq8l3KmNsJ7nOCuYUQS9QMZpj5GHYx0/edit

.. image:: dataset-repeat-1.svg

.. image:: dataset-repeat-2.svg
