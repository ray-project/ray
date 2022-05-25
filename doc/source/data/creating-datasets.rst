.. _creating_datasets:

=================
Creating Datasets
=================

You can get started by creating Datasets from synthetic data using ``ray.data.range()`` and ``ray.data.from_items()``.
Datasets can hold either plain Python objects (i.e. their schema is a Python type), or Arrow records
(in which case their schema is Arrow).

.. code-block:: python

    import ray

    # Create a Dataset of Python objects.
    ds = ray.data.range(10000)
    # -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

    ds.take(5)
    # -> [0, 1, 2, 3, 4]

    ds.count()
    # -> 10000

    # Create a Dataset of Arrow records.
    ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
    # -> Dataset(num_blocks=200, num_rows=10000, schema={col1: int64, col2: string})

    ds.show(5)
    # -> {'col1': 0, 'col2': '0'}
    # -> {'col1': 1, 'col2': '1'}
    # -> {'col1': 2, 'col2': '2'}
    # -> {'col1': 3, 'col2': '3'}
    # -> {'col1': 4, 'col2': '4'}

    ds.schema()
    # -> col1: int64
    # -> col2: string

Datasets can be created from files on local disk or remote datasources such as S3.
Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations:

.. code-block:: python

    # Read a directory of files in remote storage.
    ds = ray.data.read_csv("s3://bucket/path")

    # Read multiple local files.
    ds = ray.data.read_csv(["/path/to/file1", "/path/to/file2"])

    # Read multiple directories.
    ds = ray.data.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

Finally, you can create a ``Dataset`` from existing data in the Ray object store or Ray-compatible distributed DataFrames:

.. code-block:: python

    import pandas as pd
    import dask.dataframe as dd

    # Create a Dataset from a list of Pandas DataFrame objects.
    pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([pdf])

    # Create a Dataset from a Dask-on-Ray DataFrame.
    dask_df = dd.from_pandas(pdf, npartitions=10)
    ds = ray.data.from_dask(dask_df)

From Torch/TensorFlow
---------------------

.. tabbed:: PyTorch

    If you already have a Torch dataset available, you can create a Ray Dataset using
    :py:class:`~ray.data.datasource.SimpleTorchDatasource`.

    .. warning::
        :py:class:`~ray.data.datasource.SimpleTorchDatasource` doesn't support parallel
        reads. You should only use this datasource for small datasets like MNIST or
        CIFAR.

    .. code-block:: python

        import ray.data
        from ray.data.datasource import SimpleTorchDatasource
        import torchvision

        dataset_factory = lambda: torchvision.datasets.MNIST("data", download=True)
        dataset = ray.data.read_datasource(
            SimpleTorchDatasource(), parallelism=1, dataset_factory=dataset_factory
        )
        dataset.take(1)
        # (<PIL.Image.Image image mode=L size=28x28 at 0x1142CCA60>, 5)

.. tabbed:: TensorFlow

    If you already have a TensorFlow dataset available, you can create a Ray Dataset
    using :py:class:`SimpleTensorFlowDatasource`.

    .. warning::
        :py:class:`SimpleTensorFlowDatasource` doesn't support parallel reads. You
        should only use this datasource for small datasets like MNIST or CIFAR.

    .. code-block:: python

        import ray.data
        from ray.data.datasource import SimpleTensorFlowDatasource
        import tensorflow_datasets as tfds

        def dataset_factory():
            return tfds.load("cifar10", split=["train"], as_supervised=True)[0]

        dataset = ray.data.read_datasource(
            SimpleTensorFlowDatasource(),
            parallelism=1,
            dataset_factory=dataset_factory
        )
        features, label = dataset.take(1)[0]
        features.shape  # TensorShape([32, 32, 3])
        label  # <tf.Tensor: shape=(), dtype=int64, numpy=7>


From 🤗 (Hugging Face) Datasets
-------------------------------

You can convert 🤗 Datasets into Ray Datasets by using
:py:class:`~ray.data.from_huggingface`. This function accesses the underlying Arrow table and
converts it into a Ray Dataset directly.

.. warning::
    :py:class:`~ray.data.from_huggingface` doesn't support parallel
    reads. This will not usually be an issue with in-memory 🤗 Datasets,
    but may fail with large memory-mapped 🤗 Datasets. 🤗 ``IterableDataset``
    objects are not supported.

.. code-block:: python

    import ray.data
    from datasets import load_dataset

    hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
    ray_datasets = ray.data.from_huggingface(hf_datasets)
    ray_datasets["train"].take(2)
    # [{'text': ''}, {'text': ' = Valkyria Chronicles III = \n'}]