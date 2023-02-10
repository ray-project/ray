.. _datasets_glossary:

=====================
Ray Datasets Glossary
=====================

.. glossary::

    Batch format
        The way batches of data are represented.

        Set ``batch_format`` in methods like :meth:`~ray.data.Dataset.iter_batches` and
        :meth:`~ray.data.Dataset.map_batches` to specify the batch type.

        .. doctest::

            >>> dataset = ray.data.range_table(10)
            >>> next(iter(dataset.iter_batches(batch_format="numpy", batch_size=5)))
            {'value': array([0, 1, 2, 3, 4])}
            >>> next(iter(dataset.iter_batches(batch_format="pandas", batch_size=5)))
               value
            0      0
            1      1
            2      2
            3      3
            4      4

        To learn more about batch formats, read
        :ref:`UDF Input Batch Formats <transform_datasets_batch_formats>`.

    Block
        A processing unit of data. A :class:`~ray.data.Dataset` consists of a list of
        blocks.

        Under the hood, :term:`Datasets <Datasets (library)>` partition :term:`records <Record>`
        into a set of distributed data blocks. This allows Datasets to perform operations
        in parallel.

        Unlike a batch, which is a user-facing object, a block is an internal abstraction.

    Block format
        The way :term:`blocks <Block>` are represented.

        Blocks are represented as Arrow tables, pandas DataFrames, and Python lists.
        To determine the block format, call :meth:`~ray.data.Dataset.dataset_format`.

    Data Shuffling
        The act of reordering :term:`records <Record>`. Shuffling prevents overfitting of models.

        To perform a global shuffle, call :meth:`~ray.data.Dataset.random_shuffle()`.

        .. doctest::

            >>> dataset = ray.data.range(10)
            >>> dataset.random_shuffle().take_all()
            [4, 7, 1, 5, 2, 3, 8, 6, 9, 0]

        If :meth:`~ray.data.Dataset.random_shuffle()` is slow, perform a local shuffle.
        It’s lower quality but more performant. To perform a local shuffle, call methods
        like :meth:`~ray.data.Dataset.iter_batches` and specify ``local_shuffle_buffer_size``.

        .. doctest::

            >>> dataset = ray.data.range(10)
            >>> next(iter(dataset.iter_batches(local_shuffle_buffer_size=5)))
            [2, 6, 7, 8, 4, 9, 3, 1, 5, 0]

        To learn more about shuffling data, read :ref:`Shuffling Data <air-shuffle>`.

    Datasets (library)
        A library for distributed data processing.

        Datasets isn’t intended as a replacement for more general data processing systems.
        Its utility is as the last-mile bridge from ETL pipeline outputs to distributed applications and libraries in Ray.

        To learn more about Ray Datasets, read :ref:`Key Concepts <dataset_concept>`.

    Dataset (object)
        A class that represents a distributed collection of data.

        :class:`Datasets <ray.data.Dataset>` expose methods to read, transform, and consume data at scale.

        To learn more about Datasets and the operations they support, read the :ref:`Datasets API Reference <data-api>`.

    Datasource
        A :class:`~ray.data.Datasource` specifies how to read and write from
        a variety of external storage and data formats.
        For example: Parquet, images, TFRecord, CSV, and MongoDB.

        To learn more about Datasources, read :ref:`Creating a Custom Datasource <custom_datasources>`.

    Record
        A single data item.

        If your dataset is :term:`tabular <Tabular Dataset>`, then records are :class:`TableRows <ray.data.row.TableRow>`.
        If your dataset is :term:`simple <Simple Dataset>`, then records are arbitrary Python objects.
        If your dataset is :term:`tensor <Tensor Dataset>`, then records are ndarrays.

    Schema
        The data type of a dataset.

        If your dataset is :term:`tabular <Tabular Dataset>`, then the schema describes
        the column names and data types. If your dataset is :term:`simple <Simple Dataset>`,
        then the schema describes the Python object type. If your dataset is :term:`tensor <Tensor Dataset>`, then the schema isn’t relevant.

        To determine a dataset's schema, call :meth:`~ray.data.Dataset.schema`.

    Simple Dataset
        A Dataset that represents a collection of arbitrary Python objects.

        .. doctest::

            >>> ray.data.from_items(["spam", "ham", "eggs"])
            Dataset(num_blocks=3, num_rows=3, schema=<class 'str'>)

    Tensor Dataset
        A Dataset that represents a collection of ndarrays.

        :term:`Tabular datasets <Tabular Dataset>` that contain tensor columns aren’t tensor datasets.

        .. doctest::

            >>> import numpy as np
            >>> ray.data.from_numpy(np.zeros((100, 32, 32, 3)))
            Dataset(num_blocks=1, num_rows=100, schema={__value__: ArrowTensorType(shape=(32, 32, 3), dtype=double)})

    Tabular Dataset
        A Dataset that represents columnar data.

        .. doctest::

            >>> ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
            Dataset(num_blocks=1, num_rows=150, schema={sepal length (cm): double, sepal width (cm): double, petal length (cm): double, petal width (cm): double, target: int64})

    User-defined function (UDF)
        A callable that transforms batches or :term:`records <Record>` of data. UDFs let you arbitrarily transform datasets.

        Call :meth:`~ray.data.Dataset.map_batches`, :meth:`~ray.data.Dataset.map`, or :meth:`~ray.data.Dataset.flat_map` to apply UDFs.

        To learn more about UDFs, read :ref:`Writing User-Defined Functions <transform_datasets_writing_udfs>`.
