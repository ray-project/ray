.. _datasets_glossary:

=====================
Ray Datasets Glossary
=====================

.. glossary::

    Batch format
        The way batches of data are represented.

        Set ``batch_format`` in methods like
        :meth:`Dataset.iter_batches() <ray.data.Dataset.iter_batches>` and
        :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>` to specify the
        batch type.

        .. doctest::

            >>> import ray
            >>> # Dataset is executed by streaming executor by default, which doesn't
            >>> # preserve the order, so we explicitly set it here.
            >>> ray.data.context.DatasetContext.get_current().execution_options.preserve_order = True
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
        A processing unit of data. A :class:`~ray.data.Dataset` consists of a
        collection of blocks.

        Under the hood, :term:`Datasets <Datasets (library)>` partition :term:`records <Record>`
        into a set of distributed data blocks. This allows Datasets to perform operations
        in parallel.

        Unlike a batch, which is a user-facing object, a block is an internal abstraction.

    Block format
        The way :term:`blocks <Block>` are represented.

        Blocks are represented as
        `Arrow tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`_,
        `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_,
        and Python lists. To determine the block format, call
        :meth:`Dataset.dataset_format() <ray.data.Dataset.dataset_format>`.

    Datasets (library)
        A library for distributed data processing.

        Datasets isn’t intended as a replacement for more general data processing systems.
        Its utility is as the last-mile bridge from ETL pipeline outputs to distributed
        ML applications and libraries in Ray.

        To learn more about Ray Datasets, read :ref:`Key Concepts <dataset_concept>`.

    Dataset (object)
        A class that represents a distributed collection of data.

        :class:`~ray.data.Dataset` exposes methods to read, transform, and consume data at scale.

        To learn more about Datasets and the operations they support, read the :ref:`Datasets API Reference <data-api>`.

    Datasource
        A :class:`~ray.data.Datasource` specifies how to read and write from
        a variety of external storage and data formats.

        Examples of Datasources include :class:`~ray.data.datasource.ParquetDatasource`,
        :class:`~ray.data.datasource.ImageDatasource`,
        :class:`~ray.data.datasource.TFRecordDatasource`,
        :class:`~ray.data.datasource.CSVDatasource`, and
        :class:`~ray.data.datasource.MongoDatasource`.

        To learn more about Datasources, read :ref:`Creating a Custom Datasource <custom_datasources>`.

    Record
        A single data item.

        If your dataset is :term:`tabular <Tabular Dataset>`, then records are :class:`TableRows <ray.data.row.TableRow>`.
        If your dataset is :term:`simple <Simple Dataset>`, then records are arbitrary Python objects.
        If your dataset is :term:`tensor <Tensor Dataset>`, then records are `NumPy ndarrays <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`_.

    Schema
        The data type of a dataset.

        If your dataset is :term:`tabular <Tabular Dataset>`, then the schema describes
        the column names and data types. If your dataset is :term:`simple <Simple Dataset>`,
        then the schema describes the Python object type. If your dataset is
        :term:`tensor <Tensor Dataset>`, then the schema describes the per-element
        tensor shape and data type.

        To determine a dataset's schema, call
        :meth:`Dataset.schema() <ray.data.Dataset.schema>`.

    Simple Dataset
        A Dataset that represents a collection of arbitrary Python objects.

        .. doctest::

            >>> import ray
            >>> ray.data.from_items(["spam", "ham", "eggs"])
            Dataset(num_blocks=3, num_rows=3, schema=<class 'str'>)

    Tensor Dataset
        A Dataset that represents a collection of ndarrays.

        :term:`Tabular datasets <Tabular Dataset>` that contain tensor columns aren’t tensor datasets.

        .. doctest::

            >>> import numpy as np
            >>> import ray
            >>> ray.data.from_numpy(np.zeros((100, 32, 32, 3)))
            Dataset(
               num_blocks=1,
               num_rows=100,
               schema={__value__: ArrowTensorType(shape=(32, 32, 3), dtype=double)}
            )

    Tabular Dataset
        A Dataset that represents columnar data.

        .. doctest::

            >>> import ray
            >>> ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
            Dataset(
               num_blocks=1,
               num_rows=150,
               schema={
                  sepal length (cm): double,
                  sepal width (cm): double,
                  petal length (cm): double,
                  petal width (cm): double,
                  target: int64
               }
            )

    User-defined function (UDF)
        A callable that transforms batches or :term:`records <Record>` of data. UDFs let you arbitrarily transform datasets.

        Call :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`,
        :meth:`Dataset.map() <ray.data.Dataset.map>`, or
        :meth:`Dataset.flat_map() <ray.data.Dataset.flat_map>` to apply UDFs.

        To learn more about UDFs, read :ref:`Writing User-Defined Functions <transform_datasets_writing_udfs>`.
