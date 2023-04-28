.. _datastreams_glossary:

=====================
Ray Data Glossary
=====================

.. glossary::

    Batch format
        The way batches of data are represented.

        Set ``batch_format`` in methods like
        :meth:`Datastream.iter_batches() <ray.data.Datastream.iter_batches>` and
        :meth:`Datastream.map_batches() <ray.data.Datastream.map_batches>` to specify the
        batch type.

        .. doctest::

            >>> import ray
            >>> # Datastream is executed by streaming executor by default, which doesn't
            >>> # preserve the order, so we explicitly set it here.
            >>> ray.data.context.DataContext.get_current().execution_options.preserve_order = True
            >>> datastream = ray.data.range(10)
            >>> next(iter(datastream.iter_batches(batch_format="numpy", batch_size=5)))
            {'id': array([0, 1, 2, 3, 4])}
            >>> next(iter(datastream.iter_batches(batch_format="pandas", batch_size=5)))
               id
            0   0
            1   1
            2   2
            3   3
            4   4

        To learn more about batch formats, read
        :ref:`UDF Input Batch Formats <transform_datastreams_batch_formats>`.

    Block
        A processing unit of data. A :class:`~ray.data.Datastream` consists of a
        collection of blocks.

        Under the hood, :term:`Ray Data <Ray Data (library)>` partition :term:`records <Record>`
        into a set of distributed data blocks. This allows it to perform operations
        in parallel.

        Unlike a batch, which is a user-facing object, a block is an internal abstraction.

    Block format
        The way :term:`blocks <Block>` are represented.

        Blocks are represented as
        `Arrow tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`_,
        `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_,
        and Python lists.

    Ray Data (library)
        A library for distributed data processing.

        Ray Data isn’t intended as a replacement for more general data processing systems.
        Its utility is as the last-mile bridge from ETL pipeline outputs to distributed
        ML applications and libraries in Ray.

        To learn more about Ray Data, read :ref:`Key Concepts <datastream_concept>`.

    Datastream (object)
        A class that produces a sequence of distributed data blocks.

        :class:`~ray.data.Datastream` exposes methods to read, transform, and consume data at scale.

        To learn more about Datastreams and the operations they support, read the :ref:`Datastreams API Reference <data-api>`.

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

        If your datastream is :term:`tabular <Tabular Datastream>`, then records are :class:`TableRows <ray.data.row.TableRow>`.
        If your datastream is :term:`simple <Simple Datastream>`, then records are arbitrary Python objects.
        If your datastream is :term:`tensor <Tensor Datastream>`, then records are `NumPy ndarrays <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`_.

    Schema
        The data type of a datastream.

        If your datastream is :term:`tabular <Tabular Datastream>`, then the schema describes
        the column names and data types. If your datastream is :term:`simple <Simple Datastream>`,
        then the schema describes the Python object type. If your datastream is
        :term:`tensor <Tensor Datastream>`, then the schema describes the per-element
        tensor shape and data type.

        To determine a datastream's schema, call
        :meth:`Datastream.schema() <ray.data.Datastream.schema>`.

    Simple Datastream
        A Datastream that represents a collection of arbitrary Python objects.

        .. doctest::

            >>> import ray
            >>> ray.data.from_items(["spam", "ham", "eggs"])
            MaterializedDatastream(num_blocks=3, num_rows=3, schema={item: string})

    Tensor Datastream
        A Datastream that represents a collection of ndarrays.

        :term:`Tabular datastreams <Tabular Datastream>` that contain tensor columns aren’t tensor datastreams.

        .. doctest::

            >>> import numpy as np
            >>> import ray
            >>> ray.data.from_numpy(np.zeros((100, 32, 32, 3)))
            MaterializedDatastream(
               num_blocks=1,
               num_rows=100,
               schema={data: numpy.ndarray(shape=(32, 32, 3), dtype=double)}
            )

    Tabular Datastream
        A Datastream that represents columnar data.

        .. doctest::

            >>> import ray
            >>> ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
            Datastream(
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
        A callable that transforms batches or :term:`records <Record>` of data. UDFs let you arbitrarily transform datastreams.

        Call :meth:`Datastream.map_batches() <ray.data.Datastream.map_batches>`,
        :meth:`Datastream.map() <ray.data.Datastream.map>`, or
        :meth:`Datastream.flat_map() <ray.data.Datastream.flat_map>` to apply UDFs.

        To learn more about UDFs, read :ref:`Writing User-Defined Functions <transform_datastreams_writing_udfs>`.
