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
        :ref:`Configuring batch formats <transform_datastreams_batch_formats>`.

    Block
        A processing unit of data. A :class:`~ray.data.Datastream` consists of a
        collection of blocks.

        Under the hood, :term:`Ray Data <Ray Data (library)>` partition :term:`records <Record>`
        into a set of distributed data blocks. This allows it to perform operations
        in parallel.

        Unlike a batch, which is a user-facing object, a block is an internal abstraction.

    Block format
        The way :term:`blocks <Block>` are represented.

        Blocks are internally represented as
        `Arrow tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`_ or
        `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_.

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
        A single data item, which is always a ``Dict[str, Any]``.

    Schema
        The name and type of the datastream fields.

        To determine a datastream's schema, call
        :meth:`Datastream.schema() <ray.data.Datastream.schema>`.
