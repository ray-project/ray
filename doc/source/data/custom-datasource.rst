.. _custom_datasources:

==================
Custom Datasources
==================

Ray Datasets supports multiple ways to :ref:`create a dataset <creating_datasets>`, 
allowing you to easily ingest data of common formats from popular sources. However, if the 
datasource you want to read from is not in the built-in list, don't worry, you can implement 
a custom one for your use case. In this guide, we will walk you through how to build 
your own custom datasource, using MongoDB as an example. By the end of the guide, you will have a
datasource that you can use as follows:

.. code-block:: python

    # Read from custom MongoDB datasource.
    # The args are passed to MongoDatasource.create_reader().
    ds = ray.data.read_datasource(
        MongoDatasource(),
        uri=MY_URI,
        database=MY_DATABASE,
        collection=MY_COLLECTION,
        pipelines=MY_QUERIES
    )

    # Write to custom MongoDB datasource.
    # The args are passed to MongoDatasource.do_write().
    ds.write_datasource(
        MongoDatasource(), uri=MY_URI, database=MY_DATABASE, collection=MY_COLLECTION
    )

A custom datasource is an implementation of :class:`~ray.data.Datasource`. In the 
example here, let's call it ``MongoDatasource``. At a high level, it will have two 
core parts to build out:

* Read support with :meth:`create_reader() <ray.data.Datasource.create_reader>`
* Write support with :meth:`do_write() <ray.data.Datasource.do_write>`.

Here are the key design choices we will make in this guide:

-  **MongoDB connector**: We use `PyMongo <https://pymongo.readthedocs.io/en/stable/>`__ to connect to MongoDB.
-  **MongoDB to Arrow conversion**: We use `PyMongoArrow <https://mongo-arrow.readthedocs.io/en/latest/>`__ to convert query results into Arrow tables, which Datasets supports as a block format.
-  **Parallel execution**: We ask the user to provide a list of MongoDB queries, with each corresponding to a read task (i.e. a :class:`~ray.data.ReadTask`) that Datasets will execute in parallel.

------------
Read support
------------

To support reading, we implement :meth:`create_reader() <ray.data.Datasource.create_reader>`, returning a :class:`~ray.data.datasource.Reader` implementation for
MongoDB. This ``Reader`` creates a list of :class:`~ray.data.ReadTask` for the given 
list of MongoDB queries. Each :class:`~ray.data.ReadTask` returns a list of blocks when called, and
each :class:`~ray.data.ReadTask` is executed in remote workers to parallelize the execution.

You can find documentation about :ref:`Ray Data blocks here <block-api>`.

First, let's handle a single MongoDB query, which is the unit of execution in
:class:`~ray.data.ReadTask`. We need to connect to MongoDB, execute the query against it,
and then convert results into Arrow format. We use ``PyMongo`` and  ``PyMongoArrow``
to achieve this.

Read more about the `PyMongoArrow function here <https://mongo-arrow.readthedocs.io/en/stable/api/api.html#pymongoarrow.api.aggregate_arrow_all>`__.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __read_single_query_start__
    :end-before: __read_single_query_end__

Once we have this building block, we can just apply it for each of the provided MongoDB 
queries. In particular, below, we construct a `_MongoDatasourceReader` by subclassing
a :class:`~ray.data.Datasource.Reader`, and we implement ``__init__`` and ``get_read_tasks``.

In ``__init__``, we pass in a couple arguments that will be eventually used in
constructing the MongoDB query in ``_read_single_query``.

In ``get_read_tasks``, we construct a :class:`~ray.data.ReadTask` object for each ``query`` ("pipeline") object.
A list of :class:`~ray.data.ReadTask` objects are returned at the end of the function call, and these
tasks are executed on remote workers by the Datasets execution engine (not shown).

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __mongo_datasource_reader_start__
    :end-before: __mongo_datasource_reader_end__

Now, we have finished implementing support for reading from a custom datasource!
Let's move on to implementing support for writing back to the custom datasource.

-------------
Write support
-------------

Similar to read support, we start with handling a single block. Again 
the ``PyMongo`` and  ``PyMongoArrow`` are used for MongoDB interactions.

Read more about the `PyMongoArrow function call <https://mongo-arrow.readthedocs.io/en/stable/api/api.html#pymongoarrow.api.write>`.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __write_single_block_start__
    :end-before: __write_single_block_end__

Unlike read support, we do not need to implement a custom interface.

Below, we implement a helper function to parallelize writing, which is
expected to return a list of :ref:`Ray ObjectRefs <objects-in-ray>`. This helper function
will later be used in the implementation of :meth:`~ray.data.Datasource.do_write`.

In short, the below function spawns multiple :ref:`Ray remote tasks <ray-remote-functions>`
and returns :ref:`their futures (object refs) <objects-in-ray>`.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __write_multiple_blocks_start__
    :end-before: __write_multiple_blocks_end__

-----------------------
Putting it all together
-----------------------

With ``_MongoDatasourceReader`` and ``_write_multiple_blocks`` above, we are 
ready to implement :meth:`create_reader() <ray.data.Datasource.create_reader>` 
and :meth:`do_write() <ray.data.Datasource.do_write>`, and put together 
a ``MongoDatasource``.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __mongo_datasource_start__
    :end-before: __mongo_datasource_end__

Now you can create a Ray Dataset from and write back to MongoDB, just like 
any other datasource!

.. code-block:: python

    # Read from MongoDB datasource.
    # The args are passed to MongoDatasource.create_reader().
    ds = ray.data.read_datasource(
        MongoDatasource(),
        uri=MY_URI, database=MY_DATABASE,
        collection=MY_COLLECTION, pipelines=MY_QUERIES
    )

    # Data processing with Dataset APIs
    # ....

    # Write to MongoDB datasource.
    # The args are passed to MongoDatasource.do_write().
    ds.write_datasource(
        MongoDatasource(), uri=MY_URI, database=MY_DATABASE, collection=MY_COLLECTION
    )