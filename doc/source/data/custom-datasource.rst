.. _building_custom_datasource:

==========================
Building Custom Datasource
==========================

Ray Datasets supports a wide range of `input formats <https://docs.ray.io/en/latest/data/dataset.html#supported-input-formats>`__ so you can
easily read from those data sources and create Dataset. However, if the datasource you want to read from is not in the list, don't worry, 
you can implement a custom one for your use case. In this guide, we will walk you through how to build your own custom datasource 
with MongoDB as an example.

A custom datasource is an implementation of :class:`<ray.data.datasource>`. In the example here, let's call it ``MongoDatasource``.
At high level, it will have two core parts to build out: read support with :meth:`create_reader() <ray.data.datasource.create_reader>` and write support with
:meth:`do_write() <ray.data.datasource.do_write>`.

Here are the key design choices we will make this in guide:
* MongoDB connector: We will use `PyMongo <https://pymongo.readthedocs.io/en/stable/>`__ to connect MongDB.
* MongoDB to Arrow conversion: We will use `PyMongoArrow <https://mongo-arrow.readthedocs.io/en/latest/>`__ to convert query results into Arrow format for Dataset.
* Parallel execution: We will assume the user to provide a list of MongoDB queries, with each corresponding to a shard (i.e. a ``ReadTask``) that can be executed in parallel.

------------
Read support
------------
To implement :meth:`create_reader() <ray.data.datasource.create_reader>` and support read, the major work will be subclassing :class:`<ray.data.datasource.datasource.Reader>`
for MongoDB. What it does is creating a list of :class:`<ray.data.ReadTask>` for the given list of MongDB queries. Each ``ReadTask`` will return a list of
``ray.data.Block`` when called, and they are executed in remote functions to parallelize the execution.

First of all, let's handle a single MongDB query, as this is the execution unit in ``ReadTask``. We need to connect to MongDB, execute the query against it,
and then convert results into Arrow format. The ``PyMongo`` and  ``PyMongoArrow`` are used to achieve these.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __read_single_query_start__
    :end-before: __read_single_query_end__

Once we have this building block, we can just apply it for each provided MongDB queries and get the implementation of :class:`<ray.data.datasource.datasource.Reader>`.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __mongo_datasource_reader_start__
    :end-before: __mongo_datasource_reader_end__

-------------
Write support
-------------
Similar to read support, we start with handling a single block. Again the ``PyMongo`` and  ``PyMongoArrow`` are used.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __write_single_block_start__
    :end-before: __write_single_block_end__

To write multiple blocks in parallel, we again use Ray remote functions to launch them in parallel.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __write_multiple_blocks_start__
    :end-before: __write_multiple_blocks_end__

------------
Put together
------------
With ``_MongoDatasourceReader`` and ``_write_multiple_blocks`` above, we are ready to implement :meth:`create_reader() <ray.data.datasource.create_reader>` and write support with
:meth:`do_write() <ray.data.datasource.do_write>`, and put together a ``MongoDatasource``.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __mongo_datasource_start__
    :end-before: __mongo_datasource_end__

Now you can create a Ray Dataset from and write back to MongoDB, just like any other data sources!

.. code-block:: python

    # Read from a custom datasource.
    ds = ray.data.read_datasource(MongoDatasource, uri=MY_URI, database=MY_DATABASE, collection=MY_COLLECTION, pipelines=MY_QUERIES)

    # Data processing with Dataset APIs
    # ....

    # Write to a custom datasource.
    ds.write_datasource(MongoDatasource, uri=MY_URI, database=MY_DATABASE, collection=MY_COLLECTION)