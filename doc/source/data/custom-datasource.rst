.. _custom_datasources:

==================
Custom Datasources
==================

.. note::

  This MongoDatasource guide below is for education only. For production use of MongoDB
  in Ray Data, see :ref:`Creating Datastream from MongoDB <datastream_mongo_db>`.

Ray Data supports multiple ways to :ref:`create a datastream <loading_data>`,
allowing you to easily ingest data of common formats from popular sources. However, if the 
datasource you want to read from is not in the built-in list, don't worry, you can implement 
a custom one for your use case. In this guide, we will walk you through how to build 
your own custom datasource, using `MongoDB <https://www.mongodb.com/docs/manual/introduction/>`__ as an example.
By the end of the guide, you will have a ``MongoDatasource`` that you can use to create datastream as follows:

.. code-block:: python

    # Read from custom MongoDB datasource to create a datastream.
    ds = ray.data.read_datasource(
        MongoDatasource(),
        uri=MY_URI,
        database=MY_DATABASE,
        collection=MY_COLLECTION,
        pipelines=MY_PIPELINES
    )

    # Write the datastream to custom MongoDB datasource.
    ds.write_datasource(
        MongoDatasource(), uri=MY_URI, database=MY_DATABASE, collection=MY_COLLECTION
    )

.. tip::

    There are a few MongoDB concepts involved here. The `URI <https://www.mongodb.com/docs/manual/reference/connection-string/>`__ points to
    a MongoDB instance, which hosts `Databases and Collections <https://www.mongodb.com/docs/manual/core/databases-and-collections/>`__. A collection
    is analogous to a table in SQL databases. MongoDB also has a `pipeline <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`__ concept,
    which expresses document processing in a series of stages (e.g. match documents with a predicate, sort results, and then select a few fields).
    The execution results of the pipelines are used to create datastream.

A custom datasource is an implementation of :class:`~ray.data.Datasource`. In the 
example here, let's call it ``MongoDatasource``. At a high level, it will have two 
core parts to build out:

* Read support with :meth:`create_reader() <ray.data.Datasource.create_reader>`
* Write support with :meth:`do_write() <ray.data.Datasource.do_write>`.

Here are the key design choices we will make in this guide:

-  **MongoDB connector**: We use `PyMongo <https://pymongo.readthedocs.io/en/stable/>`__ to connect to MongoDB.
-  **MongoDB to Arrow conversion**: We use `PyMongoArrow <https://mongo-arrow.readthedocs.io/en/latest/>`__ to convert MongoDB execution results into Arrow tables, which Datastreams supports as a data format.
-  **Parallel execution**: We ask the user to provide a list of MongoDB pipelines, with each corresponding to a partition of the MongoDB collection, which will be executed in parallel with :class:`~ray.data.ReadTask`.

For example, suppose you have a MongoDB collection with 4 documents, which have a ``partition_field`` with values 0, 1, 2, 3.
You can compose two MongoDB pipelines (each handled by a :class:`~ray.data.ReadTask`) as follows to read the collection in parallel:

.. code-block:: python

    # A list of pipelines. Each pipeline is a series of stages, typed as List[Dict].
    my_pipelines = [
        # The first pipeline: match documents in partition range [0, 2)
        [
          {
            "$match": {
                "partition_field": {
                    "$gte": 0
                    "$lt": 2
                }
            }
          }
        ],
        # The second pipeline: match documents in partition range [2, 4)
        [
          {
            "$match": {
                "partition_field": {
                    "$gte": 2
                    "$lt": 4 
                }
  
            }
          }
        ],
    ]

------------
Read support
------------

To support reading, we implement :meth:`create_reader() <ray.data.Datasource.create_reader>`, returning a :class:`~ray.data.datasource.Reader` implementation for
MongoDB. This ``Reader`` creates a list of :class:`~ray.data.ReadTask` for the given 
list of MongoDB pipelines. Each :class:`~ray.data.ReadTask` returns a list of blocks when called, and
each :class:`~ray.data.ReadTask` is executed in remote workers to parallelize the execution.

You can find documentation about Ray Data :ref:`block concept here <datastream_concept>` and :ref:`block APIs here <block-api>`.

First, let's handle a single MongoDB pipeline, which is the unit of execution in
:class:`~ray.data.ReadTask`. We need to connect to MongoDB, execute the pipeline against it,
and then convert results into Arrow format. We use ``PyMongo`` and  ``PyMongoArrow``
to achieve this.

.. literalinclude:: ./doc_code/custom_datasource.py
    :language: python
    :start-after: __read_single_partition_start__ 
    :end-before: __read_single_partition_end__

Once we have this building block, we can just apply it for each of the provided MongoDB 
pipelines. In particular, below, we construct a `_MongoDatasourceReader` by subclassing
:class:`~ray.data.Datasource.Reader`, and implement the ``__init__`` and ``get_read_tasks``.

In ``__init__``, we pass in a couple arguments that will be eventually used in
constructing the MongoDB pipeline in ``_read_single_partition``.

In ``get_read_tasks``, we construct a :class:`~ray.data.ReadTask` object for each ``pipeline`` object.
This will need to provide a :class:`~ray.data.block.BlockMetadata` and a no-arg read function as arguments.
The :class:`~ray.data.block.BlockMetadata` contains metadata like number of rows, size in bytes and schema
that we know about the block prior to actually executing the read task; the no-arg read function is just
a wrapper of ``_read_single_partition``.
A list of :class:`~ray.data.ReadTask` objects are returned by ``get_read_tasks``, and these
tasks are executed on remote workers. You can find more details about `Datastream read execution here <https://docs.ray.io/en/master/data/key-concepts.html#reading-data>`__.

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

Now you can create a Datastream from and write back to MongoDB, just like 
any other datasource!

.. code-block:: python

    # Read from MongoDB datasource and create a datastream.
    # The args are passed to MongoDatasource.create_reader().
    ds = ray.data.read_datasource(
        MongoDatasource(),
        uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
        database="my_db",
        collection=="my_collection",
        pipelines=my_pipelines, # See the example definition of ``my_pipelines`` above
    )

    # Data preprocessing with Datastream APIs here
    # ...

    # Write the datastream back to MongoDB datasource.
    # The args are passed to MongoDatasource.do_write().
    ds.write_datasource(
        MongoDatasource(),
        uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
        database="my_db",
        collection="my_collection"
    )
