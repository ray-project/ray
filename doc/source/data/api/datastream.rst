.. _datastream-api:

Datastream API
==============

.. currentmodule:: ray.data

Constructor
-----------

.. autosummary::
   :toctree: doc/

   Datastream

Basic Transformations
---------------------

.. autosummary::
   :toctree: doc/

   Datastream.map
   Datastream.map_batches
   Datastream.flat_map
   Datastream.filter
   Datastream.add_column
   Datastream.drop_columns
   Datastream.select_columns
   Datastream.random_sample
   Datastream.limit

Sorting, Shuffling, Repartitioning
----------------------------------

.. autosummary::
   :toctree: doc/

   Datastream.sort
   Datastream.random_shuffle
   Datastream.randomize_block_order
   Datastream.repartition

Splitting and Merging Datastreams
---------------------------------

.. autosummary::
   :toctree: doc/

   Datastream.split
   Datastream.split_at_indices
   Datastream.split_proportionately
   Datastream.streaming_split
   Datastream.train_test_split
   Datastream.union
   Datastream.zip

Grouped and Global Aggregations
-------------------------------

.. autosummary::
   :toctree: doc/

   Datastream.groupby
   Datastream.aggregate
   Datastream.sum
   Datastream.min
   Datastream.max
   Datastream.mean
   Datastream.std

Converting to Pipeline
----------------------

.. autosummary::
   :toctree: doc/

   Datastream.repeat
   Datastream.window

Consuming Data
---------------------

.. autosummary::
   :toctree: doc/

   Datastream.show
   Datastream.take
   Datastream.take_batch
   Datastream.take_all
   Datastream.iterator
   Datastream.iter_rows
   Datastream.iter_batches
   Datastream.iter_torch_batches
   Datastream.iter_tf_batches

I/O and Conversion
------------------

.. autosummary::
   :toctree: doc/

   Datastream.write_parquet
   Datastream.write_json
   Datastream.write_csv
   Datastream.write_numpy
   Datastream.write_tfrecords
   Datastream.write_webdataset
   Datastream.write_mongo
   Datastream.write_datasource
   Datastream.to_torch
   Datastream.to_tf
   Datastream.to_dask
   Datastream.to_mars
   Datastream.to_modin
   Datastream.to_spark
   Datastream.to_pandas
   Datastream.to_pandas_refs
   Datastream.to_numpy_refs
   Datastream.to_arrow_refs
   Datastream.to_random_access_dataset

Inspecting Metadata
-------------------

.. autosummary::
   :toctree: doc/

   Datastream.count
   Datastream.schema
   Datastream.default_batch_format
   Datastream.num_blocks
   Datastream.size_bytes
   Datastream.input_files
   Datastream.stats
   Datastream.get_internal_block_refs

Execution
---------

.. autosummary::
    :toctree: doc/

    Datastream.materialize
    ActorPoolStrategy

Serialization
-------------

.. autosummary::
   :toctree: doc/

   Datastream.has_serializable_lineage
   Datastream.serialize_lineage
   Datastream.deserialize_lineage

Internals
---------

.. autosummary::
   :toctree: doc/

   Datastream.__init__
   Datastream.dataset_format
   Datastream.fully_executed
   Datastream.is_fully_executed
   Datastream.lazy
