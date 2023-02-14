.. _dataset-api:

Dataset API
===========

.. currentmodule:: ray.data

Constructor
-----------

.. autosummary::
   :toctree: doc/

   Dataset

Basic Transformations
---------------------

.. autosummary::
   :toctree: doc/

   Dataset.map
   Dataset.map_batches
   Dataset.flat_map
   Dataset.filter
   Dataset.add_column
   Dataset.drop_columns
   Dataset.select_columns
   Dataset.random_sample
   Dataset.limit

Sorting, Shuffling, Repartitioning
----------------------------------

.. autosummary::
   :toctree: doc/

   Dataset.sort
   Dataset.random_shuffle
   Dataset.randomize_block_order
   Dataset.repartition

Splitting and Merging Datasets
------------------------------

.. autosummary::
   :toctree: doc/

   Dataset.split
   Dataset.split_at_indices
   Dataset.split_proportionately
   Dataset.train_test_split
   Dataset.union
   Dataset.zip

Grouped and Global Aggregations
-------------------------------

.. autosummary::
   :toctree: doc/

   Dataset.groupby
   Dataset.aggregate
   Dataset.sum
   Dataset.min
   Dataset.max
   Dataset.mean
   Dataset.std

Converting to Pipeline
----------------------

.. autosummary::
   :toctree: doc/

   Dataset.repeat
   Dataset.window

Consuming Datasets
------------------

.. autosummary::
   :toctree: doc/

   Dataset.show
   Dataset.take
   Dataset.take_all
   Dataset.iterator
   Dataset.iter_rows
   Dataset.iter_batches
   Dataset.iter_torch_batches
   Dataset.iter_tf_batches

I/O and Conversion
------------------

.. autosummary::
   :toctree: doc/

   Dataset.write_parquet
   Dataset.write_json
   Dataset.write_csv
   Dataset.write_numpy
   Dataset.write_tfrecords
   Dataset.write_mongo
   Dataset.write_datasource
   Dataset.to_torch
   Dataset.to_tf
   Dataset.to_dask
   Dataset.to_mars
   Dataset.to_modin
   Dataset.to_spark
   Dataset.to_pandas
   Dataset.to_pandas_refs
   Dataset.to_numpy_refs
   Dataset.to_arrow_refs
   Dataset.to_random_access_dataset

Inspecting Metadata
-------------------

.. autosummary::
   :toctree: doc/

   Dataset.count
   Dataset.schema
   Dataset.default_batch_format
   Dataset.num_blocks
   Dataset.size_bytes
   Dataset.input_files
   Dataset.stats
   Dataset.get_internal_block_refs

Execution
---------

.. autosummary::
   :toctree: doc/

   Dataset.fully_executed
   Dataset.is_fully_executed
   Dataset.lazy

Serialization
-------------

.. autosummary::
   :toctree: doc/

   Dataset.has_serializable_lineage
   Dataset.serialize_lineage
   Dataset.deserialize_lineage
