.. _dataset-api:

Dataset API
==============

.. currentmodule:: ray.data

Constructor
-----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset

Basic Transformations
---------------------

.. autosummary::
   :nosignatures:
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
   :nosignatures:
   :toctree: doc/

   Dataset.sort
   Dataset.random_shuffle
   Dataset.randomize_block_order
   Dataset.repartition

Splitting and Merging Datasets
---------------------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.split
   Dataset.split_at_indices
   Dataset.split_proportionately
   Dataset.streaming_split
   Dataset.train_test_split
   Dataset.union
   Dataset.zip

Grouped and Global Aggregations
-------------------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.groupby
   Dataset.unique
   Dataset.aggregate
   Dataset.sum
   Dataset.min
   Dataset.max
   Dataset.mean
   Dataset.std

Consuming Data
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.show
   Dataset.take
   Dataset.take_batch
   Dataset.take_all
   Dataset.iterator
   Dataset.iter_rows
   Dataset.iter_batches
   Dataset.iter_torch_batches
   Dataset.iter_tf_batches

I/O and Conversion
------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_parquet
   Dataset.write_json
   Dataset.write_csv
   Dataset.write_numpy
   Dataset.write_tfrecords
   Dataset.write_webdataset
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
   :nosignatures:
   :toctree: doc/

   Dataset.count
   Dataset.columns
   Dataset.schema
   Dataset.num_blocks
   Dataset.size_bytes
   Dataset.input_files
   Dataset.stats
   Dataset.get_internal_block_refs

Execution
---------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Dataset.materialize
    ActorPoolStrategy

Serialization
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.has_serializable_lineage
   Dataset.serialize_lineage
   Dataset.deserialize_lineage

.. _block-api:

Internals
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   block.Block
   block.BlockExecStats
   block.BlockMetadata
   block.BlockAccessor
