.. _dataset-api:

Dataset API
===========

.. autoclass:: ray.data.Dataset

**Basic Transformations**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.map
    ray.data.Dataset.map_batches
    ray.data.Dataset.flat_map
    ray.data.Dataset.filter
    ray.data.Dataset.add_column
    ray.data.Dataset.drop_columns
    ray.data.Dataset.select_columns
    ray.data.Dataset.random_sample
    ray.data.Dataset.limit

**Sorting, Shuffling, Repartitioning**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.sort
    ray.data.Dataset.random_shuffle
    ray.data.Dataset.randomize_block_order
    ray.data.Dataset.repartition

**Splitting and Merging Datasets**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.split
    ray.data.Dataset.split_at_indices
    ray.data.Dataset.split_proportionately
    ray.data.Dataset.train_test_split
    ray.data.Dataset.union
    ray.data.Dataset.zip

**Grouped and Global Aggregations**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.groupby
    ray.data.Dataset.aggregate
    ray.data.Dataset.sum
    ray.data.Dataset.min
    ray.data.Dataset.max
    ray.data.Dataset.mean
    ray.data.Dataset.std

**Converting to Pipelines**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.repeat
    ray.data.Dataset.window

**Consuming Datasets**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.show
    ray.data.Dataset.take
    ray.data.Dataset.take_all
    ray.data.Dataset.iterator

**I/O and Conversion**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.write_parquet
    ray.data.Dataset.write_json
    ray.data.Dataset.write_csv
    ray.data.Dataset.write_numpy
    ray.data.Dataset.write_datasource
    ray.data.Dataset.to_torch
    ray.data.Dataset.to_tf
    ray.data.Dataset.to_dask
    ray.data.Dataset.to_mars
    ray.data.Dataset.to_modin
    ray.data.Dataset.to_spark
    ray.data.Dataset.to_pandas
    ray.data.Dataset.to_pandas_refs
    ray.data.Dataset.to_numpy_refs
    ray.data.Dataset.to_arrow_refs
    ray.data.Dataset.to_random_access_dataset

**Inspecting Metadata**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.count
    ray.data.Dataset.schema
    ray.data.Dataset.default_batch_format
    ray.data.Dataset.num_blocks
    ray.data.Dataset.size_bytes
    ray.data.Dataset.input_files
    ray.data.Dataset.stats
    ray.data.Dataset.get_internal_block_refs

**Execution**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.fully_executed
    ray.data.Dataset.is_fully_executed
    ray.data.Dataset.lazy

**Serialization**

.. autosummary::
    :nosignatures:

    ray.data.Dataset.has_serializable_lineage
    ray.data.Dataset.serialize_lineage
    ray.data.Dataset.deserialize_lineage

Basic Transformations
---------------------

.. automethod:: ray.data.Dataset.map

.. automethod:: ray.data.Dataset.map_batches

.. automethod:: ray.data.Dataset.flat_map

.. automethod:: ray.data.Dataset.filter

.. automethod:: ray.data.Dataset.add_column

.. automethod:: ray.data.Dataset.drop_columns

.. automethod:: ray.data.Dataset.select_columns

.. automethod:: ray.data.Dataset.random_sample

.. automethod:: ray.data.Dataset.limit

Sorting, Shuffling, Repartitioning
----------------------------------

.. automethod:: ray.data.Dataset.sort

.. automethod:: ray.data.Dataset.random_shuffle

.. automethod:: ray.data.Dataset.randomize_block_order

.. automethod:: ray.data.Dataset.repartition

Splitting and Merging Datasets
------------------------------

.. automethod:: ray.data.Dataset.split

.. automethod:: ray.data.Dataset.split_at_indices

.. automethod:: ray.data.Dataset.split_proportionately

.. automethod:: ray.data.Dataset.train_test_split

.. automethod:: ray.data.Dataset.union

.. automethod:: ray.data.Dataset.zip

Grouped and Global Aggregations
-------------------------------

.. automethod:: ray.data.Dataset.groupby

.. automethod:: ray.data.Dataset.aggregate

.. automethod:: ray.data.Dataset.sum

.. automethod:: ray.data.Dataset.min

.. automethod:: ray.data.Dataset.max

.. automethod:: ray.data.Dataset.mean

.. automethod:: ray.data.Dataset.std

Converting to Pipeline
----------------------

.. automethod:: ray.data.Dataset.repeat

.. automethod:: ray.data.Dataset.window

Consuming Datasets
------------------

.. automethod:: ray.data.Dataset.show

.. automethod:: ray.data.Dataset.take

.. automethod:: ray.data.Dataset.take_all

.. automethod:: ray.data.Dataset.iterator

.. automethod:: ray.data.Dataset.iter_rows

.. automethod:: ray.data.Dataset.iter_batches

.. automethod:: ray.data.Dataset.iter_torch_batches

.. automethod:: ray.data.Dataset.iter_tf_batches

I/O and Conversion
------------------

.. automethod:: ray.data.Dataset.write_parquet

.. automethod:: ray.data.Dataset.write_json

.. automethod:: ray.data.Dataset.write_csv

.. automethod:: ray.data.Dataset.write_numpy

.. automethod:: ray.data.Dataset.write_datasource

.. automethod:: ray.data.Dataset.to_torch

.. automethod:: ray.data.Dataset.to_tf

.. automethod:: ray.data.Dataset.to_dask

.. automethod:: ray.data.Dataset.to_mars

.. automethod:: ray.data.Dataset.to_modin

.. automethod:: ray.data.Dataset.to_spark

.. automethod:: ray.data.Dataset.to_pandas

.. automethod:: ray.data.Dataset.to_pandas_refs

.. automethod:: ray.data.Dataset.to_numpy_refs

.. automethod:: ray.data.Dataset.to_arrow_refs

.. automethod:: ray.data.Dataset.to_random_access_dataset

Inspecting Metadata
-------------------

.. automethod:: ray.data.Dataset.count

.. automethod:: ray.data.Dataset.schema

.. automethod:: ray.data.Dataset.default_batch_format

.. automethod:: ray.data.Dataset.num_blocks

.. automethod:: ray.data.Dataset.size_bytes

.. automethod:: ray.data.Dataset.input_files

.. automethod:: ray.data.Dataset.stats

.. automethod:: ray.data.Dataset.get_internal_block_refs

Execution
---------

.. automethod:: ray.data.Dataset.fully_executed

.. automethod:: ray.data.Dataset.is_fully_executed

.. automethod:: ray.data.Dataset.lazy

Serialization
-------------

.. automethod:: ray.data.Dataset.has_serializable_lineage

.. automethod:: ray.data.Dataset.serialize_lineage

.. automethod:: ray.data.Dataset.deserialize_lineage
