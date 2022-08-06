.. _dataset-api:

Dataset API
===========

.. autoclass:: ray.data.Dataset

Basic Transformations
---------------------

.. automethod:: ray.data.Dataset.map

.. automethod:: ray.data.Dataset.map_batches

.. automethod:: ray.data.Dataset.flat_map

.. automethod:: ray.data.Dataset.filter

.. automethod:: ray.data.Dataset.add_column

.. automethod:: ray.data.Dataset.drop_columns

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

Accessing Datasets
------------------

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


Other
-----

.. TODO put these in the right section.

.. automethod:: ray.data.Dataset.take

.. automethod:: ray.data.Dataset.take_all

.. automethod:: ray.data.Dataset.show

.. automethod:: ray.data.Dataset.count

.. automethod:: ray.data.Dataset.schema

.. automethod:: ray.data.Dataset.num_blocks

.. automethod:: ray.data.Dataset.size_bytes

.. automethod:: ray.data.Dataset.input_files
    
.. automethod:: ray.data.Dataset.fully_executed
    
.. automethod:: ray.data.Dataset.is_fully_executed
    
.. automethod:: ray.data.Dataset.stats
    
.. automethod:: ray.data.Dataset.get_internal_block_refs
    
.. automethod:: ray.data.Dataset.lazy
    
.. automethod:: ray.data.Dataset.has_serializable_lineage
    
.. automethod:: ray.data.Dataset.serialize_lineage
    
.. automethod:: ray.data.Dataset.deserialize_lineage