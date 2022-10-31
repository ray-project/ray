.. _dataset-pipeline-api:

DatasetPipeline API
===================

.. autoclass:: ray.data.dataset_pipeline.DatasetPipeline

**Basic Transformations**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.map
    ray.data.DatasetPipeline.map_batches
    ray.data.DatasetPipeline.flat_map
    ray.data.DatasetPipeline.foreach_window
    ray.data.DatasetPipeline.filter
    ray.data.DatasetPipeline.add_column
    ray.data.DatasetPipeline.drop_columns
    ray.data.DatasetPipeline.select_columns

**Sorting, Shuffling, Repartitioning**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.sort_each_window
    ray.data.DatasetPipeline.random_shuffle_each_window
    ray.data.DatasetPipeline.randomize_block_order_each_window
    ray.data.DatasetPipeline.repartition_each_window

**Splitting DatasetPipelines**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.split
    ray.data.DatasetPipeline.split_at_indices

**Creating DatasetPipelines**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.repeat
    ray.data.DatasetPipeline.rewindow
    ray.data.DatasetPipeline.from_iterable

**Consuming DatasetPipelines**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.show
    ray.data.DatasetPipeline.show_windows
    ray.data.DatasetPipeline.take
    ray.data.DatasetPipeline.take_all
    ray.data.DatasetPipeline.iter_rows
    ray.data.DatasetPipeline.iter_batches
    ray.data.DatasetPipeline.iter_torch_batches
    ray.data.DatasetPipeline.iter_tf_batches

**I/O and Conversion**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.write_json
    ray.data.DatasetPipeline.write_csv
    ray.data.DatasetPipeline.write_parquet
    ray.data.DatasetPipeline.write_datasource
    ray.data.DatasetPipeline.to_tf
    ray.data.DatasetPipeline.to_torch

**Inspecting Metadata**

.. autosummary::
    :nosignatures:

    ray.data.DatasetPipeline.schema
    ray.data.DatasetPipeline.count
    ray.data.DatasetPipeline.stats
    ray.data.DatasetPipeline.sum

Basic transformations
---------------------

.. automethod:: ray.data.DatasetPipeline.map

.. automethod:: ray.data.DatasetPipeline.map_batches

.. automethod:: ray.data.DatasetPipeline.flat_map

.. automethod:: ray.data.DatasetPipeline.foreach_window

.. automethod:: ray.data.DatasetPipeline.filter

.. automethod:: ray.data.DatasetPipeline.add_column

.. automethod:: ray.data.DatasetPipeline.drop_columns

.. automethod:: ray.data.DatasetPipeline.select_columns

Sorting, Shuffling, Repartitioning
----------------------------------

.. automethod:: ray.data.DatasetPipeline.sort_each_window

.. automethod:: ray.data.DatasetPipeline.random_shuffle_each_window

.. automethod:: ray.data.DatasetPipeline.randomize_block_order_each_window

.. automethod:: ray.data.DatasetPipeline.repartition_each_window

Splitting DatasetPipelines
--------------------------

.. automethod:: ray.data.DatasetPipeline.split

.. automethod:: ray.data.DatasetPipeline.split_at_indices

Creating DatasetPipelines
-------------------------

.. automethod:: ray.data.DatasetPipeline.repeat

.. automethod:: ray.data.DatasetPipeline.rewindow

.. automethod:: ray.data.DatasetPipeline.from_iterable

Consuming DatasetPipelines
--------------------------

.. automethod:: ray.data.DatasetPipeline.show

.. automethod:: ray.data.DatasetPipeline.show_windows

.. automethod:: ray.data.DatasetPipeline.take

.. automethod:: ray.data.DatasetPipeline.take_all

.. automethod:: ray.data.DatasetPipeline.iter_rows

.. automethod:: ray.data.DatasetPipeline.iter_batches

.. automethod:: ray.data.DatasetPipeline.iter_epochs

.. automethod:: ray.data.DatasetPipeline.iter_tf_batches

.. automethod:: ray.data.DatasetPipeline.iter_torch_batches

.. automethod:: ray.data.DatasetPipeline.iter_datasets


I/O and Conversion
------------------

.. automethod:: ray.data.DatasetPipeline.write_json

.. automethod:: ray.data.DatasetPipeline.write_csv

.. automethod:: ray.data.DatasetPipeline.write_parquet

.. automethod:: ray.data.DatasetPipeline.write_datasource

.. automethod:: ray.data.DatasetPipeline.to_tf

.. automethod:: ray.data.DatasetPipeline.to_torch


Inspecting Metadata
-------------------

.. automethod:: ray.data.DatasetPipeline.schema

.. automethod:: ray.data.DatasetPipeline.count

.. automethod:: ray.data.DatasetPipeline.stats

.. automethod:: ray.data.DatasetPipeline.sum