.. _dataset-pipeline-api:

DatasetPipeline API
===================

.. currentmodule:: ray.data

Constructor
-----------

.. autosummary::
   :toctree: doc/

   DatasetPipeline

Basic Transformations
---------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.map
   DatasetPipeline.map_batches
   DatasetPipeline.flat_map
   DatasetPipeline.foreach_window
   DatasetPipeline.filter
   DatasetPipeline.add_column
   DatasetPipeline.drop_columns
   DatasetPipeline.select_columns

Sorting, Shuffling, Repartitioning
----------------------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.sort_each_window
   DatasetPipeline.random_shuffle_each_window
   DatasetPipeline.randomize_block_order_each_window
   DatasetPipeline.repartition_each_window

Splitting DatasetPipelines
--------------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.split
   DatasetPipeline.split_at_indices

Creating DatasetPipelines
-------------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.repeat
   DatasetPipeline.rewindow
   DatasetPipeline.from_iterable

Consuming DatasetPipelines
--------------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.show
   DatasetPipeline.show_windows
   DatasetPipeline.take
   DatasetPipeline.take_all
   DatasetPipeline.iterator
   DatasetPipeline.iter_rows
   DatasetPipeline.iter_batches
   DatasetPipeline.iter_torch_batches
   DatasetPipeline.iter_tf_batches

I/O and Conversion
------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.write_json
   DatasetPipeline.write_csv
   DatasetPipeline.write_parquet
   DatasetPipeline.write_datasource
   DatasetPipeline.to_tf
   DatasetPipeline.to_torch

Inspecting Metadata
-------------------

.. autosummary::
   :toctree: doc/

   DatasetPipeline.schema
   DatasetPipeline.count
   DatasetPipeline.stats
   DatasetPipeline.sum
