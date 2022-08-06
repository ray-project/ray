.. _dataset-pipeline-api:

DatasetPipeline API
===================

.. autoclass:: ray.data.dataset_pipeline.DatasetPipeline

Map transformations
-------------------

.. automethod:: ray.data.DatasetPipeline.map

.. automethod:: ray.data.DatasetPipeline.map_batches

.. automethod:: ray.data.DatasetPipeline.flat_map

.. automethod:: ray.data.DatasetPipeline.filter

.. automethod:: ray.data.DatasetPipeline.add_column

.. automethod:: ray.data.DatasetPipeline.drop_columns

Shuffling transformations
-------------------------

.. automethod:: ray.data.DatasetPipeline.randomize_block_order_each_window

.. automethod:: ray.data.DatasetPipeline.random_shuffle_each_window

Splitting and Merging DatasetPipelines
--------------------------------------

.. automethod:: ray.data.DatasetPipeline.split

.. automethod:: ray.data.DatasetPipeline.split_at_indices

Accessing DatasetPipelines
--------------------------

.. automethod:: ray.data.DatasetPipeline.iter_rows

.. automethod:: ray.data.DatasetPipeline.iter_batches

.. automethod:: ray.data.DatasetPipeline.iter_epochs

.. automethod:: ray.data.DatasetPipeline.iter_tf_batches

.. automethod:: ray.data.DatasetPipeline.iter_torch_batches

.. automethod:: ray.data.DatasetPipeline.iter_datasets



Other
-----

.. TODO put these in the right section.

.. automethod:: ray.data.DatasetPipeline.rewindow

.. automethod:: ray.data.DatasetPipeline.repeat

.. automethod:: ray.data.DatasetPipeline.schema

.. automethod:: ray.data.DatasetPipeline.count

.. automethod:: ray.data.DatasetPipeline.sum

.. automethod:: ray.data.DatasetPipeline.show_windows

.. automethod:: ray.data.DatasetPipeline.repartition_each_window

.. automethod:: ray.data.DatasetPipeline.sort_each_window

.. automethod:: ray.data.DatasetPipeline.write_json

.. automethod:: ray.data.DatasetPipeline.write_csv

.. automethod:: ray.data.DatasetPipeline.write_parquet

.. automethod:: ray.data.DatasetPipeline.write_datasource

.. automethod:: ray.data.DatasetPipeline.take

.. automethod:: ray.data.DatasetPipeline.take_all

.. automethod:: ray.data.DatasetPipeline.show

.. automethod:: ray.data.DatasetPipeline.to_tf

.. automethod:: ray.data.DatasetPipeline.to_torch

.. automethod:: ray.data.DatasetPipeline.foreach_window

.. automethod:: ray.data.DatasetPipeline.stats

.. automethod:: ray.data.DatasetPipeline.from_iterable