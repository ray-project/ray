Dataset
-------

.. currentmodule:: ray.data

.. autoclass:: Dataset







Basic Transformations
---------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.add_column
   
      Dataset.drop_columns
   
      Dataset.filter
   
      Dataset.flat_map
   
      Dataset.limit
   
      Dataset.map
   
      Dataset.map_batches
   
      Dataset.random_sample
   
      Dataset.select_columns
   




Consuming Data
--------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.iter_batches
   
      Dataset.iter_rows
   
      Dataset.iter_tf_batches
   
      Dataset.iter_torch_batches
   
      Dataset.iterator
   
      Dataset.show
   
      Dataset.take
   
      Dataset.take_all
   
      Dataset.take_batch
   




Execution
---------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.materialize
   




Grouped and Global aggregations
-------------------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.aggregate
   
      Dataset.groupby
   
      Dataset.max
   
      Dataset.mean
   
      Dataset.min
   
      Dataset.std
   
      Dataset.sum
   
      Dataset.unique
   




I/O and Conversion
------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.to_dask
   
      Dataset.to_mars
   
      Dataset.to_modin
   
      Dataset.to_pandas
   
      Dataset.to_spark
   
      Dataset.to_tf
   
      Dataset.to_torch
   
      Dataset.write_csv
   
      Dataset.write_images
   
      Dataset.write_json
   
      Dataset.write_mongo
   
      Dataset.write_numpy
   
      Dataset.write_parquet
   
      Dataset.write_tfrecords
   
      Dataset.write_webdataset
   




Inspecting Metadata
-------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.columns
   
      Dataset.count
   
      Dataset.input_files
   
      Dataset.num_blocks
   
      Dataset.schema
   
      Dataset.size_bytes
   
      Dataset.stats
   




Sorting, Shuffling and Repartitioning
-------------------------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.random_shuffle
   
      Dataset.randomize_block_order
   
      Dataset.repartition
   
      Dataset.sort
   




Splitting and Merging datasets
------------------------------


.. autosummary::
   :nosignatures:
   :toctree: doc

   
      Dataset.split
   
      Dataset.split_at_indices
   
      Dataset.split_proportionately
   
      Dataset.streaming_split
   
      Dataset.train_test_split
   
      Dataset.union
   
      Dataset.zip
   



