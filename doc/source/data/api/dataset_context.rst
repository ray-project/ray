.. _dataset-context-api:

DatasetContext API
==================

.. currentmodule:: ray.data

Constructor
-----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   context.DatasetContext

Attributes
----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   context.DatasetContext.target_max_block_size
   context.DatasetContext.target_min_block_size
   context.DatasetContext.streaming_read_buffer_size
   context.DatasetContext.min_parallelism

Get DatasetContext
----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   context.DatasetContext.get_current
