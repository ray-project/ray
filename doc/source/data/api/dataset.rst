.. _dataset-api:

Dataset API
==============

.. include:: ray.data.Dataset.rst


Schema
------
.. currentmodule:: ray.data

.. autoclass:: Schema
    :members:



Developer API
-------------

.. currentmodule:: ray.data

.. autosummary::
  :nosignatures:
  :toctree: doc/

  Dataset.to_pandas_refs
  Dataset.to_numpy_refs
  Dataset.to_arrow_refs
  Dataset.iter_internal_ref_bundles
  block.Block
  block.BlockExecStats
  block.BlockMetadata
  block.BlockAccessor

Deprecated API
--------------

.. currentmodule:: ray.data

.. autosummary::
  :nosignatures:
  :toctree: doc/

  Dataset.iter_tf_batches
  Dataset.to_torch
