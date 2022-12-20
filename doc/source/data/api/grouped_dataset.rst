.. _grouped-dataset-api:

GroupedDataset API
==================

.. currentmodule:: ray.data

GroupedDataset objects are returned by groupby call: Dataset.groupby().

Constructor
-----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   GroupedDataset

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   GroupedDataset.count
   GroupedDataset.sum
   GroupedDataset.min
   GroupedDataset.max
   GroupedDataset.mean
   GroupedDataset.std

Function Application
--------------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   GroupedDataset.aggregate
   GroupedDataset.map_groups

Aggregate Function
------------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   aggregate.AggregateFn
   aggregate.Count
   aggregate.Sum
   aggregate.Max
   aggregate.Mean
   aggregate.Std
   aggregate.AbsMax
