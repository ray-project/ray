.. _grouped-dataset-api:

GroupedData API
=====================

.. currentmodule:: ray.data

GroupedData objects are returned by groupby call: Dataset.groupby().

Constructor
-----------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedData

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedData.count
   grouped_dataset.GroupedData.sum
   grouped_dataset.GroupedData.min
   grouped_dataset.GroupedData.max
   grouped_dataset.GroupedData.mean
   grouped_dataset.GroupedData.std

Function Application
--------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedData.aggregate
   grouped_dataset.GroupedData.map_groups

Aggregate Function
------------------

.. autosummary::
   :toctree: doc/

   aggregate.AggregateFn
   aggregate.Count
   aggregate.Sum
   aggregate.Max
   aggregate.Mean
   aggregate.Std
   aggregate.AbsMax
