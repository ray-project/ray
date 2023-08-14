.. _grouped-dataset-api:

GroupedData API
===============

.. currentmodule:: ray.data

GroupedData objects are returned by groupby call: Dataset.groupby().

Constructor
-----------

.. autosummary::
   :toctree: doc/

   grouped_data.GroupedData

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :toctree: doc/

   grouped_data.GroupedData.count
   grouped_data.GroupedData.sum
   grouped_data.GroupedData.min
   grouped_data.GroupedData.max
   grouped_data.GroupedData.mean
   grouped_data.GroupedData.std

Function Application
--------------------

.. autosummary::
   :toctree: doc/

   grouped_data.GroupedData.aggregate
   grouped_data.GroupedData.map_groups

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
