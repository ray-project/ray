.. _grouped-dataset-api:

GroupedDatastream API
=====================

.. currentmodule:: ray.data

GroupedDatastream objects are returned by groupby call: Dataset.groupby().

Constructor
-----------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDatastream

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDatastream.count
   grouped_dataset.GroupedDatastream.sum
   grouped_dataset.GroupedDatastream.min
   grouped_dataset.GroupedDatastream.max
   grouped_dataset.GroupedDatastream.mean
   grouped_dataset.GroupedDatastream.std

Function Application
--------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDatastream.aggregate
   grouped_dataset.GroupedDatastream.map_groups

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
