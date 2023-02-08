.. _grouped-dataset-api:

GroupedDataset API
==================

.. currentmodule:: ray.data

GroupedDataset objects are returned by groupby call: Dataset.groupby().

Constructor
-----------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDataset

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDataset.count
   grouped_dataset.GroupedDataset.sum
   grouped_dataset.GroupedDataset.min
   grouped_dataset.GroupedDataset.max
   grouped_dataset.GroupedDataset.mean
   grouped_dataset.GroupedDataset.std

Function Application
--------------------

.. autosummary::
   :toctree: doc/

   grouped_dataset.GroupedDataset.aggregate
   grouped_dataset.GroupedDataset.map_groups

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
