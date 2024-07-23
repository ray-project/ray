.. _grouped-dataset-api:

GroupedData API
===============

.. currentmodule:: ray.data

GroupedData objects are returned by groupby call: 
:meth:`Dataset.groupby() <ray.data.Dataset.groupby>`.

Constructor
-----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   grouped_data.GroupedData

Computations / Descriptive Stats
--------------------------------

.. autosummary::
   :nosignatures:
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
   :nosignatures:
   :toctree: doc/

   grouped_data.GroupedData.aggregate
   grouped_data.GroupedData.map_groups

Aggregate Function
------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   aggregate.AggregateFn
