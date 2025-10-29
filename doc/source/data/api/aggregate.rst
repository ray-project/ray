.. _aggregations_api_ref:

Aggregation API
===============

Pass :class:`AggregateFnV2 <ray.data.aggregate.AggregateFnV2>` objects to
:meth:`Dataset.aggregate() <ray.data.Dataset.aggregate>` or 
:meth:`Dataset.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>` to 
compute aggregations.

.. currentmodule:: ray.data.aggregate

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AggregateFnV2
    AggregateFn
    Count
    Sum
    Min
    Max
    Mean
    Std
    AbsMax
    Quantile
    Unique
    ValueCounter
    MissingValuePercentage
    ZeroPercentage
    ApproximateQuantile
    ApproximateTopK