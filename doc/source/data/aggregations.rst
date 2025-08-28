.. _aggregations:

Aggregations
============

Ray Data provides a flexible and performant API for performing aggregations on :class:`~ray.data.dataset.Dataset`. 

Basic Aggregations
------------------

Ray Data provides several built-in aggregation functions like 
* :class:`~ray.data.aggregate.Count`, * :class:`~ray.data.aggregate.Sum`, * :class:`~ray.data.aggregate.Mean`,
* :class:`~ray.data.aggregate.Min`, * :class:`~ray.data.aggregate.Max`, * :class:`~ray.data.aggregate.Std`,
* :class:`~ray.data.aggregate.Quantile`
 
These can be used directly with datasets like shown below:

.. testcode::

    import ray
    from ray.data.aggregate import Count, Mean, Quantile

    # Create a sample dataset
    ds = ray.data.range(100)
    ds = ds.add_column("group_key", lambda x: x % 3)

    # Count all rows
    result = ds.aggregate(Count())
    # result: {'count()': 100}

    # Calculate mean per group
    result = ds.groupby("group_key").aggregate(Mean(on="id")).take_all()
    # result: [{'group_key': 0, 'mean(id)': ...},
    #          {'group_key': 1, 'mean(id)': ...},
    #          {'group_key': 2, 'mean(id)': ...}]

    # Calculate 75th percentile
    result = ds.aggregate(Quantile(on="id", q=0.75))
    # result: {'quantile(id)': 75.0}


Using Multiple Aggregations
---------------------------

Each of the preceding methods also has a corresponding :ref:`AggregateFnV2 <aggregations_api_ref>` object. These objects can be used in
:meth:`~ray.data.Dataset.aggregate()` or :meth:`Dataset.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>` to compute multiple aggregations at once.

.. testcode::

    import ray
    from ray.data.aggregate import Count, Mean, Min, Max, Std

    ds = ray.data.range(100)
    ds = ds.add_column("group_key", lambda x: x % 3)

    # Compute multiple aggregations at once
    result = ds.groupby("group_key").aggregate(
        Count(on="id"),
        Mean(on="id"),
        Min(on="id"),
        Max(on="id"),
        Std(on="id")
    ).take_all()
    # result: [{'group_key': 0, 'count(id)': 34, 'mean(id)': ..., 'min(id)': ..., 'max(id)': ..., 'std(id)': ...},
    #          {'group_key': 1, 'count(id)': 33, 'mean(id)': ..., 'min(id)': ..., 'max(id)': ..., 'std(id)': ...},
    #          {'group_key': 2, 'count(id)': 33, 'mean(id)': ..., 'min(id)': ..., 'max(id)': ..., 'std(id)': ...}]
    

Custom Aggregations
--------------------

For more complex aggregation needs, Ray Data allows you to create custom aggregations by implementing the :class:`~ray.data.aggregate.AggregateFnV2` interface. The AggregateFnV2 interface provides a framework for implementing distributed aggregations with three key methods:

1. `aggregate_block`: Processes a single block of data and returns a partial aggregation result
2. `combine`: Merges two partial aggregation results into a single result
3. `_finalize`: Transforms the final accumulated result into the desired output format

The aggregation process follows these steps:

1. **Initialization**: For each group (if grouping) or for the entire dataset, an initial accumulator is created using `zero_factory`
2. **Block Aggregation**: The `aggregate_block` method is applied to each block independently
3. **Combination**: The `combine` method merges partial results into a single accumulator
4. **Finalization**: The `_finalize` method transforms the final accumulator into the desired output

Example: Creating a Custom Mean Aggregator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's an example of creating a custom aggregator that calculates the Mean of values in a column:

.. testcode::

    import numpy as np
    from ray.data.aggregate import AggregateFnV2
    from ray.data._internal.util import is_null
    from ray.data.block import Block, BlockAccessor, AggType, U
    import pyarrow.compute as pc
    from typing import List, Optional

    class Mean(AggregateFnV2):
        """Defines mean aggregation."""

        def __init__(
            self,
            on: Optional[str] = None,
            ignore_nulls: bool = True,
            alias_name: Optional[str] = None,
        ):
            super().__init__(
                alias_name if alias_name else f"mean({str(on)})",
                on=on,
                ignore_nulls=ignore_nulls,
                # NOTE: We've to copy returned list here, as some
                #       aggregations might be modifying elements in-place
                zero_factory=lambda: list([0, 0]),  # noqa: C410
            )

        def aggregate_block(self, block: Block) -> AggType:
            block_acc = BlockAccessor.for_block(block)
            count = block_acc.count(self._target_col_name, self._ignore_nulls)

            if count == 0 or count is None:
                # Empty or all null.
                return None

            sum_ = block_acc.sum(self._target_col_name, self._ignore_nulls)

            if is_null(sum_):
                # In case of ignore_nulls=False and column containing 'null'
                # return as is (to prevent unnecessary type conversions, when, for ex,
                # using Pandas and returning None)
                return sum_

            return [sum_, count]

        def combine(self, current_accumulator: AggType, new: AggType) -> AggType:
            return [current_accumulator[0] + new[0], current_accumulator[1] + new[1]]

        def _finalize(self, accumulator: AggType) -> Optional[U]:
            if accumulator[1] == 0:
                return np.nan

            return accumulator[0] / accumulator[1]


.. note::
    Internally, aggregations support both the :ref:`hash-shuffle backend <hash-shuffle>` and the :ref:`range based backend <range-partitioning-shuffle>`.

    Hash-shuffling can provide better performance for aggregations in certain cases. For more information see `comparison between hash based shuffling and Range Based shuffling approach <https://www.anyscale.com/blog/ray-data-joins-hash-shuffle#performance-benchmarks/>`_ .

    To use the hash-shuffle algorithm for aggregations, you need to set the shuffle strategy explicitly:    
    ``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE`` before creating a ``Dataset``
    
