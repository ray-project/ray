.. _aggregations:

==============
Aggregations
==============

Ray Data provides a flexible and performant API for performing aggregations on :class:`~ray.data.dataset.Dataset`. 
Internally, aggregations are powered by the :ref:`hash-shuffle backend <hash-shuffle>`.

.. note::
    To use the hash-shuffle algorithm for aggregations, you need to set the shuffle strategy explicitly:
    
    .. testcode::
        
        from ray.data.context import DataContext, ShuffleStrategy
        
        # Set hash-shuffle as the shuffle strategy
        DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

Basic Aggregations
-----------------

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
-------------------------

You can pass multiple aggregation functions to compute several metrics at once:

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
------------------

For more complex aggregation needs, Ray Data allows you to create custom aggregations by implementing the :class:`~ray.data.aggregate.AggregateFnV2` interface. The AggregateFnV2 interface provides a framework for implementing distributed aggregations with three key methods:

1. `aggregate_block`: Processes a single block of data and returns a partial aggregation result
2. `combine`: Merges two partial aggregation results into a single result
3. `_finalize`: Transforms the final accumulated result into the desired output format

The aggregation process follows these steps:

1. **Initialization**: For each group (if grouping) or for the entire dataset, an initial accumulator is created using `zero_factory`
2. **Block Aggregation**: The `aggregate_block` method is applied to each block independently
3. **Combination**: The `combine` method merges partial results into a single accumulator
4. **Finalization**: The `_finalize` method transforms the final accumulator into the desired output

Example: Creating a Missing Value Percentage Aggregator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's an example of creating a custom aggregator that calculates the percentage of null values in a column:

.. testcode::

    from ray.data.aggregate import AggregateFnV2
    from ray.data.block import Block, BlockAccessor
    import pyarrow.compute as pc
    from typing import List, Optional

    class MissingValuePercentage(AggregateFnV2):
        def __init__(self, on: str):
            # Initialize with a list accumulator [null_count, total_count]
            super().__init__(
                f"missing_pct({on})",
                on=on,
                ignore_nulls=False,
                zero_factory=lambda: [0, 0]
            )

        def aggregate_block(self, block: Block) -> List[int]:
            # Process a single block of data
            block_acc = BlockAccessor.for_block(block)
            table = block_acc.to_arrow()
            column = table.column(self._target_col_name)
            
            total_count = len(column)
            null_count = pc.sum(pc.is_null(column, nan_is_null=True).cast("int32")).as_py()
            
            return [null_count, total_count]

        def combine(self, current_accumulator: List[int], new: List[int]) -> List[int]:
            # Merge two partial results
            return [
                current_accumulator[0] + new[0],  # Sum null counts
                current_accumulator[1] + new[1],  # Sum total counts
            ]

        def _finalize(self, accumulator: List[int]) -> Optional[float]:
            # Transform final result into percentage
            if accumulator[1] == 0:
                return None
            return (accumulator[0] / accumulator[1]) * 100.0

    # Usage example
    ds = ray.data.from_items([
        {"value": 1}, {"value": None}, {"value": 3}, {"value": None}
    ])
    result = ds.aggregate(MissingValuePercentage(on="value"))
    # result: {'missing_pct(value)': 50.0}


