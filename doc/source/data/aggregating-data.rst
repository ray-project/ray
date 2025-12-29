.. _aggregations:

Aggregating Data
================

Ray Data provides a flexible and performant API for performing aggregations on :class:`~ray.data.dataset.Dataset`. 

Basic Aggregations
------------------

Ray Data provides several built-in aggregation functions like :class:`~ray.data.Dataset.max`,
:class:`~ray.data.Dataset.min`, :class:`~ray.data.Dataset.sum`.

These can be used directly on a Dataset or a GroupedData object, as shown below:

.. testcode::

    import ray

    # Create a sample dataset
    ds = ray.data.range(100)
    ds = ds.add_column("group_key", lambda x: x % 3)
    # Schema: {'id': int64, 'group_key': int64}

    # Find the max
    result = ds.max("id")
    # result: 99

    # Find the minimum value per group
    result = ds.groupby("group_key").min("id")
    # result: [{'group_key': 0, 'min(id)': 0}, {'group_key': 1, 'min(id)': 1}, {'group_key': 2, 'min(id)': 2}]

The full list of built-in aggregation functions is available in the :ref:`Dataset API reference <dataset-api>`.

Each of the preceding methods also has a corresponding :ref:`AggregateFnV2 <aggregations_api_ref>` object. These objects can be used in :meth:`~ray.data.Dataset.aggregate()` or :meth:`Dataset.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>`.

Aggregation objects can be used directly with a Dataset like shown below:

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

Multiple aggregations can also be computed at once:

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

You can create custom aggregations by implementing the :class:`~ray.data.aggregate.AggregateFnV2` interface. The AggregateFnV2 interface has three key methods to implement:

1. `aggregate_batch`: Processes a single batch of data and returns a partial aggregation result
2. `combine`: Merges two partial aggregation results into a single result
3. `finalize`: Transforms the final accumulated result into the desired output format

The aggregation process follows these steps:

1. **Initialization**: For each group (if grouping) or for the entire dataset, an initial accumulator is created using `zero_factory`
2. **Batch Aggregation**: The `aggregate_batch` method is applied to each batch independently. Batches are provided in the format specified by `batch_format`
3. **Combination**: The `combine` method merges partial results into a single accumulator
4. **Finalization**: The `finalize` method transforms the final accumulator into the desired output

Example: Creating a Custom Mean Aggregator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's an example of creating a custom aggregator that calculates the Mean of values in a column:

.. testcode::

    import numpy as np
    import ray
    from ray.data.aggregate import AggregateFnV2
    from typing import Dict, Optional, Tuple

    class CustomMean(AggregateFnV2[Tuple[float, int], float]):
        """Defines mean aggregation."""

        def __init__(
            self,
            on: Optional[str] = None,
            ignore_nulls: bool = True,
            alias_name: Optional[str] = None,
        ):
            super().__init__(
                alias_name if alias_name else f"custom_mean({str(on)})",
                on=on,
                ignore_nulls=ignore_nulls,
                batch_format="numpy",  # Work with familiar numpy arrays
                zero_factory=lambda: (0.0, 0),  # (sum, count) tuple
            )

        def aggregate_batch(self, batch: Dict[str, np.ndarray]) -> Tuple[float, int]:
            """Aggregate data within a single batch."""
            column_data = batch[self._target_col_name]
            
            if self._ignore_nulls:
                # Remove null values using numpy
                valid_mask = ~np.isnan(column_data.astype(float))
                column_data = column_data[valid_mask]
            
            count = len(column_data)
            if count == 0:
                return (0.0, 0)
            
            sum_val = np.sum(column_data)
            return (sum_val, count)

        def combine(
            self, current: Tuple[float, int], new: Tuple[float, int]
        ) -> Tuple[float, int]:
            """Combine two partial results."""
            return (current[0] + new[0], current[1] + new[1])

        def finalize(self, accumulator: Tuple[float, int]) -> Optional[float]:
            """Calculate final mean."""
            sum_val, count = accumulator
            return sum_val / count if count > 0 else None

    # Usage example:
    ds = ray.data.range(1000)
    ds = ds.add_column("group", lambda row: row["id"] % 3)
    ds = ds.add_column("value", lambda row: row["id"] * 2.5)
    
    result = ds.groupby("group").aggregate(CustomMean(on="value")).take_all()
    print(result)
    # Output: [{'group': 2, 'custom_mean(value)': 1250.0}, {'group': 1, 'custom_mean(value)': 1247.5}, {'group': 0, 'custom_mean(value)': 1248.75}]


.. note::
    Internally, aggregations support both the :ref:`hash-shuffle backend <hash-shuffle>` and the :ref:`range based backend <range-partitioning-shuffle>`.

    Hash-shuffling can provide better performance for aggregations in certain cases. For more information see `comparison between hash based shuffling and Range Based shuffling approach <https://www.anyscale.com/blog/ray-data-joins-hash-shuffle#performance-benchmarks/>`_ .

    To use the hash-shuffle algorithm for aggregations, you need to set the shuffle strategy explicitly:    
    ``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE`` before creating a ``Dataset``
    
