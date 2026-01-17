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

Each of the preceding methods also has a corresponding :ref:`AggregateFunction <aggregations_api_ref>` object. These objects can be used in :meth:`~ray.data.Dataset.aggregate()` or :meth:`Dataset.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>`.

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

You can create custom aggregations by subclassing :class:`~ray.data.aggregate.AggregateFunction`. The AggregateFunction interface has three key methods to implement:

1. ``aggregate``: Processes a batch of data and returns a partial aggregation result
2. ``combine``: Merges two partial aggregation results into a single result
3. ``finalize``: (Optional) Transforms the final accumulated result into the desired output format

The aggregation process follows these steps:

1. **Initialization**: For each group (if grouping) or for the entire dataset, an initial accumulator is created using ``zero_factory``
2. **Batch Aggregation**: The ``aggregate`` method is applied to each batch independently
3. **Combination**: The ``combine`` method merges partial results into a single accumulator
4. **Finalization**: The ``finalize`` method transforms the final accumulator into the desired output

The ``batch_format`` parameter controls the data format passed to ``aggregate()``:

- ``"pyarrow"``: Receives a ``pyarrow.Table`` (default, recommended for performance)
- ``"pandas"``: Receives a ``pandas.DataFrame``
- ``"numpy"``: Receives a ``Dict[str, np.ndarray]``

If the ``on`` parameter is specified, the batch contains only that column (but still as the same container type).

Example: Creating a Custom Mean Aggregator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's an example of creating a custom aggregator that calculates the Mean of values in a column:

.. testcode::

    import numpy as np
    from ray.data.aggregate import AggregateFunction
    import pyarrow.compute as pc
    from typing import List, Optional, Union

    class CustomMean(AggregateFunction[List[Union[int, float]], float]):
        """Defines mean aggregation."""

        def __init__(
            self,
            on: str,
            ignore_nulls: bool = True,
            alias_name: Optional[str] = None,
        ):
            super().__init__(
                alias_name if alias_name else f"mean({on})",
                on=on,
                ignore_nulls=ignore_nulls,
                batch_format="pyarrow",
                # NOTE: We've to copy returned list here, as some
                #       aggregations might be modifying elements in-place
                zero_factory=lambda: list([0, 0]),  # noqa: C410
            )

        def aggregate(self, batch: "pyarrow.Table") -> Optional[List[Union[int, float]]]:
            # batch is a pyarrow.Table containing only the 'on' column
            column = batch.column(self._target_col_name)
            mode = "only_valid" if self._ignore_nulls else "all"
            count = pc.count(column, mode=mode).as_py()

            if count == 0 or count is None:
                # Empty or all null.
                return None

            sum_ = pc.sum(column, skip_nulls=self._ignore_nulls).as_py()

            if sum_ is None:
                return None

            return [sum_, count]

        def combine(
            self, current_accumulator: List[Union[int, float]], new: List[Union[int, float]]
        ) -> List[Union[int, float]]:
            return [current_accumulator[0] + new[0], current_accumulator[1] + new[1]]

        def finalize(self, accumulator: List[Union[int, float]]) -> Optional[float]:
            if accumulator[1] == 0:
                return np.nan

            return accumulator[0] / accumulator[1]


.. note::
    Internally, aggregations support both the :ref:`hash-shuffle backend <hash-shuffle>` and the :ref:`range based backend <range-partitioning-shuffle>`.

    Hash-shuffling can provide better performance for aggregations in certain cases. For more information see `comparison between hash based shuffling and Range Based shuffling approach <https://www.anyscale.com/blog/ray-data-joins-hash-shuffle#performance-benchmarks/>`_ .

    To use the hash-shuffle algorithm for aggregations, you need to set the shuffle strategy explicitly:    
    ``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE`` before creating a ``Dataset``
    
