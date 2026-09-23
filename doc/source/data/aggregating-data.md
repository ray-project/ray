---
myst:
  html_meta:
    description: "Aggregate Ray Data Datasets with built-in aggregations and custom aggregators, including a worked example building a custom mean aggregator."
---

(aggregations)=

# Aggregating data

This page describes how to aggregate a {class}`~ray.data.dataset.Dataset` with the built-in aggregation functions in Ray Data and with custom aggregators that you implement.

(basic-aggregations)=

## Use built-in aggregations

Ray Data provides built-in aggregation functions such as {class}`~ray.data.Dataset.max`, {class}`~ray.data.Dataset.min`, and {class}`~ray.data.Dataset.sum`.

You can call these functions directly on a Dataset or on a GroupedData object, as the following example shows:

```{testcode}
import ray

# Create a sample dataset
ds = ray.data.range(100)
ds = ds.add_column("group_key", lambda x: x["id"].to_numpy() % 3)
# Schema: {'id': int64, 'group_key': int64}

# Find the max
result = ds.max("id")
# result: 99

# Find the minimum value per group
result = ds.groupby("group_key").min("id")
# result: [{'group_key': 0, 'min(id)': 0}, {'group_key': 1, 'min(id)': 1}, {'group_key': 2, 'min(id)': 2}]
```

For the full list of built-in aggregation functions, see the {ref}`Dataset API reference <dataset-api>`.

Each of the preceding methods also has a corresponding {ref}`AggregateFnV2 <aggregations_api_ref>` object. Pass these objects to {meth}`~ray.data.Dataset.aggregate()` or {meth}`Dataset.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>`.

The following example uses aggregation objects directly with a Dataset:

```{testcode}
import ray
from ray.data.aggregate import Count, Mean, Quantile

# Create a sample dataset
ds = ray.data.range(100)
ds = ds.add_column("group_key", lambda x: x["id"].to_numpy() % 3)

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
```

You can also compute multiple aggregations at once:

```{testcode}
import ray
from ray.data.aggregate import Count, Mean, Min, Max, Std

ds = ray.data.range(100)
ds = ds.add_column("group_key", lambda x: x["id"].to_numpy() % 3)

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
```

(custom-aggregations)=

## Create custom aggregations

To create a custom aggregation, implement the {class}`~ray.data.aggregate.AggregateFnV2` interface. The interface has three key methods that you implement:

1. `aggregate_block`: Processes a single block of data and returns a partial aggregation result.
1. `combine`: Merges two partial aggregation results into a single result.
1. `finalize`: Transforms the final accumulated result into the desired output format.

Ray Data runs an aggregation in the following steps:

1. **Initialization**: Ray Data creates an initial accumulator with `zero_factory` for each group, or for the entire dataset when you don't group.
1. **Block aggregation**: Ray Data applies the `aggregate_block` method to each block independently.
1. **Combination**: The `combine` method merges partial results into a single accumulator.
1. **Finalization**: The `finalize` method transforms the final accumulator into the desired output.

### Example: Create a custom mean aggregator

The following example creates a custom aggregator that calculates the mean of the values in a column:

```{testcode}
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

    def finalize(self, accumulator: AggType) -> Optional[U]:
        if accumulator[1] == 0:
            return np.nan

        return accumulator[0] / accumulator[1]
```

:::{note}
Hash-based shuffling can improve aggregation performance in some cases. For more information, see this [comparison of hash-based and range-based shuffling](https://www.anyscale.com/blog/ray-data-joins-hash-shuffle#performance-benchmarks/).
:::
