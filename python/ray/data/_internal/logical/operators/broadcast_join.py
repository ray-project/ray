"""Broadcast join implementation for Ray Data using map_batches pattern.

This module provides the BroadcastJoinFunction class which implements broadcast joins
using PyArrow's native join functionality. Broadcast joins are useful when one dataset
is significantly smaller than the other, allowing the smaller dataset to be broadcast
to all partitions of the larger dataset.
"""

from typing import Optional, Tuple

import ray
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data.block import DataBatch
from ray.data.dataset import Dataset

_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left outer",
    JoinType.RIGHT_OUTER: "right outer",
    JoinType.FULL_OUTER: "full outer",
}


class BroadcastJoinFunction:
    """A callable class that performs broadcast joins using PyArrow.

    This class is designed to be used with Dataset.map_batches() to implement
    broadcast joins. The small table dataset is coalesced and materialized in __init__,
    and each call performs a PyArrow join on a batch from the large table.

    Broadcast joins are particularly efficient when one dataset is much smaller than
    the other, as the smaller dataset can be loaded into memory and broadcast to
    all partitions of the larger dataset.

    Examples:
        .. testcode::

            # Create sample datasets for demonstration
            import ray
            from ray.data import from_items

            # Create small dataset (will be broadcasted)
            small_data = [{"id": i, "name": f"item_{i}"} for i in range(3)]
            small_ds = from_items(small_data)

            # Create large dataset
            large_data = [{"id": i, "value": f"val_{i}"} for i in range(10)]
            large_ds = from_items(large_data)

            # Create a broadcast join function
            join_fn = BroadcastJoinFunction(
                small_table_dataset=small_ds,
                join_type=JoinType.INNER,
                large_table_key_columns=("id",),
                small_table_key_columns=("id",),
                datasets_swapped=False
            )

            # Apply the join function to the larger dataset
            result = large_ds.map_batches(join_fn, batch_format="pyarrow")
    """

    def __init__(
        self,
        small_table_dataset: Dataset,
        join_type: JoinType,
        large_table_key_columns: Tuple[str, ...],
        small_table_key_columns: Tuple[str, ...],
        large_table_columns_suffix: Optional[str] = None,
        small_table_columns_suffix: Optional[str] = None,
        datasets_swapped: bool = False,
    ):
        """Initialize the broadcast join function.

        Args:
            small_table_dataset: The small dataset to be broadcasted to all partitions.
            join_type: Type of join to perform (inner, left_outer, right_outer,
                full_outer).
            large_table_key_columns: Join key columns for the large table.
            small_table_key_columns: Join key columns for the small table.
            large_table_columns_suffix: Suffix to append to large table column names
                to avoid conflicts.
            small_table_columns_suffix: Suffix to append to small table column names
                to avoid conflicts.
            datasets_swapped: Whether the original left/right datasets were swapped
                for optimization purposes.
        """
        self.join_type = join_type
        self.large_table_key_columns = large_table_key_columns
        self.small_table_key_columns = small_table_key_columns
        self.large_table_columns_suffix = large_table_columns_suffix
        self.small_table_columns_suffix = small_table_columns_suffix
        self.datasets_swapped = datasets_swapped

        # Coalesce the small dataset to a single partition and materialize
        try:
            # Repartition to 1 partition and materialize
            coalesced_ds = small_table_dataset.repartition(1).materialize()

            # Get PyArrow table reference from the dataset
            arrow_refs = coalesced_ds.to_arrow_refs()
            if len(arrow_refs) != 1:
                # Combine multiple references into one
                import pyarrow as pa

                arrow_tables = [ray.get(ref) for ref in arrow_refs]
                if arrow_tables:
                    self.small_table = pa.concat_tables(arrow_tables)
                else:
                    self.small_table = pa.table({})
            else:
                self.small_table = ray.get(arrow_refs[0])
        except Exception as e:
            raise UserWarning(
                f"Warning: {e}. \nThe dataset being broadcast is likely too large "
                f"to fit in memory."
            )

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the large table.

        This method is called for each batch of the large dataset, performing
        a join with the broadcasted small dataset using PyArrow's native join
        functionality.

        Args:
            batch: Batch from the large table to be joined with the small table.

        Returns:
            Joined batch containing the result of the join operation.

        Raises:
            UserWarning: If the small dataset cannot be materialized due to size.
        """
        import pyarrow as pa

        # Convert batch to PyArrow table if needed
        if isinstance(batch, dict):
            batch = pa.table(batch)

        # Get the appropriate PyArrow join type
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]

        # Determine whether to coalesce keys based on whether key column names are
        # the same
        coalesce_keys = list(self.large_table_key_columns) == list(
            self.small_table_key_columns
        )

        if self.datasets_swapped:
            # When datasets are swapped:
            # - batch comes from the originally RIGHT dataset (larger)
            # - small_table is the originally LEFT dataset (smaller, broadcasted)
            # We need to maintain LEFT.join(RIGHT) semantics
            # So we do: small_table.join(batch) = LEFT.join(RIGHT_BATCH)

            # For swapped datasets, we need to handle join types carefully:
            # - inner: works the same
            # - left_outer: becomes small_table.join(batch, "left outer") =
            #   LEFT.join(RIGHT_BATCH, "left outer")
            # - right_outer: becomes small_table.join(batch, "right outer") =
            #   LEFT.join(RIGHT_BATCH, "right outer")
            # - full_outer: becomes small_table.join(batch, "full outer") =
            #   LEFT.join(RIGHT_BATCH, "full outer")

            joined_table = self.small_table.join(
                batch,
                join_type=arrow_join_type,
                keys=list(self.small_table_key_columns),
                right_keys=list(self.large_table_key_columns),
                left_suffix=self.small_table_columns_suffix,
                right_suffix=self.large_table_columns_suffix,
                coalesce_keys=coalesce_keys,
            )
        else:
            # Normal case:
            # - batch comes from the originally LEFT dataset (larger)
            # - small_table is the originally RIGHT dataset (smaller, broadcasted)
            # We maintain LEFT.join(RIGHT) semantics: batch.join(small_table)

            joined_table = batch.join(
                self.small_table,
                join_type=arrow_join_type,
                keys=list(self.large_table_key_columns),
                right_keys=list(self.small_table_key_columns),
                left_suffix=self.large_table_columns_suffix,
                right_suffix=self.small_table_columns_suffix,
                coalesce_keys=coalesce_keys,
            )

        return joined_table
