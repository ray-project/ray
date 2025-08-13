"""Broadcast join implementation for Ray Data using map_batches pattern."""

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
    """

    def __init__(
        self,
        small_table_dataset: Dataset,
        join_type: JoinType,
        large_table_key_columns: Tuple[str],
        small_table_key_columns: Tuple[str],
        large_table_columns_suffix: Optional[str] = None,
        small_table_columns_suffix: Optional[str] = None,
        datasets_swapped: bool = False,
    ):
        """Initialize the broadcast join function.

        Args:
            small_table_dataset: The small dataset to be broadcasted
            join_type: Type of join to perform
            large_table_key_columns: Join keys for large table
            small_table_key_columns: Join keys for small table
            large_table_columns_suffix: Suffix for large table columns
            small_table_columns_suffix: Suffix for small table columns
            datasets_swapped: Whether the original left/right datasets were swapped
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
                f"Warning: {e}. \nThe dataset being broadcast is likely too large to fit in memory."
            )

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the large table.

        Args:
            batch: Batch from large table

        Returns:
            Joined batch
        """
        import pyarrow as pa

        # Convert batch to PyArrow table if needed
        if isinstance(batch, dict):
            batch = pa.table(batch)

        # Get the appropriate PyArrow join type
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]

        # Determine whether to coalesce keys based on whether key column names are the same
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
            # - left_outer: becomes small_table.join(batch, "left outer") = LEFT.join(RIGHT_BATCH, "left outer")
            # - right_outer: becomes small_table.join(batch, "right outer") = LEFT.join(RIGHT_BATCH, "right outer")
            # - full_outer: becomes small_table.join(batch, "full outer") = LEFT.join(RIGHT_BATCH, "full outer")
            
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
