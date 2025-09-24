"""Broadcast join implementation for Ray Data using map_batches pattern.

This module provides the BroadcastJoinFunction class which implements broadcast joins
using PyArrow's native join functionality. Broadcast joins are useful when one dataset
is significantly smaller than the other, allowing the smaller dataset to be broadcast
to all partitions of the larger dataset.
"""

from typing import Optional, Tuple

import pyarrow as pa

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

    The implementation is stateless and fault-tolerant, inheriting Ray Data's
    standard fault tolerance behavior through map_batches execution.
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

        # Validate that the join type is supported
        if join_type not in _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP:
            raise ValueError(
                f"Join type '{join_type}' is not supported in broadcast joins. "
                f"Supported types are: {list(_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP.keys())}"
            )

        # Materialize and coalesce the small dataset for broadcasting
        # Using repartition(1) ensures all data is in a single partition for efficient broadcasting
        coalesced_ds = small_table_dataset.repartition(1).materialize()

        # Convert to PyArrow table for efficient in-memory operations
        arrow_refs = coalesced_ds.to_arrow_refs()

        if len(arrow_refs) == 0:
            # Handle empty dataset case
            self.small_table = pa.table({})
        elif len(arrow_refs) == 1:
            # Single reference - most common case for coalesced dataset
            self.small_table = ray.get(arrow_refs[0])
        else:
            # Multiple references - concatenate them
            arrow_tables = [ray.get(ref) for ref in arrow_refs]
            self.small_table = (
                pa.concat_tables(arrow_tables) if arrow_tables else pa.table({})
            )

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the large table.

        Args:
            batch: A batch of data from the large dataset to join with the small table.

        Returns:
            The joined result as a PyArrow table.

        Raises:
            ValueError: If the join operation cannot be performed due to incompatible schemas.
        """
        try:
            # Convert batch to PyArrow table if needed
            if isinstance(batch, dict):
                batch = pa.table(batch)

            # Handle empty batch case
            if batch.num_rows == 0:
                return self._create_empty_result_table(batch)

            # Handle empty small table case
            if self.small_table.num_rows == 0:
                return self._handle_empty_small_table(batch)
        except Exception as e:
            raise ValueError(f"Error preparing data for broadcast join: {e}") from e

        # Get the appropriate PyArrow join type
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]

        # Determine whether to coalesce keys based on whether key column names are the same
        coalesce_keys = list(self.large_table_key_columns) == list(
            self.small_table_key_columns
        )

        try:
            # Perform the PyArrow join
            # The join parameters depend on whether datasets were swapped for optimization
            if self.datasets_swapped:
                # When datasets are swapped, small_table becomes the left table in the join
                # We need to adjust the join type to preserve original left/right semantics
                swapped_join_type = self._get_swapped_join_type(arrow_join_type)

                joined_table = self.small_table.join(
                    batch,
                    join_type=swapped_join_type,
                    keys=list(self.small_table_key_columns),
                    right_keys=(
                        list(self.large_table_key_columns)
                        if self.small_table_key_columns != self.large_table_key_columns
                        else None
                    ),
                    left_suffix=self.small_table_columns_suffix,
                    right_suffix=self.large_table_columns_suffix,
                    coalesce_keys=coalesce_keys,
                )
            else:
                # Normal case: large batch joins with small table
                joined_table = batch.join(
                    self.small_table,
                    join_type=arrow_join_type,
                    keys=list(self.large_table_key_columns),
                    right_keys=(
                        list(self.small_table_key_columns)
                        if self.large_table_key_columns != self.small_table_key_columns
                        else None
                    ),
                    left_suffix=self.large_table_columns_suffix,
                    right_suffix=self.small_table_columns_suffix,
                    coalesce_keys=coalesce_keys,
                )

            return joined_table
        except Exception as e:
            raise ValueError(f"PyArrow join operation failed: {e}") from e

    def _get_swapped_join_type(self, original_join_type: str) -> str:
        """Get the appropriate join type when datasets are swapped."""
        # When datasets are swapped, we need to reverse left/right semantics
        join_type_mapping = {
            "inner": "inner",
            "left outer": "right outer",
            "right outer": "left outer",
            "full outer": "full outer",
        }
        return join_type_mapping.get(original_join_type, original_join_type)

    def _create_empty_result_table(self, batch: pa.Table) -> pa.Table:
        """Create an empty result table with proper schema for join operations.

        Args:
            batch: The batch from the large dataset to determine schema.

        Returns:
            An empty PyArrow table with the correct schema for the join result.
        """
        # Get column names from both tables
        batch_columns = set(batch.column_names)
        small_table_columns = set(self.small_table.column_names)

        # Create result schema based on join type
        result_schema_fields = []

        # Add batch columns with suffix if needed
        for col_name in batch.column_names:
            if col_name in small_table_columns and self.large_table_columns_suffix:
                new_name = f"{col_name}{self.large_table_columns_suffix}"
            else:
                new_name = col_name
            result_schema_fields.append(
                pa.field(new_name, batch.schema.field(col_name).type)
            )

        # Add small table columns with suffix if needed
        for col_name in self.small_table.column_names:
            if col_name in batch_columns and self.small_table_columns_suffix:
                new_name = f"{col_name}{self.small_table_columns_suffix}"
            elif col_name not in batch_columns:
                new_name = col_name
            else:
                continue  # Skip duplicate key columns
            result_schema_fields.append(
                pa.field(new_name, self.small_table.schema.field(col_name).type)
            )

        result_schema = pa.schema(result_schema_fields)
        return pa.table([], schema=result_schema)

    def _handle_empty_small_table(self, batch: pa.Table) -> pa.Table:
        """Handle the case where the small table is empty.

        Args:
            batch: The batch from the large dataset.

        Returns:
            The appropriate result based on join type semantics.
        """
        # When datasets are swapped, we need to adjust join semantics:
        # - small_table represents the original LEFT dataset
        # - batch represents the original RIGHT dataset

        if self.datasets_swapped:
            # Small table is empty original LEFT, batch is original RIGHT
            if self.join_type in [JoinType.INNER, JoinType.LEFT_OUTER]:
                # Inner and left outer joins return empty when left side is empty
                return self._create_empty_result_table(batch)
            elif self.join_type in [JoinType.RIGHT_OUTER, JoinType.FULL_OUTER]:
                # Right and full outer joins return the right side when left side is empty
                # Add null columns for the missing left side
                return self._add_null_columns_for_missing_left(batch)
            else:
                return self._create_empty_result_table(batch)
        else:
            # Small table is original RIGHT, batch is original LEFT
            if self.join_type in [JoinType.INNER, JoinType.RIGHT_OUTER]:
                # Inner and right outer joins return empty when right side is empty
                return self._create_empty_result_table(batch)
            elif self.join_type in [JoinType.LEFT_OUTER, JoinType.FULL_OUTER]:
                # Left and full outer joins return the left side when right side is empty
                # Add null columns for the missing right side
                return self._add_null_columns_for_missing_right(batch)
            else:
                return self._create_empty_result_table(batch)

    def _add_null_columns_for_missing_right(self, batch: pa.Table) -> pa.Table:
        """Add null columns for missing right side in outer joins.

        Args:
            batch: The batch from the large dataset.

        Returns:
            The batch with null columns added for the missing right side.
        """
        result_table = batch

        # Add null columns for each column in the small table
        for col_name in self.small_table.column_names:
            if (
                col_name not in self.small_table_key_columns
                or col_name not in batch.column_names
            ):
                # Add null column with appropriate name and type
                if col_name in batch.column_names and self.small_table_columns_suffix:
                    new_col_name = f"{col_name}{self.small_table_columns_suffix}"
                else:
                    new_col_name = col_name

                col_type = self.small_table.schema.field(col_name).type
                null_array = pa.array([None] * batch.num_rows, type=col_type)
                result_table = result_table.append_column(new_col_name, null_array)

        return result_table

    def _add_null_columns_for_missing_left(self, batch: pa.Table) -> pa.Table:
        """Add null columns for missing left side in outer joins when datasets are swapped.

        This method handles the case where the original left dataset (small table) is empty
        and we need to add null columns for the missing left side columns.

        Args:
            batch: The batch from the large dataset (original right side).

        Returns:
            The batch with null columns added for the missing left side.
        """
        result_table = batch

        # Add null columns for each column in the small table (original left side)
        for col_name in self.small_table.column_names:
            if (
                col_name not in self.small_table_key_columns
                or col_name not in batch.column_names
            ):
                # Add null column with appropriate name and type
                if col_name in batch.column_names and self.small_table_columns_suffix:
                    new_col_name = f"{col_name}{self.small_table_columns_suffix}"
                else:
                    new_col_name = col_name

                col_type = self.small_table.schema.field(col_name).type
                null_array = pa.array([None] * batch.num_rows, type=col_type)
                result_table = result_table.append_column(new_col_name, null_array)

        return result_table
