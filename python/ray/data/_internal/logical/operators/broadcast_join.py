"""Broadcast join implementation for Ray Data using map_batches pattern.

This module provides the BroadcastJoinFunction class which implements broadcast joins
using PyArrow's native join functionality. Broadcast joins are useful when one dataset
is significantly smaller than the other, allowing the smaller dataset to be broadcast
to all partitions of the larger dataset.

PyArrow join documentation:
https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.join

Architecture and Streaming Execution Integration:
-------------------------------------------------
The broadcast join implementation leverages Ray Data's streaming execution model through
the map_batches operator pattern:

1. **Streaming Properties**: Uses map_batches which creates a MapOperator (OneToOneOperator),
   maintaining streaming execution without materializing the full dataset.

2. **Memory Management**: Small dataset is materialized and coalesced via repartition(1),
   then stored as ObjectRefs to avoid driver OOM. Workers materialize the small table
   lazily when processing batches.

3. **Backpressure**: Inherits standard map_batches backpressure mechanisms from MapOperator,
   preventing memory overload during join execution.

4. **Fault Tolerance**: Leverages Ray's task retry mechanism through map_batches,
   automatically retrying failed join operations on individual batches.

5. **Resource Allocation**: Uses standard map_batches resource patterns with configurable
   concurrency parameter for controlling parallelism.

6. **Join Type Handling**:
   - inner: Straightforward broadcast join
   - left_outer: Iterate over left dataset with broadcast right
   - right_outer: Swap datasets to iterate right with broadcast left
   - full_outer: Falls back to hash shuffle join (broadcast causes duplicates)

The implementation avoids driver OOM by materializing the small dataset only in worker
processes, not on the driver. ObjectRefs are stored and materialized lazily.
"""

from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_RUN_END_ENCODED_TYPES,
    MIN_PYARROW_VERSION_VIEW_TYPES,
)
from ray.data._internal.logical.operators.join_operator import (
    JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP,
    JoinType,
)
from ray.data.block import DataBatch
from ray.data.dataset import Dataset

if TYPE_CHECKING:
    import pyarrow as pa


def _validate_join_keys(
    large_table_keys: Tuple[str, ...], small_table_keys: Tuple[str, ...]
) -> None:
    """Validate join key columns are properly specified.

    Args:
        large_table_keys: Join key columns for the large table.
        small_table_keys: Join key columns for the small table.

    Raises:
        ValueError: If key columns are invalid or mismatched.
    """
    if not large_table_keys:
        raise ValueError(
            "large_table_key_columns must contain at least one column name"
        )

    if not small_table_keys:
        raise ValueError(
            "small_table_key_columns must contain at least one column name"
        )

    if len(large_table_keys) != len(small_table_keys):
        raise ValueError(
            f"Number of key columns must match: "
            f"large_table has {len(large_table_keys)} keys, "
            f"small_table has {len(small_table_keys)} keys"
        )


def _estimate_dataset_size_bytes(dataset: Dataset) -> Optional[int]:
    """Estimate the in-memory size of a dataset in bytes.

    Args:
        dataset: The dataset to estimate size for.

    Returns:
        Estimated size in bytes, or None if unable to estimate.
    """
    stats = dataset.stats()
    if hasattr(stats, "dataset_bytes_spilled"):
        return getattr(stats, "dataset_bytes_spilled", None)
    return None


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

    Examples:
        Create a broadcast join for inner join:

        >>> import ray
        >>> from ray.data._internal.logical.operators.broadcast_join import (
        ...     BroadcastJoinFunction,
        ... )
        >>> from ray.data._internal.logical.operators.join_operator import JoinType
        >>>
        >>> # Large dataset
        >>> large_ds = ray.data.range(1000).map(
        ...     lambda row: {"id": row["id"], "value": row["id"] * 2}
        ... )
        >>>
        >>> # Small dataset to broadcast
        >>> small_ds = ray.data.range(10).map(
        ...     lambda row: {"id": row["id"], "label": f"label_{row['id']}"}
        ... )
        >>>
        >>> # Create broadcast join function
        >>> join_fn = BroadcastJoinFunction(
        ...     small_table_dataset=small_ds,
        ...     join_type=JoinType.INNER,
        ...     large_table_key_columns=("id",),
        ...     small_table_key_columns=("id",),
        ... )
        >>>
        >>> # Apply to large dataset
        >>> result = large_ds.map_batches(join_fn, batch_format="pyarrow")  # doctest: +SKIP

        Performance considerations:
        - Best for small datasets (< 1 GB) broadcast to many partitions
        - Avoids shuffle overhead for skewed join keys
        - Each worker holds full small dataset in memory
        - Not suitable for large broadcast datasets (may cause OOM)
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

        Raises:
            ValueError: If join type is not supported, key columns are invalid,
                or dataset is None.
        """
        # Validate inputs
        if small_table_dataset is None:
            raise ValueError("small_table_dataset cannot be None")

        # Use helper function to validate join keys
        _validate_join_keys(large_table_key_columns, small_table_key_columns)

        # Validate that the join type is supported
        if join_type not in JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP:
            supported_types = ", ".join(
                [str(jt.value) for jt in JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP.keys()]
            )
            raise ValueError(
                f"Join type '{join_type}' is not supported in broadcast joins. "
                f"Supported types are: {supported_types}"
            )

        self.join_type = join_type
        self.large_table_key_columns = large_table_key_columns
        self.small_table_key_columns = small_table_key_columns
        self.large_table_columns_suffix = large_table_columns_suffix
        self.small_table_columns_suffix = small_table_columns_suffix
        self.datasets_swapped = datasets_swapped

        # Materialize and coalesce the small dataset for broadcasting
        coalesced_ds = small_table_dataset.repartition(1).materialize()

        # Warn if broadcasting a large dataset
        self._warn_if_large_dataset(coalesced_ds)

        # Store object references instead of materializing on driver to avoid OOM
        self._small_table_refs = coalesced_ds.to_arrow_refs()
        self._small_table_schema = self._get_schema(coalesced_ds)
        self._cached_small_table = None

    def _warn_if_large_dataset(self, dataset: Dataset) -> None:
        """Warn if broadcasting a dataset larger than 1 GB."""
        estimated_size_bytes = _estimate_dataset_size_bytes(dataset)
        if estimated_size_bytes and estimated_size_bytes > 1024 * 1024 * 1024:
            import warnings

            warnings.warn(
                f"Broadcasting a dataset of size {estimated_size_bytes / (1024**3):.2f} GB. "
                "Large broadcast datasets may cause out-of-memory errors on worker nodes. "
                "Consider using a hash shuffle join (broadcast=False) for large datasets.",
                UserWarning,
                stacklevel=4,
            )

    def _get_schema(self, dataset: Dataset) -> "pa.Schema":
        """Get schema from dataset, defaulting to empty schema if None."""
        import pyarrow as pa

        schema = dataset.schema()
        return schema if schema is not None else pa.schema([])

    @property
    def small_table(self) -> "pa.Table":
        """Lazily materialize the small table to avoid driver OOM.

        This property ensures that the small table is only materialized when needed
        and within the worker processes, not on the driver.

        Memory Management:
        - Uses Ray's object store for efficient shared memory access
        - Cached after first access to avoid repeated materialization
        - Each worker materializes the table once and reuses it

        Streaming Execution:
        - ObjectRefs stored on driver (minimal memory)
        - Actual data materialized in workers during join execution
        - Supports Ray's task retry mechanism automatically

        Returns:
            PyArrow table containing the small dataset.
        """
        if self._cached_small_table is not None:
            return self._cached_small_table

        # Materialize the small table from references
        import pyarrow as pa

        if len(self._small_table_refs) == 0:
            self._cached_small_table = pa.table({}, schema=self._small_table_schema)
        elif len(self._small_table_refs) == 1:
            # Single reference - most common case for coalesced dataset
            # This is zero-copy from Ray's object store
            self._cached_small_table = ray.get(self._small_table_refs[0])
        else:
            # Multiple references - concatenate them
            # This should be rare after repartition(1)
            arrow_tables = [ray.get(ref) for ref in self._small_table_refs]
            self._cached_small_table = (
                pa.concat_tables(arrow_tables)
                if arrow_tables
                else pa.table({}, schema=self._small_table_schema)
            )

        return self._cached_small_table

    @property
    def small_table_column_names(self) -> List[str]:
        """Get column names without materializing the table."""
        return self._small_table_schema.names

    @property
    def small_table_schema_field(self) -> Any:
        """Get schema field accessor without materializing the table."""
        return self._small_table_schema.field

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the large table.

        Args:
            batch: A batch of data from the large dataset to join with the small table.

        Returns:
            The joined result as a PyArrow table.

        Raises:
            ValueError: If the join operation cannot be performed due to incompatible schemas.
        """
        # Convert batch to PyArrow table if needed
        if isinstance(batch, dict):
            import pyarrow as pa

            batch = pa.table(batch)

        # Handle empty batch case
        if batch.num_rows == 0:
            return self._create_empty_result_table(batch)

        # Handle empty small table case
        if self.small_table.num_rows == 0:
            return self._handle_empty_small_table(batch)

        # Get the appropriate PyArrow join type
        arrow_join_type = JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]

        # Determine whether to coalesce keys based on whether key column names are the same
        coalesce_keys = list(self.large_table_key_columns) == list(
            self.small_table_key_columns
        )

        # Validate that join key columns don't have unsupported types
        self._validate_join_key_types(batch, self.large_table_key_columns, "large")
        self._validate_join_key_types(
            self.small_table, self.small_table_key_columns, "small"
        )

        # Fix null types in non-key columns before joining
        # PyArrow join doesn't support null types in non-key fields
        batch = self._fix_null_types(batch, self.large_table_key_columns)
        small_table = self._fix_null_types(
            self.small_table, self.small_table_key_columns
        )

        # Perform the PyArrow join
        # When datasets are swapped, small_table becomes the left table in the join.
        # Join type semantics are preserved relative to original datasets, not physical positions.
        if self.datasets_swapped:
            joined_table = small_table.join(
                batch,
                join_type=arrow_join_type,
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
            joined_table = batch.join(
                small_table,
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

    def _is_pa_join_not_supported(self, type: "pa.DataType") -> bool:
        """Check if a PyArrow type is not supported in join operations.

        PyArrow doesn't support joins on columns with certain complex types
        (lists, structs, maps, unions, extension types, etc.).

        Args:
            type: The PyArrow data type to check.

        Returns:
            True if the type cannot be used in join operations, False otherwise.
        """
        import pyarrow as pa

        pyarrow_version = get_pyarrow_version()
        is_v12 = pyarrow_version >= MIN_PYARROW_VERSION_RUN_END_ENCODED_TYPES
        is_v16 = pyarrow_version >= MIN_PYARROW_VERSION_VIEW_TYPES

        return (
            pa.types.is_map(type)
            or pa.types.is_union(type)
            or pa.types.is_list(type)
            or pa.types.is_struct(type)
            or pa.types.is_null(type)
            or pa.types.is_large_list(type)
            or pa.types.is_fixed_size_list(type)
            or (is_v12 and pa.types.is_run_end_encoded(type))
            or (
                is_v16
                and (
                    pa.types.is_binary_view(type)
                    or pa.types.is_string_view(type)
                    or pa.types.is_list_view(type)
                )
            )
        )

    def _validate_join_key_types(
        self, table: "pa.Table", key_columns: Tuple[str, ...], table_name: str
    ) -> None:
        """Validate that join key columns don't have unsupported types.

        Args:
            table: The PyArrow table to validate.
            key_columns: Tuple of join key column names.
            table_name: Name of the table (for error messages).

        Raises:
            ValueError: If any join key column has an unsupported type.
        """

        from ray.air.util.transform_pyarrow import _is_pa_extension_type

        conflicting_columns = []
        for col_name in key_columns:
            if col_name not in table.column_names:
                continue
            col_type = table.schema.field(col_name).type
            if _is_pa_extension_type(col_type) or self._is_pa_join_not_supported(
                col_type
            ):
                conflicting_columns.append(col_name)

        if conflicting_columns:
            raise ValueError(
                f"Cannot join on columns with unjoinable types. "
                f"{table_name.capitalize()} join key columns {conflicting_columns} "
                f"have unjoinable types (map, union, list, struct, etc.) "
                f"which cannot be used for join operations."
            )

    def _fix_null_types(
        self, table: "pa.Table", key_columns: Tuple[str, ...]
    ) -> "pa.Table":
        """Cast null types to nullable string types in non-key columns.

        PyArrow's join operation doesn't support null types in non-key fields.
        See: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.join

        Args:
            table: The PyArrow table to fix.
            key_columns: Tuple of key column names that should not be modified.

        Returns:
            A new table with null types cast to nullable string types.
        """
        import pyarrow as pa

        columns_to_fix = {}
        for col_name in table.column_names:
            if col_name not in key_columns:
                col_type = table.schema.field(col_name).type
                if pa.types.is_null(col_type):
                    columns_to_fix[col_name] = pa.string()

        if not columns_to_fix:
            return table

        new_fields = []
        for field in table.schema:
            if field.name in columns_to_fix:
                new_fields.append(
                    pa.field(field.name, columns_to_fix[field.name], nullable=True)
                )
            else:
                new_fields.append(field)

        return table.cast(pa.schema(new_fields))

    def _create_empty_result_table(self, batch: "pa.Table") -> "pa.Table":
        """Create an empty result table with proper schema for join operations.

        Args:
            batch: The batch from the large dataset to determine schema.

        Returns:
            An empty PyArrow table with the correct schema for the join result.
        """
        # Get column names from both tables
        batch_columns = set(batch.column_names)
        small_table_columns = set(self.small_table_column_names)

        # Create result schema based on join type
        result_schema_fields = []
        import pyarrow as pa

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
        for col_name in self.small_table_column_names:
            if col_name in batch_columns and self.small_table_columns_suffix:
                new_name = f"{col_name}{self.small_table_columns_suffix}"
            elif col_name not in batch_columns:
                new_name = col_name
            else:
                continue  # Skip duplicate key columns
            result_schema_fields.append(
                pa.field(new_name, self.small_table_schema_field(col_name).type)
            )

        result_schema = pa.schema(result_schema_fields)
        return pa.table([], schema=result_schema)

    def _handle_empty_small_table(self, batch: "pa.Table") -> "pa.Table":
        """Handle the case where the small table is empty.

        PyArrow doesn't handle empty tables correctly for outer joins, so we need
        to manually construct the result based on join semantics.

        Args:
            batch: The batch from the large dataset.

        Returns:
            The appropriate result based on join type semantics.
        """
        # Determine if we should return empty or add null columns based on join type
        # When swapped: small_table = original LEFT, batch = original RIGHT
        # When not swapped: small_table = original RIGHT, batch = original LEFT

        should_return_empty = False
        if self.datasets_swapped:
            # Empty LEFT: inner/left_outer return empty, right_outer/full_outer return RIGHT with nulls
            should_return_empty = self.join_type in [
                JoinType.INNER,
                JoinType.LEFT_OUTER,
            ]
        else:
            # Empty RIGHT: inner/right_outer return empty, left_outer/full_outer return LEFT with nulls
            should_return_empty = self.join_type in [
                JoinType.INNER,
                JoinType.RIGHT_OUTER,
            ]

        if should_return_empty:
            return self._create_empty_result_table(batch)
        else:
            return self._add_null_columns(batch)

    def _add_null_columns(self, batch: "pa.Table") -> "pa.Table":
        """Add null columns for missing side in outer joins.

        Args:
            batch: The batch from the large dataset.

        Returns:
            The batch with null columns added for the missing side.
        """
        import pyarrow as pa

        result_table = batch
        for col_name in self.small_table_column_names:
            if (
                col_name not in self.small_table_key_columns
                and col_name not in batch.column_names
            ):
                col_type = self.small_table_schema_field(col_name).type
                null_array = pa.array([None] * batch.num_rows, type=col_type)
                result_table = result_table.append_column(col_name, null_array)

        return result_table
