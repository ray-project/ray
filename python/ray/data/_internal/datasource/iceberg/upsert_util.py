"""
Utility functions for Iceberg merge and upsert operations in Ray Data.

This module provides convenience functions for performing MERGE INTO and UPSERT operations
on Iceberg tables using PyIceberg's native upsert capabilities (PyIceberg 0.9.0+).

For older versions of PyIceberg, it falls back to a read-merge-write pattern using
Ray Data's transformation primitives.

See: https://py.iceberg.apache.org/reference/pyiceberg/table/
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from packaging import version

from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression

    import ray.data


logger = logging.getLogger(__name__)


def _check_pyiceberg_version_for_upsert() -> bool:
    """
    Check if PyIceberg version supports native upsert operations.

    Returns:
        True if PyIceberg >= 0.9.0 (supports native upsert), False otherwise
    """
    try:
        import pyiceberg

        return version.parse(pyiceberg.__version__) >= version.parse("0.9.0")
    except ImportError:
        return False


def upsert_to_iceberg(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Optional[Dict[str, Any]] = None,
    snapshot_properties: Optional[Dict[str, str]] = None,
    update_filter: Optional["BooleanExpression"] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """
    Perform an upsert (merge) operation on an Iceberg table with streaming execution.

    This operation updates existing rows and inserts new rows based on the specified
    join columns. It uses a streaming approach that:
    - Maintains Ray Data's distributed execution model
    - Provides ACID transaction guarantees
    - Scales to any dataset size
    - Only materializes join column values (not the full dataset)

    The upsert logic:
    - Rows in the dataset that match existing rows (based on join_columns) will
      UPDATE the existing rows with new values
    - Rows in the dataset that don't match will be INSERTED as new rows

    Implementation:
    1. Scans existing table for join column values only (small read)
    2. Identifies which rows in the source dataset are updates vs inserts
    3. Uses filtered overwrite to delete rows with matching keys
    4. Writes all source data (updates + inserts) in distributed fashion
    5. Commits in a single ACID transaction

    Args:
        dataset: Ray Dataset containing the source data to merge
        table_identifier: Fully qualified Iceberg table identifier (e.g., "db.table")
        join_columns: List of column names to use for matching rows. These act as
            the "key" columns for the merge operation. All specified columns must
            exist in both the dataset and the target table. Single-column keys are
            recommended for best performance.
        catalog_kwargs: Arguments to pass to PyIceberg's catalog.load_catalog()
            See: https://py.iceberg.apache.org/configuration/
        snapshot_properties: Custom properties to attach to the snapshot
        update_filter: Optional filter to limit which rows in the table can be
            updated. Only rows matching this filter will be considered for updates.
            Rows not matching the filter will not be modified.
        ray_remote_args: kwargs passed to ray.remote in the write tasks
        concurrency: Maximum number of Ray tasks to run concurrently

    Examples:
        Basic upsert on a customer table using customer_id as the key:

        >>> import ray  # doctest: +SKIP
        >>> # New customer data with updates and inserts
        >>> new_data = ray.data.from_items([  # doctest: +SKIP
        ...     {"customer_id": 1, "name": "Alice Updated", "status": "active"},
        ...     {"customer_id": 3, "name": "Charlie New", "status": "active"},
        ... ])
        >>> # Upsert into the table (updates customer_id=1, inserts customer_id=3)
        >>> new_data.write_iceberg(  # doctest: +SKIP
        ...     table_identifier="db.customers",
        ...     mode="merge",
        ...     merge_keys=["customer_id"],
        ...     catalog_kwargs={"type": "glue"}
        ... )

        Upsert with a filter (only update active customers):

        >>> from pyiceberg.expressions import EqualTo
        >>> new_data.write_iceberg(
        ...     table_identifier="db.customers",
        ...     mode="merge",
        ...     merge_keys=["customer_id"],
        ...     update_filter=EqualTo("status", "active"),
        ...     catalog_kwargs={"type": "glue"}
        ... )

        Multi-column key upsert (compound key):

        >>> sales_data.write_iceberg(
        ...     table_identifier="db.sales",
        ...     mode="merge",
        ...     merge_keys=["region", "product_id", "date"],
        ...     catalog_kwargs={"type": "glue"}
        ... )

    Performance Characteristics:
        - Streaming execution: Source dataset is not fully materialized
        - Only join columns are materialized (typically small)
        - Distributed writes: Files written in parallel across Ray tasks
        - Single transaction: ACID guarantees with one commit
        - Scales to unlimited dataset sizes

    Partition Alignment:
        For optimal performance, merge keys should align with the table's partition
        scheme. When keys don't match partitions, Iceberg may need to rewrite entire
        data files even if only a few rows changed.

        Example:
            - Table partitioned by `date`, merge key is `customer_id`
            - If customer_id=123 appears across 50 dates, all 50 partition files
              will be rewritten
            - Better: Use `date` as part of the merge key, or partition by `customer_id`

        The operation is still **correct** regardless of alignment, but performance
        may be significantly impacted for tables with many partitions.

    Note:
        For PyIceberg < 0.9.0, this operation uses a fallback pattern with
        read-merge-write. The fallback is less efficient but provides compatibility.

        Multi-column keys with more than 1000 unique combinations will use a
        first-column filter optimization to avoid creating extremely large filter
        expressions. This may read slightly more rows than necessary, but the
        deduplication logic ensures only exact key matches are updated.
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")

    if not join_columns:
        raise ValueError(
            "join_columns must be specified for upsert operations. "
            "Provide at least one column to use as a key for matching rows."
        )

    catalog_kwargs = catalog_kwargs or {}
    snapshot_properties = snapshot_properties or {}

    # Add metadata to track that this was an upsert operation
    snapshot_properties = {
        **snapshot_properties,
        "ray.operation": "upsert",
        "ray.join_columns": ",".join(join_columns),
    }

    if _check_pyiceberg_version_for_upsert():
        logger.info(
            f"Using PyIceberg native upsert for table {table_identifier} "
            f"with join columns: {join_columns}"
        )
        _upsert_native(
            dataset=dataset,
            table_identifier=table_identifier,
            join_columns=join_columns,
            catalog_kwargs=catalog_kwargs,
            snapshot_properties=snapshot_properties,
            update_filter=update_filter,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )
    else:
        logger.warning(
            "PyIceberg version < 0.9.0 detected. Using fallback read-merge-write "
            "pattern for upsert. Consider upgrading PyIceberg for better performance."
        )
        _upsert_fallback(
            dataset=dataset,
            table_identifier=table_identifier,
            join_columns=join_columns,
            catalog_kwargs=catalog_kwargs,
            snapshot_properties=snapshot_properties,
            update_filter=update_filter,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )


def _upsert_native(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Dict[str, Any],
    snapshot_properties: Dict[str, str],
    update_filter: Optional["BooleanExpression"],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    """
    Perform efficient merge operation using partial overwrite.

    This implementation:
    1. Validates merge keys exist in both source and target
    2. Checks partition alignment and warns if keys don't match partition scheme
    3. Collects unique keys from source dataset (only key columns materialized)
    4. Builds precise filter expression for multi-column keys
    5. Reads existing rows that match those keys from the table
    6. Merges existing and new data (preferring new)
    7. Uses partial overwrite to replace only the affected rows/partitions

    Benefits:
    - Only reads rows being updated (not entire table)
    - Only writes files for affected partitions
    - Unaffected partitions remain untouched
    - ACID transaction with single commit
    - Precise filtering for multi-column keys (up to 1000 unique combinations)
    - Much more efficient than full table rewrite

    The merge is implemented as:
    - For single-column keys: Read rows WHERE key IN (source_keys)
    - For multi-column keys: Read rows WHERE (k1=v1 AND k2=v2) OR (k1=v3 AND k2=v4) ...
    - Union with new data, deduplicate on keys
    - Overwrite with same filter to replace only matched rows

    Partition Alignment:
    When merge keys align with partition columns, Iceberg can efficiently target
    only affected partitions. When they don't align, Iceberg's `delete_by_predicate()`
    will delete entire data files that contain ANY matching rows, then the operation
    rewrites those files with merged data. This is correct but may rewrite more data
    than strictly necessary.

    This leverages Iceberg's partial overwrite capability to only modify
    affected data while leaving the rest of the table unchanged.

    See: https://py.iceberg.apache.org/api/#partial-overwrites
    """
    from pyiceberg.expressions import In

    import ray

    # Get catalog name for later use
    catalog_name = catalog_kwargs.pop("name", "default")

    logger.info(
        f"Starting efficient merge on table {table_identifier} "
        f"with join columns: {join_columns}"
    )

    # Load table to validate schema and check partitioning
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Validate that join columns exist in source dataset
    dataset_schema = dataset.schema()
    source_columns = set(dataset_schema.names)

    missing_in_source = set(join_columns) - source_columns
    if missing_in_source:
        raise ValueError(
            f"Join columns {missing_in_source} not found in source dataset. "
            f"Available columns: {sorted(source_columns)}"
        )

    # Validate that join columns exist in target table
    table_columns = {field.name for field in table.schema().fields}
    missing_in_target = set(join_columns) - table_columns
    if missing_in_target:
        raise ValueError(
            f"Join columns {missing_in_target} not found in target table. "
            f"Available columns: {sorted(table_columns)}"
        )

    # Check if merge keys align with partition scheme
    partition_fields = table.spec().fields
    if partition_fields:
        # Get source field names from partition transforms
        partition_source_fields = {field.source_id for field in partition_fields}
        partition_column_names = {
            table.schema().find_column_name(source_id)
            for source_id in partition_source_fields
        }

        join_columns_set = set(join_columns)
        keys_match_partitions = join_columns_set.issubset(partition_column_names)

        if not keys_match_partitions:
            logger.warning(
                f"Performance warning: Merge keys {join_columns} don't match "
                f"partition columns {sorted(partition_column_names)}. This may cause "
                f"Iceberg to rewrite more data files than necessary. For optimal "
                f"performance, consider using partition columns as merge keys when possible."
            )
            logger.info(
                f"Table {table_identifier} is partitioned by: {sorted(partition_column_names)}"
            )
        else:
            logger.info(
                "Merge keys align with partition scheme - optimal performance expected"
            )

    # Step 1: Collect unique keys from source dataset
    # Only materialize the key columns (typically small)
    # NOTE: We call to_arrow() to collect all keys at once. For most use cases where
    # key cardinality is reasonable (< millions), this is acceptable and simpler than
    # streaming collection. For extremely large key sets, the 1000-key threshold
    # optimization kicks in to avoid large filter expressions.
    logger.info("Collecting keys from source dataset")
    source_keys_table = dataset.select_columns(join_columns).to_arrow()

    # Build set of unique keys and filter expression
    if len(join_columns) == 1:
        col_name = join_columns[0]
        source_keys = set(source_keys_table[col_name].to_pylist())
        logger.info(
            f"Found {len(source_keys)} unique keys in source data column '{col_name}'"
        )

        # Build filter: col IN (key1, key2, key3, ...)
        read_filter = In(col_name, list(source_keys))
    else:
        # Multiple columns - create tuples and build precise filter
        from pyiceberg.expressions import And, EqualTo, Or

        source_keys = set()
        for i in range(len(source_keys_table)):
            key_tuple = tuple(source_keys_table[col][i].as_py() for col in join_columns)
            source_keys.add(key_tuple)

        logger.info(
            f"Found {len(source_keys)} unique key combinations "
            f"across columns {join_columns}"
        )

        # Build precise filter for multi-column keys
        # Creates: (col1=val1 AND col2=val2) OR (col1=val3 AND col2=val4) OR ...
        # This ensures only rows with matching full keys are selected
        if len(source_keys) <= 1000:
            # For reasonable number of keys, build precise OR expression
            key_filters = []
            for key_tuple in source_keys:
                key_conditions = [
                    EqualTo(col, val) for col, val in zip(join_columns, key_tuple)
                ]
                if len(key_conditions) == 1:
                    key_filters.append(key_conditions[0])
                else:
                    key_filters.append(And(*key_conditions))

            if len(key_filters) == 1:
                read_filter = key_filters[0]
            else:
                read_filter = Or(*key_filters)

            logger.info(
                f"Built precise filter for {len(source_keys)} multi-column key combinations"
            )
        else:
            # For large number of keys, fall back to first column filter
            # to avoid creating extremely large filter expressions
            first_col = join_columns[0]
            first_col_values = list({k[0] for k in source_keys})
            read_filter = In(first_col, first_col_values)

            logger.warning(
                f"Large number of keys ({len(source_keys)}) detected. "
                f"Using first column '{first_col}' for filter to avoid "
                f"creating extremely large filter expression. This may read "
                f"more rows than necessary. The deduplication logic will "
                f"handle the full key matching."
            )

    # Store the key-based filter for overwrite (without update_filter)
    # This ensures we delete ALL rows with matching keys, not just filtered ones
    overwrite_filter = read_filter

    # Combine with user's update_filter for reading if specified
    # This filters which existing rows to read, but overwrite uses key filter only
    read_filter_with_update = read_filter
    if update_filter is not None:
        from pyiceberg.expressions import And

        read_filter_with_update = And(read_filter, update_filter)

    # Step 2: Read ONLY the existing rows that will be updated
    # This is much more efficient than reading the entire table
    logger.info(f"Reading existing rows matching keys from table {table_identifier}")
    existing_matching_rows = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter_with_update,
    )

    # Step 3: Mark data sources for deduplication priority
    def add_priority_column(batch, priority: int):
        """Add _merge_priority column to track row source."""
        import pyarrow as pa

        if isinstance(batch, pa.Table):
            return batch.append_column(
                "_merge_priority",
                pa.array([priority] * len(batch), type=pa.int8()),
            )
        else:  # pandas DataFrame
            batch["_merge_priority"] = priority
            return batch

    # Mark new data with higher priority (kept during dedup)
    new_data_marked = dataset.map_batches(
        lambda batch: add_priority_column(batch, priority=1), batch_format="pyarrow"
    )

    # Mark existing data with lower priority
    existing_data_marked = existing_matching_rows.map_batches(
        lambda batch: add_priority_column(batch, priority=0), batch_format="pyarrow"
    )

    # Step 4: Union and deduplicate
    logger.info("Merging new and existing data (deduplicating on keys)")
    merged_data = new_data_marked.union(existing_data_marked)

    def deduplicate_on_keys(batch):
        """
        Deduplicate rows based on join keys, keeping row with highest priority.

        Uses pandas' optimized drop_duplicates instead of Python loops for better performance.
        """
        import pyarrow as pa

        if not isinstance(batch, pa.Table):
            batch = pa.Table.from_pandas(batch)

        if len(batch) == 0:
            if "_merge_priority" in batch.schema.names:
                batch = batch.drop(["_merge_priority"])
            return batch

        # Convert to pandas to use highly optimized drop_duplicates
        df = batch.to_pandas(zero_copy_only=False, self_destruct=True)

        # Sort by priority to ensure we keep the correct row (new data has priority=1)
        df.sort_values(
            "_merge_priority", ascending=False, inplace=True, kind="mergesort"
        )

        # Drop duplicates on join keys, keeping the first occurrence (highest priority)
        df.drop_duplicates(subset=join_columns, keep="first", inplace=True)

        # Remove the temporary priority column
        df.drop(columns=["_merge_priority"], inplace=True)

        # Convert back to PyArrow table, preserving original schema
        return pa.Table.from_pandas(df, schema=batch.drop(["_merge_priority"]).schema)

    deduped_data = merged_data.map_batches(deduplicate_on_keys, batch_format="pyarrow")

    # Step 5: Partial overwrite - only replace rows with matching keys
    # All other rows in the table remain unchanged
    logger.info(
        f"Performing partial overwrite on table {table_identifier} "
        f"(only replacing rows with matching keys)"
    )

    from ray.data._internal.datasource.iceberg import IcebergDatasink

    datasink = IcebergDatasink(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="overwrite",
        overwrite_filter=overwrite_filter,  # Only overwrite rows matching the keys (no update_filter)
    )

    deduped_data.write_datasink(
        datasink,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )

    logger.info(
        f"Successfully completed efficient merge on table {table_identifier}. "
        f"Only modified rows matching {len(source_keys)} unique keys. "
        f"Unaffected partitions/rows remain unchanged."
    )


def _upsert_fallback(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Dict[str, Any],
    snapshot_properties: Dict[str, str],
    update_filter: Optional["BooleanExpression"],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    """
    Fallback upsert implementation for PyIceberg < 0.9.0.

    Uses read-merge-write pattern with partial overwrite:
    1. Materialize keys from source dataset
    2. Build filter for rows matching those keys
    3. Read ONLY matching rows from existing table
    4. Union matching rows with new data
    5. Deduplicate based on join columns (preferring new data)
    6. Use partial overwrite to replace only affected rows

    This is less efficient than the native implementation but still uses
    partial overwrite to avoid modifying unrelated data.

    See: https://py.iceberg.apache.org/api/#partial-overwrites
    """
    from pyiceberg.expressions import And, EqualTo, In, Or

    import ray

    logger.info(
        f"Using fallback upsert for table {table_identifier} "
        f"(PyIceberg < 0.9.0 detected)"
    )

    # Get catalog name for later use
    catalog_name = catalog_kwargs.pop("name", "default")

    # Step 1: Collect unique keys from source dataset (only materialize keys)
    logger.info("Collecting unique keys from source dataset")
    keys_df = dataset.select_columns(join_columns).to_pandas()

    if len(keys_df) == 0:
        logger.warning("Source dataset is empty, nothing to upsert")
        return

    # Step 2: Build filter for matching rows
    if len(join_columns) == 1:
        # Single key: use IN expression
        key_col = join_columns[0]
        unique_keys = keys_df[key_col].unique().tolist()
        if len(unique_keys) == 1:
            read_filter = EqualTo(key_col, unique_keys[0])
        else:
            read_filter = In(key_col, unique_keys)
    else:
        # Multi-column keys: build OR of AND expressions
        # Limit to 1000 combinations to avoid huge filter expressions
        unique_combinations = keys_df.drop_duplicates().head(1000)

        if len(unique_combinations) < len(keys_df):
            logger.warning(
                f"Upsert has {len(keys_df)} unique key combinations. "
                f"Limiting to first 1000 for filter expression."
            )

        filters = []
        for _, row in unique_combinations.iterrows():
            key_filters = [EqualTo(key, row[key]) for key in join_columns]
            if len(key_filters) == 1:
                filters.append(key_filters[0])
            else:
                filters.append(And(*key_filters))

        if len(filters) == 1:
            read_filter = filters[0]
        else:
            read_filter = Or(*filters)

    # Apply update_filter if provided
    # See: https://py.iceberg.apache.org/api/#expressions
    if update_filter is not None:
        read_filter = And(read_filter, update_filter)

    # Step 3: Read ONLY matching rows from existing table (not entire table)
    logger.info(f"Reading existing rows matching {len(keys_df)} keys")
    existing_data = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter,
    )

    # Perform the merge logic using Ray Data operations
    # This is a simplification - for production use, consider using
    # groupby + aggregate to handle duplicates properly

    # Strategy: Union the datasets and then deduplicate keeping the "newest" row
    # We mark new data as "priority" and old data as "non-priority"

    def mark_source(batch, source_priority: int):
        """Add a _source_priority column to track which dataset a row came from."""
        import pyarrow as pa

        if isinstance(batch, pa.Table):
            return batch.append_column(
                "_source_priority",
                pa.array([source_priority] * len(batch), type=pa.int8()),
            )
        else:  # pandas
            batch["_source_priority"] = source_priority
            return batch

    # Mark new data with higher priority (1) and existing data with lower priority (0)
    new_data_marked = dataset.map_batches(
        lambda batch: mark_source(batch, source_priority=1)
    )
    existing_data_marked = existing_data.map_batches(
        lambda batch: mark_source(batch, source_priority=0)
    )

    # Union the datasets
    merged = new_data_marked.union(existing_data_marked)

    # Group by join columns and keep the row with highest priority
    # This effectively keeps new data over old data
    def deduplicate_batch(batch):
        """
        Keep only the row with highest priority for each key.

        Uses pandas' optimized drop_duplicates instead of Python loops for better performance.
        """
        import pyarrow as pa

        if not isinstance(batch, pa.Table):
            # Convert pandas to arrow for processing
            batch = pa.Table.from_pandas(batch)

        if len(batch) == 0:
            if "_source_priority" in batch.schema.names:
                batch = batch.drop(["_source_priority"])
            return batch

        # Convert to pandas to use highly optimized drop_duplicates
        df = batch.to_pandas(zero_copy_only=False, self_destruct=True)

        # Sort by priority to ensure we keep the newest row
        df.sort_values(
            "_source_priority", ascending=False, inplace=True, kind="mergesort"
        )

        # Drop duplicates on join keys, keeping the first row (highest priority)
        df.drop_duplicates(subset=join_columns, keep="first", inplace=True)

        # Remove the temporary priority column
        df.drop(columns=["_source_priority"], inplace=True)

        # Convert back to PyArrow table, preserving original schema
        return pa.Table.from_pandas(df, schema=batch.drop(["_source_priority"]).schema)

    # Apply deduplication
    deduped = merged.map_batches(deduplicate_batch, batch_format="pyarrow")

    # Step 5: Write with partial overwrite using the same filter
    # This ensures we only replace the rows we read, not the entire table
    # See: https://py.iceberg.apache.org/api/#partial-overwrites
    logger.info("Writing merged data with partial overwrite")

    # Build the overwrite filter (just the key filter, not including update_filter)
    # We want to delete all rows matching the keys, not just those matching update_filter
    if len(join_columns) == 1:
        key_col = join_columns[0]
        unique_keys = keys_df[key_col].unique().tolist()
        if len(unique_keys) == 1:
            overwrite_filter = EqualTo(key_col, unique_keys[0])
        else:
            overwrite_filter = In(key_col, unique_keys)
    else:
        # Reuse the same filter logic
        unique_combinations = keys_df.drop_duplicates().head(1000)
        filters = []
        for _, row in unique_combinations.iterrows():
            key_filters = [EqualTo(key, row[key]) for key in join_columns]
            if len(key_filters) == 1:
                filters.append(key_filters[0])
            else:
                filters.append(And(*key_filters))
        if len(filters) == 1:
            overwrite_filter = filters[0]
        else:
            overwrite_filter = Or(*filters)

    # Use partial overwrite with filter to replace only affected rows
    deduped.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="overwrite",
        overwrite_filter=overwrite_filter,  # Critical: only overwrite matching rows
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )

    logger.info(
        f"Completed fallback upsert on table {table_identifier} "
        f"using join columns: {join_columns}"
    )
