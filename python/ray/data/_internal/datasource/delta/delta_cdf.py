"""
Change Data Feed (CDF) reading functionality for Delta Lake.

This module provides distributed CDF reading by splitting version ranges
across Ray tasks for scalable incremental data processing.
"""

from typing import Dict, List, Optional

import pyarrow as pa

import ray


def read_delta_cdf_distributed(
    path: str,
    starting_version: int,
    ending_version: Optional[int],
    columns: Optional[List[str]],
    predicate: Optional[str],
    storage_options: Optional[Dict[str, str]],
    override_num_blocks: Optional[int],
):
    """
    Read Change Data Feed with distributed execution.

    Splits the version range across Ray tasks for scalable CDF reading
    without driver materialization or temporary files.

    Args:
        path: Path to Delta table
        starting_version: Starting version for CDF
        ending_version: Ending version for CDF (None = latest)
        columns: Column projection
        predicate: SQL predicate for filtering
        storage_options: Cloud authentication
        override_num_blocks: Number of parallel tasks

    Returns:
        Dataset with CDF records
    """
    from deltalake import DeltaTable

    # Load table to determine ending version
    dt_kwargs = {}
    if storage_options:
        dt_kwargs["storage_options"] = storage_options

    dt = DeltaTable(path, **dt_kwargs)

    # Determine actual ending version
    actual_ending_version = (
        ending_version if ending_version is not None else dt.version()
    )

    # Calculate number of tasks for parallelism
    num_tasks = override_num_blocks if override_num_blocks else 200
    version_range = actual_ending_version - starting_version + 1

    if version_range <= 0:
        # No versions to read - return empty CDF dataset
        empty_schema = pa.schema(
            [
                ("_change_type", pa.string()),
                ("_commit_version", pa.int64()),
                ("_commit_timestamp", pa.timestamp("us")),
            ]
        )
        from ray.data import from_arrow

        return from_arrow(pa.table({}, schema=empty_schema))

    # Split version range into chunks for distributed processing
    versions_per_task = max(1, version_range // num_tasks)
    version_chunks = []
    for i in range(0, version_range, versions_per_task):
        chunk_start = starting_version + i
        chunk_end = min(
            starting_version + i + versions_per_task - 1, actual_ending_version
        )
        version_chunks.append((chunk_start, chunk_end))

    # Create distributed CDF reading tasks
    @ray.remote
    def read_cdf_range(
        table_path: str,
        start_ver: int,
        end_ver: int,
        cols: Optional[List[str]],
        pred: Optional[str],
        storage_opts: Optional[Dict[str, str]],
    ):
        """Read CDF for a specific version range in a Ray task."""
        import pyarrow as pa
        from deltalake import DeltaTable

        # Create DeltaTable in this task
        dt_kwargs = {}
        if storage_opts:
            dt_kwargs["storage_options"] = storage_opts

        task_dt = DeltaTable(table_path, **dt_kwargs)

        # Load CDF for this version range
        cdf_reader = task_dt.load_cdf(
            starting_version=start_ver,
            ending_version=end_ver,
            columns=cols,
            predicate=pred,
            allow_out_of_range=False,
        )

        # Collect batches for this version range
        tables = []
        schema = None
        for batch in cdf_reader:
            if schema is None:
                schema = batch.schema
            if batch.num_rows > 0:
                tables.append(pa.Table.from_batches([batch]))

        # Return empty table with schema if no data found
        # This prevents from_arrow_refs from failing with None references
        if not tables:
            if schema is None:
                # Get schema from the table if no batches were produced
                arrow_schema = task_dt.schema().to_pyarrow()
                if cols:
                    # Filter schema for requested columns, skipping missing ones
                    available_fields = []
                    missing_cols = []
                    for c in cols:
                        try:
                            available_fields.append(arrow_schema.field(c))
                        except KeyError:
                            # Column doesn't exist in Delta table schema
                            missing_cols.append(c)
                    
                    # If no columns match, create empty schema
                    if not available_fields:
                        if missing_cols:
                            # All requested columns are missing - this is likely an error
                            raise ValueError(
                                f"Requested columns not found in Delta table: {missing_cols}. "
                                f"Available columns: {arrow_schema.names}"
                            )
                        arrow_schema = pa.schema([])
                    else:
                        arrow_schema = pa.schema(available_fields)
                schema = arrow_schema
            return pa.table({}, schema=schema)

        return pa.concat_tables(tables)

    # Distribute CDF reading across tasks
    table_refs = [
        read_cdf_range.remote(
            path,
            start,
            end,
            columns,
            predicate,
            storage_options,
        )
        for start, end in version_chunks
    ]

    # Create dataset from distributed Arrow table references
    from ray.data import from_arrow_refs

    return from_arrow_refs(table_refs)
