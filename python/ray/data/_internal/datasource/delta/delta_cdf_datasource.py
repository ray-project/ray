"""
Delta Lake Change Data Feed (CDF) datasource with streaming execution.

This module provides streaming CDF reading functionality that properly integrates
with Ray Data's execution model. Change Data Feed enables incremental processing
of Delta table changes between versions.

Delta Lake CDF documentation: https://docs.delta.io/latest/delta-change-data-feed.html
Python deltalake package: https://delta-io.github.io/delta-rs/python/
"""

import logging
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


class DeltaCDFDatasource(Datasource):
    """
    Datasource for reading Delta Lake Change Data Feed with streaming execution.

    This datasource implements proper streaming execution by yielding CDF batches
    incrementally without materializing entire version ranges in memory.

    The datasource splits version ranges across parallel Ray tasks. Each task
    yields batches as they are read from deltalake, enabling true streaming
    execution with backpressure control.

    Examples:
        Read CDF for incremental ETL:

        >>> from ray.data._internal.datasource.delta import DeltaCDFDatasource
        >>> datasource = DeltaCDFDatasource( # doctest: +SKIP
        ...     path="s3://bucket/table",
        ...     starting_version=10,
        ...     ending_version=20,
        ... )
        >>> ds = datasource.read_as_dataset() # doctest: +SKIP

        Read CDF with column projection:

        >>> datasource = DeltaCDFDatasource( # doctest: +SKIP
        ...     path="s3://bucket/table",
        ...     starting_version=0,
        ...     columns=["id", "name", "value"],
        ... )
        >>> ds = datasource.read_as_dataset() # doctest: +SKIP

    Note:
        This datasource properly integrates with Ray Data's streaming execution.
        Batches are yielded incrementally, not materialized in memory, enabling
        efficient processing of large CDF version ranges.
    """

    def __init__(
        self,
        path: str,
        *,
        starting_version: int = 0,
        ending_version: Optional[int] = None,
        storage_options: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[str] = None,
    ):
        """
        Initialize CDF datasource.

        Args:
            path: Path to Delta Lake table
            starting_version: Starting version for CDF reads (default: 0)
            ending_version: Ending version for CDF reads (None = latest version)
            storage_options: Cloud storage authentication credentials:
                - AWS S3: {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}
                - GCS: {"GOOGLE_SERVICE_ACCOUNT": "path/to/key.json"}
                - Azure: {"AZURE_STORAGE_ACCOUNT_KEY": "..."}
            columns: List of column names to read (None = all columns)
            predicate: SQL predicate for filtering CDF records
        """
        from ray.data._internal.util import _check_import

        _check_import(self, module="deltalake", package="deltalake")

        self.path = path
        self.starting_version = starting_version
        self.ending_version = ending_version
        self.storage_options = storage_options or {}
        self.columns = columns
        self.predicate = predicate

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """
        Create streaming ReadTask objects for CDF reading.

        Splits version range across parallel tasks. Each task yields batches
        incrementally without materializing the entire version range in memory.

        Args:
            parallelism: Number of parallel tasks
            per_task_row_limit: Maximum rows per task (optional)

        Returns:
            List of ReadTask objects that stream CDF data
        """
        from deltalake import DeltaTable

        # Load table metadata on driver (minimal - only for version info)
        dt = DeltaTable(self.path, storage_options=self.storage_options)
        actual_ending_version = (
            self.ending_version if self.ending_version is not None else dt.version()
        )

        # Calculate version chunks
        version_range = actual_ending_version - self.starting_version + 1
        if version_range <= 0:
            # No versions to read
            logger.info(
                f"No CDF versions to read: starting={self.starting_version}, "
                f"ending={actual_ending_version}"
            )
            return []

        # Determine number of tasks (min of parallelism and version range)
        num_tasks = min(parallelism, version_range)
        versions_per_task = max(1, version_range // num_tasks)

        logger.info(
            f"Creating {num_tasks} CDF read tasks for version range "
            f"{self.starting_version}-{actual_ending_version}"
        )

        read_tasks = []
        for i in range(num_tasks):
            chunk_start = self.starting_version + i * versions_per_task
            chunk_end = min(
                self.starting_version + (i + 1) * versions_per_task - 1,
                actual_ending_version,
            )

            # Skip if this task has no versions (edge case)
            if chunk_start > chunk_end:
                continue

            # Create streaming read function that YIELDS blocks incrementally
            read_fn = self._make_cdf_read_fn(chunk_start, chunk_end)

            metadata = BlockMetadata(
                num_rows=None,  # Unknown until read
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )

            read_tasks.append(
                ReadTask(read_fn, metadata, per_task_row_limit=per_task_row_limit)
            )

        return read_tasks

    def _make_cdf_read_fn(self, start_ver: int, end_ver: int) -> callable:
        """
        Create a CDF read function for a specific version range.

        This function will be executed in a Ray worker task and yields
        batches incrementally as they are read.

        Args:
            start_ver: Starting version for this task
            end_ver: Ending version for this task

        Returns:
            Callable that yields CDF blocks incrementally
        """

        def read_cdf_range() -> Iterable[Block]:
            """
            Read CDF for version range and yield blocks incrementally.

            This function runs in a Ray worker task and yields batches
            as they are read, enabling true streaming execution.
            """
            import pyarrow as pa
            from deltalake import DeltaTable

            # Load table in worker task (not driver)
            dt = DeltaTable(self.path, storage_options=self.storage_options)

            # Stream CDF batches from deltalake
            try:
                cdf_reader = dt.load_cdf(
                    starting_version=start_ver,
                    ending_version=end_ver,
                    columns=self.columns,
                    predicate=self.predicate,
                    allow_out_of_range=False,
                )

                # YIELD batches incrementally - this enables streaming!
                # Each batch is yielded as soon as it's read, not accumulated
                batch_count = 0
                for batch in cdf_reader:
                    if batch.num_rows > 0:
                        batch_count += 1
                        yield pa.Table.from_batches([batch])

                if batch_count == 0:
                    # No batches yielded, return empty table with schema
                    logger.debug(
                        f"No CDF data in version range {start_ver}-{end_ver}, "
                        f"yielding empty table with schema"
                    )
                    yield self._create_empty_cdf_table(dt)

            except Exception as e:
                # Handle errors gracefully - yield empty table with schema
                logger.warning(
                    f"Error reading CDF for versions {start_ver}-{end_ver}: {e}. "
                    f"Yielding empty table with schema."
                )
                yield self._create_empty_cdf_table(dt)

        return read_cdf_range

    def _create_empty_cdf_table(self, dt: Any) -> pa.Table:
        """
        Create an empty CDF table with proper schema.

        Args:
            dt: DeltaTable instance to get schema from

        Returns:
            Empty PyArrow table with CDF schema
        """
        # Get base schema from table
        arrow_schema = dt.schema().to_pyarrow()

        # Filter schema for requested columns if specified
        if self.columns:
            available_fields = []
            for col in self.columns:
                try:
                    available_fields.append(arrow_schema.field(col))
                except KeyError:
                    logger.debug(f"Column {col} not found in table schema, skipping")
            if available_fields:
                arrow_schema = pa.schema(available_fields)

        # Add CDF metadata columns to schema
        cdf_fields = [
            pa.field("_change_type", pa.string()),
            pa.field("_commit_version", pa.int64()),
            pa.field("_commit_timestamp", pa.timestamp("us")),
        ]
        full_schema = pa.schema(list(arrow_schema) + cdf_fields)

        return pa.table({}, schema=full_schema)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate in-memory data size for CDF reads.

        Returns:
            None (size unknown until read)
        """
        # CDF size is unknown until read
        return None

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaCDF"

    def read_as_dataset(
        self,
        *,
        parallelism: int = -1,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
    ):
        """
        Read CDF data as Ray Dataset with streaming execution.

        Args:
            parallelism: Parallelism level (default: 200)
            ray_remote_args: Ray remote arguments for tasks
            concurrency: Maximum concurrent tasks
            override_num_blocks: Number of output blocks

        Returns:
            Dataset containing CDF records
        """
        from ray.data import read_datasource

        return read_datasource(
            self,
            parallelism=parallelism if parallelism != -1 else 200,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

    def __repr__(self) -> str:
        """String representation of datasource."""
        return (
            f"DeltaCDFDatasource(path={self.path}, "
            f"versions={self.starting_version}-{self.ending_version})"
        )
