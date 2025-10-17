"""
Delta Lake datasource implementation for reading Delta tables.

This module provides the DeltaDatasource class for reading Delta Lake tables,
including support for time travel, partition filtering, and Change Data Feed (CDF).

Delta Lake documentation: https://delta.io/
Python deltalake package: https://delta-io.github.io/delta-rs/python/
"""

import logging
from typing import Any, Dict, List, Optional, Union

from ray.data._internal.util import _check_import
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.partitioning import Partitioning
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class DeltaDatasource(Datasource):
    """
    Datasource for reading Delta Lake tables with Ray Data.

    This datasource provides Delta Lake reading capabilities including:
    - Time travel: Read specific versions or timestamps
    - Partition filtering: Filter partitions before reading
    - Change Data Feed (CDF): Read incremental changes between versions
    - Multi-cloud support: S3, GCS, Azure, HDFS, local filesystems

    The datasource uses Ray's ReadTask pattern for proper streaming execution.
    For snapshot reads, it delegates to read_parquet() after resolving Delta table
    metadata and file paths. For CDF mode, it creates streaming ReadTasks that yield
    batches incrementally.

    Examples:
        Read latest version of Delta table:

        >>> import ray
        >>> from ray.data._internal.datasource.delta import DeltaDatasource
        >>> datasource = DeltaDatasource("s3://bucket/table") # doctest: +SKIP
        >>> ds = datasource.read_as_dataset() # doctest: +SKIP

        Read specific version (time travel):

        >>> datasource = DeltaDatasource( # doctest: +SKIP
        ...     "s3://bucket/table",
        ...     version=5
        ... )
        >>> ds = datasource.read_as_dataset() # doctest: +SKIP

        Read Change Data Feed for incremental ETL:

        >>> datasource = DeltaDatasource( # doctest: +SKIP
        ...     "s3://bucket/table",
        ...     cdf=True,
        ...     starting_version=10,
        ...     ending_version=20
        ... )
        >>> ds = datasource.read_as_dataset() # doctest: +SKIP

    Note:
        This datasource properly integrates with Ray Data's streaming execution.
        Snapshot reads delegate to read_parquet() for efficient distributed file reads.
        CDF reads use streaming ReadTasks that yield batches incrementally without
        materializing entire version ranges in memory.
    """

    def __init__(
        self,
        path: str,
        *,
        version: Optional[Union[int, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        partition_filters: Optional[List[tuple]] = None,
        cdf: bool = False,
        starting_version: int = 0,
        ending_version: Optional[int] = None,
        filesystem: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        partitioning: Partitioning = Partitioning("hive"),
        **arrow_parquet_args,
    ):
        """
        Initialize Delta Lake datasource.

        Args:
            path: Path to Delta Lake table. Supports:
                - Local: /path/to/table
                - S3: s3://bucket/path
                - GCS: gs://bucket/path
                - Azure: abfss://container@account.dfs.core.windows.net/path
                - HDFS: hdfs://namenode:port/path

            version: Version to read for time travel. Can be:
                - Integer version number (e.g., 5)
                - ISO 8601 timestamp string (e.g., "2024-01-01T00:00:00Z")
                - None for latest version (default)

            storage_options: Cloud storage authentication credentials:
                - AWS S3: {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}
                - GCS: {"GOOGLE_SERVICE_ACCOUNT": "path/to/key.json"}
                - Azure: {"AZURE_STORAGE_ACCOUNT_KEY": "..."}

            partition_filters: Delta Lake partition filters as list of tuples.
                Format: [(column, operator, value), ...]
                Operators: "=", "!=", "in", "not in"
                Examples:
                - [("year", "=", "2024")]
                - [("year", "=", "2024"), ("month", "in", ["01", "02"])]

            cdf: Enable Change Data Feed mode. If True, reads incremental changes
                between versions instead of snapshot data.

            starting_version: Starting version for CDF reads (default: 0).
                Only used when cdf=True.

            ending_version: Ending version for CDF reads.
                None means latest version. Only used when cdf=True.

            filesystem: PyArrow filesystem for reading files. If None, automatically
                detected from path scheme.

            columns: List of column names to read. If None, reads all columns.

            partitioning: Partitioning scheme for reading files. Defaults to Hive
                partitioning.

            **arrow_parquet_args: Additional arguments passed to PyArrow parquet reader.
                See PyArrow documentation for available options.
        """
        # Import check for deltalake package
        _check_import(self, module="deltalake", package="deltalake")

        # Validate path
        if not isinstance(path, str):
            raise ValueError(
                "Only single Delta table path supported (not list of paths)"
            )

        self.path = path
        self.version = version
        self.storage_options = storage_options or {}
        self.partition_filters = partition_filters
        self.cdf = cdf
        self.starting_version = starting_version
        self.ending_version = ending_version
        self.filesystem = filesystem
        self.columns = columns
        self.partitioning = partitioning
        self.arrow_parquet_args = arrow_parquet_args

        # Initialize Delta table lazily (loaded when needed)
        self._delta_table = None

    @property
    def delta_table(self):
        """
        Lazy-load Delta table object.

        Returns:
            DeltaTable instance

        Raises:
            ImportError: If deltalake package not installed
            ValueError: If table doesn't exist or is invalid
        """
        if self._delta_table is None:
            from deltalake import DeltaTable

            # Build DeltaTable constructor arguments
            dt_kwargs = {}
            if self.storage_options:
                dt_kwargs["storage_options"] = self.storage_options

            # Add version for snapshot reads (not CDF)
            if self.version is not None and not self.cdf:
                dt_kwargs["version"] = self.version

            self._delta_table = DeltaTable(self.path, **dt_kwargs)

        return self._delta_table

    def get_file_paths(self) -> List[str]:
        """
        Get list of Parquet file paths from Delta table.

        Applies partition filters at Delta level before retrieving file paths.

        Returns:
            List of file URIs to read

        Raises:
            ValueError: If partition filters are invalid
        """
        # Get file paths from Delta table with optional partition filters
        if self.partition_filters is not None:
            file_paths = self.delta_table.file_uris(
                partition_filters=self.partition_filters
            )
        else:
            file_paths = self.delta_table.file_uris()

        return file_paths

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """
        Get read tasks for Delta table snapshot reads.

        For snapshot reads, returns empty list to signal that read_as_dataset()
        should delegate to read_parquet for efficiency.

        For CDF reads, this datasource shouldn't be used directly - use
        DeltaCDFDatasource instead.

        Args:
            parallelism: Number of parallel read tasks to create
            per_task_row_limit: Maximum rows per task (optional)

        Returns:
            Empty list (snapshot reads use read_parquet delegation)
        """
        # Snapshot reads use read_parquet delegation
        # Return empty list to signal that read_as_dataset should be used
        return []

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate in-memory data size for the Delta table.

        Returns:
            None (unknown for Delta snapshot reads)
        """
        # For snapshot reads, we don't have file metadata readily available
        # Return None and let Ray Data's execution engine handle it
        return None

    def read_as_dataset(
        self,
        *,
        parallelism: int = -1,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        meta_provider: Optional[Any] = None,
        partition_filter: Optional[Any] = None,
        shuffle: Optional[str] = None,
        include_paths: bool = False,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
    ):
        """
        Read Delta table as Ray Dataset.

        For CDF mode, delegates to DeltaCDFDatasource.
        For snapshot mode, delegates to read_parquet for efficiency.

        Args:
            parallelism: Parallelism level (deprecated, use override_num_blocks)
            ray_remote_args: Ray remote arguments for tasks
            meta_provider: Parquet metadata provider
            partition_filter: Path partition filter callback
            shuffle: Shuffle mode ("files" or None)
            include_paths: Include file paths in output
            concurrency: Maximum concurrent tasks
            override_num_blocks: Number of output blocks

        Returns:
            Dataset containing Delta table data
        """
        if self.cdf:
            # Delegate to DeltaCDFDatasource for streaming CDF execution
            from ray.data._internal.datasource.delta.delta_cdf_datasource import (
                DeltaCDFDatasource,
            )
            from ray.data._internal.datasource.delta.utilities import (
                convert_pyarrow_filter_to_sql,
            )

            # Convert PyArrow filters to SQL predicate for Delta Lake CDF
            pyarrow_filters = self.arrow_parquet_args.get("filters")
            sql_predicate = convert_pyarrow_filter_to_sql(pyarrow_filters)

            cdf_datasource = DeltaCDFDatasource(
                path=self.path,
                starting_version=self.starting_version,
                ending_version=self.ending_version,
                storage_options=self.storage_options,
                columns=self.columns,
                predicate=sql_predicate,
            )

            return cdf_datasource.read_as_dataset(
                parallelism=parallelism,
                ray_remote_args=ray_remote_args,
                concurrency=concurrency,
                override_num_blocks=override_num_blocks,
            )
        else:
            # Snapshot reads delegate to read_parquet for efficiency
            return self._read_snapshot(
                parallelism=parallelism,
                ray_remote_args=ray_remote_args,
                meta_provider=meta_provider,
                partition_filter=partition_filter,
                shuffle=shuffle,
                include_paths=include_paths,
                concurrency=concurrency,
                override_num_blocks=override_num_blocks,
            )

    def _read_snapshot(
        self,
        parallelism: int,
        ray_remote_args: Optional[Dict[str, Any]],
        meta_provider: Optional[Any],
        partition_filter: Optional[Any],
        shuffle: Optional[str],
        include_paths: bool,
        concurrency: Optional[int],
        override_num_blocks: Optional[int],
    ):
        """
        Read Delta table snapshot using read_parquet.

        Gets file paths from Delta table metadata and delegates to
        read_parquet for efficient distributed reading.

        Args:
            parallelism: Parallelism level
            ray_remote_args: Ray remote arguments
            meta_provider: Parquet metadata provider
            partition_filter: Partition filter callback
            shuffle: Shuffle mode
            include_paths: Include file paths
            concurrency: Max concurrent tasks
            override_num_blocks: Number of blocks

        Returns:
            Dataset containing snapshot data
        """
        from ray.data import read_parquet

        # Get Parquet file paths from Delta table
        file_paths = self.get_file_paths()

        # Delegate to read_parquet with Delta-resolved file paths
        return read_parquet(
            file_paths,
            filesystem=self.filesystem,
            columns=self.columns,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=self.partitioning,
            shuffle=shuffle,
            include_paths=include_paths,
            file_extensions=["parquet"],
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **self.arrow_parquet_args,
        )

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaLake"

    def get_table_version(self) -> int:
        """
        Get current Delta table version.

        Returns:
            Current version number

        Examples:
            >>> datasource = DeltaDatasource("/path/to/table") # doctest: +SKIP
            >>> version = datasource.get_table_version() # doctest: +SKIP
        """
        return self.delta_table.version()

    def get_table_schema(self):
        """
        Get Delta table schema.

        Returns:
            PyArrow schema

        Examples:
            >>> datasource = DeltaDatasource("/path/to/table") # doctest: +SKIP
            >>> schema = datasource.get_table_schema() # doctest: +SKIP
        """
        return self.delta_table.schema().to_pyarrow()

    def get_table_metadata(self) -> Dict[str, Any]:
        """
        Get Delta table metadata.

        Returns:
            Dictionary with table metadata including version, num_files, etc.

        Examples:
            >>> datasource = DeltaDatasource("/path/to/table") # doctest: +SKIP
            >>> metadata = datasource.get_table_metadata() # doctest: +SKIP
            >>> print(f"Version: {metadata['version']}") # doctest: +SKIP
        """
        dt = self.delta_table
        file_paths = self.get_file_paths()

        return {
            "version": dt.version(),
            "num_files": len(file_paths),
            "schema": dt.schema().to_pyarrow(),
            "partition_columns": dt.metadata().partition_columns,
        }

    def __repr__(self) -> str:
        """String representation of datasource."""
        mode = "CDF" if self.cdf else "snapshot"
        version_info = f", version={self.version}" if self.version else ""
        return f"DeltaDatasource(path={self.path}, mode={mode}{version_info})"
