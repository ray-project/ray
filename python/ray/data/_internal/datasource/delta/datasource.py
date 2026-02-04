"""Datasource for reading Delta Lake tables."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data._internal.datasource.delta.utils import (
    create_filesystem_from_storage_options,
)
from ray.data._internal.util import _check_import, _is_local_scheme
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.file_meta_provider import FileMetadataProvider
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class DeltaDatasource(Datasource):
    """Datasource for reading Delta Lake tables with Ray Data."""

    def __init__(
        self,
        path: str,
        *,
        version: Optional[Union[int, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        partition_filters: Optional[List[tuple]] = None,
        filesystem: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        partitioning: Partitioning = Partitioning("hive"),
        meta_provider: Optional[FileMetadataProvider] = None,
        partition_filter: Optional[PathPartitionFilter] = None,
        shuffle: Optional[Any] = None,
        include_paths: bool = False,
        **arrow_parquet_args,
    ):
        """Initialize Delta Lake datasource.

        Args:
            path: Path to Delta Lake table.
            version: Version to read for time travel (integer or ISO 8601 timestamp).
            storage_options: Cloud storage authentication credentials.
            partition_filters: Delta Lake partition filters as list of tuples.
            filesystem: PyArrow filesystem for reading files.
            columns: List of column names to read.
            partitioning: Partitioning scheme for reading files.
            meta_provider: File metadata provider for reading files.
            partition_filter: Path partition filter for filtering partitions.
            shuffle: Shuffle mode for reading files ("files" or None).
            include_paths: Whether to include file paths in the output.
            **arrow_parquet_args: Additional arguments passed to PyArrow parquet reader.
        """
        _check_import(self, module="deltalake", package="deltalake")

        if not isinstance(path, str):
            raise ValueError(
                "Only single Delta table path supported (not list of paths)"
            )
        if not path or not path.strip():
            raise ValueError("Delta table path cannot be empty")

        # Validate version type
        if version is not None and not isinstance(version, (int, str)):
            raise TypeError(
                f"version must be int or str (ISO 8601 timestamp), got {type(version).__name__}"
            )

        self.path = path
        self.version = version
        self.storage_options = storage_options or {}
        # Normalize partition filters: convert values to strings per delta-rs requirements
        self.partition_filters = self._normalize_partition_filters(partition_filters)
        self.filesystem = filesystem
        self.columns = columns
        self.partitioning = partitioning
        self.meta_provider = meta_provider
        self.partition_filter = partition_filter
        self.shuffle = shuffle
        self.include_paths = include_paths
        self.arrow_parquet_args = arrow_parquet_args
        self._delta_table = None
        self._delta_table_version: Optional[Union[int, str]] = None
        self._delta_table_storage_options: Dict[str, str] = dict(self.storage_options)

    @property
    def delta_table(self):
        """Lazy-load Delta table object."""
        if self._delta_table is None or self._needs_new_table():
            from deltalake import DeltaTable

            dt_kwargs = {}
            if self.storage_options:
                dt_kwargs["storage_options"] = self.storage_options
            # DeltaTable constructor only accepts int version, not timestamp strings
            # For timestamp strings, create table first then use load_as_version()
            if self.version is not None:
                if isinstance(self.version, int):
                    dt_kwargs["version"] = self.version
            self._delta_table = DeltaTable(self.path, **dt_kwargs)
            # Handle timestamp string versions using load_as_version()
            if self.version is not None and isinstance(self.version, str):
                self._delta_table.load_as_version(self.version)
            self._delta_table_version = self.version
            self._delta_table_storage_options = dict(self.storage_options)
        return self._delta_table

    def _normalize_partition_filters(
        self, partition_filters: Optional[List[tuple]]
    ) -> Optional[List[tuple]]:
        """Normalize partition filter values to strings per delta-rs requirements.

        delta-rs requires partition filter values to be strings:
        - Integers: 2024 -> "2024"
        - None/null: None -> "" (empty string)
        - Other types: converted to string

        Args:
            partition_filters: List of (column, op, value) tuples.

        Returns:
            Normalized partition filters with string values.
        """
        if partition_filters is None:
            return None

        normalized = []
        for filter_tuple in partition_filters:
            if len(filter_tuple) != 3:
                raise ValueError(
                    f"Partition filter must be (column, op, value) tuple, got: {filter_tuple}"
                )
            column, op, value = filter_tuple

            # Validate operator
            valid_ops = {"=", "!=", "in", "not in"}
            if op not in valid_ops:
                raise ValueError(
                    f"Invalid partition filter operator '{op}'. Valid: {valid_ops}"
                )

            # Normalize value to string
            if value is None:
                normalized_value = ""  # NULL represented as empty string in delta-rs
            elif isinstance(value, (list, tuple)):
                # For "in" and "not in" operators
                normalized_value = ["" if v is None else str(v) for v in value]
            else:
                normalized_value = str(value)

            normalized.append((column, op, normalized_value))

        return normalized

    def get_file_paths(self) -> List[str]:
        """Get list of Parquet file paths from Delta table.

        Uses file_uris() which returns absolute URIs. These are passed to
        ParquetDatasource which handles URI resolution internally.
        """
        if self.partition_filters is not None:
            return self.delta_table.file_uris(partition_filters=self.partition_filters)
        return self.delta_table.file_uris()

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Get read tasks for Delta table snapshot reads."""
        file_paths = self.get_file_paths()
        if not file_paths:
            return []

        from ray.data._internal.datasource.parquet_datasource import ParquetDatasource

        # If no filesystem was supplied, try to construct one from storage options
        # so that Parquet reads use the same credentials as Delta metadata access.
        filesystem = self.filesystem
        if filesystem is None:
            filesystem = create_filesystem_from_storage_options(
                self.path, self.storage_options
            )

        # Extract ParquetDatasource-specific kwargs from arrow_parquet_args
        # ParquetDatasource expects dataset_kwargs and to_batch_kwargs, not arbitrary kwargs
        # Similar to read_parquet: extract dataset_kwargs, pass rest as to_batch_kwargs
        arrow_parquet_args_copy = dict(self.arrow_parquet_args)
        dataset_kwargs = arrow_parquet_args_copy.pop("dataset_kwargs", None)
        # All remaining args (like filter, batch_size, etc.) go to to_batch_kwargs
        to_batch_kwargs = arrow_parquet_args_copy if arrow_parquet_args_copy else None

        parquet_datasource = ParquetDatasource(
            file_paths,
            columns=self.columns,
            filesystem=filesystem,
            partitioning=self.partitioning,
            meta_provider=self.meta_provider,
            partition_filter=self.partition_filter,
            shuffle=self.shuffle,
            include_paths=self.include_paths,
            dataset_kwargs=dataset_kwargs,
            to_batch_kwargs=to_batch_kwargs,
        )

        return parquet_datasource.get_read_tasks(
            parallelism, per_task_row_limit, data_context=data_context
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for the Delta table."""
        return None

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.

        Local filesystem reads require all tasks to run on the same node
        to ensure files are accessible.
        """
        # Check for local:// scheme (Ray's custom scheme)
        if _is_local_scheme(self.path):
            return False

        # Check for regular local filesystem paths
        path_lower = self.path.lower()
        if path_lower.startswith("file://"):
            return False

        # Check for paths without scheme (local filesystem)
        if "://" not in self.path:
            return False

        return True

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaLake"

    def __repr__(self) -> str:
        """String representation for debugging."""
        parts = [f"path={self.path!r}"]
        if self.version is not None:
            parts.append(f"version={self.version!r}")
        if self.columns:
            parts.append(f"columns={self.columns}")
        if self.partition_filters:
            parts.append(f"partition_filters={self.partition_filters}")
        return f"DeltaDatasource({', '.join(parts)})"

    def _needs_new_table(self) -> bool:
        """Return True if cached DeltaTable is stale for current settings.

        Checks if version or storage options have changed since last load.
        """
        # Check if version changed
        if self._delta_table_version != self.version:
            return True
        # Compare storage options by content, not reference
        current_opts = dict(self.storage_options) if self.storage_options else {}
        cached_opts = self._delta_table_storage_options or {}
        if current_opts != cached_opts:
            return True
        return False
