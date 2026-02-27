"""Datasource for reading Delta Lake tables.

This module provides a Ray Data datasource for reading Delta Lake tables.

Delta Lake: https://delta.io/
delta-rs Python library: https://delta-io.github.io/delta-rs/python/
PyArrow: https://arrow.apache.org/docs/python/
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
        storage_options: Optional[Dict[str, str]] = None,
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
            storage_options: Cloud storage authentication credentials.
            filesystem: PyArrow filesystem for reading files.
            columns: List of column names to read.
            partitioning: Partitioning scheme for reading files.
            meta_provider: File metadata provider for reading files.
            partition_filter: Path partition filter for filtering partitions.
            shuffle: Shuffle mode for reading files ("files" or None).
            include_paths: Whether to include file paths in the output.
            **arrow_parquet_args: Additional arguments passed to PyArrow
                parquet reader.
        """
        _check_import(self, module="deltalake", package="deltalake")

        if not isinstance(path, str):
            raise ValueError(
                "Only single Delta table path supported (not list of paths)"
            )
        if not path or not path.strip():
            raise ValueError("Delta table path cannot be empty")

        self.path = path
        self.storage_options = storage_options or {}
        self.filesystem = filesystem
        self.columns = columns
        self.partitioning = partitioning
        self.meta_provider = meta_provider
        self.partition_filter = partition_filter
        self.shuffle = shuffle
        self.include_paths = include_paths
        self.arrow_parquet_args = arrow_parquet_args
        self._delta_table = None
        self._delta_table_storage_options: Dict[str, str] = dict(self.storage_options)

    @property
    def delta_table(self):
        """Lazy-load Delta table object.

        Reference: https://delta-io.github.io/delta-rs/python/api/deltalake.html#deltalake.DeltaTable
        """
        if self._delta_table is None or self._needs_new_table():
            from deltalake import DeltaTable

            dt_kwargs = {}
            if self.storage_options:
                dt_kwargs["storage_options"] = self.storage_options
            self._delta_table = DeltaTable(self.path, **dt_kwargs)
            self._delta_table_storage_options = dict(self.storage_options)
        return self._delta_table

    def get_file_paths(self) -> List[str]:
        """Get list of Parquet file paths from Delta table.

        Reference: https://delta-io.github.io/delta-rs/python/api/deltalake.html#deltalake.DeltaTable.file_uris

        Returns:
            List of absolute file URIs to read.
        """
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

        from ray.data._internal.datasource.parquet_datasource import (
            ParquetDatasource,
        )

        filesystem = self.filesystem
        if filesystem is None:
            filesystem = create_filesystem_from_storage_options(
                self.path, self.storage_options
            )

        arrow_parquet_args_copy = dict(self.arrow_parquet_args)
        dataset_kwargs = arrow_parquet_args_copy.pop("dataset_kwargs", None)
        to_batch_kwargs = (
            arrow_parquet_args_copy if arrow_parquet_args_copy else None
        )

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
        """Whether this datasource supports distributed reads."""
        if _is_local_scheme(self.path):
            return False
        path_lower = self.path.lower()
        if path_lower.startswith("file://"):
            return False
        if "://" not in self.path:
            return False
        return True

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaLake"

    def __repr__(self) -> str:
        """String representation for debugging."""
        parts = [f"path={self.path!r}"]
        if self.columns:
            parts.append(f"columns={self.columns}")
        return f"DeltaDatasource({', '.join(parts)})"

    def _needs_new_table(self) -> bool:
        """Return True if cached DeltaTable is stale for current settings."""
        current_opts = dict(self.storage_options) if self.storage_options else {}
        cached_opts = self._delta_table_storage_options or {}
        if current_opts != cached_opts:
            return True
        return False
