"""
Delta Lake datasource implementation for reading Delta tables.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import pyarrow.fs as pa_fs

from ray.data._internal.datasource.delta.utilities import to_pyarrow_schema

from ray.data._internal.util import _check_import, _is_local_scheme
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.file_meta_provider import FileMetadataProvider
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.util.annotations import PublicAPI

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

        self.path = path
        self.version = version
        self.storage_options = storage_options or {}
        self.partition_filters = partition_filters
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

    def get_file_paths(self) -> List[str]:
        """Get list of Parquet file paths from Delta table."""
        if self.partition_filters is not None:
            return self.delta_table.file_uris(partition_filters=self.partition_filters)
        return self.delta_table.file_uris()

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Get read tasks for Delta table snapshot reads."""
        file_paths = self.get_file_paths()
        if not file_paths:
            return []

        from ray.data._internal.datasource.parquet_datasource import ParquetDatasource

        filesystem = self.filesystem
        # If no filesystem was supplied, try to construct one from storage options
        # so that Parquet reads use the same credentials as Delta metadata access.
        if filesystem is None:
            path_lower = self.path.lower()
            if path_lower.startswith(("s3://", "s3a://")):
                access_key = self.storage_options.get("AWS_ACCESS_KEY_ID")
                secret_key = self.storage_options.get("AWS_SECRET_ACCESS_KEY")
                session_token = self.storage_options.get("AWS_SESSION_TOKEN")
                region = self.storage_options.get("AWS_REGION")
                if access_key or secret_key or session_token or region:
                    filesystem = pa_fs.S3FileSystem(
                        access_key=access_key,
                        secret_key=secret_key,
                        session_token=session_token,
                        region=region,
                    )
            elif path_lower.startswith(("abfss://", "abfs://")):
                token = self.storage_options.get("AZURE_STORAGE_TOKEN")
                if token:
                    filesystem = pa_fs.AzureFileSystem(token=token)

        parquet_datasource = ParquetDatasource(
            file_paths,
            columns=self.columns,
            filesystem=filesystem,
            partitioning=self.partitioning,
            meta_provider=self.meta_provider,
            partition_filter=self.partition_filter,
            shuffle=self.shuffle,
            include_paths=self.include_paths,
            **self.arrow_parquet_args,
        )

        return parquet_datasource.get_read_tasks(parallelism, per_task_row_limit)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for the Delta table."""
        return None

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads."""
        return not _is_local_scheme(self.path)

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaLake"

    def get_table_version(self) -> int:
        """Get current Delta table version."""
        return self.delta_table.version()

    def get_table_schema(self):
        """Get Delta table schema."""
        return to_pyarrow_schema(self.delta_table.schema())

    def get_table_metadata(self) -> Dict[str, Any]:
        """Get Delta table metadata."""
        dt = self.delta_table
        file_paths = self.get_file_paths()
        return {
            "version": dt.version(),
            "num_files": len(file_paths),
            "schema": to_pyarrow_schema(dt.schema()),
            "partition_columns": dt.metadata().partition_columns,
        }

    def __repr__(self) -> str:
        """String representation of datasource."""
        version_info = f", version={self.version}" if self.version else ""
        return f"DeltaDatasource(path={self.path}{version_info})"

    def _needs_new_table(self) -> bool:
        """Return True if cached DeltaTable is stale for current settings."""
        if self._delta_table is None:
            return True
        if self._delta_table_version != self.version:
            return True
        if self._delta_table_storage_options != dict(self.storage_options):
            return True
        return False
