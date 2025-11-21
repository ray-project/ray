"""Internal implementation of the unified read() function for Ray Data.

This module provides automatic format detection and routes to appropriate readers.
All path expansion, glob patterns, and file collection is handled by native readers.

The implementation uses PyArrow filesystems for path resolution and file system operations.
See https://arrow.apache.org/docs/python/filesystems.html for PyArrow filesystem documentation.
"""

import inspect
import logging
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)

import pyarrow.fs as pafs

from ray.data import Dataset
from ray.data._internal.format_detection import LakehouseFormat
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FileShuffleConfig,
    Partitioning,
    PathPartitionFilter,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class LakehouseTable:
    """Represents a detected lakehouse table."""

    path: str
    format: LakehouseFormat


@dataclass
class ReadConfig:
    """Configuration for read operations."""

    paths: List[str]
    format: Optional[str] = None
    filesystem: Optional["pafs.FileSystem"] = None
    parallelism: int = -1
    num_cpus: Optional[float] = None
    num_gpus: Optional[float] = None
    memory: Optional[float] = None
    ray_remote_args: Optional[Dict[str, Any]] = None
    arrow_open_file_args: Optional[Dict[str, Any]] = None
    meta_provider: Optional[BaseFileMetadataProvider] = None
    partition_filter: Optional[PathPartitionFilter] = None
    partitioning: Optional[Partitioning] = None
    include_paths: bool = False
    ignore_missing_paths: bool = False
    shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None
    file_extensions: Optional[List[str]] = None
    concurrency: Optional[int] = None
    override_num_blocks: Optional[int] = None
    reader_args: Dict[str, Any] = field(default_factory=dict)


def _detect_lakehouse_tables(
    paths: List[str], filesystem: "pafs.FileSystem"
) -> List[LakehouseTable]:
    """Detect lakehouse tables in paths (only checks directories).

        Args:
    paths: List of paths to check for lakehouse table markers.
    filesystem: PyArrow filesystem to use for inspection.

        Returns:
    List of detected lakehouse tables.
    """
    from ray.data._internal.format_detection import _detect_lakehouse_format

    tables = []
        for path in paths:
        file_info = filesystem.get_file_info(path)
        if file_info.type == pafs.FileType.Directory:
            fmt = _detect_lakehouse_format(path, filesystem)
            if fmt:
                tables.append(LakehouseTable(path=path, format=LakehouseFormat(fmt)))
    return tables


def _group_files_by_format(
    paths: List[str], warn_on_binary: bool = True
) -> Dict[str, List[str]]:
    """Group file paths by detected format using extension detection.

        Args:
    paths: List of file paths to group.
    warn_on_binary: Whether to warn when files fall back to binary format.

        Returns:
    Dictionary mapping format names to lists of file paths.
    """
    from pathlib import Path

    from ray.data._internal.format_detection import (
        _EXTENSION_TO_READER_MAP,
        _get_reader_for_path,
    )

    files_by_format: Dict[str, List[str]] = {}
        unknown_files = []
    unknown_extensions = set()

    for path in paths:
        format_name = _get_reader_for_path(path)
        if format_name:
            if format_name not in files_by_format:
                files_by_format[format_name] = []
            files_by_format[format_name].append(path)
                else:
            unknown_files.append(path)
            # Extract extension for warning
            path_obj = Path(path)
            ext = path_obj.suffix.lower().lstrip(".")
            if ext:
                unknown_extensions.add(ext)

        if unknown_files:
            if warn_on_binary:
            ext_list = (
                ", ".join(sorted(unknown_extensions))
                if unknown_extensions
                else "no extension"
            )
            logger.warning(
                f"Detected {len(unknown_files)} file(s) with unrecognized format(s) "
                f"(extensions: {ext_list}). Falling back to binary reader. "
                f"Examples: {unknown_files[:3]}. "
                f"Use format parameter to specify the correct format, or ensure files have "
                f"recognized extensions. Supported formats: {sorted(_EXTENSION_TO_READER_MAP.keys())}"
            )
        files_by_format["binary"] = unknown_files

    return files_by_format


class ReaderRegistry:
    """Registry of format readers.

    This class maintains mappings between format names and their corresponding
    reader functions. Readers are lazily loaded on first use.
    """

    def __init__(self):
        self._readers = None
        self._lakehouse_readers = None

    def _ensure_readers_loaded(self):
        """Lazy load readers on first use."""
        if self._readers is not None:
            return

        from ray.data.read_api import (
            read_audio,
            read_avro,
            read_bigquery,
            read_binary_files,
            read_clickhouse,
            read_csv,
            read_databricks_tables,
            read_delta,
            read_delta_sharing_tables,
            read_hudi,
            read_iceberg,
            read_images,
            read_json,
            read_lance,
            read_mcap,
            read_mongo,
            read_numpy,
            read_parquet,
            read_parquet_bulk,
            read_snowflake,
            read_sql,
            read_text,
            read_tfrecords,
            read_unity_catalog,
            read_videos,
            read_webdataset,
        )

        self._readers = {
            "parquet": read_parquet,
            "parquet_bulk": read_parquet_bulk,
            "csv": read_csv,
            "json": read_json,
            "text": read_text,
            "images": read_images,
            "audio": read_audio,
            "video": read_videos,
            "numpy": read_numpy,
            "avro": read_avro,
            "tfrecords": read_tfrecords,
            "webdataset": read_webdataset,
            "lance": read_lance,
            "mcap": read_mcap,
            "binary": read_binary_files,
            "delta": read_delta,
            "delta_sharing": read_delta_sharing_tables,
            "hudi": read_hudi,
            "iceberg": read_iceberg,
            "sql": read_sql,
            "bigquery": read_bigquery,
            "mongo": read_mongo,
            "mongodb": read_mongo,
            "clickhouse": read_clickhouse,
            "snowflake": read_snowflake,
            "databricks": read_databricks_tables,
            "unity_catalog": read_unity_catalog,
        }

        self._lakehouse_readers = {
            LakehouseFormat.DELTA: read_delta,
            LakehouseFormat.HUDI: read_hudi,
            LakehouseFormat.ICEBERG: read_iceberg,
        }

    def get_format_reader(self, format: str) -> Callable:
        """Get reader function for a file format.

        Args:
            format: Format name (e.g., "parquet", "csv").

        Returns:
            Reader function.

        Raises:
            ValueError: If format is not supported.
        """
        self._ensure_readers_loaded()

        format_lower = format.lower()
        if format_lower not in self._readers:
            raise ValueError(
                f"Unsupported format: '{format}'. "
                f"Supported formats: {sorted(self._readers.keys())}"
            )

        return self._readers[format_lower]

    def get_lakehouse_reader(self, format: Union[LakehouseFormat, str]) -> Callable:
        """Get reader function for a lakehouse format.

        Args:
            format: Lakehouse format name or enum value.

        Returns:
            Reader function.

        Raises:
            ValueError: If format is not supported.
        """
        self._ensure_readers_loaded()

        if isinstance(format, str):
            try:
                format = LakehouseFormat(format.lower())
            except ValueError:
                raise ValueError(f"Unsupported lakehouse format: '{format}'")

        if format not in self._lakehouse_readers:
            raise ValueError(f"No reader registered for lakehouse format: {format}")

        return self._lakehouse_readers[format]


class DatasetReader:
    """Reads datasets using appropriate readers based on file types."""

    def __init__(self, config: ReadConfig, registry: ReaderRegistry):
        self.config = config
        self.registry = registry

    def read_lakehouse_tables(self, tables: List[LakehouseTable]) -> Dataset:
        """Read lakehouse tables and combine them.

        Args:
            tables: List of detected lakehouse tables.

        Returns:
            Combined dataset from all tables.

        Raises:
            ValueError: If no tables provided or multiple formats detected.
        """
        if not tables:
            raise ValueError("No lakehouse tables provided")

        if len(tables) > 1:
            formats_found = {table.format for table in tables}
            if len(formats_found) > 1:
                format_list = ", ".join(sorted(f.value for f in formats_found))
                raise ValueError(
                    f"Multiple lakehouse formats detected: {format_list}. "
                    "Use format parameter to specify which to read."
                )

        datasets = []
        for table in tables:
            reader_func = self.registry.get_lakehouse_reader(table.format)
            ds = self._call_reader(
                reader_func=reader_func, paths=table.path, is_lakehouse=True
            )
            datasets.append(ds)

        return self._combine_datasets(datasets)

    def read_file_groups(self, files_by_format: Dict[str, List[str]]) -> Dataset:
        """Read file groups and combine them.

        Args:
            files_by_format: Dictionary mapping format names to file paths.

        Returns:
            Combined dataset from all file groups.
        """
        datasets = []

        for format_name, file_list in sorted(files_by_format.items()):
            if not file_list:
                continue

            reader_func = self.registry.get_format_reader(format_name)
            ds = self._call_reader(
                reader_func=reader_func, paths=file_list, is_lakehouse=False
            )
            datasets.append(ds)

        return self._combine_datasets(datasets)

    def _call_reader(
        self, reader_func: Callable, paths: Union[str, List[str]], is_lakehouse: bool
    ) -> Dataset:
        """Call a reader function with appropriate arguments.

        This method uses introspection to determine which parameters the reader
        function accepts and passes only compatible arguments.

        Args:
            reader_func: Reader function to call.
            paths: Path or list of paths to read.
            is_lakehouse: Whether this is a lakehouse table reader.

        Returns:
            Dataset from the reader.
        """
        reader_sig = inspect.signature(reader_func)
        reader_params = set(reader_sig.parameters.keys())

        kwargs = {}

        if is_lakehouse:
            for param_name in ["path", "table_uri", "uri"]:
                if param_name in reader_params:
                    kwargs[param_name] = paths
                    break
        else:
            if "paths" in reader_params:
                kwargs["paths"] = paths
            elif "path" in reader_params:
                kwargs["path"] = paths

        common_args = {
            "filesystem": self.config.filesystem,
            "parallelism": self.config.parallelism,
            "num_cpus": self.config.num_cpus,
            "num_gpus": self.config.num_gpus,
            "memory": self.config.memory,
            "ray_remote_args": self.config.ray_remote_args,
            "meta_provider": self.config.meta_provider,
            "partition_filter": self.config.partition_filter,
            "include_paths": self.config.include_paths,
            "ignore_missing_paths": self.config.ignore_missing_paths,
            "shuffle": self.config.shuffle,
            "concurrency": self.config.concurrency,
            "override_num_blocks": self.config.override_num_blocks,
            "arrow_open_file_args": self.config.arrow_open_file_args,
            "partitioning": self.config.partitioning,
        }

        for key, value in common_args.items():
            if key in reader_params and value is not None:
                kwargs[key] = value

        kwargs.update(self.config.reader_args)
            return reader_func(**kwargs)

    def _combine_datasets(self, datasets: List[Dataset]) -> Dataset:
        """Combine multiple datasets using union.

        Args:
            datasets: List of datasets to combine.

        Returns:
            Combined dataset.
        """
        if len(datasets) == 1:
            return datasets[0]

            result = datasets[0]
            for ds in datasets[1:]:
                result = result.union(ds)
            return result


def _validate_read_parameters(
    paths: Union[str, List[str]],
    format: Optional[str],
    parallelism: int,
    num_cpus: Optional[float],
    num_gpus: Optional[float],
    memory: Optional[float],
    file_extensions: Optional[List[str]],
    concurrency: Optional[int],
    max_files: Optional[int],
) -> None:
    """Validate input parameters for ray.data.read().

    Args:
        paths: Path or list of paths.
        format: Optional format hint.
        parallelism: Parallelism setting.
        num_cpus: Number of CPUs per task.
        num_gpus: Number of GPUs per task.
        memory: Memory per task.
        file_extensions: Optional file extension filter.
        concurrency: Maximum concurrent tasks.
        max_files: Maximum number of files.

    Raises:
        ValueError: If paths is empty.
    """
    paths_list = [paths] if isinstance(paths, str) else paths
    if not paths_list:
        raise ValueError("paths cannot be empty")


def read_impl(
    paths: Union[str, List[str]],
    *,
    format: Optional[str] = None,
    filesystem: Optional["pafs.FileSystem"] = None,
    parallelism: int = -1,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Optional[Partitioning] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    max_files: Optional[int] = None,
    strict: bool = False,
    warn_on_binary_fallback: bool = True,
    **reader_args,
) -> Dataset:
    """Internal implementation of ray.data.read().

    This function handles format detection and routes to appropriate readers.
    All path expansion, glob patterns, and file collection is handled by native readers.

    Args:
        paths: Path or list of paths to read.
        format: Optional format hint to bypass auto-detection.
        filesystem: Optional PyArrow filesystem.
        parallelism: Deprecated, use override_num_blocks.
        num_cpus: CPUs per read task.
        num_gpus: GPUs per read task.
        memory: Memory per read task.
        ray_remote_args: Ray remote arguments.
        arrow_open_file_args: PyArrow file opening arguments.
        meta_provider: Custom metadata provider.
        partition_filter: Partition filtering function.
        partitioning: Partitioning scheme.
        include_paths: Include file paths in rows.
        ignore_missing_paths: Ignore missing files.
        shuffle: File shuffling configuration.
        file_extensions: Filter by file extensions.
        concurrency: Maximum concurrent tasks.
        override_num_blocks: Override output block count.
        max_files: Maximum number of files (unused, kept for compatibility).
        strict: Strict mode flag (unused, kept for compatibility).
        warn_on_binary_fallback: Warn when falling back to binary reader.
        **reader_args: Format-specific reader arguments.

    Returns:
        Dataset from the read operation.

    Raises:
        ValueError: If paths are empty or incompatible formats are mixed.
    """
    _validate_read_parameters(
        paths=paths,
        format=format,
        parallelism=parallelism,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        file_extensions=file_extensions,
        concurrency=concurrency,
        max_files=max_files,
    )

    # Normalize paths to list
    if isinstance(paths, str):
        paths = [paths]

    # Resolve filesystem
    if filesystem is None:
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        _, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    # Create configuration
    config = ReadConfig(
        paths=paths,
        filesystem=filesystem,
        format=format,
        parallelism=parallelism,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        ray_remote_args=ray_remote_args,
        arrow_open_file_args=arrow_open_file_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        include_paths=include_paths,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        file_extensions=file_extensions,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        reader_args=reader_args,
    )

    registry = ReaderRegistry()
    reader = DatasetReader(config, registry)

    # Fast path: format is explicitly specified - pass paths directly to reader
    if format is not None:
        reader_func = registry.get_format_reader(format)
        return reader._call_reader(
            reader_func=reader_func, paths=paths, is_lakehouse=False
        )

    # Auto-detection: check for lakehouse tables and detect file formats
    lakehouse_tables = _detect_lakehouse_tables(paths, filesystem)

    # Separate lakehouse directories from regular paths
    lakehouse_paths = {table.path for table in lakehouse_tables}
    regular_paths = [p for p in paths if p not in lakehouse_paths]

    # Warn about directories that weren't detected as lakehouse tables
    if warn_on_binary_fallback:
        undetected_dirs = []
        for path in regular_paths:
            file_info = filesystem.get_file_info(path)
            if file_info.type == pafs.FileType.Directory:
                undetected_dirs.append(path)
        if undetected_dirs:
            logger.warning(
                f"Found {len(undetected_dirs)} directory path(s) that were not detected as "
                f"lakehouse tables: {undetected_dirs[:3]}. "
                f"These will be treated as regular file directories. "
                f"If these are lakehouse tables, ensure they contain the required markers "
                f"(_delta_log for Delta, .hoodie for Hudi, metadata/ for Iceberg)."
            )

    # If only lakehouse tables, route directly
    if lakehouse_tables and not regular_paths:
        return reader.read_lakehouse_tables(lakehouse_tables)

    # Group regular paths by format
    files_by_format = (
        _group_files_by_format(regular_paths, warn_on_binary=warn_on_binary_fallback)
        if regular_paths
        else {}
    )

    # Validate lakehouse tables aren't mixed with regular files
    if lakehouse_tables and files_by_format:
        raise ValueError(
            f"Cannot mix lakehouse tables with regular files. "
            f"Found {len(lakehouse_tables)} lakehouse table(s) and "
            f"{sum(len(files) for files in files_by_format.values())} regular file(s). "
            "Use format parameter to specify which to read."
        )

    # Read data with appropriate readers
    datasets = []

    if lakehouse_tables:
        datasets.append(reader.read_lakehouse_tables(lakehouse_tables))

    if files_by_format:
        datasets.append(reader.read_file_groups(files_by_format))

    if not datasets:
        raise ValueError(f"No data to read from paths: {paths}")

    return reader._combine_datasets(datasets)
