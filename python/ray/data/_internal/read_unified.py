"""Internal implementation of the unified read() function for Ray Data.

This module contains the implementation of ray.data.read(), which automatically
detects file formats and lakehouse table structures to select the appropriate
reader function.
"""

import inspect
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePosixPath
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import pyarrow.fs as pafs

import ray
from ray.data import Dataset
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FileShuffleConfig,
    Partitioning,
    PathPartitionFilter,
)

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


@ray.remote
def _collect_path_remote(
    path: str,
    filesystem: "pafs.FileSystem",
    detect_lakehouse: bool,
    ignore_missing: bool,
    max_files: Optional[int],
) -> Tuple[List[str], List[Dict[str, str]], Dict[str, str]]:
    """Ray task to collect files from a single path in parallel.

    Args:
        path: Path to collect from.
        filesystem: PyArrow filesystem.
        detect_lakehouse: Whether to detect lakehouse tables.
        ignore_missing: Whether to ignore missing paths.
        max_files: Maximum files to collect.

    Returns:
        Tuple of (regular_files, lakehouse_tables_dicts, errors).
    """
    regular_files = []
    lakehouse_tables = []
    errors = {}

    try:
        file_info = filesystem.get_file_info(path)

        if file_info.type == pafs.FileType.Directory:
            if detect_lakehouse:
                detector = LakehouseDetector(filesystem)
                lakehouse_format = detector.detect(path)
                if lakehouse_format:
                    lakehouse_tables.append(
                        {"path": path, "format": lakehouse_format.value}
                    )
                    return regular_files, lakehouse_tables, errors

            selector = pafs.FileSelector(path, recursive=True)
            files = filesystem.get_file_info(selector)

            for f in files:
                if f.type == pafs.FileType.File:
                    regular_files.append(f.path)
                    if max_files and len(regular_files) >= max_files:
                        break

        elif file_info.type == pafs.FileType.File:
            regular_files.append(path)

        elif file_info.type == pafs.FileType.NotFound:
            if not ignore_missing:
                errors[path] = "Path not found"

    except PermissionError as e:
        if not ignore_missing:
            errors[path] = f"Permission denied: {e}"
    except Exception as e:
        if not ignore_missing:
            errors[path] = str(e)

    return regular_files, lakehouse_tables, errors


class DataSource(str, Enum):
    """Supported data sources/filesystems."""

    S3 = "s3"
    GCS = "gs"
    AZURE = "azure"
    HDFS = "hdfs"
    HTTP = "http"
    HTTPS = "https"
    LOCAL = "local"
    UNKNOWN = "unknown"


class LakehouseFormat(str, Enum):
    """Supported lakehouse table formats."""

    DELTA = "delta"
    HUDI = "hudi"
    ICEBERG = "iceberg"


class FileFormat(str, Enum):
    """Supported file formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    TEXT = "text"
    IMAGES = "images"
    AUDIO = "audio"
    VIDEO = "video"
    NUMPY = "numpy"
    AVRO = "avro"
    TFRECORDS = "tfrecords"
    WEBDATASET = "webdataset"
    LANCE = "lance"
    BINARY = "binary"

    @classmethod
    def list_formats(cls) -> List[str]:
        """Return a list of all supported format names.

        Returns:
            List of format names (e.g., ['parquet', 'csv', 'json', ...])
        """
        return [fmt.value for fmt in cls if fmt != cls.BINARY]

    @classmethod
    def is_valid(cls, format_name: str) -> bool:
        """Check if a format name is valid.

        Args:
            format_name: Format name to check.

        Returns:
            True if format is valid, False otherwise.
        """
        format_lower = format_name.lower()
        return format_lower in {fmt.value for fmt in cls}


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
    max_files: Optional[int] = None
    strict: bool = False
    warn_on_binary_fallback: bool = True
    dry_run: bool = False


@dataclass
class FileGroup:
    """A group of files of the same type."""

    file_type: str
    file_paths: List[str]
    reader_func: Callable


@dataclass
class LakehouseTable:
    """Information about a detected lakehouse table."""

    path: str
    format: LakehouseFormat


@dataclass
class DetectionResult:
    """Results of file and lakehouse detection."""

    regular_files: List[str] = field(default_factory=list)
    lakehouse_tables: List[LakehouseTable] = field(default_factory=list)
    detected_sources: Dict[DataSource, int] = field(default_factory=dict)
    format_counts: Dict[str, int] = field(default_factory=dict)
    total_size_bytes: int = 0

    @property
    def all_files(self) -> List[str]:
        """Get all files (regular + lakehouse)."""
        return self.regular_files + [table.path for table in self.lakehouse_tables]


class SourceDetector:
    """Detects data source/filesystem from paths."""

    # Map URL schemes to data sources
    SCHEME_MAP = {
        "s3": DataSource.S3,
        "s3a": DataSource.S3,
        "s3n": DataSource.S3,
        "gs": DataSource.GCS,
        "gcs": DataSource.GCS,
        "az": DataSource.AZURE,
        "abfs": DataSource.AZURE,
        "abfss": DataSource.AZURE,
        "wasb": DataSource.AZURE,
        "wasbs": DataSource.AZURE,
        "hdfs": DataSource.HDFS,
        "http": DataSource.HTTP,
        "https": DataSource.HTTPS,
        "file": DataSource.LOCAL,
        "local": DataSource.LOCAL,
    }

    @classmethod
    def detect(cls, path: str) -> DataSource:
        """Detect the data source from a path.

        Args:
            path: Path to analyze.

        Returns:
            Detected DataSource.
        """
        # Parse URL to extract scheme using urllib
        parsed = urlparse(path)

        # Check for explicit scheme (e.g., s3://bucket/path)
        if parsed.scheme:
            return cls.SCHEME_MAP.get(parsed.scheme.lower(), DataSource.UNKNOWN)

        # Paths without scheme are considered local
        return DataSource.LOCAL

    @classmethod
    def detect_from_paths(cls, paths: List[str]) -> Dict[DataSource, int]:
        """Detect sources from multiple paths and count them.

        Args:
            paths: List of paths to analyze.

        Returns:
            Dictionary mapping DataSource to count.
        """
        source_counts: Dict[DataSource, int] = {}

        for path in paths:
            source = cls.detect(path)
            source_counts[source] = source_counts.get(source, 0) + 1

        return source_counts


class LakehouseDetector:
    """Detects lakehouse table formats based on directory structure."""

    # Characteristic markers for each lakehouse format
    MARKERS = {
        LakehouseFormat.DELTA: "_delta_log",
        LakehouseFormat.HUDI: ".hoodie",
        LakehouseFormat.ICEBERG: "metadata",
    }

    def __init__(self, filesystem: "pafs.FileSystem"):
        self.filesystem = filesystem

    def detect(self, path: str) -> Optional[LakehouseFormat]:
        """Detect if a path is a lakehouse table.

        Args:
            path: Path to check.

        Returns:
            LakehouseFormat if detected, None otherwise.
        """
        try:
            file_info = self.filesystem.get_file_info(path)
            if file_info.type != pafs.FileType.Directory:
                return None

            # Get directory contents
            base_names = self._get_directory_contents(path)

            # Check for Delta Lake
            if self.MARKERS[LakehouseFormat.DELTA] in base_names:
                logger.info(f"Detected Delta Lake table at: {path}")
                return LakehouseFormat.DELTA

            # Check for Hudi
            if self.MARKERS[LakehouseFormat.HUDI] in base_names:
                logger.info(f"Detected Apache Hudi table at: {path}")
                return LakehouseFormat.HUDI

            # Check for Iceberg (requires deeper inspection)
            if self.MARKERS[LakehouseFormat.ICEBERG] in base_names:
                if self._is_iceberg_table(path):
                    logger.info(f"Detected Apache Iceberg table at: {path}")
                    return LakehouseFormat.ICEBERG

        except Exception as e:
            logger.debug(f"Error detecting lakehouse format for {path}: {e}")

        return None

    def _get_directory_contents(self, path: str) -> Set[str]:
        """Get base names of items in a directory."""
        selector = pafs.FileSelector(path, recursive=False)
        contents = self.filesystem.get_file_info(selector)
        # Use PurePosixPath for cleaner path manipulation
        return {PurePosixPath(item.path).name for item in contents}

    def _is_iceberg_table(self, path: str) -> bool:
        """Check if a path with metadata directory is an Iceberg table."""
        try:
            metadata_path = f"{path}/metadata"
            metadata_files = self._get_directory_contents(metadata_path)

            # Iceberg tables have version-hint.text or .metadata.json files
            return "version-hint.text" in metadata_files or any(
                f.endswith(".metadata.json") for f in metadata_files
            )
        except Exception:
            return False


class FileTypeDetector:
    """Detects file types based on extensions.

    Builds extension mappings from datasource classes to ensure consistency.
    """

    def __init__(self):
        """Build extension map from datasource FILE_EXTENSIONS constants."""
        from ray.data._internal.datasource.audio_datasource import AudioDatasource
        from ray.data._internal.datasource.avro_datasource import AvroDatasource
        from ray.data._internal.datasource.csv_datasource import CSVDatasource
        from ray.data._internal.datasource.image_datasource import ImageDatasource
        from ray.data._internal.datasource.json_datasource import JSON_FILE_EXTENSIONS
        from ray.data._internal.datasource.mcap_datasource import McapDatasource
        from ray.data._internal.datasource.numpy_datasource import NumpyDatasource
        from ray.data._internal.datasource.parquet_bulk_datasource import (
            ParquetBulkDatasource,
        )
        from ray.data._internal.datasource.tfrecords_datasource import (
            TFRecordsDatasource,
        )
        from ray.data._internal.datasource.video_datasource import VideoDatasource
        from ray.data._internal.datasource.webdataset_datasource import (
            WebDatasetDatasource,
        )

        # Build extension map from datasource constants
        self.extension_map = {}

        # Tabular formats
        for ext in ParquetBulkDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.PARQUET
        # Add compressed parquet variants
        for compression in ["gz", "gzip", "bz2", "snappy", "lz4", "zstd"]:
            self.extension_map[f"parquet.{compression}"] = FileFormat.PARQUET

        for ext in CSVDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.CSV
        # CSV also supports bz2
        self.extension_map["csv.bz2"] = FileFormat.CSV

        for ext in JSON_FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.JSON
        # JSON also supports bz2
        for base in ["json", "jsonl"]:
            self.extension_map[f"{base}.bz2"] = FileFormat.JSON

        for ext in AvroDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.AVRO
        # Add compressed avro variants
        for compression in ["gz", "gzip", "bz2", "snappy"]:
            self.extension_map[f"avro.{compression}"] = FileFormat.AVRO

        for ext in TFRecordsDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.TFRECORDS

        for ext in McapDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.BINARY  # MCAP is binary

        # Media formats
        for ext in ImageDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.IMAGES

        for ext in AudioDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.AUDIO

        for ext in VideoDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.VIDEO

        # Special formats
        for ext in NumpyDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.NUMPY

        for ext in WebDatasetDatasource._FILE_EXTENSIONS:
            self.extension_map[ext.lower()] = FileFormat.WEBDATASET

        # Lance doesn't have _FILE_EXTENSIONS constant, define manually
        self.extension_map["lance"] = FileFormat.LANCE

        # Text format (simple extension)
        self.extension_map["txt"] = FileFormat.TEXT

    def detect_file_type(
        self, file_path: str, strict: bool = False
    ) -> Optional[FileFormat]:
        """Detect file type from path extension.

        Args:
            file_path: Path to the file.
            strict: If True, raise ValueError for unknown extensions.

        Returns:
            FileFormat if detected, None otherwise.

        Raises:
            ValueError: If strict=True and extension is not recognized.
        """
        # Extract filename from path using PurePosixPath (works with URLs too)
        filename = PurePosixPath(file_path).name

        # Normalize to lowercase for case-insensitive matching
        lower_filename = filename.lower()

        # Try compound extensions first (e.g., .csv.gz, .parquet.snappy)
        # Check up to 3 levels (e.g., .tar.gz, .parquet.snappy)
        for ext_len in [3, 2, 1]:
            parts = lower_filename.rsplit(".", ext_len)
            if len(parts) == ext_len + 1:
                potential_ext = ".".join(parts[1:])
                if potential_ext in self.extension_map:
                    return self.extension_map[potential_ext]

        # No extension or unknown extension
        if strict and "." in filename:
            # Extract the final extension using PurePosixPath
            ext_part = PurePosixPath(filename).suffix.lstrip(".")
            supported = ", ".join(sorted(set(self.extension_map.keys())))
            raise ValueError(
                f"Unknown file extension: '.{ext_part}' in file '{filename}'. "
                f"Supported extensions: {supported}. "
                f"Use format parameter to explicitly specify the format or set strict=False."
            )

        return None

    def group_files_by_type(
        self, file_paths: List[str], strict: bool = False, warn_on_binary: bool = True
    ) -> Dict[FileFormat, List[str]]:
        """Group files by their detected type.

        Args:
            file_paths: List of file paths.
            strict: If True, raise error for unknown extensions.
            warn_on_binary: If True, log warning when falling back to binary.

        Returns:
            Dictionary mapping FileFormat to list of paths.

        Raises:
            ValueError: If strict=True and unknown extensions found.
        """
        files_by_type: Dict[FileFormat, List[str]] = {}
        unknown_files = []

        for file_path in file_paths:
            try:
                file_type = self.detect_file_type(file_path, strict=strict)

                if file_type:
                    if file_type not in files_by_type:
                        files_by_type[file_type] = []
                    files_by_type[file_type].append(file_path)
                else:
                    unknown_files.append(file_path)
            except ValueError:
                if strict:
                    raise
                unknown_files.append(file_path)

        # Handle unknown files as binary
        if unknown_files:
            if warn_on_binary:
                logger.warning(
                    f"Detected {len(unknown_files)} files with unknown extensions. "
                    f"Falling back to binary reader. Examples: {unknown_files[:3]}. "
                    f"Use format parameter to specify the correct format or set strict=True to raise an error."
                )
            else:
                logger.info(
                    f"Detected {len(unknown_files)} files with unknown extensions. "
                    f"Reading as binary files."
                )
            files_by_type[FileFormat.BINARY] = unknown_files

        if not files_by_type:
            raise ValueError(
                f"No supported file types detected in {len(file_paths)} files. "
                f"Supported extensions: {sorted(self.extension_map.keys())}"
            )

        # Log detection results
        logger.info(
            f"Detected file types: {', '.join(f'{k.value}={len(v)}' for k, v in files_by_type.items())}"
        )

        return files_by_type


class PathCollector:
    """Collects files from paths, handling directories and lakehouse detection.

    Supports glob patterns and file extension filtering.
    """

    PARALLEL_THRESHOLD = 3
    CLOUD_SCHEMES = {
        "s3",
        "s3a",
        "s3n",
        "gs",
        "gcs",
        "az",
        "abfs",
        "abfss",
        "wasb",
        "wasbs",
    }

    def __init__(
        self,
        filesystem: "pafs.FileSystem",
        ignore_missing: bool = False,
        max_files: Optional[int] = None,
        file_extensions: Optional[List[str]] = None,
    ):
        self.filesystem = filesystem
        self.ignore_missing = ignore_missing
        self.max_files = max_files
        self.file_extensions = self._normalize_extensions(file_extensions)
        self.lakehouse_detector = LakehouseDetector(filesystem)
        self._file_count = 0

    def _normalize_extensions(
        self, extensions: Optional[List[str]]
    ) -> Optional[Set[str]]:
        """Normalize file extensions to lowercase with leading dot.

        Args:
            extensions: List of file extensions (e.g., ['csv', '.parquet', 'JSON']).

        Returns:
            Set of normalized extensions (e.g., {'.csv', '.parquet', '.json'}).
        """
        if not extensions:
            return None

        normalized = set()
        for ext in extensions:
            ext = ext.lower().strip()
            if not ext.startswith("."):
                ext = f".{ext}"
            normalized.add(ext)

        return normalized

    def _matches_extensions(self, file_path: str) -> bool:
        """Check if a file path matches the configured extensions filter.

        Args:
            file_path: Path to check.

        Returns:
            True if file matches extensions filter or no filter is set.
        """
        if not self.file_extensions:
            return True

        # Get the file extension (including compound extensions like .csv.gz)
        file_lower = file_path.lower()
        for ext in self.file_extensions:
            if file_lower.endswith(ext):
                return True

        return False

    def _expand_glob_pattern(self, pattern: str) -> List[str]:
        """Expand a glob pattern to matching file paths.

        Args:
            pattern: Glob pattern (e.g., 'data/*.parquet', 's3://bucket/**/*.csv').

        Returns:
            List of matching file paths.
        """
        try:
            # Use PyArrow filesystem's get_file_info with FileSelector
            # to handle glob patterns
            import fnmatch

            from pyarrow.fs import FileSelector

            # Check if this is a glob pattern
            has_glob = any(char in pattern for char in ["*", "?", "[", "]"])
            if not has_glob:
                return [pattern]

            # Parse URL to separate scheme from path
            parsed = urlparse(pattern)
            if parsed.scheme:
                # Reconstruct scheme part (e.g., "s3://")
                scheme_part = f"{parsed.scheme}://"
                remaining = parsed.netloc + parsed.path
            else:
                scheme_part = ""
                remaining = pattern

            # Find base directory by locating first glob character
            base_path_part = self._extract_base_path(remaining)
            base_path = scheme_part + base_path_part

            # Use FileSelector to recursively list files
            recursive = "**" in pattern
            selector = FileSelector(base_path, recursive=recursive)
            file_infos = self.filesystem.get_file_info(selector)

            # Filter matching files using glob pattern matching
            matching_files = []
            pattern_without_scheme = remaining

            for file_info in file_infos:
                if file_info.type != pafs.FileType.File:
                    continue

                # Extract path portion for matching
                file_path_for_matching = self._strip_scheme(file_info.path, scheme_part)

                if fnmatch.fnmatch(file_path_for_matching, pattern_without_scheme):
                    matching_files.append(file_info.path)

            if not matching_files and not self.ignore_missing:
                logger.warning(f"Glob pattern '{pattern}' matched no files")

            return matching_files

        except Exception as e:
            logger.warning(f"Error expanding glob pattern '{pattern}': {e}")
            if not self.ignore_missing:
                raise ValueError(f"Failed to expand glob pattern '{pattern}': {e}")
            return []

    def _extract_base_path(self, path: str) -> str:
        """Extract base directory from a path containing glob patterns.

        Args:
            path: Path string that may contain glob characters.

        Returns:
            Base directory path before the first glob character.
        """
        glob_chars = ["*", "?", "["]
        first_glob_idx = min(
            (path.find(c) for c in glob_chars if c in path),
            default=len(path),
        )

        # Get everything before first glob
        base_part = path[:first_glob_idx]

        # Use PurePosixPath to get parent directory
        if "/" in base_part:
            return str(PurePosixPath(base_part).parent)
        return "."

    def _strip_scheme(self, path: str, scheme_part: str) -> str:
        """Remove scheme portion from a path for pattern matching.

        Args:
            path: Full path with possible scheme.
            scheme_part: Scheme part to remove (e.g., "s3://").

        Returns:
            Path without scheme.
        """
        if scheme_part and path.startswith(scheme_part):
            return path[len(scheme_part) :]
        return path

    def collect(
        self, paths: List[str], detect_lakehouse: bool = True
    ) -> DetectionResult:
        """Collect files from paths using adaptive parallelism.

        Supports glob patterns and file extension filtering.

        Args:
            paths: List of paths or glob patterns to collect from.
            detect_lakehouse: Whether to detect lakehouse tables.

        Returns:
            DetectionResult with regular files and lakehouse tables.

        Raises:
            ValueError: If max_files exceeded or path traversal detected.
        """
        # Expand glob patterns first
        expanded_paths = []
        for path in paths:
            if any(char in path for char in ["*", "?", "[", "]"]):
                # This is a glob pattern
                matched_paths = self._expand_glob_pattern(path)
                expanded_paths.extend(matched_paths)
            else:
                expanded_paths.append(path)

        if not expanded_paths:
            if not self.ignore_missing:
                raise ValueError("No files found matching the provided patterns")
            logger.warning("No files found matching the provided patterns")
            return DetectionResult()

        logger.debug(
            f"Expanded {len(paths)} input path(s) to {len(expanded_paths)} file path(s)"
        )

        if self._should_parallelize(expanded_paths):
            return self._collect_parallel(expanded_paths, detect_lakehouse)

        return self._collect_sequential(expanded_paths, detect_lakehouse)

    def _should_parallelize(self, paths: List[str]) -> bool:
        """Determine if parallel collection would be beneficial.

        Args:
            paths: List of paths to evaluate.

        Returns:
            True if parallelization should be used, False otherwise.
        """
        if len(paths) < self.PARALLEL_THRESHOLD:
            return False

        is_cloud = any(self._is_cloud_path(path) for path in paths)

        should_use = is_cloud or len(paths) >= 5

        if should_use:
            logger.debug(
                f"Using parallel path collection for {len(paths)} paths "
                f"(cloud_storage={is_cloud})"
            )

        return should_use

    def _is_cloud_path(self, path: str) -> bool:
        """Check if a path is from cloud storage."""
        parsed = urlparse(path)
        return parsed.scheme.lower() in self.CLOUD_SCHEMES if parsed.scheme else False

    def _collect_parallel(
        self, paths: List[str], detect_lakehouse: bool = True
    ) -> DetectionResult:
        """Collect files from paths using Ray tasks for parallelism.

        Args:
            paths: List of paths to collect from.
            detect_lakehouse: Whether to detect lakehouse tables.

        Returns:
            DetectionResult with regular files and lakehouse tables.
        """
        result = DetectionResult()
        result.detected_sources = SourceDetector.detect_from_paths(paths)

        if result.detected_sources:
            source_summary = ", ".join(
                f"{source.value}={count}"
                for source, count in sorted(result.detected_sources.items())
            )
            logger.info(f"Detected data sources: {source_summary}")

        try:
            available_cpus = int(ray.cluster_resources().get("CPU", 1))
            max_tasks = min(len(paths), available_cpus * 2, 50)

            logger.debug(
                f"Launching {min(len(paths), max_tasks)} Ray tasks "
                f"for parallel path collection"
            )

            fs_ref = ray.put(self.filesystem)

            futures = []
            for path in paths:
                self._validate_path_security(path)
                future = _collect_path_remote.remote(
                    path=path,
                    filesystem=fs_ref,
                    detect_lakehouse=detect_lakehouse,
                    ignore_missing=self.ignore_missing,
                    max_files=self.max_files,
                )
                futures.append((path, future))

            results = ray.get([f for _, f in futures])

            all_errors = {}
            for (path, _), (regular_files, lakehouse_tables_dicts, errors) in zip(
                futures, results
            ):
                result.regular_files.extend(regular_files)
                all_errors.update(errors)

                for table_dict in lakehouse_tables_dicts:
                    result.lakehouse_tables.append(
                        LakehouseTable(
                            path=table_dict["path"],
                            format=LakehouseFormat(table_dict["format"]),
                        )
                    )

            if all_errors:
                error_msg = "\n".join(f"  {p}: {e}" for p, e in all_errors.items())
                if not self.ignore_missing:
                    raise ValueError(f"Errors during path collection:\n{error_msg}")
                logger.warning(f"Errors during path collection:\n{error_msg}")

        except Exception as e:
            logger.warning(
                f"Parallel collection failed, falling back to sequential: {e}"
            )
            return self._collect_sequential(paths, detect_lakehouse)

        return result

    def _collect_sequential(
        self, paths: List[str], detect_lakehouse: bool = True
    ) -> DetectionResult:
        """Collect files from paths sequentially (original implementation).

        Args:
            paths: List of paths to collect from.
            detect_lakehouse: Whether to detect lakehouse tables.

        Returns:
            DetectionResult with regular files and lakehouse tables.
        """
        result = DetectionResult()
        self._file_count = 0

        result.detected_sources = SourceDetector.detect_from_paths(paths)

        if result.detected_sources:
            source_summary = ", ".join(
                f"{source.value}={count}"
                for source, count in sorted(result.detected_sources.items())
            )
            logger.info(f"Detected data sources: {source_summary}")

        for path in paths:
            try:
                self._validate_path_security(path)

                if self.max_files and self._file_count >= self.max_files:
                    logger.warning(
                        f"Reached max_files limit of {self.max_files}. "
                        f"Processed {len(result.regular_files)} regular files and "
                        f"{len(result.lakehouse_tables)} lakehouse tables. "
                        f"Increase max_files to collect more."
                    )
                    return result

                file_info = self.filesystem.get_file_info(path)

                if file_info.type == pafs.FileType.Directory:
                    self._handle_directory(path, detect_lakehouse, result)
                elif file_info.type == pafs.FileType.File:
                    # Apply file_extensions filter if configured
                    if self._matches_extensions(path):
                        result.regular_files.append(path)
                        self._file_count += 1
                    else:
                        logger.debug(
                            f"Skipping file '{path}' due to file_extensions filter"
                        )
                elif file_info.type == pafs.FileType.NotFound:
                    if not self.ignore_missing:
                        suggestion = self._suggest_similar_path(path)
                        if suggestion:
                            raise FileNotFoundError(
                                f"Path not found: '{path}'. Did you mean '{suggestion}'?"
                            )
                        raise FileNotFoundError(f"Path not found: '{path}'")
                else:
                    logger.warning(
                        f"Skipping non-file, non-directory path: {path} (type: {file_info.type})"
                    )

            except FileNotFoundError:
                if not self.ignore_missing:
                    raise
                logger.warning(f"Path not found (ignoring): {path}")
            except PermissionError as e:
                logger.error(f"Permission denied accessing '{path}': {e}")
                if not self.ignore_missing:
                    raise ValueError(f"Permission denied: {path}") from e
            except Exception as e:
                if not self.ignore_missing:
                    raise
                logger.warning(f"Error accessing path {path}: {e}")

        return result

    def _validate_path_security(self, path: str) -> None:
        """Validate path for security concerns.

        Args:
            path: Path to validate.

        Raises:
            ValueError: If path contains potential security issues.
        """
        # Allow cloud storage paths (s3://, gs://, etc.) - they have their own security
        parsed = urlparse(path)
        if parsed.scheme:
            return

        # Normalize path using PurePosixPath for consistent handling
        try:
            normalized_path = PurePosixPath(path)
            # Check if path attempts to traverse upward
            if ".." in normalized_path.parts:
                raise ValueError(
                    f"Potential path traversal detected in path: '{path}'. "
                    f"Paths with '../' are not allowed for security reasons."
                )
        except ValueError as e:
            # PurePosixPath may raise ValueError for invalid paths
            raise ValueError(f"Invalid path: '{path}': {e}")

    def _suggest_similar_path(self, path: str) -> Optional[str]:
        """Suggest a similar path if the given path doesn't exist.

        Args:
            path: Path that doesn't exist.

        Returns:
            Suggested similar path or None.
        """
        # Try common variations
        suggestions = []

        # Try without trailing slash
        if path.endswith("/"):
            suggestions.append(path.rstrip("/"))

        # Try with trailing slash
        if not path.endswith("/"):
            suggestions.append(path + "/")

        # Try parent directory using PurePosixPath (maybe they mistyped the last component)
        try:
            parent = str(PurePosixPath(path).parent)
            if parent and parent != ".":
                parent_info = self.filesystem.get_file_info(parent)
                if parent_info.type == pafs.FileType.Directory:
                    return parent
        except Exception:
            pass

        # Check if any suggestions exist
        for suggestion in suggestions:
            try:
                info = self.filesystem.get_file_info(suggestion)
                if info.type in (pafs.FileType.Directory, pafs.FileType.File):
                    return suggestion
            except Exception:
                continue

        return None

    def _handle_directory(
        self, path: str, detect_lakehouse: bool, result: DetectionResult
    ):
        """Handle a directory path with optional file extension filtering."""
        if detect_lakehouse:
            lakehouse_format = self.lakehouse_detector.detect(path)
            if lakehouse_format:
                result.lakehouse_tables.append(
                    LakehouseTable(path=path, format=lakehouse_format)
                )
                logger.info(f"Detected {lakehouse_format.value} table at: {path}")
                return

        # Regular directory - list all files recursively
        try:
            selector = pafs.FileSelector(path, recursive=True)
            files = self.filesystem.get_file_info(selector)

            files_added = 0
            files_skipped = 0
            for f in files:
                if f.type == pafs.FileType.File:
                    # Apply file_extensions filter if configured
                    if not self._matches_extensions(f.path):
                        files_skipped += 1
                        continue

                    if self.max_files and self._file_count >= self.max_files:
                        logger.warning(
                            f"Reached max_files limit of {self.max_files} while listing directory '{path}'. "
                            f"Added {files_added} files from this directory. "
                            f"Increase max_files to include more files."
                        )
                        return

                    result.regular_files.append(f.path)
                    self._file_count += 1
                    files_added += 1
                elif f.type == pafs.FileType.NotFound:
                    logger.debug(
                        f"Skipping broken symlink or inaccessible file: {f.path}"
                    )

            if self.file_extensions and files_skipped > 0:
                logger.debug(
                    f"Filtered {files_skipped} file(s) from directory '{path}' "
                    f"based on file_extensions filter"
                )

            if files_added == 0 and not result.lakehouse_tables:
                if self.file_extensions:
                    logger.warning(
                        f"Directory '{path}' contains no files matching extensions "
                        f"{self.file_extensions}. "
                        f"If this is unexpected, check file extensions filter."
                    )
                else:
                    logger.warning(
                        f"Directory '{path}' is empty or contains no readable files. "
                        f"If this is unexpected, check file permissions."
                    )

        except PermissionError as e:
            logger.error(f"Permission denied accessing directory '{path}': {e}")
            if not self.ignore_missing:
                raise ValueError(
                    f"Permission denied accessing directory: {path}"
                ) from e
        except Exception as e:
            logger.error(f"Error listing directory '{path}': {e}")
            if not self.ignore_missing:
                raise


class ReaderRegistry:
    """Registry of format readers."""

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
            # File-based formats
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
            "binary": read_binary_files,
            # Lakehouse formats
            "delta": read_delta,
            "delta_sharing": read_delta_sharing_tables,
            "hudi": read_hudi,
            "iceberg": read_iceberg,
            # Database sources
            "sql": read_sql,
            "bigquery": read_bigquery,
            "mongo": read_mongo,
            "mongodb": read_mongo,  # Alias
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
            format: Format name as a string (e.g., 'parquet', 'delta', 'hudi').

        Returns:
            Reader function for the specified format.

        Raises:
            ValueError: If the format is not supported.
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
        """Get reader function for a lakehouse format."""
        self._ensure_readers_loaded()

        if isinstance(format, str):
            try:
                format = LakehouseFormat(format.lower())
            except ValueError:
                raise ValueError(f"Unsupported lakehouse format: '{format}'")

        if format not in self._lakehouse_readers:
            raise ValueError(f"No reader registered for lakehouse format: {format}")

        return self._lakehouse_readers[format]

    def get_supported_formats(self) -> List[str]:
        """Get list of supported format names."""
        file_formats = [f.value for f in FileFormat]
        lakehouse_formats = [f.value for f in LakehouseFormat]
        return sorted(file_formats + lakehouse_formats)


class DatasetReader:
    """Reads datasets using appropriate readers based on file types."""

    def __init__(self, config: ReadConfig, registry: ReaderRegistry):
        self.config = config
        self.registry = registry

    def read_lakehouse_tables(self, tables: List[LakehouseTable]) -> Dataset:
        """Read lakehouse tables and combine them.

        Args:
            tables: List of lakehouse tables to read.

        Returns:
            Combined Dataset.

        Raises:
            ValueError: If multiple lakehouse formats are detected.
        """
        if not tables:
            raise ValueError("No lakehouse tables provided")

        # Error if multiple formats detected
        if len(tables) > 1:
            formats_found = {table.format for table in tables}
            if len(formats_found) > 1:
                format_list = ", ".join(sorted(f.value for f in formats_found))
                raise ValueError(
                    f"Multiple lakehouse formats detected: {format_list}. "
                    "Ray Data reads tables using a single reader. "
                    "Use the format parameter to specify which to read:\n"
                    f"  - ray.data.read(paths, format='delta')\n"
                    f"  - ray.data.read(paths, format='hudi')\n"
                    f"  - ray.data.read(paths, format='iceberg')"
                )

        datasets = []
        for table in tables:
            reader_func = self.registry.get_lakehouse_reader(table.format)
            logger.info(f"Reading {table.format.value} table from: {table.path}")

            ds = self._call_reader(
                reader_func=reader_func, paths=table.path, is_lakehouse=True
            )
            datasets.append(ds)

        return self._combine_datasets(datasets)

    def read_file_groups(self, files_by_type: Dict[FileFormat, List[str]]) -> Dataset:
        """Read file groups and combine them.

        Args:
            files_by_type: Dictionary mapping file types to paths.

        Returns:
            Combined Dataset.
        """
        datasets = []

        for file_type, file_list in sorted(files_by_type.items()):
            logger.info(f"Reading {len(file_list)} {file_type.value} files")

            reader_func = self.registry.get_format_reader(file_type)
            ds = self._call_reader(
                reader_func=reader_func, paths=file_list, is_lakehouse=False
            )
            datasets.append(ds)

        return self._combine_datasets(datasets)

    def _call_reader(
        self, reader_func: Callable, paths: Union[str, List[str]], is_lakehouse: bool
    ) -> Dataset:
        """Call a reader function with appropriate arguments.

        Args:
            reader_func: Reader function to call.
            paths: Path(s) to read.
            is_lakehouse: Whether this is a lakehouse reader.

        Returns:
            Dataset from the reader.
        """
        reader_sig = inspect.signature(reader_func)
        reader_params = set(reader_sig.parameters.keys())

        kwargs = {}

        if is_lakehouse:
            if "path" in reader_params:
                kwargs["path"] = paths
            elif "table_uri" in reader_params:
                kwargs["table_uri"] = paths
            elif "uri" in reader_params:
                kwargs["uri"] = paths
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
        }

        if self.config.arrow_open_file_args is not None:
            common_args["arrow_open_file_args"] = self.config.arrow_open_file_args
        if self.config.partitioning is not None:
            common_args["partitioning"] = self.config.partitioning

        for key, value in common_args.items():
            if key in reader_params and value is not None:
                kwargs[key] = value

        kwargs.update(self.config.reader_args)

        try:
            return reader_func(**kwargs)
        except Exception as e:
            logger.error(f"Error reading with {reader_func.__name__}: {e}")
            raise

    def _combine_datasets(self, datasets: List[Dataset]) -> Dataset:
        """Combine multiple datasets using union.

        Args:
            datasets: List of datasets to combine.

        Returns:
            Combined dataset.

        Raises:
            Exception: If schemas are incompatible and cannot be unified.
        """
        if len(datasets) == 1:
            return datasets[0]

        logger.info(f"Concatenating {len(datasets)} datasets using union()")

        try:
            result = datasets[0]
            for ds in datasets[1:]:
                result = result.union(ds)
            return result
        except Exception as e:
            # Provide helpful error message for schema mismatches
            error_msg = (
                f"Failed to union datasets. This usually happens when datasets have "
                f"incompatible schemas that cannot be unified. Original error: {str(e)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e


def _setup_read_components(
    paths: Union[str, List[str]],
    filesystem: Optional["pafs.FileSystem"],
    ignore_missing_paths: bool,
    max_files: Optional[int],
    file_extensions: Optional[List[str]],
    **config_kwargs,
) -> Tuple[
    List[str],
    "pafs.FileSystem",
    ReadConfig,
    ReaderRegistry,
    PathCollector,
    DatasetReader,
]:
    """Set up components needed for reading data.

    Args:
        paths: Input paths (string or list).
        filesystem: Optional filesystem instance.
        ignore_missing_paths: Whether to ignore missing paths.
        max_files: Maximum files to collect.
        file_extensions: File extensions to filter.
        **config_kwargs: Additional ReadConfig arguments.

    Returns:
        Tuple of (normalized_paths, filesystem, config, registry, collector, reader).
    """
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
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        **config_kwargs,
    )

    # Initialize core components
    registry = ReaderRegistry()
    collector = PathCollector(
        filesystem,
        ignore_missing_paths,
        max_files=max_files,
        file_extensions=file_extensions,
    )
    reader = DatasetReader(config, registry)

    return paths, filesystem, config, registry, collector, reader


def _read_with_format_hint(
    format: str,
    paths: List[str],
    collector: PathCollector,
    registry: ReaderRegistry,
    reader: DatasetReader,
) -> Dataset:
    """Read data when format is explicitly specified.

    Args:
        format: Format string (e.g., 'parquet', 'csv').
        paths: List of paths to read.
        collector: PathCollector instance.
        registry: ReaderRegistry instance.
        reader: DatasetReader instance.

    Returns:
        Dataset with read data.

    Raises:
        ValueError: If no files found.
    """
    # Collect files without lakehouse detection (format is known)
    detection_result = collector.collect(paths, detect_lakehouse=False)

    if not detection_result.regular_files:
        raise ValueError(
            f"No files found in paths: {paths}. "
            "Set ignore_missing_paths=True to allow empty reads."
        )

    reader_func = registry.get_format_reader(format)
    logger.info(
        f"Using format hint: {format} for {len(detection_result.regular_files)} files"
    )

    # Warn about potential schema mismatches when using format hints
    if len(detection_result.regular_files) > 1:
        logger.info(
            f"Reading {len(detection_result.regular_files)} files with format '{format}'. "
            "If files have different schemas, union() may fail. "
            "Consider reading files with incompatible schemas separately."
        )

    return reader._call_reader(
        reader_func=reader_func,
        paths=detection_result.regular_files,
        is_lakehouse=False,
    )


def _validate_no_mixed_types(
    detection_result: DetectionResult, files_by_type: Dict[FileFormat, List[str]]
) -> None:
    """Validate that lakehouse tables and regular files aren't mixed, and only one file type exists.

    Args:
        detection_result: Detection result with lakehouse tables.
        files_by_type: Dictionary mapping FileFormat to list of paths.

    Raises:
        ValueError: If validation fails.
    """
    # Error if mixing lakehouse tables with regular files
    if detection_result.lakehouse_tables and files_by_type:
        raise ValueError(
            f"Cannot mix lakehouse tables with regular files. Found "
            f"{len(detection_result.lakehouse_tables)} lakehouse table(s) and "
            f"{sum(len(files) for files in files_by_type.values())} regular file(s). "
            "Use the format parameter to specify which to read, or read them separately:\n"
            "  - ray.data.read(paths, format='delta')  # Read only lakehouse tables\n"
            "  - ray.data.read(paths, format='parquet')  # Read only regular files"
        )

    # Error if multiple file types detected
    if len(files_by_type) > 1:
        format_summary = ", ".join(
            f"{fmt.value}={len(files)}" for fmt, files in files_by_type.items()
        )
        raise ValueError(
            f"Multiple file types detected: {format_summary}. "
            "Ray Data reads files using a single reader. "
            "Use one of these approaches:\n"
            "  - Specify format parameter: ray.data.read(paths, format='parquet')\n"
            "  - Filter by extension: ray.data.read(paths, file_extensions=['parquet'])\n"
            "  - Read separately: ray.data.read_parquet(...) and ray.data.read_csv(...)"
        )


def _create_dry_run_metadata(
    detection_result: DetectionResult, files_by_type: Dict[FileFormat, List[str]]
) -> Dict[str, Any]:
    """Create metadata dictionary for dry-run mode.

    Args:
        detection_result: Detection result.
        files_by_type: Dictionary mapping FileFormat to list of paths.

    Returns:
        Dictionary with detection metadata.
    """
    logger.info("Dry-run mode: Returning detection metadata instead of reading data")
    return {
        "lakehouse_tables": [
            {"path": t.path, "format": t.format.value}
            for t in detection_result.lakehouse_tables
        ],
        "file_groups": {fmt.value: len(files) for fmt, files in files_by_type.items()},
        "total_files": len(detection_result.regular_files),
        "total_lakehouse_tables": len(detection_result.lakehouse_tables),
        "detected_sources": {
            source.value: count
            for source, count in detection_result.detected_sources.items()
        },
    }


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
        paths: Input paths to validate.
        format: Format string to validate.
        parallelism: Parallelism value to validate.
        num_cpus: CPU count to validate.
        num_gpus: GPU count to validate.
        memory: Memory value to validate.
        file_extensions: File extensions to validate.
        concurrency: Concurrency value to validate.
        max_files: Max files value to validate.

    Raises:
        TypeError: If parameter types are incorrect.
        ValueError: If parameter values are invalid.
    """
    # Validate paths parameter
    if paths is None:
        raise TypeError(
            "paths cannot be None. Please provide a file path string or list of paths."
        )

    if isinstance(paths, (dict, set)):
        raise TypeError(
            f"paths must be a string or list of strings, got {type(paths).__name__}. "
            f"Example: ray.data.read('file.csv') or ray.data.read(['file1.csv', 'file2.csv'])"
        )

    if not isinstance(paths, (str, list)):
        raise TypeError(
            f"paths must be a string or list of strings, got {type(paths).__name__}. "
            f"If you passed a Path object, convert it to string: str(path)"
        )

    # Convert single path to list for uniform validation
    paths_list = [paths] if isinstance(paths, str) else paths

    if not paths_list:
        raise ValueError(
            "paths cannot be an empty list. Please provide at least one file path."
        )

    # Validate each path in the list
    for i, path in enumerate(paths_list):
        if path is None:
            raise TypeError(
                f"paths[{i}] is None. All paths must be valid strings. "
                f"Remove None values from your path list."
            )
        if not isinstance(path, str):
            raise TypeError(
                f"paths[{i}] must be a string, got {type(path).__name__}. "
                f"Convert non-string paths to strings."
            )
        if not path or not path.strip():
            raise ValueError(
                f"paths[{i}] is empty or whitespace-only. "
                f"Please provide a valid file path."
            )

    # Validate format parameter
    if format is not None:
        if not isinstance(format, str):
            raise TypeError(
                f"format must be a string, got {type(format).__name__}. "
                f"Example: format='parquet'"
            )
        if not format.strip():
            raise ValueError(
                "format cannot be empty. Provide a valid format like 'parquet' or 'csv'."
            )

        # Provide helpful error for common mistakes
        if format.startswith("."):
            raise ValueError(
                f"format should not include the dot. Use format='{format[1:]}' instead of format='{format}'"
            )

    # Validate numeric parameters
    if parallelism is not None and not isinstance(parallelism, int):
        raise TypeError(
            f"parallelism must be an integer, got {type(parallelism).__name__}. "
            f"Use parallelism={int(parallelism) if isinstance(parallelism, (float, bool)) else -1}"
        )

    if parallelism is not None and parallelism < -1:
        raise ValueError(
            f"parallelism must be >= -1, got {parallelism}. "
            f"Use -1 for automatic parallelism."
        )

    if parallelism == 0:
        raise ValueError(
            "parallelism cannot be 0. Use -1 for automatic parallelism or a positive integer."
        )

    if num_cpus is not None and num_cpus < 0:
        raise ValueError(f"num_cpus must be non-negative, got {num_cpus}")

    if num_gpus is not None and num_gpus < 0:
        raise ValueError(f"num_gpus must be non-negative, got {num_gpus}")

    if memory is not None and memory < 0:
        raise ValueError(f"memory must be non-negative, got {memory}")

    # Validate file_extensions
    if file_extensions is not None:
        if not isinstance(file_extensions, list):
            raise TypeError(
                f"file_extensions must be a list, got {type(file_extensions).__name__}. "
                f"Example: file_extensions=['csv', 'parquet']"
            )
        for i, ext in enumerate(file_extensions):
            if ext is None:
                raise TypeError(
                    f"file_extensions[{i}] is None. All extensions must be valid strings."
                )
            if not isinstance(ext, str):
                raise TypeError(
                    f"file_extensions[{i}] must be a string, got {type(ext).__name__}"
                )

    # Validate concurrency
    if concurrency is not None:
        if not isinstance(concurrency, int):
            raise TypeError(
                f"concurrency must be an integer, got {type(concurrency).__name__}"
            )
        if concurrency <= 0:
            raise ValueError(f"concurrency must be positive, got {concurrency}")

    # Validate max_files
    if max_files is not None:
        if not isinstance(max_files, int):
            raise TypeError(
                f"max_files must be an integer, got {type(max_files).__name__}"
            )
        if max_files < 0:
            raise ValueError(
                f"max_files must be non-negative, got {max_files}. "
                f"Use None for no limit."
            )
        if max_files == 0:
            raise ValueError(
                "max_files cannot be 0. Use None for no limit or a positive integer."
            )


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
    dry_run: bool = False,
    **reader_args,
) -> Dataset:
    """Internal implementation of ray.data.read().

    This function handles the core logic for automatic file type detection,
    lakehouse format detection, and dynamic reader selection.

    See ray.data.read() for full documentation of parameters.
    """
    # Validate all input parameters
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

    # Set up all components needed for reading
    paths, filesystem, config, registry, collector, reader = _setup_read_components(
        paths=paths,
        filesystem=filesystem,
        ignore_missing_paths=ignore_missing_paths,
        max_files=max_files,
        file_extensions=file_extensions,
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
        shuffle=shuffle,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        reader_args=reader_args,
    )

    # Fast path: format is explicitly specified
    if format is not None:
        return _read_with_format_hint(format, paths, collector, registry, reader)

    # Auto-detection path: collect files and detect formats
    detection_result = collector.collect(paths, detect_lakehouse=True)

    # Log source information for single-file reads
    if len(paths) == 1 and detection_result.detected_sources:
        sources = list(detection_result.detected_sources.keys())
        if sources:
            logger.info(f"Reading from {sources[0].value} source: {paths[0]}")

    # Fast path: only lakehouse tables detected
    if detection_result.lakehouse_tables and not detection_result.regular_files:
        return reader.read_lakehouse_tables(detection_result.lakehouse_tables)

    # Validate we found some data
    if not detection_result.regular_files and not detection_result.lakehouse_tables:
        raise ValueError(
            f"No files found in paths: {paths}. "
            "Set ignore_missing_paths=True to allow empty reads."
        )

    # Detect file types for regular files
    type_detector = FileTypeDetector()
    files_by_type = type_detector.group_files_by_type(
        detection_result.regular_files,
        strict=strict,
        warn_on_binary=warn_on_binary_fallback,
    )

    # Handle dry-run mode
    if dry_run:
        return _create_dry_run_metadata(detection_result, files_by_type)

    # Validate no mixed types (single reader policy)
    _validate_no_mixed_types(detection_result, files_by_type)

    # Read data with appropriate readers
    datasets = []

    if detection_result.lakehouse_tables:
        logger.info(
            f"Reading {len(detection_result.lakehouse_tables)} lakehouse tables"
        )
        lakehouse_ds = reader.read_lakehouse_tables(detection_result.lakehouse_tables)
        datasets.append(lakehouse_ds)

    if files_by_type:
        file_ds = reader.read_file_groups(files_by_type)
        datasets.append(file_ds)

    if not datasets:
        raise ValueError(f"No data to read from paths: {paths}")

    return reader._combine_datasets(datasets)


def _detect_lakehouse_format(
    path: str, filesystem: "pyarrow.fs.FileSystem"
) -> Optional[str]:
    """Detect if a path is a Delta Lake, Hudi, or Iceberg table.

    Args:
        path: Path to check for lakehouse format.
        filesystem: PyArrow filesystem to use for inspection.

    Returns:
        The format name ('delta', 'hudi', 'iceberg') or None if not detected.
    """
    detector = LakehouseDetector(filesystem)
    result = detector.detect(path)
    return result.value if result else None
