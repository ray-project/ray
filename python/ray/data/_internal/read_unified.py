"""Internal implementation of the unified read() function for Ray Data.

This module contains the implementation of ray.data.read(), which automatically
detects file formats and lakehouse table structures to select the appropriate
reader function.
"""

import inspect
import logging
from dataclasses import dataclass, field
from enum import Enum
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
    HTML = "html"
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
    concurrency: Optional[int] = None
    override_num_blocks: Optional[int] = None
    reader_args: Dict[str, Any] = field(default_factory=dict)
    max_files: Optional[int] = None
    strict: bool = False
    warn_on_binary_fallback: bool = True
    dry_run: bool = False
    on_mixed_types: str = "union"


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
        # Check for explicit scheme (e.g., s3://bucket/path)
        if "://" in path:
            scheme = path.split("://")[0].lower()
            return cls.SCHEME_MAP.get(scheme, DataSource.UNKNOWN)

        # Paths without scheme are considered local
        # Unless they start with / which is also local
        if path.startswith("/") or path.startswith("./") or path.startswith("../"):
            return DataSource.LOCAL

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
        return {item.path.split("/")[-1] for item in contents}

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
    """Detects file types based on extensions."""

    FORMAT_ALIASES = {
        "jpeg": "images",
        "jpg": "images",
        "png": "images",
        "gif": "images",
        "jsonl": "json",
        "ndjson": "json",
        "tsv": "csv",
        "tab": "csv",
        "mp3": "audio",
        "wav": "audio",
        "mp4": "video",
        "avi": "video",
        "txt": "text",
        "tar": "webdataset",
    }

    EXTENSION_MAP = {
        "parquet": FileFormat.PARQUET,
        "parquet.gz": FileFormat.PARQUET,
        "parquet.gzip": FileFormat.PARQUET,
        "parquet.bz2": FileFormat.PARQUET,
        "parquet.snappy": FileFormat.PARQUET,
        "parquet.lz4": FileFormat.PARQUET,
        "parquet.zstd": FileFormat.PARQUET,
        "csv": FileFormat.CSV,
        "csv.gz": FileFormat.CSV,
        "csv.br": FileFormat.CSV,
        "csv.zst": FileFormat.CSV,
        "csv.lz4": FileFormat.CSV,
        "csv.bz2": FileFormat.CSV,
        "json": FileFormat.JSON,
        "jsonl": FileFormat.JSON,
        "json.gz": FileFormat.JSON,
        "jsonl.gz": FileFormat.JSON,
        "json.br": FileFormat.JSON,
        "jsonl.br": FileFormat.JSON,
        "json.zst": FileFormat.JSON,
        "jsonl.zst": FileFormat.JSON,
        "json.lz4": FileFormat.JSON,
        "jsonl.lz4": FileFormat.JSON,
        "json.bz2": FileFormat.JSON,
        "jsonl.bz2": FileFormat.JSON,
        "txt": FileFormat.TEXT,
        "png": FileFormat.IMAGES,
        "jpg": FileFormat.IMAGES,
        "jpeg": FileFormat.IMAGES,
        "tif": FileFormat.IMAGES,
        "tiff": FileFormat.IMAGES,
        "bmp": FileFormat.IMAGES,
        "gif": FileFormat.IMAGES,
        "mp3": FileFormat.AUDIO,
        "wav": FileFormat.AUDIO,
        "aac": FileFormat.AUDIO,
        "flac": FileFormat.AUDIO,
        "ogg": FileFormat.AUDIO,
        "m4a": FileFormat.AUDIO,
        "wma": FileFormat.AUDIO,
        "alac": FileFormat.AUDIO,
        "aiff": FileFormat.AUDIO,
        "pcm": FileFormat.AUDIO,
        "amr": FileFormat.AUDIO,
        "opus": FileFormat.AUDIO,
        "mp4": FileFormat.VIDEO,
        "mkv": FileFormat.VIDEO,
        "mov": FileFormat.VIDEO,
        "avi": FileFormat.VIDEO,
        "wmv": FileFormat.VIDEO,
        "flv": FileFormat.VIDEO,
        "webm": FileFormat.VIDEO,
        "m4v": FileFormat.VIDEO,
        "3gp": FileFormat.VIDEO,
        "mpeg": FileFormat.VIDEO,
        "mpg": FileFormat.VIDEO,
        "npy": FileFormat.NUMPY,
        "avro": FileFormat.AVRO,
        "avro.gz": FileFormat.AVRO,
        "avro.gzip": FileFormat.AVRO,
        "avro.bz2": FileFormat.AVRO,
        "avro.snappy": FileFormat.AVRO,
        "tfrecords": FileFormat.TFRECORDS,
        "html": FileFormat.HTML,
        "htm": FileFormat.HTML,
        "tar": FileFormat.WEBDATASET,
        "lance": FileFormat.LANCE,
    }

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
        # Extract filename from path (handle URLs and directories)
        filename = file_path.split("/")[-1] if "/" in file_path else file_path

        # Normalize to lowercase for case-insensitive matching
        lower_filename = filename.lower()

        # Try compound extensions first (e.g., .csv.gz, .parquet.snappy)
        # Check up to 3 levels (e.g., .tar.gz, .parquet.snappy)
        for ext_len in [3, 2, 1]:
            parts = lower_filename.rsplit(".", ext_len)
            if len(parts) == ext_len + 1:
                potential_ext = ".".join(parts[1:])
                if potential_ext in self.EXTENSION_MAP:
                    return self.EXTENSION_MAP[potential_ext]

        # No extension or unknown extension
        if strict and "." in filename:
            ext_part = filename.rsplit(".", 1)[1]
            supported = ", ".join(sorted(set(self.EXTENSION_MAP.keys())))
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
                f"Supported extensions: {sorted(self.EXTENSION_MAP.keys())}"
            )

        # Log detection results
        logger.info(
            f"Detected file types: {', '.join(f'{k.value}={len(v)}' for k, v in files_by_type.items())}"
        )

        return files_by_type


class PathCollector:
    """Collects files from paths, handling directories and lakehouse detection."""

    PARALLEL_THRESHOLD = 3
    CLOUD_SCHEMES = {"s3", "s3a", "s3n", "gs", "gcs", "az", "abfs", "abfss", "wasb", "wasbs"}

    def __init__(
        self,
        filesystem: "pafs.FileSystem",
        ignore_missing: bool = False,
        max_files: Optional[int] = None,
    ):
        self.filesystem = filesystem
        self.ignore_missing = ignore_missing
        self.max_files = max_files
        self.lakehouse_detector = LakehouseDetector(filesystem)
        self._file_count = 0

    def collect(
        self, paths: List[str], detect_lakehouse: bool = True
    ) -> DetectionResult:
        """Collect files from paths using adaptive parallelism.

        Args:
            paths: List of paths to collect from.
            detect_lakehouse: Whether to detect lakehouse tables.

        Returns:
            DetectionResult with regular files and lakehouse tables.

        Raises:
            ValueError: If max_files exceeded or path traversal detected.
        """
        if self._should_parallelize(paths):
            return self._collect_parallel(paths, detect_lakehouse)

        return self._collect_sequential(paths, detect_lakehouse)

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
        if "://" in path:
            scheme = path.split("://")[0].lower()
            return scheme in self.CLOUD_SCHEMES
        return False

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
                    result.regular_files.append(path)
                    self._file_count += 1
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
        # Check for path traversal attempts
        normalized = path.replace("\\", "/")

        # Allow cloud storage paths (s3://, gs://, etc.)
        if "://" in path:
            return

        # Check for traversal patterns
        if "../" in normalized or "/.." in normalized:
            raise ValueError(
                f"Potential path traversal detected in path: '{path}'. "
                f"Paths with '../' are not allowed for security reasons."
            )

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

        # Try parent directory (maybe they mistyped the last component)
        parent = "/".join(path.rstrip("/").split("/")[:-1])
        if parent:
            try:
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
        """Handle a directory path."""
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
            for f in files:
                if f.type == pafs.FileType.File:
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

            if files_added == 0 and not result.lakehouse_tables:
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
        self._format_readers = None
        self._lakehouse_readers = None

    def _ensure_readers_loaded(self):
        """Lazy load readers on first use."""
        if self._format_readers is not None:
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
            read_html,
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

        self._format_readers = {
            FileFormat.PARQUET: read_parquet,
            FileFormat.CSV: read_csv,
            FileFormat.JSON: read_json,
            FileFormat.TEXT: read_text,
            FileFormat.IMAGES: read_images,
            FileFormat.AUDIO: read_audio,
            FileFormat.VIDEO: read_videos,
            FileFormat.NUMPY: read_numpy,
            FileFormat.AVRO: read_avro,
            FileFormat.TFRECORDS: read_tfrecords,
            FileFormat.HTML: read_html,
            FileFormat.WEBDATASET: read_webdataset,
            FileFormat.BINARY: read_binary_files,
            FileFormat.LANCE: read_lance,
        }

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
            "html": read_html,
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

    def get_format_reader(self, format: Union[FileFormat, str]) -> Callable:
        """Get reader function for a file format."""
        self._ensure_readers_loaded()

        if isinstance(format, str):
            try:
                format = FileFormat(format.lower())
            except ValueError:
                raise ValueError(
                    f"Unsupported format: '{format}'. "
                    f"Supported formats: {[f.value for f in FileFormat]}"
                )

        if format not in self._format_readers:
            raise ValueError(f"No reader registered for format: {format}")

        return self._format_readers[format]

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
        """
        if not tables:
            raise ValueError("No lakehouse tables provided")

        # Log if multiple formats detected
        if len(tables) > 1:
            formats_found = {table.format for table in tables}
            if len(formats_found) > 1:
                logger.warning(
                    f"Multiple lakehouse formats detected: {formats_found}. "
                    "Reading each separately."
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
        """
        if len(datasets) == 1:
            return datasets[0]

        logger.info(f"Concatenating {len(datasets)} datasets")
        result = datasets[0]
        for ds in datasets[1:]:
            result = result.union(ds)
        return result


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
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    max_files: Optional[int] = None,
    strict: bool = False,
    warn_on_binary_fallback: bool = True,
    dry_run: bool = False,
    on_mixed_types: str = "union",
    **reader_args,
) -> Dataset:
    """Internal implementation of ray.data.read().

    This function handles the core logic for automatic file type detection,
    lakehouse format detection, and dynamic reader selection.

    See ray.data.read() for full documentation of parameters.
    """
    if isinstance(paths, str):
        paths = [paths]

    if filesystem is None:
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        _, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    config = ReadConfig(
        paths=paths,
        format=format,
        filesystem=filesystem,
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
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        reader_args=reader_args,
    )

    registry = ReaderRegistry()
    collector = PathCollector(filesystem, ignore_missing_paths, max_files=max_files)
    reader = DatasetReader(config, registry)

    if format is not None:
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

        return reader._call_reader(
            reader_func=reader_func,
            paths=detection_result.regular_files,
            is_lakehouse=False,
        )

    detection_result = collector.collect(paths, detect_lakehouse=True)

    # Log source information for single-file reads
    if len(paths) == 1 and detection_result.detected_sources:
        sources = list(detection_result.detected_sources.keys())
        if sources:
            logger.info(f"Reading from {sources[0].value} source: {paths[0]}")

    if detection_result.lakehouse_tables and not detection_result.regular_files:
        return reader.read_lakehouse_tables(detection_result.lakehouse_tables)

    if not detection_result.regular_files and not detection_result.lakehouse_tables:
        raise ValueError(
            f"No files found in paths: {paths}. "
            "Set ignore_missing_paths=True to allow empty reads."
        )

    type_detector = FileTypeDetector()
    files_by_type = type_detector.group_files_by_type(
        detection_result.regular_files,
        strict=strict,
        warn_on_binary=warn_on_binary_fallback,
    )

    if dry_run:
        logger.info(
            "Dry-run mode: Returning detection metadata instead of reading data"
        )
        return {
            "lakehouse_tables": [
                {"path": t.path, "format": t.format.value}
                for t in detection_result.lakehouse_tables
            ],
            "file_groups": {
                fmt.value: len(files) for fmt, files in files_by_type.items()
            },
            "total_files": len(detection_result.regular_files),
            "total_lakehouse_tables": len(detection_result.lakehouse_tables),
            "detected_sources": {
                source.value: count
                for source, count in detection_result.detected_sources.items()
            },
        }

    datasets = []

    if detection_result.lakehouse_tables:
        logger.info(
            f"Reading {len(detection_result.lakehouse_tables)} lakehouse tables"
        )
        lakehouse_ds = reader.read_lakehouse_tables(detection_result.lakehouse_tables)
        datasets.append(lakehouse_ds)

    if files_by_type:
        # Check on_mixed_types setting
        if len(files_by_type) > 1:
            format_summary = ", ".join(
                f"{fmt.value}={len(files)}" for fmt, files in files_by_type.items()
            )
            if on_mixed_types == "fail":
                raise ValueError(
                    f"Multiple file types detected: {format_summary}. "
                    "Set on_mixed_types='union' to combine them or specify format parameter."
                )
            elif on_mixed_types == "warn":
                logger.warning(
                    f"Multiple file types detected: {format_summary}. "
                    "They will be combined using union()."
                )

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
