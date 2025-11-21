"""Format detection utilities for the unified read() API.

This module provides automatic file format detection for ray.data.read() based on
file extensions and directory structure markers. It supports:

1. File formats: Parquet, CSV, JSON, images, audio, video, NumPy, Avro, TFRecords, etc.
2. Lakehouse formats: Delta Lake, Apache Hudi, Apache Iceberg (detected by directory markers)

The detection logic prioritizes:
- Explicit format hints (if provided)
- Lakehouse format markers (_delta_log, .hoodie, metadata/)
- File extensions (using a centralized mapping built from datasource classes)
- Binary fallback for unknown file types

Related documentation:
- Delta Lake: https://delta.io/
- Apache Hudi: https://hudi.apache.org/
- Apache Iceberg: https://iceberg.apache.org/
- PyArrow filesystems: https://arrow.apache.org/docs/python/filesystems.html
"""

import logging
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

from ray.data._internal.datasource.audio_datasource import AudioDatasource
from ray.data._internal.datasource.avro_datasource import AvroDatasource
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.image_datasource import ImageDatasource
from ray.data._internal.datasource.json_datasource import JSON_FILE_EXTENSIONS
from ray.data._internal.datasource.mcap_datasource import MCAPDatasource
from ray.data._internal.datasource.numpy_datasource import NumpyDatasource
from ray.data._internal.datasource.parquet_bulk_datasource import ParquetBulkDatasource
from ray.data._internal.datasource.tfrecords_datasource import TFRecordDatasource
from ray.data._internal.datasource.video_datasource import VideoDatasource
from ray.data._internal.datasource.webdataset_datasource import WebDatasetDatasource

if TYPE_CHECKING:
    import pyarrow.fs

logger = logging.getLogger(__name__)


class LakehouseFormat(str, Enum):
    """Supported lakehouse table formats.

    Lakehouse formats are detected by directory structure markers:
    - Delta Lake: _delta_log directory
    - Apache Hudi: .hoodie directory
    - Apache Iceberg: metadata/ directory with version files
    """

    DELTA = "delta"
    HUDI = "hudi"
    ICEBERG = "iceberg"


def _detect_lakehouse_format(
    path: str, filesystem: "pyarrow.fs.FileSystem"
) -> Optional[str]:
    """Detect if a path is a Delta Lake, Hudi, or Iceberg table.

    Lakehouse formats are detected by directory structure markers:
    - Delta Lake: presence of _delta_log directory
    - Apache Hudi: presence of .hoodie directory
    - Apache Iceberg: presence of metadata/ directory with version files

    See https://delta.io/ for Delta Lake documentation.
    See https://hudi.apache.org/ for Apache Hudi documentation.
    See https://iceberg.apache.org/ for Apache Iceberg documentation.

    Args:
        path: Path to check for lakehouse format.
        filesystem: PyArrow filesystem to use for inspection.
            See https://arrow.apache.org/docs/python/filesystems.html

    Returns:
        The format name ('delta', 'hudi', 'iceberg') or None if not detected.
    """
    import pyarrow.fs as pafs

    file_info = filesystem.get_file_info(path)
    if file_info.type != pafs.FileType.Directory:
        return None

    # Get directory contents
    selector = pafs.FileSelector(path, recursive=False)
    contents = filesystem.get_file_info(selector)
    base_names = {Path(item.path).name for item in contents}

    # Check for lakehouse markers
    if "_delta_log" in base_names:
        return "delta"
    if ".hoodie" in base_names:
        return "hudi"
    if "metadata" in base_names:
        # Check if it's Iceberg
        metadata_path = f"{path}/metadata"
        meta_selector = pafs.FileSelector(metadata_path, recursive=False)
        meta_contents = filesystem.get_file_info(meta_selector)
        meta_names = {Path(item.path).name for item in meta_contents}
        if "version-hint.text" in meta_names or any(
            f.endswith(".metadata.json") for f in meta_names
        ):
            return "iceberg"

    return None


def _build_extension_to_reader_map() -> Dict[str, str]:
    """Build extension to reader mapping from datasource constants.

    This function automatically pulls file extensions from datasource classes,
    creating a unified mapping for format detection. Extensions are normalized
    to lowercase for case-insensitive matching.

    The extension map is built dynamically from datasource _FILE_EXTENSIONS constants,
    ensuring that updates to datasource supported extensions are automatically reflected
    in format detection without requiring manual updates to this module.

    Special cases:
    - Text files (.txt) are added manually as TextDatasource doesn't define _FILE_EXTENSIONS
    - Lance files (.lance) are added manually as LanceDatasource is directory-based

    Returns:
        Dictionary mapping lowercase file extensions (without leading dot) to reader names.
        For example: {"parquet": "parquet", "csv": "csv", "png": "images"}
    """
    extension_map = {}

    # Structured data formats
    for ext in ParquetBulkDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "parquet"

    for ext in CSVDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "csv"

    for ext in JSON_FILE_EXTENSIONS:
        extension_map[ext.lower()] = "json"

    # Media formats
    for ext in ImageDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "images"

    for ext in AudioDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "audio"

    for ext in VideoDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "video"

    # Array and binary formats
    for ext in NumpyDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "numpy"

    for ext in AvroDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "avro"

    for ext in TFRecordDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "tfrecords"

    # Special formats
    for ext in WebDatasetDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "webdataset"

    for ext in MCAPDatasource._FILE_EXTENSIONS:
        extension_map[ext.lower()] = "mcap"

    # Manual additions for formats without _FILE_EXTENSIONS
    extension_map["txt"] = "text"  # TextDatasource handles text files
    extension_map["lance"] = "lance"  # LanceDatasource handles Lance datasets

    return extension_map


# Build extension map once at module load time
_EXTENSION_TO_READER_MAP = _build_extension_to_reader_map()


def _get_file_extension(path: str) -> Optional[str]:
    """Get the file extension from a path, handling compound extensions.

    This function matches against the full extension map built from datasource
    constants, ensuring all compound extensions (e.g., csv.gz, json.br) are properly
    detected.

    Examples:
        data.parquet -> parquet
        data.csv.gz -> csv.gz (matched against extension map)
        data.json.br -> json.br (matched against extension map)

    Args:
        path: File path.

    Returns:
        Extension without leading dot (e.g., "csv", "csv.gz", "json.br"), or None if no extension.
    """
    path_obj = Path(path)
    name = path_obj.name.lower()

    # Try to match against all extensions in the map (including compound extensions)
    # Check longest matches first (e.g., "csv.gz" before "csv")
    sorted_extensions = sorted(_EXTENSION_TO_READER_MAP.keys(), key=len, reverse=True)

    for ext in sorted_extensions:
        if name.endswith(f".{ext}"):
            return ext

    # Fallback: try standard single extension
    suffix = path_obj.suffix.lower()
    return suffix[1:] if suffix else None


def _get_reader_for_path(path: str) -> Optional[str]:
    """Get the reader name for a file path based on extension.

    Args:
        path: File path.

    Returns:
        Reader name (e.g., 'parquet', 'csv') or None if unknown.
    """
    ext = _get_file_extension(path)
    if not ext:
        return None

    return _EXTENSION_TO_READER_MAP.get(ext)
