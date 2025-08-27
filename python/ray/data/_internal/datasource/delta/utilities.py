"""
Utility functions for Delta Lake datasource.

This module contains helper functions for Delta Lake functionality.
"""

import logging
from typing import Any, Dict, Optional

import pyarrow.fs as pa_fs
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def try_get_deltatable(
    path: str, storage_options: Optional[Dict[str, Any]] = None
) -> Optional[DeltaTable]:
    """Try to get a DeltaTable instance, returning None if it doesn't exist.

    Args:
        path: Path to the Delta table
        storage_options: Storage options for the filesystem

    Returns:
        DeltaTable instance if table exists, None otherwise
    """
    try:
        return DeltaTable(path, storage_options=storage_options or {})
    except Exception:
        return None


def get_storage_options_for_path(path: str) -> Dict[str, Any]:
    """Get basic storage options for a given path.

    Args:
        path: Path to the Delta table

    Returns:
        Dict with storage options
    """
    storage_options = {}

    # Basic path-based storage options
    if path.startswith("s3://") or path.startswith("s3a://"):
        # AWS S3 - basic options
        storage_options["AWS_REGION"] = "us-east-1"
    elif path.startswith("gs://"):
        # Google Cloud Storage - basic options
        pass
    elif path.startswith("abfss://") or path.startswith("abfs://"):
        # Azure Blob Storage - basic options
        pass
    elif path.startswith("hdfs://"):
        # HDFS - basic options
        pass

    return storage_options


def create_writer_properties(
    compression: Optional[str] = None,
    compression_level: Optional[int] = None,
    max_row_group_size: Optional[int] = None,
    data_page_size_limit: Optional[int] = None,
    **kwargs
) -> Any:
    """Create a deltalake.WriterProperties instance for fine-tuning Parquet writing.

    Args:
        compression: Compression type ('UNCOMPRESSED', 'SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD', 'LZ4_RAW')
        compression_level: Compression level (1-22 for ZSTD, 1-11 for BROTLI, 1-9 for GZIP)
        max_row_group_size: Maximum number of rows in row group
        data_page_size_limit: Limit DataPage size in bytes
        **kwargs: Additional writer properties

    Returns:
        deltalake.WriterProperties instance
    """
    try:
        from deltalake import WriterProperties

        return WriterProperties(
            compression=compression,
            compression_level=compression_level,
            max_row_group_size=max_row_group_size,
            data_page_size_limit=data_page_size_limit,
            **kwargs
        )
    except ImportError:
        logger.warning("deltalake not available, cannot create WriterProperties")
        return None


def create_bloom_filter_properties(
    set_bloom_filter_enabled: bool = True,
    fpp: Optional[float] = None,
    ndv: Optional[int] = None,
) -> Any:
    """Create a deltalake.BloomFilterProperties instance for Parquet bloom filters.

    Args:
        set_bloom_filter_enabled: If True, enables bloom filters
        fpp: False positive probability (0.0 to 1.0)
        ndv: Number of distinct values

    Returns:
        deltalake.BloomFilterProperties instance
    """
    try:
        from deltalake import BloomFilterProperties

        return BloomFilterProperties(
            set_bloom_filter_enabled=set_bloom_filter_enabled, fpp=fpp, ndv=ndv
        )
    except ImportError:
        logger.warning("deltalake not available, cannot create BloomFilterProperties")
        return None


def create_column_properties(
    dictionary_enabled: Optional[bool] = None,
    statistics_enabled: Optional[str] = None,  # 'NONE', 'CHUNK', 'PAGE'
    bloom_filter_properties: Optional[Any] = None,
) -> Any:
    """Create a deltalake.ColumnProperties instance for column-specific settings.

    Args:
        dictionary_enabled: Enable dictionary encoding for the column
        statistics_enabled: Statistics level ('NONE', 'CHUNK', 'PAGE')
        bloom_filter_properties: BloomFilterProperties instance

    Returns:
        deltalake.ColumnProperties instance
    """
    try:
        from deltalake import ColumnProperties

        return ColumnProperties(
            dictionary_enabled=dictionary_enabled,
            statistics_enabled=statistics_enabled,
            bloom_filter_properties=bloom_filter_properties,
        )
    except ImportError:
        logger.warning("deltalake not available, cannot create ColumnProperties")
        return None
