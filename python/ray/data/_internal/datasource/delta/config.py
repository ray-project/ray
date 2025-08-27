"""
Configuration classes and enums for Delta Lake datasource.

This module contains the configuration data classes and enums used
by the simplified Delta Lake datasource implementation.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyarrow as pa


@dataclass
class DeltaWriteConfig:
    """Configuration for Delta Lake write operations.

    Args:
        mode: Write mode for the Delta table
        partition_cols: Columns to partition by
        schema: Table schema
        storage_options: Storage options for the filesystem
        write_options: Additional write options
        name: Table name
        description: Table description
        configuration: Table configuration
        schema_mode: Schema mode ('merge' or 'overwrite')
        target_file_size: Target file size in bytes
        writer_properties: deltalake.WriterProperties instance
        post_commithook_properties: deltalake.PostCommitHookProperties instance
        commit_properties: deltalake.CommitProperties instance
    """

    mode: str = "append"
    partition_cols: Optional[List[str]] = None
    schema: Optional[pa.Schema] = None
    storage_options: Optional[Dict[str, Any]] = None
    write_options: Optional[Dict[str, Any]] = None

    # Deltalake-specific options
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[Dict[str, str]] = None
    schema_mode: Optional[str] = None  # 'merge' or 'overwrite'
    target_file_size: Optional[int] = None
    writer_properties: Optional[Any] = None  # deltalake.WriterProperties
    post_commithook_properties: Optional[
        Any
    ] = None  # deltalake.PostCommitHookProperties
    commit_properties: Optional[Any] = None  # deltalake.CommitProperties


@dataclass
class DeltaSinkWriteResult:
    """Result of a Delta Lake write operation.

    Args:
        path: Path to the Delta table
        files_written: Number of files written
        bytes_written: Total bytes written
        partitions_written: List of partition paths written
        metadata: Additional metadata about the write operation
    """

    path: str
    files_written: int
    bytes_written: int
    partitions_written: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
