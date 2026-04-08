"""Filesystem factory and serialization helpers for Delta Lake datasink.

This module handles filesystem resolution, caching, and serialization to avoid
rebuilding filesystems on every block write.

PyArrow filesystems: https://arrow.apache.org/docs/python/api/filesystems.html
"""

from dataclasses import dataclass
from typing import Optional, Tuple

import pyarrow.fs as pa_fs

from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem


@dataclass(frozen=True)
class DeltaFSConfig:
    """Serializable filesystem configuration for worker reconstruction."""

    table_uri: str  # original URI (scheme preserved)
    table_root: str  # resolved single path root
    storage_options: dict  # serializable, used by delta-rs + auth


def resolve_table_root_and_fs(
    table_uri: str, filesystem: Optional[pa_fs.FileSystem]
) -> Tuple[str, pa_fs.FileSystem]:
    """Resolve table root path and filesystem.

    Args:
        table_uri: Original table URI.
        filesystem: Optional user-supplied filesystem.

    Returns:
        Tuple of (table_root, raw_filesystem).

    Raises:
        ValueError: If multiple paths are resolved.
    """
    resolved_paths, raw_fs = _resolve_paths_and_filesystem(table_uri, filesystem)
    if len(resolved_paths) != 1:
        raise ValueError(f"Expected exactly one path, got {len(resolved_paths)}")
    return resolved_paths[0], raw_fs


def build_subtree_fs(table_root: str, raw_fs: pa_fs.FileSystem) -> pa_fs.FileSystem:
    """Build SubTreeFileSystem rooted at table path with retry support.

    Args:
        table_root: Resolved table root path.
        raw_fs: Raw PyArrow filesystem.

    Returns:
        RetryingPyFileSystem wrapped SubTreeFileSystem.
    """
    fs = pa_fs.SubTreeFileSystem(table_root, raw_fs)
    ctx = DataContext.get_current()
    return RetryingPyFileSystem.wrap(fs, retryable_errors=ctx.retried_io_errors)


def ensure_dir_exists(raw_fs: pa_fs.FileSystem, table_root: str) -> None:
    """Ensure table root directory exists (best-effort).

    Args:
        raw_fs: Raw PyArrow filesystem.
        table_root: Table root path.
    """
    try:
        raw_fs.create_dir(table_root, recursive=True)
    except (FileExistsError, pa_fs.FileNotFoundError, OSError):
        pass


def make_fs_config(
    table_uri: str,
    filesystem: Optional[pa_fs.FileSystem],
    storage_options: dict,
) -> Tuple[DeltaFSConfig, pa_fs.FileSystem]:
    """Create filesystem config and driver filesystem.

    Args:
        table_uri: Original table URI.
        filesystem: Optional user-supplied filesystem.
        storage_options: Storage authentication options.

    Returns:
        Tuple of (DeltaFSConfig, driver_filesystem).
    """
    table_root, raw_fs = resolve_table_root_and_fs(table_uri, filesystem)
    ensure_dir_exists(raw_fs, table_root)
    fs = build_subtree_fs(table_root, raw_fs)
    return (
        DeltaFSConfig(
            table_uri=table_uri,
            table_root=table_root,
            storage_options=storage_options,
        ),
        fs,
    )


def worker_filesystem(config: DeltaFSConfig) -> pa_fs.FileSystem:
    """Build filesystem on worker from serializable config.

    Uses config.table_root directly to avoid re-resolution differences.
    Reconstructs filesystem using storage_options to ensure credentials are available.

    Args:
        config: Serializable filesystem configuration.

    Returns:
        Worker filesystem (SubTreeFileSystem with retry support).
    """
    from ray.data._internal.datasource.delta.utils import (
        create_filesystem_from_storage_options,
    )

    # Build filesystem from storage_options if credentials are provided
    raw_fs = create_filesystem_from_storage_options(
        config.table_uri, config.storage_options
    )
    if raw_fs is None:
        # Fall back to default resolution if no explicit credentials
        _, raw_fs = resolve_table_root_and_fs(config.table_uri, None)
    ensure_dir_exists(raw_fs, config.table_root)
    return build_subtree_fs(config.table_root, raw_fs)
