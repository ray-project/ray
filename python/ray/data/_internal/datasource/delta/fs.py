"""Filesystem helpers for the Delta Lake datasink.

This module isolates filesystem reconstruction so the adapter remains
pickleable. The MVP build resolves a path-only PyArrow filesystem on the
driver and rebuilds the same filesystem on each worker. PR 7 extends this
to thread cloud credentials through.

PyArrow filesystem: https://arrow.apache.org/docs/python/api/filesystems.html
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pyarrow.fs as pa_fs


@dataclass
class _FsConfig:
    """Picklable filesystem reconstruction record."""

    table_uri: str
    storage_options: Dict[str, str]
    # Absolute directory for plain local ``table_uri`` values (from_uri path).
    # Used to resolve Delta-relative file paths; see module docstring on
    # ``make_fs_config``.
    local_filesystem_root: Optional[str] = None


def make_fs_config(
    table_uri: str,
    filesystem: Optional[pa_fs.FileSystem],
    storage_options: Dict[str, str],
) -> Tuple[_FsConfig, pa_fs.FileSystem]:
    """Return ``(picklable_config, driver_filesystem)``.

    The driver uses the returned filesystem directly. The config travels on
    the pickled adapter; workers call ``worker_filesystem(config)`` to
    materialise their own filesystem instance.

    For a **local** POSIX ``table_uri``, ``FileSystem.from_uri`` returns
    ``(LocalFileSystem, base_path)``, but ``pq.write_table(..., path,
    filesystem=fs)`` resolves *relative* ``path`` against the process working
    directory, not ``base_path``. Callers therefore also record
    ``local_filesystem_root=base_path`` and join it to Delta-relative paths
    when writing, validating, and cleaning up files.
    """
    if filesystem is not None:
        return (
            _FsConfig(
                table_uri=table_uri,
                storage_options=dict(storage_options),
                local_filesystem_root=None,
            ),
            filesystem,
        )
    fs, path = pa_fs.FileSystem.from_uri(table_uri)
    local_root = path if (path and isinstance(fs, pa_fs.LocalFileSystem)) else None
    return (
        _FsConfig(
            table_uri=table_uri,
            storage_options=dict(storage_options),
            local_filesystem_root=local_root,
        ),
        fs,
    )


def worker_filesystem(config: _FsConfig) -> pa_fs.FileSystem:
    """Materialise the filesystem on a worker from a picklable config."""
    fs, _ = pa_fs.FileSystem.from_uri(config.table_uri)
    return fs
