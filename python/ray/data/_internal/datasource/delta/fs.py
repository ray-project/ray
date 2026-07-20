"""Filesystem helpers for the Delta Lake datasink.

This module isolates filesystem reconstruction so the datasink remains
pickleable. The driver resolves a PyArrow filesystem once (either the
explicit ``filesystem=`` the caller supplied, or one built from ``table_uri``
via ``FileSystem.from_uri``); each worker rebuilds an equivalent filesystem
from a small picklable config rather than pickling the filesystem object
itself -- with one exception: an explicitly-supplied filesystem is carried
through as-is, since PyArrow filesystems such as ``S3FileSystem`` are
themselves picklable, and a caller-supplied filesystem may not be
reconstructable from ``table_uri`` alone (e.g. a ``SubTreeFileSystem``
rooted somewhere other than the table's own URI, or one carrying
credentials the worker's ambient environment doesn't have).

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
    # Set only when the caller supplied an explicit filesystem. Carried to
    # the worker as-is -- see module docstring.
    explicit_filesystem: Optional[pa_fs.FileSystem] = None


def make_fs_config(
    table_uri: str,
    filesystem: Optional[pa_fs.FileSystem],
    storage_options: Dict[str, str],
) -> Tuple[_FsConfig, pa_fs.FileSystem]:
    """Return ``(picklable_config, driver_filesystem)``.

    The driver uses the returned filesystem directly. The config travels on
    the pickled datasink; workers call ``worker_filesystem(config)`` to
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
                explicit_filesystem=filesystem,
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
    """Materialise the filesystem on a worker from a picklable config.

    Returns the explicitly-supplied filesystem as-is when there is one
    (matching the driver exactly, including its root/credentials);
    otherwise rebuilds a path-only filesystem from ``table_uri``.
    """
    if config.explicit_filesystem is not None:
        return config.explicit_filesystem
    fs, _ = pa_fs.FileSystem.from_uri(config.table_uri)
    return fs
