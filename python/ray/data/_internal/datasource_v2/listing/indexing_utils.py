import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSelector, FileType

from ray.data.datasource.file_meta_provider import _handle_read_os_error

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PathContents:
    """Contents of a path: files (path, size) and subdirectories to expand."""

    files: List[Tuple[str, Optional[int]]]
    subdirs: List[str]


def _expand_directory(
    base_path: str,
    filesystem: pa.fs.FileSystem,
    ignore_missing_path: bool,
    *,
    root_path: Optional[str] = None,
) -> PathContents:
    """List one level of a directory.

    Hidden-prefix (``.``/``_``) exclusion is applied relative to ``root_path``
    (the top-level path the traversal started from), not the immediate parent,
    so a nested entry is only excluded when its path *relative to the root*
    begins with an excluded prefix. When ``root_path`` is ``None`` it defaults
    to ``base_path``.
    """
    exclude_prefixes = [".", "_"]

    if root_path is None:
        root_path = base_path

    selector = FileSelector(
        base_path, recursive=False, allow_not_found=ignore_missing_path
    )
    children = filesystem.get_file_info(selector)

    # Lineage reconstruction doesn't work if tasks aren't deterministic, and
    # `filesystem.get_file_info` might return files in a non-deterministic order. So, we
    # sort the files.
    assert isinstance(children, list), type(children)
    children.sort(key=lambda file_: file_.path)

    files: List[Tuple[str, Optional[int]]] = []
    subdirs: List[str] = []

    for child in children:
        if not child.path.startswith(root_path):
            continue

        relative = child.path[len(root_path) :].lstrip("/")
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue

        if child.type == FileType.File:
            files.append((child.path, child.size))
        elif child.type == FileType.Directory:
            subdirs.append(child.path)
        elif child.type == FileType.UNKNOWN:
            logger.warning(f"Discovered file with unknown type: '{child.path}'")
            continue
        else:
            assert child.type == FileType.NotFound
            raise FileNotFoundError(child.path)

    return PathContents(files=files, subdirs=subdirs)


def _get_path_contents(
    path: str,
    filesystem: pa.fs.FileSystem,
    ignore_missing_path: bool,
    *,
    root_path: Optional[str] = None,
) -> PathContents:
    """Get files and subdirs for a path. Handles File, Directory, and NotFound.

    Only one level of a directory is expanded; discovered subdirectories are
    returned in :attr:`PathContents.subdirs` for the caller to expand.
    """
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)

    if file_info.type == FileType.File:
        return PathContents(files=[(path, file_info.size)], subdirs=[])
    elif file_info.type == FileType.Directory:
        return _expand_directory(
            path, filesystem, ignore_missing_path, root_path=root_path
        )
    elif file_info.type == FileType.NotFound and ignore_missing_path:
        return PathContents(files=[], subdirs=[])
    else:
        raise FileNotFoundError(path)


def _get_file_infos(
    path: str,
    filesystem: pa.fs.FileSystem,
    ignore_missing_path: bool,
    *,
    _root_path: Optional[str] = None,
) -> Iterable[Tuple[str, Optional[int]]]:
    """Recursively expand a path (file or directory) into ``(path, size)`` tuples."""
    if _root_path is None:
        _root_path = path
    contents = _get_path_contents(
        path, filesystem, ignore_missing_path, root_path=_root_path
    )
    yield from contents.files
    for subdir in contents.subdirs:
        yield from _get_file_infos(
            subdir, filesystem, ignore_missing_path, _root_path=_root_path
        )
