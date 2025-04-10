import abc
import logging
from typing import Iterable, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSelector, FileType

from ray.data.datasource.file_meta_provider import _handle_read_os_error

logger = logging.getLogger(__name__)


class FileIndexer(abc.ABC):
    @abc.abstractmethod
    def list_files(
        self, path: str, filesystem: pa.fs.FileSystem
    ) -> Iterable[Tuple[str, Optional[int]]]:
        """List files and their on-disk sizes for the given path.

        Args:
            path: A path pointing to a file or directory.
            filesystem: A PyArrow filesystem object.

        Returns:
            An iterator of tuples, where each tuple contains a file path and the on-disk
            size of the file in bytes.
        """
        ...


class NonSamplingFileIndexer(FileIndexer):
    """A file indexer that exhaustively lists files.

    This implementation works with paths that point to files or directories, although
    it's slow if you try to list lots of paths pointing to files rather than a single
    directory.
    """

    def __init__(self, *, ignore_missing_paths: bool):
        self._ignore_missing_paths = ignore_missing_paths

    def list_files(
        self, path: str, filesystem: pa.fs.FileSystem
    ) -> Iterable[Tuple[str, Optional[int]]]:
        yield from _get_file_infos(path, filesystem, self._ignore_missing_paths)


def _get_file_infos(
    path: str, filesystem: pa.fs.FileSystem, ignore_missing_path: bool
) -> Iterable[Tuple[str, Optional[int]]]:
    from pyarrow.fs import FileType

    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)

    if file_info.type == FileType.Directory:
        yield from _expand_directory(path, filesystem, ignore_missing_path)
    elif file_info.type == FileType.File:
        yield (path, file_info.size)
    elif file_info.type == FileType.NotFound and ignore_missing_path:
        pass
    else:
        raise FileNotFoundError(path)


def _expand_directory(
    base_path: str, filesystem: pa.fs.FileSystem, ignore_missing_path: bool
) -> Iterable[Tuple[str, Optional[int]]]:
    exclude_prefixes = [".", "_"]
    selector = FileSelector(
        base_path, recursive=False, allow_not_found=ignore_missing_path
    )
    files = filesystem.get_file_info(selector)

    # Lineage reconstruction doesn't work if tasks aren't deterministic, and
    # `filesystem.get_file_info` might return files in a non-deterministic order. So, we
    # sort the files.
    assert isinstance(files, list), type(files)
    files.sort(key=lambda file_: file_.path)

    for file_ in files:
        if not file_.path.startswith(base_path):
            continue

        relative = file_.path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue

        if file_.type == FileType.File:
            yield (file_.path, file_.size)
        elif file_.type == FileType.Directory:
            yield from _expand_directory(file_.path, filesystem, ignore_missing_path)
        elif file_.type == FileType.UNKNOWN:
            logger.warning(f"Discovered file with unknown type: '{file_.path}'")
            continue
        else:
            assert file_.type == FileType.NotFound
            raise FileNotFoundError(file_.path)
