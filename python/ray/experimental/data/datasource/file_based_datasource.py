import logging
from typing import Optional, List, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.experimental.data.impl.arrow_block import ArrowRow, ArrowBlock
from ray.experimental.data.impl.block_list import BlockMetadata
from ray.experimental.data.datasource.datasource import (Datasource, ReadTask)

logger = logging.getLogger(__name__)


class FileBasedDatasource(Datasource[Union[ArrowRow, int]]):
    """File-based datasource, for reading and writing files.

    This class should not be used directly, and should instead be subclassed
    and tailored to particular file formats. Classes deriving from this class
    must implement _read_file().

    Current subclasses: JSONDatasource, CSVDatasource
    """

    def prepare_read(self,
                     parallelism: int,
                     paths: Union[str, List[str]],
                     filesystem: Optional["pyarrow.fs.FileSystem"] = None,
                     **reader_args) -> List[ReadTask]:
        """Creates and returns read tasks for a file-based datasource.
        """
        import pyarrow as pa
        import numpy as np

        paths, file_infos, filesystem = _resolve_paths_and_filesystem(
            paths, filesystem)
        file_sizes = [file_info.size for file_info in file_infos]

        read_file = self._read_file

        def read_files(read_paths: List[str]):
            logger.debug(f"Reading {len(read_paths)} files.")
            tables = []
            for read_path in read_paths:
                with filesystem.open_input_file(read_path) as f:
                    tables.append(read_file(f, **reader_args))
            return ArrowBlock(pa.concat_tables(tables))

        read_tasks = [
            ReadTask(
                lambda read_paths=read_paths: read_files(read_paths),
                BlockMetadata(
                    num_rows=None,
                    size_bytes=sum(file_sizes),
                    schema=None,
                    input_files=read_paths)) for read_paths, file_sizes in zip(
                        np.array_split(paths, parallelism),
                        np.array_split(file_sizes, parallelism))
            if len(read_paths) > 0
        ]

        return read_tasks

    def _read_file(self, f, **reader_args):
        """Reads a single file, passing all kwargs to the reader.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _read_files().")


def _expand_directory(path: str,
                      filesystem: "pyarrow.fs.FileSystem",
                      exclude_prefixes: List[str] = [".", "_"]) -> List[str]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".

    Returns:
        A list of file paths contained in the provided directory.
    """
    from pyarrow.fs import FileSelector
    selector = FileSelector(path, recursive=True)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    filtered_paths = []
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path):]
        if any(relative.startswith(prefix) for prefix in [".", "_"]):
            continue
        filtered_paths.append((file_path, file_))
    # We sort the paths to guarantee a stable order.
    return zip(*sorted(filtered_paths, key=lambda x: x[0]))


def _resolve_paths_and_filesystem(
        paths: Union[str, List[str]],
        filesystem: "pyarrow.fs.FileSystem" = None
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and ensures that all paths use the same filesystem, and expands all
    directory paths to the underlying file paths.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    from pyarrow.fs import FileType, _resolve_filesystem_and_path

    if isinstance(paths, str):
        paths = [paths]
    elif (not isinstance(paths, list)
          or any(not isinstance(p, str) for p in paths)):
        raise ValueError(
            "paths must be a path string or a list of path strings.")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    resolved_paths = []
    for path in paths:
        resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
            path, filesystem)
        if filesystem is None:
            filesystem = resolved_filesystem
        elif type(resolved_filesystem) != type(filesystem):
            raise ValueError("All paths must use same filesystem.")
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    expanded_paths = []
    file_infos = []
    for path in resolved_paths:
        file_info = filesystem.get_file_info(path)
        if file_info.type == FileType.Directory:
            paths, file_infos_ = _expand_directory(path, filesystem)
            expanded_paths.extend(paths)
            file_infos.extend(file_infos_)
        elif file_info.type == FileType.File:
            expanded_paths.append(path)
            file_infos.append(file_info)
        else:
            raise FileNotFoundError(path)
    return expanded_paths, file_infos, filesystem
