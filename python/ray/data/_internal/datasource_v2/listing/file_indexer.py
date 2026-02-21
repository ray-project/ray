import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional

from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data._internal.datasource_v2.listing.indexing_utils import _get_file_infos
from ray.data._internal.util import make_async_gen
from ray.data.block import BlockColumn
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)


class FileIndexer(ABC):
    @abstractmethod
    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileManifest]:
        """List files and their on-disk sizes for the given path.

        Args:
            paths: A column of paths pointing to files or directories.
            filesystem: A PyArrow filesystem object.
            pruners: A list of file pruners to apply.
            preserve_order: Whether to preserve order in file listing.

        Returns:
            An iterator of `FileManifest` objects, each of which contains a file path
            and the on-disk size of the file in bytes.
        """
        ...


@dataclass
class FileInfo:
    """File information for file listing."""

    path: str
    size: Optional[int]


class NonSamplingFileIndexer(FileIndexer):
    """A file indexer that exhaustively lists files.

    This implementation works with paths that point to files or directories,
    although it's slow if you try to list lots of paths pointing to files
    rather than a single directory.
    """

    _MAX_PATHS_PER_LIST_FILES_OUTPUT = env_integer(
        "RAY_DATA_MAX_PATHS_PER_LIST_FILES_OUTPUT", 1000
    )

    _QUEUE_SIZE_PER_THREAD = env_integer(
        "RAY_DATA_LIST_FILES_QUEUE_SIZE_PER_THREAD",
        _MAX_PATHS_PER_LIST_FILES_OUTPUT * 4,
    )

    _THREADED_NUM_WORKERS = env_integer("RAY_DATA_LIST_FILES_THREADED_NUM_WORKERS", 4)

    def __init__(self, *, ignore_missing_paths: bool):
        self._ignore_missing_paths = ignore_missing_paths

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileManifest]:
        file_info_iterator = (
            self._get_file_info_iterator_threaded(paths, filesystem, preserve_order)
            if self._THREADED_NUM_WORKERS > 1
            else self._get_file_info_iterator_sequential(paths, filesystem)
        )

        yield from self._process_file_infos_to_manifests(
            file_info_iterator, pruners or []
        )

    def _get_file_info_iterator_sequential(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
    ) -> Iterable[FileInfo]:
        for input_path in paths.to_pylist():
            resolved_paths, _ = _resolve_paths_and_filesystem(input_path, filesystem)
            assert len(resolved_paths) == 1

            for path, file_size in _get_file_infos(
                resolved_paths[0], filesystem, self._ignore_missing_paths
            ):
                yield FileInfo(path=path, size=file_size)

    def _get_file_info_iterator_threaded(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        paths_list = paths.to_pylist()
        if len(paths_list) == 0:
            return

        num_workers = min(self._THREADED_NUM_WORKERS, len(paths_list))

        def process_paths(
            path_iterator: Iterator[str],
        ) -> Iterator[FileInfo]:
            for input_path in path_iterator:
                resolved_paths, _ = _resolve_paths_and_filesystem(
                    input_path, filesystem
                )
                assert len(resolved_paths) == 1

                for path, file_size in _get_file_infos(
                    resolved_paths[0],
                    filesystem,
                    self._ignore_missing_paths,
                ):
                    yield FileInfo(path=path, size=file_size)

        yield from make_async_gen(
            base_iterator=iter(paths_list),
            fn=process_paths,
            preserve_ordering=preserve_order,
            num_workers=num_workers,
            buffer_size=self._QUEUE_SIZE_PER_THREAD,
        )

    def _process_file_infos_to_manifests(
        self,
        file_infos: Iterable[FileInfo],
        pruners: List[FilePruner],
    ) -> Iterable[FileManifest]:
        running_paths: List[str] = []
        running_file_sizes: List[int] = []
        manifests_count = 0
        files_count = 0

        for file_info in file_infos:
            path, file_size = file_info.path, file_info.size

            if file_size == 0:
                logger.warning(f"Skipping zero-size file: {path!r}")
                continue

            if not all(pruner.should_include(path) for pruner in pruners):
                continue

            running_paths.append(path)
            running_file_sizes.append(file_size)
            files_count += 1

            if len(running_paths) >= self._MAX_PATHS_PER_LIST_FILES_OUTPUT:
                manifests_count += 1
                yield FileManifest.construct_manifest(running_paths, running_file_sizes)
                running_paths = []
                running_file_sizes = []

        if running_paths:
            manifests_count += 1
            yield FileManifest.construct_manifest(running_paths, running_file_sizes)

        logger.debug(
            f"Listing files: constructed {manifests_count} manifests "
            f"with {files_count} files"
        )
