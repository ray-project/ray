import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Tuple

from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    FileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data._internal.datasource_v2.listing.indexing_utils import _get_file_infos
from ray.data._internal.util import make_async_gen
from ray.data.block import BlockColumn
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)


class FileIndexer(ABC):
    @property
    @abstractmethod
    def file_chunker(self) -> FileChunker:
        """The file chunker that this indexer uses."""
        ...

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

    _DEFAULT_MAX_PATHS_PER_OUTPUT = env_integer(
        "RAY_DATA_MAX_PATHS_PER_LIST_FILES_OUTPUT", 1000
    )

    _DEFAULT_NUM_WORKERS = env_integer("RAY_DATA_LIST_FILES_THREADED_NUM_WORKERS", 4)

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        file_chunker: Optional[FileChunker] = None,
    ):
        self._ignore_missing_paths = ignore_missing_paths
        self._max_paths_per_output = (
            max_paths_per_output
            if max_paths_per_output is not None
            else self._DEFAULT_MAX_PATHS_PER_OUTPUT
        )
        self._num_workers = (
            num_workers if num_workers is not None else self._DEFAULT_NUM_WORKERS
        )
        self._queue_size_per_thread = env_integer(
            "RAY_DATA_LIST_FILES_QUEUE_SIZE_PER_THREAD",
            self._max_paths_per_output * 4,
        )
        self._file_chunker: FileChunker = (
            file_chunker if file_chunker is not None else WholeFileChunker()
        )

    @property
    def file_chunker(self) -> FileChunker:
        """The file chunker that this indexer uses.

        Exposed primarily for tests and shuffle-aware planning code that needs
        to introspect or override the chunking strategy.
        """
        return self._file_chunker

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
            if self._num_workers > 1
            else self._get_file_info_iterator_sequential(paths, filesystem)
        )

        # Stage pipeline: list → prune (cheap, inline) → chunk (may read
        # per-file metadata) → batch into manifests. Pruning runs *before*
        # chunking so we never read a footer for a file we'd discard.
        pruned = self._filter_file_infos(file_info_iterator, pruners or [])
        chunk_records = self._generate_chunk_records(pruned, filesystem, preserve_order)
        yield from self._batch_chunk_records_to_manifests(chunk_records)

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

        num_workers = min(self._num_workers, len(paths_list))

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
            buffer_size=self._queue_size_per_thread,
        )

    def _filter_file_infos(
        self,
        file_infos: Iterable[FileInfo],
        pruners: List[FilePruner],
    ) -> Iterator[FileInfo]:
        """Drop zero-size and pruned files before any per-file metadata read."""
        for file_info in file_infos:
            if file_info.size is None or file_info.size == 0:
                logger.warning(f"Skipping zero-size file: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info

    def _generate_chunk_records(
        self,
        file_infos: Iterable[FileInfo],
        filesystem: "FileSystem",
        preserve_order: bool,
    ) -> Iterator[Tuple[str, int, Optional[ChunkMetadata]]]:
        """Drive the chunker per file, yielding ``(path, chunk_size, metadata)``.

        When the chunker reads per-file metadata (e.g. ``ParquetFileChunker``
        reading footers), fan the work across the indexer's thread pool so the
        I/O parallelizes even for a single input directory — ``make_async_gen``
        over the *discovered files*, not the input paths. Chunkers that don't
        read metadata (whole-file / line-delimited) are driven inline to avoid
        a pointless thread hand-off.
        """
        chunker = self._file_chunker

        def chunk(
            infos: Iterator[FileInfo],
        ) -> Iterator[Tuple[str, int, Optional[ChunkMetadata]]]:
            for fi in infos:
                for chunk_metadata, chunk_size in chunker.generate_chunk_metadatas(
                    fi.path, fi.size, filesystem
                ):
                    yield fi.path, chunk_size, chunk_metadata

        if chunker.reads_file_metadata and self._num_workers > 1:
            yield from make_async_gen(
                base_iterator=file_infos,
                fn=chunk,
                preserve_ordering=preserve_order,
                num_workers=self._num_workers,
                buffer_size=self._queue_size_per_thread,
            )
        else:
            yield from chunk(iter(file_infos))

    def _batch_chunk_records_to_manifests(
        self,
        chunk_records: Iterable[Tuple[str, int, Optional[ChunkMetadata]]],
    ) -> Iterable[FileManifest]:
        """Batch chunk records into ``FileManifest`` blocks of bounded size."""
        running_paths: List[str] = []
        running_file_sizes: List[int] = []
        running_chunk_metadatas: List[Optional[ChunkMetadata]] = []
        manifests_count = 0
        chunks_count = 0

        for path, chunk_size, chunk_metadata in chunk_records:
            running_paths.append(path)
            running_file_sizes.append(chunk_size)
            running_chunk_metadatas.append(chunk_metadata)
            chunks_count += 1

            if len(running_paths) >= self._max_paths_per_output:
                manifests_count += 1
                yield FileManifest.construct_manifest(
                    running_paths,
                    running_file_sizes,
                    running_chunk_metadatas,
                )
                running_paths = []
                running_file_sizes = []
                running_chunk_metadatas = []

        if running_paths:
            manifests_count += 1
            yield FileManifest.construct_manifest(
                running_paths,
                running_file_sizes,
                running_chunk_metadatas,
            )

        logger.debug(
            f"Listing files: constructed {manifests_count} manifests "
            f"with {chunks_count} file chunks"
        )
