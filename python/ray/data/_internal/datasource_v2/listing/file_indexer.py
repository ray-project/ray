import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Tuple, Union

import numpy as np
from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    FileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data._internal.datasource_v2.listing.indexing_utils import (
    _get_file_infos,
    _get_path_contents,
)
from ray.data._internal.dynamic_work_queue import parallel_process_work_stealing
from ray.data.block import BlockColumn
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


class FileIndexer(ABC):
    @property
    @abstractmethod
    def file_chunker(self) -> FileChunker:
        """The file chunker that this indexer uses."""
        ...

    def as_whole_file_indexer(self) -> Optional["FileIndexer"]:
        """An equivalent indexer that emits each file exactly once, or ``None``.

        Metadata-only consumers -- currently the ``PushdownCountFiles`` rule --
        need a listing where one file means one manifest row and listing itself
        does no per-file IO. An indexer that chunks files, bin-packs them, or
        reads metadata while listing cannot provide that, and would over-count.

        Default ``None`` means "cannot provide it", so such consumers decline
        and fall back to a real read. Fail-closed on purpose: a wrong ``count()``
        is silent, a declined optimization is merely slower.
        """
        return None

    @abstractmethod
    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
    ) -> Iterable[FileManifest]:
        """List files and their on-disk sizes for the given path.

        Args:
            paths: A column of paths pointing to files or directories.
            filesystem: A PyArrow filesystem object.
            pruners: A list of file pruners to apply.
            preserve_order: Whether to preserve order in file listing.
            predicate: Pushed-down row filter. Indexers that read file
                metadata (e.g. the footer-based Parquet indexer) use it to skip
                row groups; others ignore it.
            limit: Pushed-down row limit, for indexers that can stop listing
                early. Others ignore it.
            projected_columns: Pushed-down column projection, for metadata-aware
                sizing. Others ignore it.
            shuffle_config: When set, listed files are shuffled after path
                discovery and before metadata fetch (footer reads, chunking).
                :meth:`list_file_infos` is never shuffled.
            execution_idx: Execution index used with ``shuffle_config`` to
                derive a per-execution seed.

        Returns:
            An iterator of `FileManifest` objects, each of which contains a file path
            and the on-disk size of the file in bytes.
        """
        ...

    @abstractmethod
    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable["FileInfo"]:
        """List files as raw ``FileInfo``\\ s (path + on-disk size).

        Unlike :meth:`list_files`, this yields the pre-chunk file stream and is
        never shuffled. The footer-based Parquet path consumes it (after an
        optional file shuffle) -- it reads each file's footer and bin-packs
        row groups itself, so it needs paths + sizes rather than pre-chunked
        manifest rows.
        """
        ...


@dataclass(frozen=True)
class FileInfo:
    """File information for file listing."""

    path: str
    size: Optional[int]


def _shuffle_file_infos(
    file_infos: List[FileInfo], seed: Optional[int]
) -> List[FileInfo]:
    """Permute ``file_infos`` with the same seeded rule as ``FileManifest.shuffle``.

    When ``seed`` is set, sort by path first so the permutation is independent
    of upstream listing order (the threaded indexer does not preserve order).
    """
    n = len(file_infos)
    if n <= 1:
        return file_infos
    if seed is not None:
        file_infos = sorted(file_infos, key=lambda fi: fi.path)
    permutation = np.random.default_rng(seed).permutation(n)
    return [file_infos[i] for i in permutation]


@dataclass(frozen=True)
class _TraversalWorkItem:
    """Work item for parallel directory traversal. Distinguishes seed paths
    (user-provided, need resolution) from subdir paths (from filesystem listing,
    use directly to avoid redundant resolution that breaks non-local
    filesystems)."""

    # Could be a file path or a directory path.
    path: str
    # True for subdirectories discovered during traversal; False for seed input paths.
    is_discovered_subdir: bool = False
    # Original seed-path index used to restore deterministic ordering when requested.
    input_path_index: Optional[int] = None
    # Top-level path the traversal started from, used to scope hidden-prefix
    # exclusion to entries whose path relative to the root is hidden.
    root_path: Optional[str] = None


@dataclass(frozen=True)
class OrderedFileResult:
    """File result with order information for sorting when preserve_order is True."""

    input_path_index: int
    # The leaf file path.
    file_path: str
    file_info: FileInfo


class NonSamplingFileIndexer(FileIndexer):
    """A file indexer that exhaustively lists files.

    This implementation works with paths that point to files or directories,
    although it's slow if you try to list lots of paths pointing to files
    rather than a single directory.
    """

    _DEFAULT_MAX_PATHS_PER_OUTPUT = env_integer(
        "RAY_DATA_MAX_PATHS_PER_LIST_FILES_OUTPUT", 1000
    )

    _DEFAULT_NUM_WORKERS = env_integer("RAY_DATA_LIST_FILES_THREADED_NUM_WORKERS", 8)

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        file_chunker: Optional[FileChunker] = None,
    ):
        self._ignore_missing_paths = ignore_missing_paths
        # Resolved paths to exclude from the listing (see
        # ``ParquetDatasourceV2``). ``frozenset()`` when unset so the membership
        # check in ``_get_path_contents`` is a cheap no-op.
        self._skip_paths = frozenset(skip_paths) if skip_paths else frozenset()
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

    def as_whole_file_indexer(self) -> "NonSamplingFileIndexer":
        """A plain per-file indexer sharing this one's traversal config.

        Metadata-only consumers (the ``PushdownCountFiles`` rule) need a listing
        that emits each file exactly once and does no per-file IO while listing.
        Subclasses that override :meth:`list_files` with a metadata-aware
        strategy -- e.g. ``FooterFileIndexer``, which footer-reads every file on
        an actor pool and emits one manifest row per row-group run -- would both
        duplicate that IO and emit a path more than once. So this deliberately
        returns a base ``NonSamplingFileIndexer`` rather than ``type(self)``,
        carrying over only the traversal settings.

        ``skip_paths`` is part of that traversal config and must carry over:
        dropping it would let excluded files back into the listing, inflating a
        pushed-down ``count()`` and turning a skipped-but-missing path into a
        ``FileNotFoundError``.
        """
        return NonSamplingFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            skip_paths=self._skip_paths,
            num_workers=self._num_workers,
            max_paths_per_output=self._max_paths_per_output,
            file_chunker=WholeFileChunker(),
        )

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
    ) -> Iterable[FileManifest]:
        # This per-file listing path ignores predicate/limit/projected_columns;
        # they're consumed by metadata-aware indexers (e.g. the footer indexer).
        # ``list_file_infos`` already skips zero-size files and applies pruners,
        # so the manifest builder only has to chunk. Shuffle, when requested,
        # happens after path discovery and before chunking.
        file_infos = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
        )
        yield from self._process_file_infos_to_manifests(file_infos)

    def _iter_file_infos_for_list(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
    ) -> Iterable[FileInfo]:
        """Path discovery, then optional file shuffle, before metadata fetch.

        :meth:`list_file_infos` stays a pure listing stream. Shuffle, when
        requested, materializes that stream and permutes whole files so
        metadata-aware subclasses (footer reads) and chunking both see the
        shuffled file order. Unshuffled listing stays streaming.
        """
        file_infos = self.list_file_infos(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
        )
        if shuffle_config is None:
            yield from file_infos
            return
        yield from _shuffle_file_infos(
            list(file_infos), seed=shuffle_config.get_seed(execution_idx)
        )

    def _get_file_info_iterator(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
        preserve_order: bool,
    ) -> Iterable[FileInfo]:
        """Threaded (work-stealing) traversal when ``num_workers > 1``, else
        sequential. Shared by :meth:`list_files` and :meth:`list_file_infos`."""
        if self._num_workers > 1:
            return self._get_file_info_iterator_threaded(
                paths, filesystem, preserve_order
            )
        return self._get_file_info_iterator_sequential(paths, filesystem)

    def _get_file_info_iterator_sequential(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
    ) -> Iterable[FileInfo]:
        for input_path in paths.to_pylist():
            resolved_paths, _ = _resolve_paths_and_filesystem(input_path, filesystem)
            assert len(resolved_paths) == 1

            for path, file_size in _get_file_infos(
                resolved_paths[0],
                filesystem,
                self._ignore_missing_paths,
                self._skip_paths,
            ):
                yield FileInfo(path=path, size=file_size)

    def _get_file_info_iterator_threaded(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Threaded file info iterator with work stealing for parallel directory
        traversal. Subdirectories are added as work items for idle workers to
        process."""
        paths_list = paths.to_pylist()
        if len(paths_list) == 0:
            return

        num_workers = self._num_workers

        seed_items = [
            _TraversalWorkItem(
                path=p,
                is_discovered_subdir=False,
                input_path_index=i if preserve_order else None,
            )
            for i, p in enumerate(paths_list)
        ]

        def process_fn(
            item: _TraversalWorkItem,
            add_work: Callable[[_TraversalWorkItem], None],
            add_result: Callable[[Union[OrderedFileResult, FileInfo]], None],
        ) -> None:
            """Process a single item, adding discovered subdirs as work and
            files as results."""
            input_path_index = item.input_path_index

            if item.is_discovered_subdir:
                # Subdir paths from filesystem listing: use directly. Re-resolution
                # would infer LocalFileSystem for scheme-less paths on S3/GCS,
                # and add redundant overhead.
                path = item.path
                root_path = item.root_path
            else:
                # Seed paths from user: resolve once to get normalized path + fs.
                resolved_paths, _ = _resolve_paths_and_filesystem(item.path, filesystem)
                assert len(resolved_paths) == 1
                path = resolved_paths[0]
                root_path = path

            contents = _get_path_contents(
                path,
                filesystem,
                self._ignore_missing_paths,
                self._skip_paths,
                root_path=root_path,
            )
            for file_path, file_size in contents.files:
                file_info_result = FileInfo(path=file_path, size=file_size)
                if preserve_order:
                    add_result(
                        OrderedFileResult(
                            input_path_index=input_path_index,
                            file_path=file_path,
                            file_info=file_info_result,
                        )
                    )
                else:
                    add_result(file_info_result)
            for subdir_path in contents.subdirs:
                add_work(
                    _TraversalWorkItem(
                        path=subdir_path,
                        is_discovered_subdir=True,
                        input_path_index=input_path_index,
                        root_path=root_path,
                    )
                )

        def _ordered_result_key(result: OrderedFileResult) -> Tuple[int, str]:
            return (result.input_path_index, result.file_path)

        if preserve_order:
            for result in parallel_process_work_stealing(
                seed_items=seed_items,
                process_fn=process_fn,
                num_workers=num_workers,
                preserve_order=True,
                order_key=_ordered_result_key,
            ):
                # Ordered mode returns `OrderedFileResult` for sorting, so unwrap.
                yield result.file_info
        else:
            yield from parallel_process_work_stealing(
                seed_items=seed_items,
                process_fn=process_fn,
                num_workers=num_workers,
            )

    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Yield pruned, non-empty ``FileInfo``\\ s (path + on-disk size).

        The raw file-info stream that :meth:`list_files` (via
        :meth:`_iter_file_infos_for_list`) optionally shuffles, then chunks
        into manifests. The footer-based Parquet path consumes this same
        stream -- it reads each file's footer and bin-packs row groups
        itself, so it needs paths + sizes rather than pre-chunked manifest
        rows. Zero-size files are skipped and ``pruners`` (file-extension /
        partition filters) are applied here, so both listing paths share one
        filtering point.
        """
        pruners = pruners or []
        file_info_iterator = self._get_file_info_iterator(
            paths, filesystem, preserve_order
        )
        for file_info in file_info_iterator:
            if file_info.size is None or file_info.size == 0:
                logger.warning(f"Skipping zero-size file: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info

    def _process_file_infos_to_manifests(
        self,
        file_infos: Iterable[FileInfo],
    ) -> Iterable[FileManifest]:
        # ``file_infos`` are already filtered (zero-size skipped, pruners applied)
        # by ``list_file_infos``; this method only chunks them into manifests.
        running_paths: List[str] = []
        running_file_sizes: List[int] = []
        running_chunk_metadatas: List[Optional[ChunkMetadata]] = []
        manifests_count = 0
        chunks_count = 0

        for file_info in file_infos:
            # ``list_file_infos`` already dropped zero/None-size files.
            assert file_info.size is not None
            path, file_size = file_info.path, file_info.size

            # Drive the chunker once per file; emit one manifest row per chunk.
            # ``chunk_metadata`` is ``None`` for whole-file chunks (the default
            # ``WholeFileChunker`` behavior).
            for (
                chunk_metadata,
                chunk_size,
            ) in self._file_chunker.generate_chunk_metadatas(path, file_size):
                running_paths.append(path)
                running_file_sizes.append(chunk_size)
                running_chunk_metadatas.append(chunk_metadata)
                chunks_count += 1

                if len(running_paths) >= self._max_paths_per_output:
                    manifests_count += 1
                    yield FileManifest.construct_manifest(
                        paths=running_paths,
                        sizes=running_file_sizes,
                        chunk_metadatas=running_chunk_metadatas,
                    )
                    running_paths = []
                    running_file_sizes = []
                    running_chunk_metadatas = []

        if running_paths:
            manifests_count += 1
            yield FileManifest.construct_manifest(
                paths=running_paths,
                sizes=running_file_sizes,
                chunk_metadatas=running_chunk_metadatas,
            )

        logger.debug(
            f"Listing files: constructed {manifests_count} manifests "
            f"with {chunks_count} file chunks"
        )
