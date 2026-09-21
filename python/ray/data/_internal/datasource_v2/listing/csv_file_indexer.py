"""CSV-aware file indexer for DataSourceV2."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    LineDelimitedFileChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    FileInfo,
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.csv_file_reader import (
    _find_record_boundary,
    _is_record_boundary,
)
from ray.data._internal.util import MiB, infer_compression

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


class RecordAlignedCSVFileIndexer(NonSamplingFileIndexer):
    """Lists CSV files and splits large uncompressed ones at record boundaries.

    Inherits directory traversal from :class:`NonSamplingFileIndexer` and
    overrides :meth:`list_files` to emit one manifest row per aligned byte
    range, the way ``FooterFileIndexer`` emits one row per Parquet row-group
    run. Each nominal ``chunk_byte_size`` boundary is pushed to the first byte
    after the next record terminator, so every record belongs to exactly one
    row. The reader aligns again before parsing, so this pass is not required
    for correctness; it runs here so that a record spanning many nominal chunks
    is discovered once per file instead of once per read task, and so the
    partitioner weighs each chunk by its real byte size.

    With ``split_files=False`` the indexer behaves like its base class: one row
    per file, no file I/O while listing.
    """

    _DEFAULT_CHUNK_BYTE_SIZE = 256 * MiB

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        filesystem: Optional["FileSystem"] = None,
        split_files: bool = True,
        chunk_byte_size: Optional[int] = None,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
    ):
        super().__init__(
            ignore_missing_paths=ignore_missing_paths,
            skip_paths=skip_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )
        self._filesystem = filesystem
        self._split_files = split_files
        self._chunk_byte_size = (
            chunk_byte_size
            if chunk_byte_size is not None
            else self._DEFAULT_CHUNK_BYTE_SIZE
        )
        if self._chunk_byte_size < 1:
            raise ValueError("chunk_byte_size must be at least 1")

    @property
    def splits_files(self) -> bool:
        """Whether large uncompressed files become several manifest rows."""
        return self._split_files

    @property
    def requires_file_io(self) -> bool:
        return self._split_files

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
    ) -> Iterable[FileManifest]:
        file_infos = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
        )
        if not self._split_files:
            yield from self._process_file_infos_to_manifests(file_infos)
            return
        yield from self._chunk_file_infos_to_manifests(file_infos, filesystem)

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        """Yield ``(chunk_metadata, chunk_size)`` for one file.

        ``None`` metadata means the whole file is read sequentially. That is
        the answer for compressed input, files that fit in one chunk, and
        filesystems without random access.
        """
        if not self._split_files or infer_compression(path) is not None:
            yield None, file_size
            return
        if file_size <= self._chunk_byte_size:
            # Whole-file reads don't need random access. In particular, small
            # files on stream-only filesystems must use open_input_stream.
            yield None, file_size
            return

        # Align the entire file in one forward pass. If one record spans many
        # nominal chunks, ``previous_end`` lets us discard every covered chunk
        # without rescanning the same tail once per read task.
        filesystem = filesystem or self._filesystem or LocalFileSystem()
        try:
            file = filesystem.open_input_file(path)
        except (pa.ArrowNotImplementedError, NotImplementedError):
            # Some custom filesystems only support sequential input streams.
            # Fall back before emitting any chunk so the reader can use
            # ``open_input_stream`` without losing or duplicating bytes.
            yield None, file_size
            return

        with file:
            actual_file_size = file.size()
            previous_end = 0
            nominal_start = 0
            while nominal_start < actual_file_size:
                raw_end = min(nominal_start + self._chunk_byte_size, actual_file_size)
                raw_start, nominal_start = nominal_start, raw_end
                if raw_end <= previous_end:
                    continue

                start = previous_end
                if raw_start > previous_end:
                    start = raw_start
                    if not _is_record_boundary(file, start, actual_file_size):
                        start = _find_record_boundary(file, start, actual_file_size)
                if raw_end <= start:
                    previous_end = start
                    continue

                end = raw_end
                if not _is_record_boundary(file, end, actual_file_size):
                    end = _find_record_boundary(file, end, actual_file_size)
                if start < end:
                    yield (
                        create_chunk_metadata(
                            LineDelimitedFileChunkMetadata,
                            chunk_byte_start_idx=start,
                            chunk_byte_end_idx=end,
                        ),
                        end - start,
                    )
                previous_end = end

    def _chunk_file_infos_to_manifests(
        self,
        file_infos: Iterable[FileInfo],
        filesystem: Optional["FileSystem"],
    ) -> Iterable[FileManifest]:
        running_paths: List[str] = []
        running_sizes: List[int] = []
        running_metadatas: List[Optional[ChunkMetadata]] = []
        manifests_count = 0
        chunks_count = 0

        def flush() -> FileManifest:
            nonlocal running_paths, running_sizes, running_metadatas
            manifest = FileManifest.construct_manifest(
                paths=running_paths,
                sizes=running_sizes,
                chunk_metadatas=running_metadatas,
            )
            running_paths, running_sizes, running_metadatas = [], [], []
            return manifest

        for file_info in file_infos:
            # ``list_file_infos`` already dropped zero/None-size files.
            assert file_info.size is not None
            for metadata, chunk_size in self.generate_chunk_metadatas(
                file_info.path, file_info.size, filesystem
            ):
                running_paths.append(file_info.path)
                running_sizes.append(chunk_size)
                running_metadatas.append(metadata)
                chunks_count += 1
                if len(running_paths) >= self._max_paths_per_output:
                    manifests_count += 1
                    yield flush()

        if running_paths:
            manifests_count += 1
            yield flush()

        logger.debug(
            f"Listing CSV files: constructed {manifests_count} manifests "
            f"with {chunks_count} chunks"
        )
