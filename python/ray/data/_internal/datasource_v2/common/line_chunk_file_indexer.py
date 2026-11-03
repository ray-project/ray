"""Listing for CSV and JSONL that splits large files into byte-range chunks.

Purpose: produce the listing rows (``FileManifest``) that the bin packer
groups into read tasks, so a file bigger than one task's budget is spread
over several tasks instead of read by one.

How it works: :class:`LineChunkFileIndexer` reuses ``NonSamplingFileIndexer``
for path expansion, extension filtering and file sizes, and replaces only
the step that turns file infos into manifest rows. A file becomes either one
row with no chunk metadata, meaning the whole file, or one row per
``chunk_size`` bytes from ``split_into_line_chunks``. Each chunk row repeats
the path and file size and carries a ``FileChunk`` whose single unit id is
the chunk's index; the reader recomputes the byte range from that index.

Design decisions:

- No file bytes are read. Chunks come from the size alone, so listing costs
  the same as listing whole files.
- ``chunk_size`` should equal the packer's bin size. Every full chunk then
  fills a bin by itself; only a file's last, shorter chunk shares a bin.
- A file stays whole when it fits in one chunk, since splitting gains
  nothing, or when its extension marks it compressed, since a compressed
  stream can't be decoded from the middle.
- ``num_rows`` is 0 and ``fully_matched`` is False on every chunk. The row
  count is unknown without reading, and these values tell limit push-down
  not to rely on it.
"""

from typing import Iterable, List, Optional

from typing_extensions import override

from ray.data._internal.datasource_v2.common.line_chunking import (
    split_into_line_chunks,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
    FileManifest,
)
from ray.data._internal.util import infer_compression


class LineChunkFileIndexer(NonSamplingFileIndexer):
    """Lists newline-delimited files (CSV, JSONL) as byte-range chunks.

    Use this indexer only when every newline ends a row. Keep
    ``NonSamplingFileIndexer`` for CSV read with ``newlines_in_values=True``,
    and when ``arrow_open_stream_args`` forces a compression codec the file
    extension doesn't show.
    """

    def __init__(
        self,
        *,
        chunk_size: int,
        ignore_missing_paths: bool,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
    ):
        """Initialize the indexer.

        Args:
            chunk_size: Bytes per chunk. Set it to the partitioner's bin size so
                each chunk fills one read task.
            ignore_missing_paths: Skip paths that don't exist instead of raising.
            skip_paths: Resolved paths to leave out of the listing.
            num_workers: Threads for directory traversal.
            max_paths_per_output: Manifest rows per listing output.
        """
        super().__init__(
            ignore_missing_paths=ignore_missing_paths,
            skip_paths=skip_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )
        assert chunk_size > 0, f"chunk_size must be positive, got {chunk_size}"
        self._chunk_size = chunk_size

    @override
    def _process_file_infos_to_manifests(
        self,
        file_infos: Iterable[FileInfo],
    ) -> Iterable[FileManifest]:
        paths: List[str] = []
        sizes: List[int] = []
        chunk_metadatas: List[Optional[ChunkMetadata]] = []

        for file_info in file_infos:
            # ``list_file_infos`` already dropped zero/None-size files.
            assert file_info.size is not None
            if (
                file_info.size <= self._chunk_size
                or infer_compression(file_info.path) is not None
            ):
                chunk_metadatas_of_file = [None]
            else:
                chunk_metadatas_of_file = [
                    chunk.to_metadata()
                    for chunk in split_into_line_chunks(
                        file_size=file_info.size, chunk_size=self._chunk_size
                    )
                ]
            for chunk_metadata in chunk_metadatas_of_file:
                paths.append(file_info.path)
                sizes.append(file_info.size)
                chunk_metadatas.append(chunk_metadata)

            if len(paths) >= self._max_paths_per_output:
                yield FileManifest.construct_manifest(
                    paths=paths, sizes=sizes, chunk_metadatas=chunk_metadatas
                )
                paths, sizes, chunk_metadatas = [], [], []

        if paths:
            yield FileManifest.construct_manifest(
                paths=paths, sizes=sizes, chunk_metadatas=chunk_metadatas
            )
