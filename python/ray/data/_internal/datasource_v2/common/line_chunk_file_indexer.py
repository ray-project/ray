"""Listing for CSV and JSONL that splits large files into byte-range chunks.

Purpose: produce the listing rows (``FileManifest``) that the bin packer
groups into read tasks, so a file bigger than one task's budget is spread
over several tasks instead of read by one.

How it works: :class:`LineChunkFileIndexer` reuses ``NonSamplingFileIndexer``
for path expansion, extension filtering, file sizes and grouping rows into
manifests, and overrides only ``_chunk_metadatas_of_file``, which says how
many rows a file gets. A file becomes either one row with no chunk
metadata, meaning the whole file, or one row per ``chunk_size`` bytes from
``split_into_line_chunks``. Each chunk row repeats the path and file size
and carries a ``FileChunk`` whose single unit id is the chunk's index; the
reader recomputes the byte range from that index.

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
from ray.data._internal.datasource_v2.interfaces.file_manifest import ChunkMetadata
from ray.data._internal.util import NumBytes, infer_compression


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
        chunk_size: NumBytes,
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
    def _chunk_metadatas_of_file(
        self, file_info: FileInfo
    ) -> List[Optional[ChunkMetadata]]:
        assert file_info.size is not None
        if file_info.size <= self._chunk_size or _is_compressed(file_info.path):
            return [None]
        return [
            chunk.to_metadata()
            for chunk in split_into_line_chunks(
                file_size=file_info.size, chunk_size=self._chunk_size
            )
        ]


def _is_compressed(path: str) -> bool:
    # ``infer_compression`` misses upper-case extensions and ``.br``.
    lowered = path.lower()
    return infer_compression(lowered) is not None or lowered.endswith(".br")
