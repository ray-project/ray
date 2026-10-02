"""Reading an MCAP file's summary, and naming the pieces of a file.

An MCAP file ends with an optional summary section addressed from the footer:
the file's schemas and channels, one ``ChunkIndex`` per chunk (byte offset,
compressed and uncompressed size, log-time bounds, the channels inside) and a
``Statistics`` record (message counts per channel, overall time bounds). Two
seeks reach it, so the whole read plan can be made from a few KB per file
without touching a payload.

Format specification: https://mcap.dev/spec
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk

if TYPE_CHECKING:
    from mcap.records import ChunkIndex, Statistics
    from mcap.summary import Summary


def read_summary(filesystem: FileSystem, path: str) -> Optional["Summary"]:
    """Return the summary of the MCAP file at ``path``, or ``None`` if it has none.

    A recorder that crashed, or one that streams to a pipe, writes no summary.
    Such a file is still readable by a linear scan; it just cannot be planned.
    """
    from mcap.reader import SeekingReader

    with filesystem.open_input_file(path) as f:
        return SeekingReader(f).get_summary()


def chunk_unit_id(path: str, chunk_start_offset: int) -> str:
    """Stable ``ReadUnit.id`` of one chunk: the file plus the chunk's byte offset.

    The offset identifies a chunk for as long as the file is not rewritten, and
    the reader seeks straight to it, so no step after listing has to map an
    index back to a position. The indexer accepts the same id in
    ``excluded_read_unit_ids`` to leave that chunk out of a listing.
    """
    return f"{path}#c={chunk_start_offset}"


def topic_unit_id(path: str, topic: str) -> str:
    """Stable ``ReadUnit.id`` of one topic of a file, at topic granularity."""
    return f"{path}#t={topic}"


def message_row_id(path: str, chunk_start_offset: int, index_in_chunk: int) -> str:
    """Deterministic id of one message: its chunk, and its position in the chunk.

    ``index_in_chunk`` counts every message record of the chunk, selected or
    not, so the id of a message does not depend on which topics or time range
    the read selected.
    """
    return f"{path}#c={chunk_start_offset}:{index_in_chunk}"


def unindexed_message_row_id(path: str, ordinal: int) -> str:
    """Deterministic id of one message of a file read by linear scan.

    Without a chunk index there is no chunk to name, so the id is the message's
    ordinal among every message record of the file, in file order.
    """
    return f"{path}#m={ordinal}"


def estimate_chunk_rows(
    chunk_index: "ChunkIndex",
    statistics: Optional["Statistics"],
    total_uncompressed_size: int,
) -> int:
    """Estimate how many messages a chunk holds.

    A chunk index does not record a message count, only sizes; the file's
    total count sits in ``Statistics``. Spread it over the chunks in
    proportion to their uncompressed size. The estimate is only a hint for the
    partitioner, and the manifest marks it as inexact (``fully_matched=False``)
    so a pushed-down limit never trusts it.
    """
    if (
        statistics is None
        or statistics.message_count == 0
        or total_uncompressed_size <= 0
    ):
        return 0
    share = chunk_index.uncompressed_size / total_uncompressed_size
    return max(1, round(statistics.message_count * share))


def chunk_run(
    chunk_index: "ChunkIndex",
    statistics: Optional["Statistics"],
    total_uncompressed_size: int,
) -> FileChunk:
    """The listing row for one chunk.

    ``size_bytes`` is the chunk's uncompressed size: what a task decompresses
    into memory to read it, and therefore what a partitioner budgets on. A
    chunk that reports no size (a writer that did not fill the field) counts
    as one byte so the packer still sees it.
    """
    return FileChunk(
        unit_ids=(chunk_index.chunk_start_offset,),
        num_rows=estimate_chunk_rows(chunk_index, statistics, total_uncompressed_size),
        size_bytes=max(chunk_index.uncompressed_size, 1),
        fully_matched=False,
    )


def topic_run_metadata(
    chunk_indexes: "List[ChunkIndex]",
    topic: str,
    num_rows: int,
) -> Dict[str, Any]:
    """The listing row for one topic of a file, at topic granularity.

    Shaped like a :class:`FileChunk` (so a partitioner could weigh it) with the
    topic added: the reader needs to know which topic the row stands for, and
    at this granularity listing rows reach the reader unchanged.
    """
    run = FileChunk(
        unit_ids=tuple(c.chunk_start_offset for c in chunk_indexes),
        num_rows=num_rows,
        size_bytes=max(sum(c.uncompressed_size for c in chunk_indexes), 1),
        fully_matched=False,
    )
    metadata = run.to_metadata()
    metadata["topic"] = topic
    return metadata
