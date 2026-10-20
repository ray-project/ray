"""Read-task cuts that keep each MCAP file in order.

The listing hands the partitioner all of a file's chunk rows in one manifest
(``MCAPSummaryIndexer``). ``OrderedFileCutter`` decides where a file may be cut
before ``OnlineBinPacker`` packs anything:

- A light file, whose rows weigh no more than one read task's budget, goes to
  the packer as one run of all its chunks, so it is never split.
- A heavy file is cut here, and its tasks leave at once, in order. A task
  closes at the last allowed boundary before the chunk that would take it past
  the budget. Without one, it runs on to the next allowed boundary.
- A file the cutter cannot place in time goes to the packer unchanged. Each of
  its rows must name one chunk and carry that chunk's ``ChunkIndex`` record, so a
  file listed whole, an unreadable file and rows without such a record pass
  through as listed.

With ``log_time_order``, a heavy file's chunks are taken in order of their
first log time. When the read keeps its blocks in order
(``DataContext.execution_options.preserve_order``), a cut is allowed only where
every chunk before it ends no later than every chunk after it starts, and on a
tied log time lies earlier in the file. Each task
merges its chunks by log time (``mcap_chunks``), so the file comes out in
log-time order from start to end. In a recording with one writer, every
boundary between chunks allows a cut, so tasks fill as far as the budget lets
them. Where chunks overlap in time, as when cameras and robot state are written
to chunks of their own, the overlapping stretch stays in one task, which may
pass the budget. A file with no allowed boundary is read by one task, as the
legacy reader reads every file. A read that does not keep order gets its blocks
in any order anyway, so there every boundary allows a cut.

Without ``log_time_order``, chunks are taken in file order, and every boundary
allows a cut.
"""

import logging
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterator, List, Optional, Sequence

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    CHUNK_INDEX_COLUMN,
    chunk_log_times,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.util import MiB
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Chunk:
    """One chunk row of a file: its listing run and its messages' log times."""

    run: FileChunk
    start_time: int
    end_time: int

    @property
    def offset(self) -> int:
        (offset,) = self.run.unit_ids
        return offset


@dataclass(frozen=True)
class _FileRows:
    """One file's listing rows. ``chunks`` is empty when they cannot be placed in time."""

    path: str
    size: int
    rows: FileManifest
    chunks: List[_Chunk]


class OrderedFileCutter(FilePartitioner):
    """Cuts heavy files into ordered read tasks and packs light files whole.

    ``packer`` is an ``OnlineBinPacker`` with bins of ``budget`` bytes. It packs
    light files and whole-file rows, and heavy files' tasks bypass it. See the
    module docstring for where a heavy file is cut. ``reserved_memory`` is the
    memory each read task reserves (``read_mcap(memory=...)``), if the read sets it.
    """

    def __init__(
        self,
        packer: FilePartitioner,
        *,
        budget: int,
        log_time_order: bool,
        reserved_memory: Optional[float] = None,
    ):
        self._packer = packer
        self._budget = budget
        self._log_time_order = log_time_order
        self._reserved_memory = reserved_memory
        # Whether a cut must keep log-time order; set per input from the read.
        self._safe_cuts_only = log_time_order
        # Heavy files' tasks, in order, handed out ahead of the packer's bins.
        self._tasks: Deque[FileManifest] = deque()

    @property
    def requires_global_input(self) -> bool:
        return self._packer.requires_global_input

    def add_input(self, input_manifest: FileManifest) -> None:
        self._safe_cuts_only = self._log_time_order and _read_keeps_order()
        for file_rows in _rows_by_file(input_manifest):
            self._place(file_rows)

    def has_partition(self) -> bool:
        return bool(self._tasks) or self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        if self._tasks:
            return self._tasks.popleft()
        return self._packer.next_partition()

    def finalize(self) -> None:
        self._packer.finalize()

    def _place(self, file_rows: _FileRows) -> None:
        """Hand one file to the packer, or cut it into tasks of its own."""
        if not file_rows.chunks:
            self._packer.add_input(file_rows.rows)
            return
        chunks = self._in_read_order(file_rows.chunks)
        if sum(chunk.run.size_bytes for chunk in chunks) <= self._budget:
            self._packer.add_input(_task_manifest(file_rows, chunks))
            return
        tasks = list(self._cut(chunks))
        if self._safe_cuts_only:
            _warn_if_merge_holds_much(
                file_rows.path, tasks, self._budget, self._reserved_memory
            )
        for task in tasks:
            self._tasks.append(_task_manifest(file_rows, task))

    def _in_read_order(self, chunks: List[_Chunk]) -> List[_Chunk]:
        """``chunks`` in the order the reader yields their messages."""
        if self._log_time_order:
            return sorted(chunks, key=lambda c: (c.start_time, c.offset))
        return sorted(chunks, key=lambda c: c.offset)

    def _cut(self, chunks: Sequence[_Chunk]) -> Iterator[Sequence[_Chunk]]:
        """Split ``chunks`` into consecutive tasks."""
        start = 0
        while start < len(chunks):
            end = self._task_end(chunks, start)
            yield chunks[start:end]
            start = end

    def _task_end(self, chunks: Sequence[_Chunk], start: int) -> int:
        """The exclusive end of the task that begins at ``chunks[start]``.

        The task ends at the last allowed boundary before the chunk that would
        take it past the budget. Without one, it runs on to the first allowed
        boundary after that chunk, or to the file's last chunk.
        """
        size = chunks[start].run.size_bytes
        latest_end = chunks[start].end_time
        # The largest byte offset among the task's chunks that end at
        # ``latest_end``.
        tied_offset = chunks[start].offset
        last_cut: Optional[int] = None
        for i in range(start + 1, len(chunks)):
            chunk = chunks[i]
            may_cut = self._may_cut(latest_end, tied_offset, chunk)
            if size + chunk.run.size_bytes > self._budget:
                if may_cut:
                    return i
                if last_cut is not None:
                    return last_cut
            elif may_cut:
                last_cut = i
            size += chunk.run.size_bytes
            if chunk.end_time > latest_end:
                latest_end, tied_offset = chunk.end_time, chunk.offset
            elif chunk.end_time == latest_end:
                tied_offset = max(tied_offset, chunk.offset)
        return len(chunks)

    def _may_cut(self, latest_end: int, tied_offset: int, next_chunk: _Chunk) -> bool:
        """Whether a cut before ``next_chunk`` is allowed.

        ``latest_end`` is the latest log time in the task so far, and
        ``tied_offset`` the largest byte offset of its chunks that end then. A
        cut must keep log-time order only when the read keeps its blocks in
        order; in file order every boundary keeps the order. The merge yields
        equal log times in file order, so a cut on a tied log time is allowed
        only when the chunks before it that end then lie earlier in the file.
        """
        if not self._safe_cuts_only:
            return True
        if latest_end != next_chunk.start_time:
            return latest_end < next_chunk.start_time
        return tied_offset < next_chunk.offset


def _read_keeps_order() -> bool:
    """Whether the running read keeps its blocks in order.

    Read from the ``DataContext`` the listing task runs under, which carries the
    dataset's execution options.
    """
    from ray.data.context import DataContext

    return DataContext.get_current().execution_options.preserve_order


def _rows_by_file(manifest: FileManifest) -> Iterator[_FileRows]:
    """The manifest's rows grouped by file, in listing order."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    records: List[Optional[bytes]] = (
        block.column(CHUNK_INDEX_COLUMN).to_pylist()
        if CHUNK_INDEX_COLUMN in block.column_names
        else [None] * len(manifest)
    )
    rows_by_path: Dict[str, List[int]] = {}
    for i, path in enumerate(manifest.paths):
        rows_by_path.setdefault(str(path), []).append(i)
    for path, rows in rows_by_path.items():
        yield _FileRows(
            path=path,
            size=int(manifest.file_sizes[rows[0]]),
            rows=FileManifest(block.take(rows)),
            chunks=_chunks(manifest, records, rows),
        )


def _chunks(
    manifest: FileManifest, records: List[Optional[bytes]], rows: List[int]
) -> List[_Chunk]:
    """The chunks that ``rows`` name, or none when a row cannot be placed in time.

    A row is placed in time when it names one chunk and carries that chunk's
    ``ChunkIndex`` record (see the module docstring).
    """
    chunks = []
    for i in rows:
        metadata = manifest.file_chunk_metadatas[i]
        times = chunk_log_times(records[i])
        if metadata is None or len(metadata["unit_ids"]) != 1 or times is None:
            return []
        chunks.append(_Chunk(FileChunk.from_metadata(metadata), *times))
    return chunks


def _task_manifest(file_rows: _FileRows, chunks: Sequence[_Chunk]) -> FileManifest:
    """One row naming ``chunks`` of the file, as the packer emits its rows.

    The packer keeps a run of several chunks whole, since MCAP's packer splits
    no run (``split_coalesced`` is off).
    """
    runs = [chunk.run for chunk in chunks]
    run = FileChunk(
        unit_ids=tuple(sorted(chunk.offset for chunk in chunks)),
        num_rows=sum(r.num_rows for r in runs),
        size_bytes=sum(r.size_bytes for r in runs),
        fully_matched=all(r.fully_matched for r in runs),
    )
    return FileManifest.construct_manifest(
        paths=[file_rows.path],
        sizes=[file_rows.size],
        chunk_metadatas=[run.to_metadata()],
    )


# An ordered task whose log-time merge would hold more than this many read-task
# budgets of overlapping chunks at once logs a warning.
_MERGE_WARNING_BUDGETS = 4


def _warn_if_merge_holds_much(
    path: str,
    tasks: Sequence[Sequence[_Chunk]],
    budget: int,
    reserved_memory: Optional[float],
) -> None:
    """Warn once per process when a task of ``path`` holds many budgets at once.

    In a read that keeps order, overlapping chunks share a task, and the task's
    log-time merge holds every chunk that overlaps the current log time
    (``mcap_chunks``). A file whose chunks all overlap is then held whole. The
    warning asks for a memory reservation, so it stays quiet when each read task
    already reserves at least what the merge would hold.
    """
    held = max(_merge_held_bytes(task) for task in tasks)
    if held <= _MERGE_WARNING_BUDGETS * budget:
        return
    if reserved_memory is not None and reserved_memory >= held:
        return
    if log_once("read_mcap_merge_holds_much"):
        logger.warning(
            "A read task of %s holds about %.0f MiB of messages at once, against "
            "the %.0f MiB a read task is sized for. The file's chunks overlap in "
            "time and the read keeps order (`preserve_order`), so the task can't "
            "be cut smaller. If read tasks run out of memory, reserve at least that "
            "much for each with `read_mcap(..., memory=...)`, or read without "
            "`preserve_order` so the file can be split.",
            path,
            held / MiB,
            budget / MiB,
        )


def _merge_held_bytes(chunks: Sequence[_Chunk]) -> int:
    """The most chunk weight the log-time merge holds at once for ``chunks``,
    counted only while two or more of them overlap.

    The merge expands a chunk at its first log time and holds its messages
    until its last one is yielded. On a tied log time it expands first. One
    chunk alone does not count, since no cut could make its task smaller.
    """
    events = sorted(
        [(chunk.start_time, 0, chunk.run.size_bytes) for chunk in chunks]
        + [(chunk.end_time, 1, chunk.run.size_bytes) for chunk in chunks]
    )
    held = open_chunks = peak = 0
    for _, is_end, size in events:
        if is_end:
            held -= size
            open_chunks -= 1
        else:
            held += size
            open_chunks += 1
            if open_chunks > 1:
                peak = max(peak, held)
    return peak
