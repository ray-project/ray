"""Read-task cuts that keep each MCAP file in order.

The listing hands the partitioner all of a file's chunk rows in one manifest
(``MCAPSummaryIndexer``). ``OrderedFileCutter`` decides where a file may be cut
before ``OnlineBinPacker`` packs anything:

- A light file, whose rows weigh no more than one read task's budget, goes to
  the packer as one run of all its chunks, so it is never split.
- A heavy file is cut here, and its tasks leave at once, in order. A task
  closes before the chunk that would take it past the budget.
- A file the cutter cannot place in time goes to the packer unchanged. Those
  are a file listed whole and an unreadable file, whose rows carry no
  ``ChunkIndex`` record.

When the read keeps its blocks in order
(``DataContext.execution_options.preserve_order``), the cutter packs light
files itself instead of the packer, in listing order, and a heavy file's tasks
leave only after the files listed before it. Files then come out in the order
they were listed.

With ``log_time_order``, a heavy file's chunks are taken in order of their
first log time. When the read also keeps its blocks in order, a heavy file is
not cut: it is packed whole like a light file and gets a task of its own. That
task merges the file's chunks by log time (``mcap_chunks``), so the file comes
out in log-time order from start to end.

Without ``log_time_order``, chunks are taken in file order.
"""

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterator, List, Optional, Sequence

import pyarrow as pa

from ray.data._internal.datasource_v2.common.online_bin_packer import _bin_items
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    CHUNK_INDEX_COLUMN,
    chunk_start_time,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
)


@dataclass(frozen=True)
class _Chunk:
    """One chunk row of a file: its listing run and its first message's log time."""

    run: FileChunk
    start_time: int

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
    """Cuts heavy files into read tasks and packs light files whole.

    ``packer`` is an ``OnlineBinPacker`` with bins of ``budget`` bytes. It packs
    light files and whole-file rows of a read that does not keep order, and
    heavy files' tasks bypass it. See the module docstring for when a heavy
    file is cut.
    """

    def __init__(self, packer: FilePartitioner, *, budget: int, log_time_order: bool):
        self._packer = packer
        self._budget = budget
        self._log_time_order = log_time_order
        # Whether the read keeps its blocks in order, and whether it reads each
        # file whole to keep log-time order; set per input from the read.
        self._keeps_order = False
        self._whole_files = False
        # Heavy files' tasks, and every file when the read keeps order, in
        # order, handed out ahead of the packer's bins.
        self._ordered = _TasksInListingOrder(budget)

    @property
    def requires_global_input(self) -> bool:
        return self._packer.requires_global_input

    def add_input(self, input_manifest: FileManifest) -> None:
        self._keeps_order = _read_keeps_order()
        self._whole_files = self._log_time_order and self._keeps_order
        for file_rows in _rows_by_file(input_manifest):
            self._place(file_rows)

    def has_partition(self) -> bool:
        return bool(self._ordered.tasks) or self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        if self._ordered.tasks:
            return self._ordered.tasks.popleft()
        return self._packer.next_partition()

    def finalize(self) -> None:
        self._ordered.seal()
        self._packer.finalize()

    def _place(self, file_rows: _FileRows) -> None:
        """Pack one file with others, or cut it into tasks of its own."""
        if not file_rows.chunks:
            self._pack(file_rows.rows)
            return
        chunks = self._in_read_order(file_rows.chunks)
        weight = sum(chunk.run.size_bytes for chunk in chunks)
        if self._whole_files or weight <= self._budget:
            self._pack(_task_manifest(file_rows, chunks))
            return
        self._ordered.seal()
        for task in self._cut(chunks):
            self._ordered.tasks.append(_task_manifest(file_rows, task))

    def _pack(self, rows: FileManifest) -> None:
        """Pack ``rows`` with other files, in listing order if the read keeps order."""
        if self._keeps_order:
            self._ordered.add(rows)
        else:
            self._packer.add_input(rows)

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
        """The exclusive end of the task that begins at ``chunks[start]``: the
        chunk that would take it past the budget, or the file's end."""
        size = chunks[start].run.size_bytes
        for i in range(start + 1, len(chunks)):
            size += chunks[i].run.size_bytes
            if size > self._budget:
                return i
        return len(chunks)


class _TasksInListingOrder:
    """Packs files into read tasks without reordering them.

    A file joins the open task while the task stays within ``budget``.
    Otherwise the open task leaves first. A file of at least ``budget`` bytes
    leaves in a task of its own. Rows weigh what ``OnlineBinPacker`` budgets.
    """

    def __init__(self, budget: int):
        self._budget = budget
        self._open: List[FileManifest] = []
        self._open_bytes = 0
        # Sealed tasks, in listing order.
        self.tasks: Deque[FileManifest] = deque()

    def add(self, file_rows: FileManifest) -> None:
        """Add one file's rows, in listing order."""
        weight = sum(item.size_bytes for item in _bin_items(file_rows))
        if self._open_bytes + weight > self._budget:
            self.seal()
        self._open.append(file_rows)
        self._open_bytes += weight
        if self._open_bytes >= self._budget:
            self.seal()

    def seal(self) -> None:
        """Close the open task, if it holds a file.

        The task keeps only each row's path, size and chunks, as the packer's
        tasks do.
        """
        if self._open:
            self.tasks.append(
                FileManifest.construct_manifest(
                    paths=[str(p) for rows in self._open for p in rows.paths],
                    sizes=[int(s) for rows in self._open for s in rows.file_sizes],
                    chunk_metadatas=[
                        md for rows in self._open for md in rows.file_chunk_metadatas
                    ],
                )
            )
            self._open, self._open_bytes = [], 0


def _read_keeps_order() -> bool:
    """Whether the running read keeps its blocks in order.

    Read from the ``DataContext`` the listing task runs under, which carries the
    dataset's execution options.
    """
    from ray.data.context import DataContext

    return DataContext.get_current().execution_options.preserve_order


def _rows_by_file(manifest: FileManifest) -> Iterator[_FileRows]:
    """The manifest's rows grouped by file, in listing order.

    A listing manifest holds one listed file, so a path listed twice arrives as
    two manifests and its copies stay apart.
    """
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    records: List[Optional[bytes]] = block.column(CHUNK_INDEX_COLUMN).to_pylist()
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

    A row is placed in time when it carries its chunk's ``ChunkIndex`` record
    (see the module docstring).
    """
    chunks = []
    for i in rows:
        metadata = manifest.file_chunk_metadatas[i]
        start_time = chunk_start_time(records[i])
        if metadata is None or start_time is None:
            return []
        chunks.append(_Chunk(FileChunk.from_metadata(metadata), start_time))
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
