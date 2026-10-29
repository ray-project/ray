"""Read-task grouping that keeps each MCAP file in order.

The listing hands the partitioner all of a file's chunk rows in one manifest
(``MCAPSummaryIndexer``). ``OnlineBinPacker`` packs those rows chunk by chunk,
so a file can be split across tasks, and the tasks can finish or be sent in any
order.

When the read keeps its blocks in order
(``DataContext.execution_options.preserve_order``), ``OrderedFileGrouper``
packs whole files into tasks itself, in listing order, so files come out in the
order they were listed. Each file is read by one task, which merges its chunks
by log time (``mcap_chunks``). A file heavier than a read task's budget then
gets a task of its own and holds that much memory, as the legacy reader does.
Otherwise the rows go to the packer as listed.
"""

from collections import deque
from typing import Deque, Dict, Iterator, List

from ray.data._internal.datasource_v2.common.online_bin_packer import _bin_items
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
)


class OrderedFileGrouper(FilePartitioner):
    """Keeps each file in one read task, in listing order, when the read keeps
    order.

    ``packer`` is an ``OnlineBinPacker`` with tasks of ``budget`` bytes, which
    packs the rows of a read that does not keep order. See the module docstring.
    """

    def __init__(self, packer: FilePartitioner, *, budget: int):
        self._packer = packer
        self._ordered = _TasksInListingOrder(budget)

    @property
    def requires_global_input(self) -> bool:
        return self._packer.requires_global_input

    def add_input(self, input_manifest: FileManifest) -> None:
        if not _read_keeps_order():
            self._packer.add_input(input_manifest)
            return
        for file_rows in _rows_by_file(input_manifest):
            self._ordered.add(_whole_file(file_rows))

    def has_partition(self) -> bool:
        return bool(self._ordered.tasks) or self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        if self._ordered.tasks:
            return self._ordered.tasks.popleft()
        return self._packer.next_partition()

    def finalize(self) -> None:
        self._ordered.seal()
        self._packer.finalize()


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


def _rows_by_file(manifest: FileManifest) -> Iterator[FileManifest]:
    """The manifest's rows grouped by file, in listing order."""
    rows_by_path: Dict[str, List[int]] = {}
    for i, path in enumerate(manifest.paths):
        rows_by_path.setdefault(str(path), []).append(i)
    block = manifest.as_block()
    for rows in rows_by_path.values():
        yield FileManifest(block.take(rows))


def _whole_file(file_rows: FileManifest) -> FileManifest:
    """One row naming every chunk of the file, or the rows unchanged.

    A file listed whole, or an unreadable file, has a row that names no chunk.
    Such rows already cover the whole file, so they pass through as listed.
    """
    metadatas = file_rows.file_chunk_metadatas
    if any(md is None or len(md["unit_ids"]) == 0 for md in metadatas):
        return file_rows
    runs = [FileChunk.from_metadata(md) for md in metadatas]
    run = FileChunk(
        unit_ids=tuple(sorted(u for r in runs for u in r.unit_ids)),
        num_rows=sum(r.num_rows for r in runs),
        size_bytes=sum(r.size_bytes for r in runs),
        fully_matched=all(r.fully_matched for r in runs),
    )
    return FileManifest.construct_manifest(
        paths=[file_rows.paths[0]],
        sizes=[file_rows.file_sizes[0]],
        chunk_metadatas=[run.to_metadata()],
    )
