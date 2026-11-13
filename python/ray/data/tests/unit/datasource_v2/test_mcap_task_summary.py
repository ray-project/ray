"""Unit tests for carrying each read task's summary records in its manifest
(``mcap_task_summary``).

The listing, the partitioner and the reader are called directly, with no Ray
cluster. A read task must read no summary and return the same rows as a task
that reads the summary from the file.
"""

import dataclasses
import importlib.util
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_datasource_v2,
    mcap_reader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    parse_summary_records,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import TimeRange
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    FILE_RECORDS_COLUMN,
    TASK_SUMMARY_COLUMN,
    task_summaries,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.context import DataContext
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    CHUNKED_FILE_MESSAGES,
    STEP,
    list_manifests,
    pack,
    round_robin_messages,
    scanner_for,
    summary_of,
    without_summary_records,
    write_cut_short,
    write_mcap,
    write_mcap_without_summary_schemas,
    write_mixed_channels,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

# A read-task budget smaller than any chunk: every chunk gets a task of its own.
ONE_CHUNK_PER_TASK = 1


class ReadLog:
    """A local filesystem that records ``(offset, bytes)`` of every read, per path."""

    def __init__(self):
        self._filesystem = LocalFileSystem()
        self.reads: Dict[str, List[Tuple[int, int]]] = defaultdict(list)

    def open_input_file(self, path):
        return _LoggedFile(self._filesystem.open_input_file(path), self.reads[path])


class _LoggedFile:
    def __init__(self, f, reads: List[Tuple[int, int]]):
        self._f = f
        self._reads = reads

    def read(self, nbytes=-1):
        offset = self._f.tell()
        data = self._f.read(nbytes)
        if data:
            self._reads.append((offset, len(data)))
        return data

    def readinto(self, buffer):
        offset = self._f.tell()
        size = self._f.readinto(buffer)
        if size:
            self._reads.append((offset, size))
        return size

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._f.close()


def set_task_budget(monkeypatch, bin_bytes) -> None:
    """Pack read tasks of ``bin_bytes`` estimated bytes; ``None`` keeps the default."""
    if bin_bytes is not None:
        monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", bin_bytes)


def table_of(manifest: FileManifest) -> pa.Table:
    """The Arrow table behind ``manifest``."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return block


def without_task_summaries(task: FileManifest) -> FileManifest:
    """``task`` with null records, so the reader reads each summary from the file."""
    block = table_of(task)
    nulls = pa.nulls(len(task), type=block.schema.field(TASK_SUMMARY_COLUMN).type)
    index = block.column_names.index(TASK_SUMMARY_COLUMN)
    return FileManifest(block.set_column(index, TASK_SUMMARY_COLUMN, nulls))


def read_tasks(scanner, tasks: List[FileManifest]) -> pa.Table:
    """Read every task, in order, into one table."""
    tables = [table for task in tasks for table in scanner.create_reader().read(task)]
    return pa.concat_tables(tables)


def _write_overlapping(path):
    # Messages out of log-time order across chunks, so a log-time merge
    # interleaves the chunks of a task.
    times = [10, 30, 20, 50, 40, 60, 15, 35]
    write_mcap(
        path,
        [
            {"topic": "/a", "data": {"t": t}, "log_time": BASE_TIME + t * STEP}
            for t in times
        ],
        chunk_size=60,
    )


def _write_unindexed(path):
    write_mcap(path, round_robin_messages(9), index=False)


def _write_round_robin(path):
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))


WRITERS = {
    "one_message_chunks": _write_round_robin,
    "overlapping_chunks": _write_overlapping,
    "schemas_only_in_chunks": write_mcap_without_summary_schemas,
    "mixed_channels": write_mixed_channels,
    "unindexed": _write_unindexed,
}
READ_OPTIONS = {
    "default": {},
    "topics_and_time": {
        "topics": ["/a", "/c"],
        "time_range": TimeRange(BASE_TIME + 2 * STEP, BASE_TIME + 30 * STEP),
    },
}


def chunk_records(path: str) -> List[Tuple[int, int]]:
    """``(offset, bytes)`` of every chunk record of ``path``, in file order."""
    return [
        (c.chunk_start_offset, c.chunk_length) for c in summary_of(path).chunk_indexes
    ]


def whole_file(path: str) -> List[Tuple[int, int]]:
    """``(offset, bytes)`` of one read of all of ``path``."""
    return [(0, os.path.getsize(path))]


# Each file's writer, and the reads its tasks make with a task per chunk.
TASK_READS = {
    "chunked": (_write_round_robin, chunk_records),
    # A file without a chunk index is scanned from its start, in one buffered read.
    "unindexed": (_write_unindexed, whole_file),
}


@pytest.mark.parametrize(
    "writer, expected_reads", TASK_READS.values(), ids=TASK_READS.keys()
)
def test_read_tasks_read_only_their_own_chunks(
    tmp_path, monkeypatch, writer, expected_reads
):
    """With a task per chunk, each task makes one read: its chunk record, or the
    whole file when it has no index. Nothing of the summary or the magic bytes
    is read."""
    path = os.path.join(tmp_path, "file.mcap")
    writer(path)
    datasource = MCAPDatasourceV2([path])
    set_task_budget(monkeypatch, ONE_CHUNK_PER_TASK)
    tasks = pack(datasource)
    read_log = ReadLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=read_log)

    table = read_tasks(scanner, tasks)

    assert sorted(read_log.reads[path]) == expected_reads(path)
    assert len(tasks) == len(expected_reads(path))
    assert table.num_rows == CHUNKED_FILE_MESSAGES


@pytest.mark.parametrize("bin_bytes", [ONE_CHUNK_PER_TASK, None])
@pytest.mark.parametrize("options", READ_OPTIONS.values(), ids=READ_OPTIONS.keys())
@pytest.mark.parametrize("writer", WRITERS.values(), ids=WRITERS.keys())
def test_rows_match_reading_the_summary_from_the_file(
    tmp_path, monkeypatch, writer, options, bin_bytes
):
    """Tasks that parse their summary records return the same tables as the same
    tasks reading each file's summary from the file."""
    path = os.path.join(tmp_path, "file.mcap")
    writer(path)
    datasource = MCAPDatasourceV2([path], **options)
    set_task_budget(monkeypatch, bin_bytes)
    tasks = pack(datasource)
    listed = sum(len(m) for m in list_manifests(datasource))
    scanner = scanner_for(datasource)

    with_records = [list(scanner.create_reader().read(task)) for task in tasks]
    from_file = [
        list(scanner.create_reader().read(without_task_summaries(task)))
        for task in tasks
    ]

    assert with_records == from_file
    if bin_bytes == ONE_CHUNK_PER_TASK and listed > 1:
        # The case compares split tasks, so the file must still be split.
        assert len(tasks) > 1


def test_whole_file_rows_carry_the_whole_summary(chunked_file, monkeypatch):
    """A file listed whole, here because its summary lists no channels, gets
    every record of its summary and reads every chunk."""
    without_summary_records(monkeypatch, "channels")
    datasource = MCAPDatasourceV2([chunked_file])
    (task,) = pack(datasource)

    (records,) = task_summaries(task)
    table = read_tasks(scanner_for(datasource), [task])

    assert task.file_chunk_metadatas[0] is None and records is not None
    assert parse_summary_records(records, chunked_file).chunk_indexes == (
        summary_of(chunked_file).chunk_indexes
    )
    assert table.num_rows == CHUNKED_FILE_MESSAGES


def test_declarations_missing_from_the_summary_are_read_back(tmp_path, monkeypatch):
    """Schemas declared only in chunk 0: a task owning a later chunk reads the
    file's summary once to find chunk 0, which declares them. The task owning
    chunk 0 reads no summary."""
    path = os.path.join(tmp_path, "file.mcap")
    write_mcap_without_summary_schemas(path)
    datasource = MCAPDatasourceV2([path])
    set_task_budget(monkeypatch, ONE_CHUNK_PER_TASK)
    tasks = pack(datasource)
    summary_reads = []
    real_read = mcap_reader.read_summary_from

    def counted_read(f, file_path):
        summary_reads.append(file_path)
        return real_read(f, file_path)

    monkeypatch.setattr(mcap_reader, "read_summary_from", counted_read)
    scanner = scanner_for(datasource)

    for task in tasks:
        read_tasks(scanner, [task])

    # Chunk 0 declares both schemas: its task needs no read-back.
    assert len(summary_reads) == len(tasks) - 1


def test_unreadable_file_still_fails_its_task_naming_it(tmp_path, chunked_file):
    """A file whose summary could not be listed carries no records, so its task
    reads the summary itself and fails naming the file."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), chunked_file)
    datasource = MCAPDatasourceV2([chunked_file, cut_short])
    tasks = pack(datasource)
    scanner = scanner_for(datasource)
    bad = [t for t in tasks if cut_short in [str(p) for p in t.paths]]

    (task,) = bad

    assert task_summaries(task) == [None]
    with pytest.raises(ValueError, match="cut_short.mcap"):
        read_tasks(scanner, [task])


@pytest.mark.parametrize("keep_order", [False, True])
@pytest.mark.parametrize("bin_bytes", [ONE_CHUNK_PER_TASK, None])
def test_a_path_listed_twice_is_read_twice(
    chunked_file, monkeypatch, bin_bytes, keep_order
):
    """A path listed twice is read twice, as Parquet reads it, whether its copies
    share a read task or not. Every packed row carries its own records."""
    options = DataContext.get_current().execution_options
    monkeypatch.setattr(options, "preserve_order", keep_order)
    set_task_budget(monkeypatch, bin_bytes)
    datasource = MCAPDatasourceV2([chunked_file, chunked_file])
    tasks = pack(datasource)

    table = read_tasks(scanner_for(datasource), tasks)

    seqs = [data["seq"] for data in table.column("data").to_pylist()]
    one_copy = list(range(CHUNKED_FILE_MESSAGES))
    assert all(records is not None for t in tasks for records in task_summaries(t))
    if keep_order:
        assert seqs == one_copy + one_copy
    else:
        assert sorted(seqs) == sorted(one_copy + one_copy)


# Chunks of each file in the partitioner test: one per message.
CHUNKS_PER_FILE = 4
# Files held after each file is added: with a task per chunk every row is packed
# at once; with the default budget all three files wait in one open task.
HELD = {
    "task_per_chunk": (ONE_CHUNK_PER_TASK, [0, 0, 0]),
    "one_task": (None, [1, 2, 3]),
}


@pytest.mark.parametrize("bin_bytes, expected", HELD.values(), ids=HELD.keys())
def test_partitioner_releases_a_file_once_its_rows_are_packed(
    tmp_path, monkeypatch, bin_bytes, expected
):
    """The listing stores a file's records once for all its rows, and the
    partitioner holds them only until the file's last row leaves in a packed
    task, so it holds nothing once packing is done."""
    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(CHUNKS_PER_FILE))
        paths.append(path)
    set_task_budget(monkeypatch, bin_bytes)
    datasource = MCAPDatasourceV2(paths)
    partitioner = datasource.get_file_partitioner()
    record_indices = []
    held_after_each_file = []

    for manifest in list_manifests(datasource):
        file_records = table_of(manifest).column(FILE_RECORDS_COLUMN)
        record_indices.append(file_records.combine_chunks().indices.to_pylist())
        partitioner.add_input(manifest)
        while partitioner.has_partition():
            partitioner.next_partition()
        held_after_each_file.append(len(partitioner._held))
    partitioner.finalize()
    while partitioner.has_partition():
        partitioner.next_partition()

    # Every row points at the one stored copy of its file's records.
    assert record_indices == [[0] * CHUNKS_PER_FILE] * len(paths)
    assert held_after_each_file == expected
    assert partitioner._held == {}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
