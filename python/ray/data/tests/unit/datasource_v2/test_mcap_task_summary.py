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
    CHUNK_INDEX_COLUMN,
    FILE_RECORDS_COLUMN,
    TASK_SUMMARY_COLUMN,
    task_summaries,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
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


def task_summary_of(task: FileManifest):
    """The summary records of a one-row task."""
    (records,) = task_summaries(task)
    return records


def without_message_index_offsets(chunk_indexes):
    """``chunk_indexes`` as a task carries them: without message index offsets."""
    return [dataclasses.replace(c, message_index_offsets={}) for c in chunk_indexes]


def without_task_summaries(task: FileManifest) -> FileManifest:
    """``task`` as a reader that reads each summary from the file sees it."""
    return FileManifest(table_of(task).drop([TASK_SUMMARY_COLUMN]))


def read_tasks(scanner, tasks: List[FileManifest]) -> pa.Table:
    """Read every task, in order, into one table."""
    tables = [table for task in tasks for table in scanner.create_reader().read(task)]
    return pa.concat_tables(tables)


def test_listing_rows_carry_their_chunk_and_the_files_declarations(chunked_file):
    """Each listing row carries its chunk's index record, and the file's schema
    and channel records are stored once for all its rows."""
    datasource = MCAPDatasourceV2([chunked_file])
    (manifest,) = list_manifests(datasource)
    block = table_of(manifest)
    summary = summary_of(chunked_file)

    file_records = block.column(FILE_RECORDS_COLUMN).combine_chunks()
    (declarations,) = file_records.dictionary.to_pylist()
    chunk_records = block.column(CHUNK_INDEX_COLUMN).to_pylist()
    declared = parse_summary_records(declarations, chunked_file)

    assert file_records.indices.to_pylist() == [0] * CHUNKED_FILE_MESSAGES
    assert declared.channels == summary.channels
    assert declared.schemas == summary.schemas
    assert not declared.chunk_indexes
    assert [
        parse_summary_records(r, chunked_file).chunk_indexes[0] for r in chunk_records
    ] == without_message_index_offsets(summary.chunk_indexes)


def test_read_tasks_read_only_their_own_chunks(chunked_file, monkeypatch):
    """With a task per chunk, each task makes one read: its chunk record. Nothing
    of the summary or the magic bytes is read."""
    datasource = MCAPDatasourceV2([chunked_file])
    set_task_budget(monkeypatch, ONE_CHUNK_PER_TASK)
    tasks = pack(datasource)
    read_log = ReadLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=read_log)

    table = read_tasks(scanner, tasks)

    chunk_records = [
        (c.chunk_start_offset, c.chunk_length)
        for c in summary_of(chunked_file).chunk_indexes
    ]
    assert len(tasks) == CHUNKED_FILE_MESSAGES
    assert sorted(read_log.reads[chunked_file]) == chunk_records
    assert table.num_rows == CHUNKED_FILE_MESSAGES


def test_task_summary_holds_the_declarations_and_the_owned_chunk_indexes(
    chunked_file, monkeypatch
):
    """A packed row's records parse to the file's channels and schemas and the
    index records of exactly the chunks the row names, without their message
    index offsets."""
    datasource = MCAPDatasourceV2([chunked_file])
    set_task_budget(monkeypatch, ONE_CHUNK_PER_TASK)
    tasks = pack(datasource)
    summary = summary_of(chunked_file)
    index_by_offset = {
        c.chunk_start_offset: c
        for c in without_message_index_offsets(summary.chunk_indexes)
    }

    for task in tasks:
        (metadata,) = task.file_chunk_metadatas
        records = task_summary_of(task)
        assert records is not None
        task_summary = parse_summary_records(records, chunked_file)

        assert task_summary.channels == summary.channels
        assert task_summary.schemas == summary.schemas
        assert task_summary.chunk_indexes == [
            index_by_offset[int(offset)] for offset in metadata["unit_ids"]
        ]


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
    "message_types": {"message_types": ["other_schema"]},
    "file_order_with_row_id": {"log_time_order": False, "include_row_id": True},
}


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
    scanner = scanner_for(datasource)

    with_records = [list(scanner.create_reader().read(task)) for task in tasks]
    from_file = [
        list(scanner.create_reader().read(without_task_summaries(task)))
        for task in tasks
    ]

    assert with_records == from_file


def test_whole_file_rows_carry_the_whole_summary(chunked_file, monkeypatch):
    """A file listed whole, here because its summary lists no channels, gets
    every record of its summary and reads every chunk."""
    without_summary_records(monkeypatch, "channels")
    datasource = MCAPDatasourceV2([chunked_file])
    (task,) = pack(datasource)

    records = task_summary_of(task)
    table = read_tasks(scanner_for(datasource), [task])

    assert task.file_chunk_metadatas[0] is None and records is not None
    assert parse_summary_records(records, chunked_file).chunk_indexes == (
        summary_of(chunked_file).chunk_indexes
    )
    assert table.num_rows == CHUNKED_FILE_MESSAGES


def test_declarations_missing_from_the_summary_are_read_back(tmp_path, monkeypatch):
    """Schemas declared only in chunk 0: a task owning a later chunk reads the
    file's summary once to find the earlier chunks, and reads chunk 0 back. The
    task owning chunk 0 reads no summary."""
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

    per_task = [read_tasks(scanner, [task]) for task in tasks]
    reads_per_task = len(summary_reads)

    assert [t.column("topic").to_pylist() for t in per_task] == [
        ["/a"],
        ["/b"],
        ["/a"],
        ["/b"],
        ["/a"],
        ["/b"],
    ]
    assert [t.column("schema_name").to_pylist()[0] for t in per_task] == [
        "test_schema",
        "other_schema",
    ] * 3
    # Chunk 0 declares both schemas: its task needs no read-back.
    assert reads_per_task == len(tasks) - 1


def test_unindexed_file_is_scanned_without_a_summary_read(tmp_path, monkeypatch):
    """A file without a summary is scanned from its start; its task reads neither
    its end nor its magic bytes first."""
    path = os.path.join(tmp_path, "file.mcap")
    _write_unindexed(path)
    datasource = MCAPDatasourceV2([path])
    (task,) = pack(datasource)
    read_log = ReadLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=read_log)

    table = read_tasks(scanner, [task])

    # One buffered read covers the small file.
    assert read_log.reads[path] == [(0, os.path.getsize(path))]
    assert table.num_rows == 9


def test_unreadable_file_still_fails_its_task_naming_it(tmp_path, chunked_file):
    """A file whose summary could not be listed carries no records, so its task
    reads the summary itself and fails naming the file."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), chunked_file)
    datasource = MCAPDatasourceV2([chunked_file, cut_short])
    tasks = pack(datasource)
    scanner = scanner_for(datasource)
    bad = [t for t in tasks if cut_short in [str(p) for p in t.paths]]

    (task,) = bad

    assert task_summary_of(task) is None
    with pytest.raises(ValueError, match="cut_short.mcap"):
        read_tasks(scanner, [task])


def test_a_file_named_by_two_rows_reads_its_summary_from_the_file(chunked_file):
    """Two rows naming one file in one task, as a path listed twice can give, make
    the reader read that file's summary instead of joining the rows' records."""
    datasource = MCAPDatasourceV2([chunked_file])
    (task,) = pack(datasource)
    block = table_of(task)
    twice = FileManifest(pa.concat_tables([block, block]))

    (owned,) = mcap_reader._owned_files(twice)

    assert owned.summary_records is None
    assert owned.offsets == {
        c.chunk_start_offset for c in summary_of(chunked_file).chunk_indexes
    }


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
    """The partitioner holds a file's records only until its last row leaves in a
    packed task, so it holds nothing once packing is done."""
    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(4))
        paths.append(path)
    set_task_budget(monkeypatch, bin_bytes)
    datasource = MCAPDatasourceV2(paths)
    partitioner = datasource.get_file_partitioner()
    held_after_each_file = []

    for manifest in list_manifests(datasource):
        partitioner.add_input(manifest)
        while partitioner.has_partition():
            partitioner.next_partition()
        held_after_each_file.append(len(partitioner._held))
    partitioner.finalize()
    while partitioner.has_partition():
        partitioner.next_partition()

    assert held_after_each_file == expected
    assert partitioner._held == {}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
