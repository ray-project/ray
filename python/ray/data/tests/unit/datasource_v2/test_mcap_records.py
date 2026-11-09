"""Unit tests for the attachment and metadata granularities of the V2 MCAP reader."""

import dataclasses
import importlib.util
import json
import os
import threading
import time
from typing import List

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_datasource_v2,
    mcap_file_indexer,
    mcap_reader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    parse_summary_records,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import TimeRange
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import (
    ATTACHMENT_RECORDS,
    METADATA_RECORDS,
    listed_records,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    attachment_unit_id,
    read_summary,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    TaskSummaryPartitioner,
    task_summaries,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.supports_metadata import MetadataType
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import KiB, MiB
from ray.data.expressions import col
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    OpenFilesLog,
    SeekLog,
    as_read_task,
    as_task,
    count_sample,
    list_manifests,
    pack,
    read_rows,
    scanner_for,
    summary_of,
    write_corrupt_attachment,
    write_cut_short,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

ATTACHMENTS = [
    # (name, media_type, log_time, create_time, data)
    ("calib.yaml", "text/yaml", 1_000, 900, b"fx: 1.0\n"),
    ("map.bin", "application/octet-stream", 2_000, 1_900, bytes(range(64))),
    ("thumb.jpg", "image/jpeg", 3_000, 2_900, b"\xff\xd8\xff" + bytes(16)),
]
METADATA = [
    ("recorder", {"version": "1.2", "host": "rig-7"}),
    ("vehicle", {"id": "v42"}),
]


def write_file(
    path,
    *,
    index=True,
    record_indexes=True,
    attachments=ATTACHMENTS,
    metadata=METADATA,
    chunked=None,
):
    """Write three messages on ``/t`` with ``attachments`` and ``metadata`` between
    them.

        write order  message 0, calib.yaml, recorder, vehicle, message 1,
                     map.bin, message 2, thumb.jpg

    Message ``i`` is logged at ``1_000 + i``. ``record_indexes=False`` keeps the
    summary and statistics but leaves out the attachment and metadata indexes.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    index_types = IndexType.ALL if index else IndexType.NONE
    if not record_indexes:
        index_types &= ~(IndexType.ATTACHMENT | IndexType.METADATA)
    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=1,
            compression=CompressionType.ZSTD,
            index_types=index_types,
            use_chunking=index if chunked is None else chunked,
            use_statistics=index,
            use_summary_offsets=index,
        )
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b"{}"
        )
        channel = writer.register_channel(
            schema_id=schema_id, topic="/t", message_encoding="json"
        )
        for i in range(3):
            if i == 1:
                for name, data in metadata:
                    writer.add_metadata(name, data)
            writer.add_message(
                channel_id=channel,
                log_time=1_000 + i,
                publish_time=1_000 + i,
                data=json.dumps({"seq": i}).encode(),
            )
            for name, media_type, log_time, create_time, data in attachments:
                if log_time // 1000 == i + 1:
                    writer.add_attachment(
                        create_time=create_time,
                        log_time=log_time,
                        name=name,
                        media_type=media_type,
                        data=data,
                    )
        writer.finish()


@pytest.fixture
def records_file(tmp_path):
    """An indexed ``write_file`` file with every attachment and metadata record."""
    path = os.path.join(tmp_path, "run.mcap")
    write_file(path)
    return path


def pack_keeping_order(datasource) -> List[FileManifest]:
    """``pack`` with ``DataContext.execution_options.preserve_order`` on."""
    from ray.data.context import DataContext

    options = DataContext.get_current().execution_options
    saved, options.preserve_order = options.preserve_order, True
    try:
        return pack(datasource)
    finally:
        options.preserve_order = saved


def record_open_bytes() -> int:
    """What opening a file adds to the weight of its first record row."""
    return mcap_datasource_v2._BIN_PACKING_BYTES // (
        mcap_file_indexer._RECORD_FILES_PER_READ_TASK + 1
    )


def test_attachment_rows(records_file):
    """Each attachment is one listing row that weighs its working set,
    ``_RECORD_WORKING_SET_RATIO`` times its data, with the file's open added to
    the first, and reads back with its fields, bytes and row id."""
    datasource = MCAPDatasourceV2(
        [records_file], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    summary = read_summary(datasource.filesystem, records_file)
    assert summary is not None
    assert len(manifest) == len(summary.attachment_indexes) == 3
    ratio = mcap_file_indexer._RECORD_WORKING_SET_RATIO
    sizes = [ratio * len(data) for *_, data in ATTACHMENTS]
    sizes[0] += record_open_bytes()
    assert [int(md["size_bytes"]) for md in manifest.file_chunk_metadatas] == sizes
    assert isinstance(datasource.get_file_partitioner(), TaskSummaryPartitioner)

    rows = read_rows(datasource, [manifest])

    assert [row["name"] for row in rows] == ["calib.yaml", "map.bin", "thumb.jpg"]
    assert [row["media_type"] for row in rows] == [a[1] for a in ATTACHMENTS]
    assert [row["log_time"] for row in rows] == [a[2] for a in ATTACHMENTS]
    assert [row["create_time"] for row in rows] == [a[3] for a in ATTACHMENTS]
    assert [row["data"] for row in rows] == [a[4] for a in ATTACHMENTS]
    assert rows[0]["path"] == records_file
    assert [row["row_id"] for row in rows] == [
        attachment_unit_id(records_file, idx.offset)
        for idx in summary.attachment_indexes
    ]


@pytest.mark.parametrize("preserve_order", [False, True])
def test_a_record_task_opens_few_files(tmp_path, preserve_order):
    """Small files spread over several record tasks of at most
    ``RAY_DATA_MCAP_RECORD_FILES_PER_READ_TASK`` (16) files each, though all of
    them fit one task's budget; every record is read once."""
    num_files = 40
    for i in range(num_files):
        write_file(os.path.join(tmp_path, f"run_{i:02d}.mcap"))
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity="attachment")

    tasks = pack_keeping_order(datasource) if preserve_order else pack(datasource)
    rows = read_rows(datasource, tasks)

    files_per_task = [len(set(task.paths)) for task in tasks]
    assert max(files_per_task) == mcap_file_indexer._RECORD_FILES_PER_READ_TASK == 16
    assert len(rows) == num_files * len(ATTACHMENTS)
    assert len({(row["path"], row["name"]) for row in rows}) == len(rows)


def write_attachment_files(directory, num_files, attachment_bytes) -> List[str]:
    """Write ``num_files`` files of ``len(attachment_bytes)`` attachments each,
    the i-th of ``attachment_bytes[i]`` bytes, and return their paths."""
    os.makedirs(directory)
    paths = [os.path.join(directory, f"run_{i:02d}.mcap") for i in range(num_files)]
    attachments = [
        (f"a{i}.bin", "application/octet-stream", 2_000, 0, bytes(size))
        for i, size in enumerate(attachment_bytes)
    ]
    for path in paths:
        write_file(path, attachments=attachments, metadata=[])
    return paths


def test_large_records_pack_one_file_per_task(tmp_path):
    """At the default 128 MiB budget, files of one 24 MiB attachment each go one
    file per read task, since each weighs its 72 MiB working set, while files of
    three 300 KiB attachments still share tasks, twelve files to a task."""
    assert mcap_datasource_v2._BIN_PACKING_BYTES == 128 * MiB
    large = write_attachment_files(tmp_path / "large", 4, [24 * MiB])
    small = write_attachment_files(tmp_path / "small", 13, [300 * KiB] * 3)

    large_tasks = pack(MCAPDatasourceV2(large, read_granularity="attachment"))
    small_tasks = pack(MCAPDatasourceV2(small, read_granularity="attachment"))

    assert [len(set(task.paths)) for task in large_tasks] == [1] * len(large)
    # 128 MiB fits 12 files of 3 * 3 * 300 KiB plus a file open of 128 MiB / 17.
    assert max(len(set(task.paths)) for task in small_tasks) == 12


def test_a_files_small_records_share_one_task(tmp_path):
    """A file of many small metadata records is opened once: its records all go
    to one read task, which reads them in file order."""
    path = os.path.join(tmp_path, "many.mcap")
    names = [f"m{i:03d}" for i in range(1_000)]
    write_file(path, metadata=[(name, {"k": name}) for name in names], attachments=[])
    datasource = MCAPDatasourceV2([path], read_granularity="metadata")

    tasks = pack_keeping_order(datasource)
    rows = read_rows(datasource, tasks)

    assert len(tasks) == 1
    assert [row["name"] for row in rows] == names


def read_with(datasource, task, filesystem, limit=None):
    """Read one task with ``filesystem``, and the scanner's ``limit`` if given."""
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=filesystem)
    if limit is not None:
        scanner = scanner.push_limit(limit)
    return [
        row for table in scanner.create_reader().read(task) for row in table.to_pylist()
    ]


def test_a_record_task_reads_its_files_a_few_at_a_time(tmp_path, monkeypatch):
    """A record task reads its files at the same time, up to
    ``_RECORD_FILES_IN_FLIGHT``, and returns their rows in manifest order, the
    same rows as one file at a time."""
    num_files = 4  # with three attachments each, still one task
    for i in range(num_files):
        write_file(os.path.join(tmp_path, f"run_{i}.mcap"))
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity="attachment")
    (task,) = pack(datasource)
    together, one_by_one = OpenFilesLog(together=num_files), OpenFilesLog()

    rows = read_with(datasource, task, together)
    monkeypatch.setattr(mcap_reader, "_RECORD_FILES_IN_FLIGHT", 1)
    expected = read_with(datasource, task, one_by_one)

    assert together.most_open == num_files and one_by_one.most_open == 1
    assert rows == expected


class BlockingOpens:
    """A local filesystem whose open of ``blocked`` waits until ``release`` is set."""

    def __init__(self, blocked: str):
        self._blocked = blocked
        self.started, self.release = threading.Event(), threading.Event()

    def open_input_file(self, path):
        if path == self._blocked:
            self.started.set()
            self.release.wait(timeout=30)
        return LocalFileSystem().open_input_file(path)


def test_closing_a_record_read_does_not_wait_for_a_running_file_read(tmp_path):
    """Closing a record read returns at once while another file of the task is
    still being read ahead; that read finishes on its own."""
    paths = [os.path.join(tmp_path, f"run_{i}.mcap") for i in range(3)]
    for path in paths:
        write_file(path)
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity="metadata")
    (task,) = pack(datasource)
    filesystem = BlockingOpens(paths[1])
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=filesystem)
    tables = scanner.create_reader().read(task)
    first = next(tables)
    assert filesystem.started.wait(timeout=10)
    start = time.monotonic()

    tables.close()

    waited = time.monotonic() - start
    filesystem.release.set()
    assert first.column("path").to_pylist() == [paths[0]] * len(METADATA)
    # Waiting for the blocked read would take its full 30 s.
    assert waited < 10


def threads_started_since(before) -> List[threading.Thread]:
    """The threads not in ``before`` still alive after up to 5 s to end."""
    deadline = time.monotonic() + 5
    started = [t for t in threading.enumerate() if t not in before]
    for thread in started:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    return [t for t in started if t.is_alive()]


def test_a_failed_record_read_leaves_no_read_ahead_thread_running(
    records_file, tmp_path, restore_max_errored_blocks
):
    """When a record read fails at a file and ``max_errored_blocks=0``, the
    read-ahead threads end, even while the caller still holds the error."""
    from ray.data.context import DataContext

    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), records_file)
    task = as_read_task(
        *(
            manifest
            for path in (cut_short, records_file)
            for manifest in list_manifests(
                MCAPDatasourceV2([path], read_granularity="attachment")
            )
        )
    )
    DataContext.get_current().max_errored_blocks = 0
    datasource = MCAPDatasourceV2([records_file], read_granularity="attachment")
    reader = scanner_for(datasource).create_reader()
    before = set(threading.enumerate())

    with pytest.raises(
        ValueError, match="cut_short.mcap' is not a complete MCAP"
    ) as error:
        list(reader.read(task))

    assert threads_started_since(before) == []
    # The held error keeps the failed read's frames alive until here.
    assert error.value is not None


def test_a_record_read_with_a_limit_opens_files_one_at_a_time(tmp_path):
    """A record read with a limit reads its files one at a time, so a limit the
    first file meets opens no other file."""
    for i in range(6):
        write_file(os.path.join(tmp_path, f"run_{i}.mcap"))
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity="metadata")
    (task,) = pack(datasource)
    log = OpenFilesLog()

    rows = read_with(datasource, task, log, limit=1)

    assert len(rows) == 1 and len(log.opened) == 1


def test_a_record_task_fails_at_an_unreadable_file_after_the_files_before_it(
    records_file, tmp_path
):
    """Reading files at the same time still returns every row of the files
    before an unreadable one, then fails naming it."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), records_file)
    last = os.path.join(tmp_path, "last.mcap")
    write_file(last)
    task = as_read_task(
        *(
            manifest
            for path in (records_file, cut_short, last)
            for manifest in list_manifests(
                MCAPDatasourceV2([path], read_granularity="attachment")
            )
        )
    )
    datasource = MCAPDatasourceV2([records_file], read_granularity="attachment")
    reader = scanner_for(datasource).create_reader()
    rows = []

    with pytest.raises(ValueError, match="cut_short.mcap' is not a complete MCAP"):
        for table in reader.read(task):
            rows.extend(table.to_pylist())

    assert [row["path"] for row in rows] == [records_file] * len(ATTACHMENTS)


def test_attachment_rows_split_across_tasks_and_honor_time_range(records_file):
    """Attachment rows split across tasks, and ``time_range`` and excluded read
    units prune the listing."""
    datasource = MCAPDatasourceV2(
        [records_file], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    first = read_rows(datasource, [FileManifest(block.slice(0, 1))])
    rest = read_rows(datasource, [FileManifest(block.slice(1))])
    assert [r["name"] for r in first + rest] == ["calib.yaml", "map.bin", "thumb.jpg"]

    ranged = MCAPDatasourceV2(
        [records_file],
        read_granularity="attachment",
        time_range=TimeRange(start_time=1_500, end_time=3_000),
    )
    (pruned,) = list_manifests(ranged)
    assert len(pruned) == 1  # only map.bin at 2_000 is listed
    rows = read_rows(ranged, [pruned])
    assert [row["name"] for row in rows] == ["map.bin"]

    excluded = {attachment_unit_id(records_file, summary_offsets(records_file)[0])}
    (remaining,) = list_manifests(datasource, excluded_read_unit_ids=excluded)
    assert len(remaining) == 2


class UnitRecorder(SynthesizedColumn):
    """A synthesized column that records the read unit of every table."""

    name = "read_unit"
    type = pa.string()

    def __init__(self):
        self.unit_ids = []

    def compute(self, position, num_rows):
        self.unit_ids.append(position.unit.id)
        return pa.array([position.unit.id] * num_rows, type=pa.string())


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_resumed_listing_leaves_out_the_records_a_task_read(records_file, granularity):
    """A record task's tables report the unit ids the listing leaves out, so a
    resumed read does not list a finished record again."""
    datasource = MCAPDatasourceV2([records_file], read_granularity=granularity)
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)
    recorder = UnitRecorder()
    scanner = dataclasses.replace(scanner, synthesized_columns=(recorder,))
    block = manifest.as_block()
    assert isinstance(block, pa.Table)

    for _ in scanner.create_reader().read(as_task(FileManifest(block.slice(0, 1)))):
        pass
    (remaining,) = list_manifests(
        datasource, excluded_read_unit_ids=set(recorder.unit_ids)
    )

    assert len(remaining) == len(manifest) - 1


def summary_offsets(path):
    """The offsets of the attachments in ``path``'s summary."""
    return [idx.offset for idx in summary_of(path).attachment_indexes]


def test_metadata_rows(records_file):
    """Each metadata record reads back as a row with its name, its key-value pairs
    in order, and a row id."""
    datasource = MCAPDatasourceV2(
        [records_file], read_granularity="metadata", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 2

    rows = read_rows(datasource, [manifest])

    assert [row["name"] for row in rows] == ["recorder", "vehicle"]
    assert rows[0]["metadata"] == [("version", "1.2"), ("host", "rig-7")]
    assert rows[1]["metadata"] == [("id", "v42")]
    assert rows[0]["row_id"].startswith(f"{records_file}#md=")


@pytest.mark.parametrize("record_indexes", [True, False], ids=["indexed", "scanned"])
@pytest.mark.parametrize(
    "granularity, field, count",
    [("attachment", "attachment_count", 3), ("metadata", "metadata_count", 2)],
)
def test_records_are_counted_as_the_listing_finds_them(
    tmp_path, monkeypatch, granularity, field, count, record_indexes
):
    """When the statistics miscount the records, the count follows the record
    index, or a scan without one, as the listing does, so ``count()`` matches
    the rows read."""
    from mcap.writer import Writer

    finish = Writer.finish

    def miscounting_finish(self):
        # A writer whose statistics count one record fewer than it indexed.
        statistics = self._Writer__statistics
        setattr(statistics, field, getattr(statistics, field) - 1)
        return finish(self)

    monkeypatch.setattr(Writer, "finish", miscounting_finish)
    path = os.path.join(tmp_path, "miscounted.mcap")
    write_file(path, record_indexes=record_indexes)
    datasource = MCAPDatasourceV2([path], read_granularity=granularity)
    (manifest,) = list_manifests(datasource)

    rows = read_rows(datasource, [manifest])
    counts = count_sample(datasource)

    assert len(rows) == count
    assert counts == [count]


def test_ordered_read_spreads_a_files_records_over_tasks(tmp_path, monkeypatch):
    """With ``preserve_order``, a file whose records outweigh one read task's
    budget goes to several tasks within the budget, in file order, as an
    unordered read spreads them."""
    budget = 20  # bytes: the 40 one-byte attachments need three tasks
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)
    path = os.path.join(tmp_path, "many.mcap")
    names = [f"a{i:02d}.bin" for i in range(40)]
    write_file(
        path,
        attachments=[(n, "application/octet-stream", 2_000, 0, b"x") for n in names],
    )
    datasource = MCAPDatasourceV2([path], read_granularity="attachment")

    tasks = pack_keeping_order(datasource)
    rows = read_rows(datasource, tasks)

    weights = [
        sum(int(md["size_bytes"]) for md in task.file_chunk_metadatas) for task in tasks
    ]
    assert len(tasks) > 1
    assert max(weights) <= budget
    assert [row["name"] for row in rows] == names


def test_record_scan_does_not_decompress_message_chunks(tmp_path, monkeypatch):
    """An unindexed file is listed whole and scanned for its records, which
    decompresses no message chunk. A scanned record's ``row_id`` is its ordinal
    among the file's records of its kind."""
    import mcap.stream_reader

    path = os.path.join(tmp_path, "noindex.mcap")
    write_file(path, index=False, chunked=True)

    def no_decompression(*args, **kwargs):
        raise AssertionError("a message chunk was decompressed")

    monkeypatch.setattr(mcap.stream_reader, "breakup_chunk", no_decompression)
    for granularity, names, count in (
        ("attachment", ["calib.yaml", "map.bin", "thumb.jpg"], 3),
        ("metadata", ["recorder", "vehicle"], 2),
    ):
        datasource = MCAPDatasourceV2(
            [path], read_granularity=granularity, include_row_id=True
        )
        (manifest,) = list_manifests(datasource)
        rows = read_rows(datasource, [manifest])
        reader = scanner_for(datasource).create_reader()
        assert len(manifest) == 1 and manifest.file_chunk_metadatas[0] is None
        assert [row["name"] for row in rows] == names
        tag = "a" if granularity == "attachment" else "md"
        assert rows[0]["row_id"] == f"{path}#{tag}-m=0"
        assert [m.num_rows for m in reader.read_metadata(manifest)] == [count]


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_records_are_read_as_blocks_are_cut(tmp_path, monkeypatch, index):
    """With one row per block, each block comes out before the next record is read."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_records
    from ray.data.context import DataContext

    path = os.path.join(tmp_path, "run.mcap")
    write_file(path, index=index)
    read = []
    real_iter_records = mcap_records.iter_records
    real_read_record_at = mcap_records.read_record_at

    def counting_iter_records(f, record_type):
        for record in real_iter_records(f, record_type):
            read.append(record)
            yield record

    def counting_read_record_at(f, offset, length):
        record = real_read_record_at(f, offset, length)
        read.append(record)
        return record

    monkeypatch.setattr(mcap_records, "iter_records", counting_iter_records)
    monkeypatch.setattr(mcap_records, "read_record_at", counting_read_record_at)
    monkeypatch.setattr(DataContext.get_current(), "target_max_block_size", 1)
    datasource = MCAPDatasourceV2([path], read_granularity="attachment")
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    task = as_task(manifest)
    read_per_block = [len(read) for _ in scanner.create_reader().read(task)]

    assert read_per_block == [1, 2, 3]


def record_indexes(path, granularity):
    """The summary's index records for ``granularity``'s kind of record."""
    summary = summary_of(path)
    if granularity == "attachment":
        return summary.attachment_indexes
    return summary.metadata_indexes


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_indexed_records_are_read_in_one_read_each(records_file, granularity):
    """A task reads each record it owns in one read of the length its index
    record gives, rather than field by field. The task carries those index
    records, so it reads no summary."""
    datasource = MCAPDatasourceV2([records_file], read_granularity=granularity)
    (task,) = pack(datasource)
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)
    offsets = sorted(
        index.offset for index in record_indexes(records_file, granularity)
    )

    tables = list(scanner.create_reader().read(task))

    assert sum(table.num_rows for table in tables) == len(offsets)
    assert seek_log.reads[records_file] == len(offsets)
    seeks = seek_log.seeks[records_file]
    assert [offset for offset in seeks if offset in offsets] == offsets


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_record_tasks_carry_their_own_index_records(
    records_file, monkeypatch, granularity
):
    """With one record per task, each task carries that record's index record
    and nothing else of the summary."""
    # A budget smaller than any record row: one record per task.
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)
    datasource = MCAPDatasourceV2([records_file], read_granularity=granularity)
    indexes = record_indexes(records_file, granularity)

    tasks = pack(datasource)

    carried = []
    for task in tasks:
        (records,) = task_summaries(task)
        assert records is not None
        carried.append(parse_summary_records(records, records_file))
    kept = [s.attachment_indexes + s.metadata_indexes for s in carried]
    assert kept == [[index] for index in indexes]
    assert not any(s.channels or s.chunk_indexes or s.statistics for s in carried)


def test_unindexed_record_scan_reads_through_a_buffer(tmp_path):
    """A file without a record index is scanned in one buffered pass: the read
    takes one buffer fill, and the count the summary read and one buffer fill."""
    path = os.path.join(tmp_path, "noindex.mcap")
    write_file(path, index=False)
    datasource = MCAPDatasourceV2([path], read_granularity="attachment")
    (manifest,) = list_manifests(datasource)
    expected = len(ATTACHMENTS)
    read_log, count_log = SeekLog(), SeekLog()
    scanner = scanner_for(datasource)
    reader = dataclasses.replace(scanner, filesystem=read_log).create_reader()
    counter = dataclasses.replace(scanner, filesystem=count_log).create_reader()

    rows = sum(table.num_rows for table in reader.read(as_task(manifest)))
    counts = [m.num_rows for m in counter.read_metadata(manifest)]

    assert rows == expected and counts == [expected]
    # The read task carries the summary, so the read takes only the buffer fill.
    assert read_log.reads[path] == 1
    # The count reads the summary, then fills the buffer once for the scan.
    assert count_log.reads[path] == 2


@pytest.mark.parametrize("max_errored_blocks", [1, 0])
def test_a_record_task_reads_past_a_file_with_a_corrupt_record(
    tmp_path, max_errored_blocks, restore_max_errored_blocks
):
    """A record task holding three files, the middle one with a corrupt
    attachment, reads every other attachment when errors can be skipped, then
    raises naming the bad file and record. With ``max_errored_blocks=0`` it
    stops at the bad file."""
    from ray.data.context import DataContext

    a, b, c = (os.path.join(tmp_path, f"{name}.mcap") for name in "abc")
    for path in (a, b, c):
        write_file(path)
    write_corrupt_attachment(b, 1)
    corrupt_offset = summary_of(b).attachment_indexes[1].offset
    DataContext.get_current().max_errored_blocks = max_errored_blocks
    datasource = MCAPDatasourceV2([a, b, c], read_granularity="attachment")
    # The three small files share one read task, which reads them ahead.
    (task,) = pack(datasource, preserve_order=True)

    paths = []
    with pytest.raises(Exception) as error:
        for table in scanner_for(datasource).create_reader().read(task):
            paths.extend(table.column("path").to_pylist())

    if max_errored_blocks:
        assert paths == [a] * 3 + [b] * 2 + [c] * 3
        assert f"1 corrupt record at byte offset {corrupt_offset}" in str(error.value)
    else:
        assert paths == [a] * 3
    assert "b.mcap" in str(error.value)


def test_a_corrupt_record_costs_only_itself(tmp_path, restore_max_errored_blocks):
    """A file read alone, with its middle attachment corrupt, gives the other two
    attachments when errors can be skipped, then raises naming that record."""
    from ray.data.context import DataContext

    path = os.path.join(tmp_path, "run.mcap")
    write_file(path)
    write_corrupt_attachment(path, 1)
    corrupt_offset = summary_of(path).attachment_indexes[1].offset
    DataContext.get_current().max_errored_blocks = 1
    datasource = MCAPDatasourceV2([path], read_granularity="attachment")
    # One file per task: it is read in turn, not ahead.
    (task,) = pack(datasource)

    names = []
    with pytest.raises(ValueError) as error:
        for table in scanner_for(datasource).create_reader().read(task):
            names.extend(table.column("name").to_pylist())

    assert names == ["calib.yaml", "thumb.jpg"]
    assert f"1 corrupt record at byte offset {corrupt_offset}" in str(error.value)
    assert "run.mcap" in str(error.value)


def test_record_the_summary_no_longer_indexes_fails_naming_the_file(records_file):
    """A record listed from an earlier version of the file fails its read with an
    error that names the file, rather than reading whatever sits at its offset."""
    datasource = MCAPDatasourceV2([records_file], read_granularity="attachment")
    (manifest,) = list_manifests(datasource)
    # Rewritten after listing with its first attachment only.
    write_file(records_file, attachments=ATTACHMENTS[:1])

    # The task carries the listed index record, which now points at a message.
    with pytest.raises(ValueError, match="run.mcap'.* may have changed since listing"):
        read_rows(datasource, [manifest])


def test_file_without_records_lists_nothing(tmp_path):
    """A file with no attachments or metadata lists nothing at those granularities."""
    path = os.path.join(tmp_path, "plain.mcap")
    write_file(path, attachments=[], metadata=[])

    assert list_manifests(MCAPDatasourceV2([path], read_granularity="attachment")) == []
    assert list_manifests(MCAPDatasourceV2([path], read_granularity="metadata")) == []


def test_record_counts_from_statistics(records_file):
    """Record counts come from the summary statistics, and filters on message
    columns do not apply to records. A time range is not counted from them."""
    for granularity, expected in (("attachment", 3), ("metadata", 2)):
        datasource = MCAPDatasourceV2([records_file], read_granularity=granularity)
        indexer = datasource._get_file_indexer()
        sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
        scanner = datasource.create_scanner(
            datasource.infer_schema(sample), datasource.filesystem
        )
        reader = scanner.create_reader()
        assert scanner.metadata_row_count_is_exact()
        assert reader.available_metadata() == {MetadataType.NUM_ROWS}
        assert [m.num_rows for m in reader.read_metadata(sample)] == [expected]
        same, residual = scanner.push_filters(col("topic") == "/t")
        assert same is scanner and residual is not None

    ranged = MCAPDatasourceV2(
        [records_file],
        read_granularity="attachment",
        time_range=TimeRange(start_time=1_500, end_time=3_000),
    )
    indexer = ranged._get_file_indexer()
    sample = sample_files(indexer, ranged.paths, ranged.filesystem, [])
    scanner = ranged.create_scanner(ranged.infer_schema(sample), ranged.filesystem)
    assert not scanner.metadata_row_count_is_exact()
    assert scanner.create_reader().available_metadata() == set()


def test_record_granularity_validation(records_file):
    """Message options are refused at the record granularities: ``topics``,
    ``message_types``, ``include_paths``, ``include_metadata=False``, and
    ``time_range`` for metadata."""
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2(
            [records_file], read_granularity="attachment", include_paths=True
        )
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2(
            [records_file], read_granularity="metadata", include_metadata=False
        )
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2([records_file], read_granularity="attachment", topics=["/t"])
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2(
            [records_file], read_granularity="metadata", message_types=["test_schema"]
        )
    with pytest.raises(ValueError, match="time_range does not apply"):
        MCAPDatasourceV2(
            [records_file],
            read_granularity="metadata",
            time_range=TimeRange(start_time=1, end_time=2),
        )


def test_listed_records_say_where_a_file_lists_its_records(tmp_path):
    """``listed_records`` gives a file's index records of a kind, none when the
    statistics count none, and ``None`` when the file must be scanned."""
    indexed, unindexed, plain = (
        os.path.join(tmp_path, f"{name}.mcap") for name in ("i", "u", "p")
    )
    write_file(indexed)
    write_file(unindexed, record_indexes=False)
    write_file(plain, attachments=[], metadata=[])

    for kind, count in ((ATTACHMENT_RECORDS, 3), (METADATA_RECORDS, 2)):
        indexes = listed_records(summary_of(indexed), kind)
        assert indexes is not None and len(indexes) == count
        assert listed_records(summary_of(unindexed), kind) is None
        assert listed_records(summary_of(plain), kind) == []
        assert listed_records(None, kind) is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
