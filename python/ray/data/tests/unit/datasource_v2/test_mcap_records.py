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
from ray.data.expressions import col
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    SeekLog,
    list_manifests,
    pack,
    read_rows,
    scanner_for,
    summary_of,
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
    path, *, index=True, attachments=ATTACHMENTS, metadata=METADATA, chunked=None
):
    """Write three messages on ``/t`` with ``attachments`` and ``metadata`` between
    them.

        write order  message 0, calib.yaml, recorder, vehicle, message 1,
                     map.bin, message 2, thumb.jpg

    Message ``i`` is logged at ``1_000 + i``.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=1,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL if index else IndexType.NONE,
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


def test_attachment_rows(records_file):
    """Each attachment is one listing row, sized by its data plus the requests
    reading it takes, and reads back with its fields, bytes and row id."""
    datasource = MCAPDatasourceV2(
        [records_file], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    summary = read_summary(datasource.filesystem, records_file)
    read_bytes = (
        mcap_datasource_v2._BIN_PACKING_BYTES
        // mcap_file_indexer._RECORDS_PER_READ_TASK
    )
    assert summary is not None
    assert len(manifest) == len(summary.attachment_indexes) == 3
    assert [int(md["size_bytes"]) for md in manifest.file_chunk_metadatas] == [
        len(data) + read_bytes for *_, data in ATTACHMENTS
    ]
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


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_a_record_task_opens_few_files(tmp_path, granularity):
    """Small files spread over several record tasks, each taking at most
    ``_RECORDS_PER_READ_TASK`` records, though all of them fit one task's
    budget; every record is read once."""
    num_files = 40
    for i in range(num_files):
        write_file(os.path.join(tmp_path, f"run_{i:02d}.mcap"))
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity=granularity)
    per_file = len(ATTACHMENTS) if granularity == "attachment" else len(METADATA)

    tasks = pack(datasource)
    rows = read_rows(datasource, tasks)

    records_per_task = [
        sum(len(md["unit_ids"]) for md in task.file_chunk_metadatas) for task in tasks
    ]
    assert len(tasks) > 1
    assert max(records_per_task) <= mcap_file_indexer._RECORDS_PER_READ_TASK
    assert len(rows) == num_files * per_file
    assert len({(row["path"], row["name"]) for row in rows}) == len(rows)


class OpenFilesLog:
    """A filesystem that records the files it opens and the most open at once.

    With ``together``, each open waits until that many files are open, so a
    read that opens them one at a time fails instead of passing by chance.
    """

    def __init__(self, together: int = 0):
        self._filesystem = LocalFileSystem()
        self._barrier = threading.Barrier(together, timeout=10) if together else None
        self._lock = threading.Lock()
        self._open_now = 0
        self.most_open = 0
        self.opened: List[str] = []

    def open_input_file(self, path):
        with self._lock:
            self._open_now += 1
            self.most_open = max(self.most_open, self._open_now)
            self.opened.append(path)
        if self._barrier is not None:
            self._barrier.wait()
        return _LoggedFile(self._filesystem.open_input_file(path), self)

    def closed(self):
        with self._lock:
            self._open_now -= 1


class _LoggedFile:
    """An open file that tells its ``OpenFilesLog`` when it closes."""

    def __init__(self, f, log: OpenFilesLog):
        self._f = f
        self._log = log

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._f.close()
        self._log.closed()


def read_with(datasource, task, filesystem, limit=None):
    """Read one task with ``filesystem``, and the scanner's ``limit`` if given."""
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=filesystem)
    if limit is not None:
        scanner = scanner.push_limit(limit)
    return [
        row for table in scanner.create_reader().read(task) for row in table.to_pylist()
    ]


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_a_record_task_reads_its_files_a_few_at_a_time(
    tmp_path, monkeypatch, granularity
):
    """A record task reads its files at the same time, up to
    ``_RECORD_FILES_IN_FLIGHT``, and returns their rows in manifest order, the
    same rows as one file at a time."""
    num_files = 4  # with three attachments each, still one task
    for i in range(num_files):
        write_file(os.path.join(tmp_path, f"run_{i}.mcap"))
    datasource = MCAPDatasourceV2([str(tmp_path)], read_granularity=granularity)
    (task,) = pack(datasource)
    together, one_by_one = OpenFilesLog(together=num_files), OpenFilesLog()

    rows = read_with(datasource, task, together)
    monkeypatch.setattr(mcap_reader, "_RECORD_FILES_IN_FLIGHT", 1)
    expected = read_with(datasource, task, one_by_one)

    assert together.most_open == num_files and one_by_one.most_open == 1
    assert rows == expected


def test_closing_the_read_ahead_does_not_wait_for_a_running_read():
    """Closing the read-ahead returns at once: reads not started are cancelled,
    and a running read finishes on its own. The garbage collector may close it
    from any thread, where waiting for the pool's threads can deadlock."""
    started, release = threading.Event(), threading.Event()

    def read(item):
        if item == 1:
            started.set()
            release.wait(timeout=30)
        return item

    reads = mcap_reader._read_ahead(read, [0, 1, 2, 3], in_flight=2)
    first = next(reads)
    assert started.wait(timeout=10)
    start = time.monotonic()

    reads.close()

    waited = time.monotonic() - start
    release.set()
    assert first.result() == 0
    # Waiting for the running read would take its full 30 s.
    assert waited < 10


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
    blocks = [
        manifest.as_block()
        for path in (records_file, cut_short, last)
        for manifest in list_manifests(
            MCAPDatasourceV2([path], read_granularity="attachment")
        )
    ]
    datasource = MCAPDatasourceV2([records_file], read_granularity="attachment")
    reader = scanner_for(datasource).create_reader()
    rows = []

    with pytest.raises(ValueError, match="cut_short.mcap' is not a complete MCAP"):
        tasks = FileManifest(pa.concat_tables(blocks, promote_options="permissive"))
        for table in reader.read(tasks):
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

    for _ in scanner.create_reader().read(FileManifest(block.slice(0, 1))):
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


def test_unindexed_file_records_are_scanned(tmp_path):
    """A file without an index is listed whole and scanned for its attachments
    and metadata, which are counted the same way."""
    path = os.path.join(tmp_path, "noindex.mcap")
    write_file(path, index=False)

    attachments = MCAPDatasourceV2(
        [path], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(attachments)
    assert len(manifest) == 1 and manifest.file_chunk_metadatas[0] is None
    rows = read_rows(attachments, [manifest])
    reader = scanner_for(attachments).create_reader()
    assert [row["name"] for row in rows] == ["calib.yaml", "map.bin", "thumb.jpg"]
    assert rows[0]["row_id"] == f"{path}#a-m=0"
    assert [m.num_rows for m in reader.read_metadata(manifest)] == [3]

    metadata = MCAPDatasourceV2([path], read_granularity="metadata")
    (manifest,) = list_manifests(metadata)
    rows = read_rows(metadata, [manifest])
    reader = scanner_for(metadata).create_reader()
    assert [row["name"] for row in rows] == ["recorder", "vehicle"]
    assert [m.num_rows for m in reader.read_metadata(manifest)] == [2]


def test_record_scan_does_not_decompress_message_chunks(tmp_path, monkeypatch):
    """Scanning an unindexed file for its records decompresses no message chunk."""
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
        datasource = MCAPDatasourceV2([path], read_granularity=granularity)
        (manifest,) = list_manifests(datasource)
        rows = read_rows(datasource, [manifest])
        reader = scanner_for(datasource).create_reader()
        assert [row["name"] for row in rows] == names
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

    read_per_block = [len(read) for _ in scanner.create_reader().read(manifest)]

    assert read_per_block == [1, 2, 3]


def record_indexes(path, granularity):
    """The summary's index records for ``granularity``'s kind of record."""
    summary = summary_of(path)
    if granularity == "attachment":
        return summary.attachment_indexes
    return summary.metadata_indexes


@pytest.mark.parametrize("packed", [True, False], ids=["packed", "listing_rows"])
@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_indexed_records_are_read_in_one_read_each(records_file, granularity, packed):
    """A task reads each record it owns in one read of the length its index
    record gives, rather than field by field. A packed task carries those index
    records and reads no summary; a task built from listing rows reads it first."""
    datasource = MCAPDatasourceV2([records_file], read_granularity=granularity)
    (task,) = pack(datasource) if packed else list_manifests(datasource)
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)
    offsets = sorted(
        index.offset for index in record_indexes(records_file, granularity)
    )

    tables = list(scanner.create_reader().read(task))

    assert sum(table.num_rows for table in tables) == len(offsets)
    # The whole file fits in the summary read's tail: one read when it is read.
    summary_reads = 0 if packed else 1
    assert seek_log.reads[records_file] == summary_reads + len(offsets)
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


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_unindexed_record_scan_reads_through_a_buffer(tmp_path, granularity):
    """A file without a record index is scanned in one buffered pass: the read
    and the count each take the summary read and one buffer fill."""
    path = os.path.join(tmp_path, "noindex.mcap")
    write_file(path, index=False)
    datasource = MCAPDatasourceV2([path], read_granularity=granularity)
    (manifest,) = list_manifests(datasource)
    expected = len(ATTACHMENTS) if granularity == "attachment" else len(METADATA)
    read_log, count_log = SeekLog(), SeekLog()
    scanner = scanner_for(datasource)
    reader = dataclasses.replace(scanner, filesystem=read_log).create_reader()
    counter = dataclasses.replace(scanner, filesystem=count_log).create_reader()

    rows = sum(table.num_rows for table in reader.read(manifest))
    counts = [m.num_rows for m in counter.read_metadata(manifest)]

    assert rows == expected and counts == [expected]
    # Each: the summary read, then one buffer fill for the whole scan.
    assert read_log.reads[path] == 2
    assert count_log.reads[path] == 2


@pytest.mark.parametrize("granularity", ["attachment", "metadata"])
def test_file_cut_short_fails_only_its_own_record_task(
    records_file, tmp_path, granularity
):
    """A file cut short is listed alone, and reading it fails with an error that
    names it, while the other file's records still read."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), records_file)
    datasource = MCAPDatasourceV2(
        [records_file, cut_short], read_granularity=granularity
    )
    manifests = list_manifests(datasource)
    (unreadable,) = [m for m in manifests if cut_short in m.paths]
    readable = [m for m in manifests if cut_short not in m.paths]

    rows = read_rows(datasource, readable)

    assert len(rows) == len(record_indexes(records_file, granularity))
    with pytest.raises(ValueError, match="cut_short.mcap' is not a complete MCAP"):
        read_rows(datasource, [unreadable])


def write_corrupt_attachment(path: str, index: int) -> None:
    """Zero the opcode of attachment ``index`` in the MCAP file at ``path``.

    The summary still indexes the record, so the file is listed as usual, but
    reading that attachment fails.
    """
    offset = summary_of(path).attachment_indexes[index].offset
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(bytes(1))


@pytest.mark.parametrize("max_errored_blocks", [1, 0])
def test_a_record_task_reads_past_a_file_with_a_corrupt_record(
    tmp_path, max_errored_blocks, restore_max_errored_blocks
):
    """A record task holding three files, the middle one with a corrupt
    attachment, also yields the attachments of the file after it when errors can
    be skipped, then raises naming the bad file. With ``max_errored_blocks=0`` it
    stops at the bad file."""
    from ray.data.context import DataContext

    a, b, c = (os.path.join(tmp_path, f"{name}.mcap") for name in "abc")
    for path in (a, b, c):
        write_file(path)
    write_corrupt_attachment(b, 1)
    DataContext.get_current().max_errored_blocks = max_errored_blocks
    datasource = MCAPDatasourceV2([a, b, c], read_granularity="attachment")
    # The three small files share one read task.
    (task,) = pack(datasource, preserve_order=True)

    paths = []
    with pytest.raises(Exception) as error:
        for table in scanner_for(datasource).create_reader().read(task):
            paths.extend(table.column("path").to_pylist())

    read = [a, c] if max_errored_blocks else [a]
    assert paths == [path for path in read for _ in ATTACHMENTS]
    assert "b.mcap" in str(error.value)


def test_record_the_summary_no_longer_indexes_fails_naming_the_file(records_file):
    """A record listed from an earlier version of the file fails its read with an
    error that names the file, rather than reading whatever sits at its offset."""
    datasource = MCAPDatasourceV2([records_file], read_granularity="attachment")
    (manifest,) = list_manifests(datasource)
    # Rewritten after listing with its first attachment only.
    write_file(records_file, attachments=ATTACHMENTS[:1])

    with pytest.raises(ValueError, match="run.mcap' indexes no Attachment record"):
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
    ``message_types``, and ``time_range`` for metadata."""
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
