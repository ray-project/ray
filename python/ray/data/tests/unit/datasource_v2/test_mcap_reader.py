"""Unit tests for reading MCAP chunk rows into tables.

The scanner and reader are driven directly, as a ``ReadFiles`` task does, with
no Ray cluster.
"""

import dataclasses
import importlib.util
import json
import os
import time

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap import mcap_message_rows
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import ChunkReads
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
    message_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ROW_ID_COLUMN,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    CHUNKED_FILE_MESSAGES,
    MIXED_TOPICS,
    SHARED_SCHEMA,
    STEP,
    SeekLog,
    chunks_read,
    infer_schema,
    list_manifests,
    mixed_encoding_messages,
    pack,
    read_all,
    record_parsed_chunks,
    round_robin_messages,
    scanner_for,
    selected_messages,
    summary_of,
    write_corrupt_chunk,
    write_corrupt_chunk_header,
    write_cut_short,
    write_mcap,
    write_mcap_without_summary_schemas,
    write_mixed_channels,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


@pytest.mark.parametrize("include_metadata", [True, False])
def test_reader_tables_match_inferred_schema(chunked_file, include_metadata):
    """The tables a read yields match the planned schema, with or without the
    metadata columns."""
    datasource = MCAPDatasourceV2(
        [chunked_file],
        include_metadata=include_metadata,
        include_row_id=True,
        include_paths=True,
    )
    (manifest,) = list_manifests(datasource)
    schema = infer_schema(datasource)

    table = read_all(datasource, manifest)

    assert table.schema.equals(schema), (table.schema, schema)
    assert table.num_rows == CHUNKED_FILE_MESSAGES
    assert table.column("path").to_pylist() == [chunked_file] * CHUNKED_FILE_MESSAGES
    if include_metadata:
        assert pa.types.is_dictionary(table.schema.field("schema_data").type)
        assert table.column("channel_metadata").to_pylist()[0] == '{"origin":"/a"}'
        assert set(table.column_names) >= {"channel_id", "schema_name"}
    else:
        assert "channel_id" not in table.column_names


def test_binary_payloads_stay_bytes(tmp_path):
    """CDR payloads are planned and read as ``binary``, byte for byte."""
    path = os.path.join(tmp_path, "cdr.mcap")
    messages = round_robin_messages(3)
    for message in messages:
        message["data"] = bytes([message["data"]["seq"]]) * 4
    write_mcap(path, messages, message_encoding="cdr")
    datasource = MCAPDatasourceV2([path])

    schema = infer_schema(datasource)
    (manifest,) = list_manifests(datasource)
    table = read_all(datasource, manifest)

    assert schema.field("data").type == pa.binary()
    assert table.schema.equals(schema)
    assert table.column("data").to_pylist() == [b"\x00" * 4, b"\x01" * 4, b"\x02" * 4]


def test_invalid_json_fails_naming_the_message(tmp_path):
    """A malformed payload on a JSON topic fails the read, naming its chunk and
    message."""
    path = os.path.join(tmp_path, "bad.mcap")
    messages = round_robin_messages(3, topics=("/json",))
    messages[1]["data"] = b"{not json"
    write_mcap(path, messages)
    datasource = MCAPDatasourceV2([path])
    assert pa.types.is_struct(infer_schema(datasource).field("data").type)

    with pytest.raises(ValueError, match=r"#c=\d+:0: message on JSON-encoded topic"):
        read_all(datasource, list_manifests(datasource)[0])


def test_reading_a_file_cut_short_fails_naming_it(tmp_path, chunked_file):
    """The read task of a file whose summary cannot be read fails with an error
    that names the file and says why."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), chunked_file)
    datasource = MCAPDatasourceV2([chunked_file, cut_short])
    (manifest,) = [m for m in list_manifests(datasource) if cut_short in m.paths]

    with pytest.raises(ValueError, match="cut_short.mcap' is not a complete MCAP"):
        read_all(datasource, manifest)


CORRUPT_CHUNK = 4  # the chunk, and so the message, a bad file loses


def write_task_files(tmp_path, bad, corrupt=write_corrupt_chunk):
    """Write ``a.mcap``, ``b.mcap`` and ``c.mcap`` (nine messages, one per chunk)
    and corrupt chunk ``CORRUPT_CHUNK`` of the files named in ``bad`` with
    ``corrupt``; return the three paths."""
    paths = [os.path.join(tmp_path, f"{name}.mcap") for name in "abc"]
    for path in paths:
        write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
        if os.path.basename(path) in bad:
            corrupt_in_place(path, CORRUPT_CHUNK, corrupt)
    return paths


def corrupt_in_place(path, index, corrupt=write_corrupt_chunk):
    """Corrupt chunk ``index`` of the MCAP file ``path`` with ``corrupt``."""
    corrupt(path + ".tmp", path, index)
    os.replace(path + ".tmp", path)


def read_one_task(paths, max_errored_blocks=1, **options):
    """Read ``paths`` as one read task under ``max_errored_blocks``, with the
    datasource ``options``; return the tables it yields and its error."""
    from ray.data.context import DataContext

    DataContext.get_current().max_errored_blocks = max_errored_blocks
    datasource = MCAPDatasourceV2(paths, include_paths=True, **options)
    # preserve_order keeps the files in the order given; the listing threads
    # otherwise finish in any order.
    task = FileManifest.concat(list_manifests(datasource, preserve_order=True))
    tables = []
    with pytest.raises(Exception) as error:
        for table in scanner_for(datasource).create_reader().read(task):
            tables.append(table)
    return tables, error.value


def rows_of(tables):
    """The ``(path, seq)`` of each row of ``tables``, in order."""
    return [
        (row["path"], row["data"]["seq"])
        for table in tables
        for row in table.select(["path", "data"]).to_pylist()
    ]


def file_rows(path, skipped=()):
    """The ``(path, seq)`` rows of a file of ``round_robin_messages``, in order,
    without the messages of the ``skipped`` chunks (one message per chunk)."""
    return [(path, seq) for seq in range(CHUNKED_FILE_MESSAGES) if seq not in skipped]


def chunk_offset(path, index):
    """The byte offset of chunk ``index`` of the MCAP file ``path``."""
    return summary_of(path).chunk_indexes[index].chunk_start_offset


CORRUPTIONS = {"payload": write_corrupt_chunk, "header": write_corrupt_chunk_header}


@pytest.mark.parametrize("log_time_order", [True, False])
@pytest.mark.parametrize("corrupt", CORRUPTIONS.values(), ids=CORRUPTIONS.keys())
def test_a_corrupt_chunk_costs_only_its_own_rows(
    tmp_path, corrupt, log_time_order, restore_max_errored_blocks
):
    """When errors can be skipped, a task whose middle file has a corrupt chunk
    yields every other row of the three files, then raises naming that file and
    chunk. That holds for a payload that does not decompress and a record that
    does not parse, in log-time and in file order."""
    a, b, c = write_task_files(tmp_path, bad={"b.mcap"}, corrupt=corrupt)

    tables, error = read_one_task([a, b, c], log_time_order=log_time_order)

    skipped = {CORRUPT_CHUNK}
    assert rows_of(tables) == file_rows(a) + file_rows(b, skipped) + file_rows(c)
    assert isinstance(error, ValueError)
    assert str(error).startswith(
        f"Skipped 1 corrupt chunk at byte offset {chunk_offset(b, CORRUPT_CHUNK)} "
        f"of MCAP file {b!r}: "
    )


def test_one_error_names_every_corrupt_chunk_of_a_file(
    tmp_path, restore_max_errored_blocks
):
    """A file with two corrupt chunks, one of each kind, loses only their two
    rows, and one error names both chunks."""
    a, b, c = write_task_files(tmp_path, bad=set())
    corrupt_in_place(b, 2, write_corrupt_chunk)
    corrupt_in_place(b, 6, write_corrupt_chunk_header)

    tables, error = read_one_task([a, b, c])

    assert rows_of(tables) == file_rows(a) + file_rows(b, {2, 6}) + file_rows(c)
    assert str(error).startswith(
        f"Skipped 2 corrupt chunks at byte offsets {chunk_offset(b, 2)}, "
        f"{chunk_offset(b, 6)} of MCAP file {b!r}: "
    )


def test_a_task_names_every_file_it_could_not_read_in_full(
    tmp_path, restore_max_errored_blocks
):
    """Two files with a corrupt chunk fail their task once, with one error that
    names both, after every other row of the three files is read."""
    a, b, c = write_task_files(tmp_path, bad={"a.mcap", "c.mcap"})

    tables, error = read_one_task([a, b, c], max_errored_blocks=-1)

    skipped = {CORRUPT_CHUNK}
    expected = file_rows(a, skipped) + file_rows(b) + file_rows(c, skipped)
    assert rows_of(tables) == expected
    assert isinstance(error, ValueError)
    assert "2 MCAP files of this read task could not be read in full" in str(error)
    assert a in str(error) and c in str(error)


def test_a_task_fails_at_its_first_corrupt_chunk_when_errors_are_not_skipped(
    tmp_path, restore_max_errored_blocks
):
    """With ``max_errored_blocks=0`` the read fails anyway, so the task raises at
    the corrupt chunk without reading the file after it."""
    a, b, c = write_task_files(tmp_path, bad={"b.mcap"})

    tables, error = read_one_task([a, b, c], max_errored_blocks=0)

    # b's rows before the corrupt chunk were still being built into a table.
    assert rows_of(tables) == file_rows(a)
    assert "b.mcap" in str(error)


def test_an_error_reading_the_file_is_not_skipped_as_a_corrupt_chunk(
    tmp_path, monkeypatch
):
    """An ``OSError`` while a chunk is decoded, here reading back an earlier
    chunk for a schema, raises even when corrupt chunks are skipped, so the
    file's read is retried rather than the chunk dropped."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_chunks

    path = os.path.join(tmp_path, "schemas_in_chunks.mcap")
    write_mcap_without_summary_schemas(path)
    summary = summary_of(path)
    # Chunks 2 to 5 declare no schema, so reading them reads chunk 0 back.
    owned = {c.chunk_start_offset for c in summary.chunk_indexes[2:]}

    def read_chunk(f, chunk_index):
        raise OSError("connection reset")

    monkeypatch.setattr(mcap_chunks, "read_chunk", read_chunk)
    reader = SelectedMessageReader(MCAPSelection(), log_time_order=False)
    reported = []

    with open(path, "rb") as f, pytest.raises(OSError, match="connection reset"):
        for _ in reader.iter_chunks(
            f, path, summary, owned, on_corrupt=lambda c, e: reported.append(c)
        ):
            pass

    assert reported == []


def test_a_corrupt_chunk_read_back_for_a_schema_costs_only_itself(tmp_path):
    """A task reading back for a schema skips an earlier corrupt chunk and finds
    the schema in the next one, so its own chunk is not reported as corrupt."""
    clean = os.path.join(tmp_path, "clean.mcap")
    write_mcap_without_summary_schemas(clean, b_declared_in_chunk_1=True)
    path = write_corrupt_chunk(os.path.join(tmp_path, "bad.mcap"), clean, 0)
    summary = summary_of(path)
    # The task owns chunks 3 to 5. Chunk 3 (/b) reads back through chunk 0,
    # which is corrupt, to chunk 1, which declares other_schema.
    owned = {c.chunk_start_offset for c in summary.chunk_indexes[3:]}
    reader = SelectedMessageReader(MCAPSelection(), log_time_order=False)
    reported = []

    with open(path, "rb") as f:
        selected = list(
            reader.iter_chunks(
                f, path, summary, owned, on_corrupt=lambda c, e: reported.append(c)
            )
        )

    assert [channel.topic for _, channel, _, _ in selected] == ["/b", "/a", "/b"]
    assert reported == []


def test_a_retried_read_reports_only_its_own_corrupt_chunks(
    chunked_file, monkeypatch, restore_max_errored_blocks
):
    """A file read that is retried starts with no corrupt chunks recorded, so a
    chunk the failed attempt recorded but the retry reads cleanly raises no
    error."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_chunks
    from ray.data.context import DataContext

    context = DataContext.get_current()
    context.max_errored_blocks = 1
    monkeypatch.setattr(context, "retried_io_errors", ["connection reset"])
    # Skip the retry's backoff wait.
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    real_read_chunks = mcap_chunks.read_chunks
    attempts = []

    def read_chunks(f, chunk_indexes, reads, on_corrupt=None):
        # The first attempt records its first chunk as corrupt, then fails.
        attempts.append(1)
        if len(attempts) == 1:
            assert on_corrupt is not None
            on_corrupt(list(chunk_indexes)[0], ValueError("stale"))
            raise OSError("connection reset")
        yield from real_read_chunks(f, chunk_indexes, reads, on_corrupt)

    monkeypatch.setattr(mcap_chunks, "read_chunks", read_chunks)
    datasource = MCAPDatasourceV2([chunked_file])

    table = read_all(datasource, *list_manifests(datasource))

    assert len(attempts) == 2
    assert table.num_rows == CHUNKED_FILE_MESSAGES


# Small files that share one read task, and which of them have a corrupt chunk:
# the first and the last are among the five.
TASK_FILES = 8
BAD_FILES = {1: [3], 5: [0, 2, 3, 5, 7]}


@pytest.mark.parametrize("num_bad", BAD_FILES.keys())
def test_a_corrupt_chunk_in_a_shared_read_costs_only_its_own_rows(
    tmp_path, num_bad, restore_max_errored_blocks
):
    """A corrupt chunk fetched in one ranged read with its neighbours costs only
    its own rows: the bad files and the task's other files keep every other
    row, and the error names each bad file, with one and with five bad files of
    eight."""
    from ray.data.context import DataContext

    paths = [os.path.join(tmp_path, f"f{i}.mcap") for i in range(TASK_FILES)]
    for path in paths:
        write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    bad = [paths[i] for i in BAD_FILES[num_bad]]
    for path in bad:
        corrupt_in_place(path, CORRUPT_CHUNK)
    # One allowed error is enough: the task raises once for all its bad files.
    DataContext.get_current().max_errored_blocks = 1
    datasource = MCAPDatasourceV2(paths, include_paths=True)
    # The eight small files share one read task, which carries their summaries.
    (task,) = pack(datasource, preserve_order=True)
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)

    tables = []
    with pytest.raises(Exception) as error:
        for table in scanner.create_reader().read(task):
            tables.append(table)

    # Each file, bad ones too, is one read from its first chunk: the corrupt
    # chunk came in the same read as its neighbours, and no summary was read.
    for path in paths:
        assert seek_log.seeks[path] == [chunk_offset(path, 0)]
    in_task_order = [str(p) for p in dict.fromkeys(task.paths.tolist())]
    good = [path for path in in_task_order if path not in bad]
    expected = [
        row
        for path in in_task_order
        for row in file_rows(path, () if path in good else {CORRUPT_CHUNK})
    ]
    assert rows_of(tables) == expected
    assert all(path in str(error.value) for path in bad)
    assert not any(path in str(error.value) for path in good)


def test_non_json_channel_under_a_decoded_plan_fails(tmp_path):
    """A read planned as decoded JSON fails on a non-JSON file the sample missed."""
    json_path = os.path.join(tmp_path, "json.mcap")
    write_mcap(json_path, round_robin_messages(3, topics=("/json",)))
    cdr_path = os.path.join(tmp_path, "cdr.mcap")
    write_mcap(
        cdr_path,
        [m for m in mixed_encoding_messages() if m["topic"] == "/cdr"],
        message_encoding="cdr",
    )
    planned = MCAPDatasourceV2([json_path])
    scanner = scanner_for(planned)
    assert scanner.decodes_json

    other = MCAPDatasourceV2([cdr_path])
    with pytest.raises(ValueError, match="'/cdr' is 'cdr'-encoded"):
        read_all(other, list_manifests(other)[0], scanner=scanner)


def test_path_partitioning_adds_columns(tmp_path):
    """Hive partitioning adds a ``vehicle`` column from the path, typed at planning."""
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    path = os.path.join(tmp_path, "vehicle=v7", "run.mcap")
    os.makedirs(os.path.dirname(path))
    write_mcap(path, round_robin_messages(3))
    datasource = MCAPDatasourceV2(
        [path], partitioning=Partitioning(PartitionStyle.HIVE)
    )

    schema = infer_schema(datasource)
    assert schema.field("vehicle").type == pa.string()

    table = read_all(datasource, list_manifests(datasource)[0])

    assert table.schema.equals(schema)
    assert table.column("vehicle").to_pylist() == ["v7"] * 3


def test_partition_key_naming_a_message_column_keeps_the_messages(tmp_path, caplog):
    """A ``topic=camera`` folder does not replace the messages' topics.

    The path value is ignored with a warning.
    """
    from ray.data._internal.datasource_v2.formats.mcap import mcap_reader
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    path = os.path.join(tmp_path, "topic=camera", "vehicle=v7", "run.mcap")
    os.makedirs(os.path.dirname(path))
    write_mcap(path, round_robin_messages(3))
    datasource = MCAPDatasourceV2(
        [path], partitioning=Partitioning(PartitionStyle.HIVE)
    )
    schema = infer_schema(datasource)
    assert schema.field("topic").type == pa.string()

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_reader.logger.addHandler(caplog.handler)
    try:
        table = read_all(datasource, list_manifests(datasource)[0])
    finally:
        mcap_reader.logger.removeHandler(caplog.handler)

    assert table.column("topic").to_pylist() == ["/a", "/b", "/c"]
    assert table.column("vehicle").to_pylist() == ["v7"] * 3
    assert any("names a message column" in r.message for r in caplog.records)


def test_splits_cover_the_file_exactly_once_with_stable_ids(chunked_file):
    """Two tasks splitting a file read each message once, with the row ids and
    order of a whole-file read."""
    datasource = MCAPDatasourceV2([chunked_file], include_row_id=True)
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    whole = read_all(datasource, manifest, scanner=scanner)
    # Two tasks: chunks {0, 1, 2, 3} and {4, ..., 8}, as a packer might cut them.
    block = manifest.as_block()
    first = read_all(datasource, FileManifest(block.slice(0, 4)), scanner=scanner)
    second = read_all(datasource, FileManifest(block.slice(4)), scanner=scanner)

    split_ids = (
        first.column(ROW_ID_COLUMN).to_pylist()
        + second.column(ROW_ID_COLUMN).to_pylist()
    )
    assert split_ids == whole.column(ROW_ID_COLUMN).to_pylist()
    assert len(set(split_ids)) == CHUNKED_FILE_MESSAGES
    assert (
        first.column("log_time").to_pylist() + second.column("log_time").to_pylist()
        == whole.column("log_time").to_pylist()
    )


def test_row_id_does_not_depend_on_the_selection(chunked_file):
    """A read of ``/b`` alone gives its messages the row ids of a read of every
    topic."""
    everything = MCAPDatasourceV2([chunked_file], include_row_id=True)
    (manifest,) = list_manifests(everything)
    whole = read_all(everything, manifest)
    by_row = dict(
        zip(
            whole.column("log_time").to_pylist(),
            whole.column(ROW_ID_COLUMN).to_pylist(),
        )
    )

    only_b = MCAPDatasourceV2([chunked_file], topics=["/b"], include_row_id=True)
    (pruned,) = list_manifests(only_b)
    subset = read_all(only_b, pruned)

    assert subset.num_rows == 3
    for log_time, row_id in zip(
        subset.column("log_time").to_pylist(), subset.column(ROW_ID_COLUMN).to_pylist()
    ):
        assert by_row[log_time] == row_id


def test_log_time_order_merges_overlapping_chunks(tmp_path):
    """Chunks that overlap in time are read in log-time order, or in file order
    with ``log_time_order=False``."""
    # Chunk 0 holds times 10 and 30, chunk 1 holds 20.
    path = os.path.join(tmp_path, "overlap.mcap")
    messages = [
        {"topic": "/a", "data": {"t": 10}, "log_time": 10},
        {"topic": "/a", "data": {"t": 30}, "log_time": 30},
        {"topic": "/a", "data": {"t": 20}, "log_time": 20},
    ]
    write_mcap(path, messages, chunk_size=60)
    summary = summary_of(path)
    assert len(summary.chunk_indexes) == 2, [
        (c.message_start_time, c.message_end_time) for c in summary.chunk_indexes
    ]

    ordered = MCAPDatasourceV2([path])
    (manifest,) = list_manifests(ordered)
    table = read_all(ordered, manifest)
    assert table.column("log_time").to_pylist() == [10, 20, 30]

    as_written = MCAPDatasourceV2([path], log_time_order=False)
    table = read_all(as_written, manifest)
    assert table.column("log_time").to_pylist() == [10, 30, 20]


@pytest.fixture
def unordered_unindexed_file(tmp_path):
    """Four messages on ``/a`` written out of log-time order, with no chunk index.

        seq        0   1   2   3
        log time   20  10  30  10

    Messages 1 and 3 tie at log time 10.
    """
    path = os.path.join(tmp_path, "unindexed.mcap")
    messages = [
        {"topic": "/a", "data": {"seq": seq}, "log_time": log_time}
        for seq, log_time in enumerate((20, 10, 30, 10))
    ]
    write_mcap(path, messages, index=False)
    return path


@pytest.mark.parametrize(
    "log_time_order, limit, expected_seqs",
    [
        (True, None, [1, 3, 0, 2]),
        (True, 2, [1, 3]),
        (False, None, [0, 1, 2, 3]),
        (False, 2, [0, 1]),
    ],
)
def test_file_without_a_chunk_index_is_sorted_by_its_task(
    unordered_unindexed_file, log_time_order, limit, expected_seqs
):
    """A file without a chunk index comes out in log-time order, ties in file
    order, and a limit keeps its earliest messages. Without ``log_time_order``
    it comes out as written."""
    datasource = MCAPDatasourceV2(
        [unordered_unindexed_file], log_time_order=log_time_order, include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)
    if limit is not None:
        scanner = scanner.push_limit(limit)

    table = read_all(datasource, manifest, scanner=scanner)

    assert [value["seq"] for value in table.column("data").to_pylist()] == expected_seqs
    # A row id is the message's position in the file, whatever the row order.
    row_ids = table.column(ROW_ID_COLUMN).to_pylist()
    assert row_ids == [f"{unordered_unindexed_file}#m={seq}" for seq in expected_seqs]


@pytest.mark.parametrize("log_time_order", [True, False])
def test_limit_parses_only_the_chunks_it_needs(
    chunked_file, log_time_order, monkeypatch
):
    """A limit of two parses the two chunks holding the first two messages and no
    other chunk, in log-time and in file order. The small file's chunks are
    fetched in one ranged read."""
    datasource = MCAPDatasourceV2([chunked_file], log_time_order=log_time_order)
    (manifest,) = list_manifests(datasource)
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)
    parsed = record_parsed_chunks(monkeypatch)

    table = read_all(datasource, manifest, scanner=scanner.push_limit(2))

    assert table.num_rows == 2
    offsets = sorted(
        c.chunk_start_offset for c in summary_of(chunked_file).chunk_indexes
    )
    assert parsed == offsets[:2]
    assert chunks_read(seek_log, chunked_file) == offsets[:1]


def selected_rows(path, chunk_reads, topics=None, log_time_order=True, f=None):
    """``(log_time, row_id)`` of each message the reader selects from ``path``."""
    reader = SelectedMessageReader(
        MCAPSelection.create(topics, None, None), log_time_order, chunk_reads
    )
    stream = f if f is not None else open(path, "rb")
    with stream:
        messages = reader.iter_chunks(stream, path, summary_of(path), None)
        return [(message.log_time, row_id) for _, _, message, row_id in messages]


# One chunk per read; the defaults; reads too small for most runs; one read
# spanning the whole file, gaps and all.
CHUNK_READS = {
    "one_per_chunk": ChunkReads(max_bytes=0, max_gap_bytes=0, first_bytes=0),
    "default": ChunkReads(),
    "small": ChunkReads(max_bytes=400, max_gap_bytes=100, first_bytes=100),
    "whole_file": ChunkReads(max_bytes=1 << 30, max_gap_bytes=1 << 30),
}


@pytest.mark.parametrize("log_time_order", [True, False])
@pytest.mark.parametrize("topics", [None, ["/a"]])
@pytest.mark.parametrize("chunk_reads", CHUNK_READS.values(), ids=CHUNK_READS.keys())
def test_rows_do_not_depend_on_how_chunks_are_fetched(
    chunked_file, log_time_order, topics, chunk_reads
):
    """The selected messages, their order and their row ids are the same whether
    each chunk is fetched alone or with its neighbours, with or without the gaps
    a topic filter leaves."""
    expected = selected_rows(
        chunked_file, CHUNK_READS["one_per_chunk"], topics, log_time_order
    )

    rows = selected_rows(chunked_file, chunk_reads, topics, log_time_order)

    assert rows == expected
    assert len(rows) == (3 if topics else CHUNKED_FILE_MESSAGES)


@pytest.mark.parametrize(
    "max_gap_bytes, expected_reads",
    # /a is in chunks 0, 3 and 6: a 1 KiB gap spans the two chunks in between.
    [(0, [0, 3, 6]), (1024, [0])],
)
def test_a_gap_up_to_the_limit_is_read_through(
    chunked_file, max_gap_bytes, expected_reads
):
    """Chunks a topic filter selects share one read when the chunks between them
    fit in ``max_gap_bytes``, and are read one by one otherwise."""
    seek_log = SeekLog()
    chunk_reads = ChunkReads(max_bytes=1 << 20, max_gap_bytes=max_gap_bytes)

    selected_rows(
        chunked_file,
        chunk_reads,
        topics=["/a"],
        f=seek_log.open_input_file(chunked_file),
    )

    offsets = sorted(
        c.chunk_start_offset for c in summary_of(chunked_file).chunk_indexes
    )
    assert chunks_read(seek_log, chunked_file) == [offsets[i] for i in expected_reads]


def test_log_time_merge_fetches_chunks_in_the_order_it_expands_them(tmp_path):
    """The merge expands chunks by first log time. A chunk that comes later in
    time but earlier in the file starts a new read, and the rows stay in
    log-time order."""
    # One message per chunk; chunk 1 holds the earliest time.
    path = os.path.join(tmp_path, "out_of_order.mcap")
    messages = [
        {"topic": "/a", "data": {"seq": seq}, "log_time": log_time}
        for seq, log_time in enumerate((20, 10, 30))
    ]
    write_mcap(path, messages)
    offsets = sorted(c.chunk_start_offset for c in summary_of(path).chunk_indexes)
    seek_log = SeekLog()

    rows = selected_rows(path, ChunkReads(), f=seek_log.open_input_file(path))

    assert [log_time for log_time, _ in rows] == [10, 20, 30]
    # Chunk 1 is read alone; chunk 0 then starts a read that spans chunk 2.
    assert chunks_read(seek_log, path) == [offsets[1], offsets[0]]


def test_limit_never_decodes_the_messages_past_it(tmp_path):
    """Messages past the limit are never added to a table, so a malformed JSON
    payload after them does not fail the read."""
    path = os.path.join(tmp_path, "late_garbage.mcap")
    messages = round_robin_messages(4, topics=("/json",))
    messages[3]["data"] = b"{not json"
    # One chunk holds every message, so the bad payload is read either way.
    write_mcap(path, messages, chunk_size=1 << 20)
    datasource = MCAPDatasourceV2([path])
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    table = read_all(datasource, manifest, scanner=scanner.push_limit(3))

    assert [value["seq"] for value in table.column("data").to_pylist()] == [0, 1, 2]
    with pytest.raises(ValueError, match="not valid JSON"):
        read_all(datasource, manifest, scanner=scanner)


def test_limit_met_by_the_first_file_never_opens_the_second(tmp_path):
    """A task whose limit is met by its first file does not open its second."""
    first = os.path.join(tmp_path, "first.mcap")
    second = os.path.join(tmp_path, "second.mcap")
    for path in (first, second):
        write_mcap(path, round_robin_messages(3))
    datasource = MCAPDatasourceV2([first, second])
    # Listed one file at a time, so the task's manifest names ``first`` first.
    manifests = [list_manifests(MCAPDatasourceV2([p]))[0] for p in (first, second)]
    one_task = FileManifest(pa.concat_tables([m.as_block() for m in manifests]))
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)

    table = read_all(datasource, one_task, scanner=scanner.push_limit(3))

    assert table.column("log_time").to_pylist() == [
        m["log_time"] for m in round_robin_messages(3)
    ]
    assert first in seek_log.seeks
    assert second not in seek_log.seeks


def test_reader_cuts_tables_at_target_block_size(chunked_file):
    """A 1-byte target block size yields a table per message, and no target yields
    one table."""
    datasource = MCAPDatasourceV2([chunked_file])
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    one_byte = dataclasses.replace(scanner, target_block_size=1)
    tables = list(one_byte.create_reader().read(manifest))
    assert len(tables) == CHUNKED_FILE_MESSAGES
    assert all(t.num_rows == 1 for t in tables)

    unbounded = dataclasses.replace(scanner, target_block_size=None)
    assert len(list(unbounded.create_reader().read(manifest))) == 1


def test_scanner_prunes_columns_and_pushes_limit(chunked_file):
    """A pruned, limited scanner reads only its columns and rows, and an empty
    projection still yields every row."""
    datasource = MCAPDatasourceV2([chunked_file], include_paths=True)
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    assert isinstance(scanner, MCAPScanner)
    pruned = scanner.prune_columns(["log_time", "path"]).push_limit(4)
    assert pruned.read_schema().names == ["log_time", "path"]
    assert pruned.pushed_limit() == 4

    table = pa.concat_tables(list(pruned.create_reader().read(manifest)))
    assert table.column_names == ["log_time", "path"]
    assert table.num_rows == 4

    # An empty projection, as in ``count()``, keeps the row count through a stub column.
    empty = scanner.prune_columns([])
    table = pa.concat_tables(list(empty.create_reader().read(manifest)))
    assert table.num_rows == CHUNKED_FILE_MESSAGES
    assert empty.read_schema().names == []
    assert empty.prune_columns(["topic"]).read_schema().names == []


def test_message_types_filter_matches_legacy_semantics(chunked_file):
    """``message_types`` keeps the channels whose schema it names, within
    ``topics``, as the legacy reader does."""
    selection = MCAPSelection.create(None, None, {"other_schema"})
    summary = summary_of(chunked_file)
    assert selection.selected_channel_ids(summary.channels, summary.schemas) == set()

    selection = MCAPSelection.create(["/a"], None, {"test_schema"})
    selected = selection.selected_channel_ids(summary.channels, summary.schemas)
    assert {summary.channels[cid].topic for cid in selected} == {"/a"}


def rows_built_one_value_at_a_time(path: str) -> pa.Table:
    """The message rows of ``path`` with one Python value per row and column, and
    the dictionary columns encoded afterwards: what the builder must equal."""
    from mcap.reader import make_reader

    schema = message_schema(include_metadata=True, include_row_id=False)
    columns = {name: [] for name in schema.names}
    with open(path, "rb") as f:
        for record_schema, channel, message in make_reader(f).iter_messages():
            values = {
                "data": message.data,
                "topic": channel.topic,
                "log_time": message.log_time,
                "publish_time": message.publish_time,
                "sequence": message.sequence,
                "channel_id": message.channel_id,
                "message_encoding": channel.message_encoding,
                "schema_name": record_schema.name if record_schema else None,
                "schema_encoding": record_schema.encoding if record_schema else None,
                "schema_data": record_schema.data if record_schema else None,
                "channel_metadata": json.dumps(
                    channel.metadata,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ),
            }
            for name, value in values.items():
                columns[name].append(value)
    arrays = [
        pa.array(columns[f.name], f.type.value_type).dictionary_encode()
        if pa.types.is_dictionary(f.type)
        else pa.array(columns[f.name], type=f.type)
        for f in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def test_rows_equal_a_row_by_row_build(tmp_path):
    """Rows built from per-channel slots equal rows built one value at a time.

    The dictionary columns hold each value once: two schemas with the same bytes
    share an entry, as do two channels without metadata, and the schema-less
    channel's ``schema_data`` is null.
    """
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mixed_channels(path)
    datasource = MCAPDatasourceV2([path])

    table = read_all(datasource, *list_manifests(datasource))

    assert table.equals(rows_built_one_value_at_a_time(path))
    (schema_data,) = table.column("schema_data").chunks
    assert schema_data.dictionary.to_pylist() == [SHARED_SCHEMA, b"uint8 c\n"]
    assert schema_data.null_count == table.num_rows // len(MIXED_TOPICS)
    (channel_metadata,) = table.column("channel_metadata").chunks
    assert [json.loads(text) for text in channel_metadata.dictionary.to_pylist()] == [
        {"origin": "/a"},
        {},
        {"ключ": "значение"},
    ]


def test_channel_metadata_is_json_stored_once_per_table():
    """A channel's metadata is stored once per table as JSON text with sorted keys,
    so each row costs a 4-byte index however large the metadata is."""
    from mcap.records import Channel, Message

    # ROS 2 Jazzy writes QoS profiles as YAML, plus a type hash.
    metadata = {
        "type_description_hash": "RIHS01_" + "7d" * 32,
        "offered_qos_profiles": "- history: keep_last\n  depth: 10\n" * 10,
    }
    channel = Channel(
        id=1, topic="/imu", message_encoding="cdr", metadata=metadata, schema_id=0
    )
    builder = _MessageTableBuilder(
        columns={"channel_metadata"},
        include_metadata=True,
        include_row_id=False,
        decode_json=False,
    )
    num_rows = 1_000
    for i in range(num_rows):
        message = Message(
            channel_id=1, log_time=i, data=b"", publish_time=i, sequence=i
        )
        builder.add((None, channel, message, f"/run.mcap#c=0:{i}"))

    column = builder.build().column("channel_metadata")

    (chunk,) = column.chunks
    (text,) = chunk.dictionary.to_pylist()
    assert text == json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    assert json.loads(column[num_rows - 1].as_py()) == metadata
    # The 4-byte indices plus the text once; a map on every row would take
    # about 450 bytes a row.
    assert column.nbytes < 5 * num_rows


@pytest.mark.parametrize("include_metadata", [True, False])
@pytest.mark.parametrize("include_row_id", [True, False])
def test_estimated_bytes_match_the_built_table(
    tmp_path, include_metadata, include_row_id
):
    """``estimated_bytes`` is the size of the table ``build`` returns, every column
    counted, ``row_id`` included."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mixed_channels(path, num_messages=400)
    builder = _MessageTableBuilder(
        columns=None,
        include_metadata=include_metadata,
        include_row_id=include_row_id,
        decode_json=False,
    )
    for selected in selected_messages(path):
        builder.add(selected)
    estimated = builder.estimated_bytes

    table = builder.build()

    # Arrow adds one offset per column and a validity bitmap where there are nulls.
    assert estimated <= table.nbytes <= 1.01 * estimated


def test_reader_cuts_tables_at_the_target_counting_every_column(tmp_path):
    """With ``row_id`` and ``path`` on, each table but the last is cut as soon as
    it reaches the target size."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mixed_channels(path, num_messages=400)
    datasource = MCAPDatasourceV2([path], include_row_id=True, include_paths=True)
    (manifest,) = list_manifests(datasource)
    target = 8 * 1024
    scanner = dataclasses.replace(scanner_for(datasource), target_block_size=target)

    tables = list(scanner.create_reader().read(manifest))

    assert len(tables) > 1
    # One row of about 350 bytes past the target, at most.
    assert all(target <= t.nbytes <= 1.05 * target for t in tables[:-1])


def test_builder_holds_rows_compactly():
    """Accumulated rows cost about their payload and ``row_id``, not a Python object
    per value: rows of 100-byte payloads and 2 KB schemas stay under 230 bytes."""
    import tracemalloc

    from mcap.records import Channel, Message, Schema

    schema = Schema(
        id=1, name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b"x" * 2048
    )
    channel = Channel(
        id=1,
        topic="/imu",
        message_encoding="cdr",
        metadata={"offered_qos_profiles": "q" * 400},
        schema_id=1,
    )
    builder = _MessageTableBuilder(
        columns=None, include_metadata=True, include_row_id=True, decode_json=False
    )
    num_rows = 20_000

    tracemalloc.start()
    try:
        start = tracemalloc.get_traced_memory()[0]
        for i in range(num_rows):
            # A fresh record per row, as reading a chunk makes.
            log_time = BASE_TIME + i * STEP
            message = Message(
                channel_id=1,
                log_time=log_time,
                data=bytes([i % 256]) * 100,
                publish_time=log_time + 1,
                sequence=i,
            )
            builder.add((schema, channel, message, f"/data/run.mcap#c=0:{i}"))
        per_row = (tracemalloc.get_traced_memory()[0] - start) / num_rows
    finally:
        tracemalloc.stop()

    # 104 for the payload, about 28 for row_id, 24 for the times and sequence, 4
    # for the channel slot, plus array over-allocation.
    assert per_row < 230


def test_build_empties_the_builder(chunked_file):
    """``build`` leaves the builder empty, so the next table holds only the rows
    added after it."""
    builder = _MessageTableBuilder(
        columns=None, include_metadata=True, include_row_id=True, decode_json=True
    )
    messages = list(selected_messages(chunked_file))
    for selected in messages[:4]:
        builder.add(selected)
    first = builder.build()
    assert (builder.num_rows, builder.estimated_bytes) == (0, 0)

    for selected in messages[4:]:
        builder.add(selected)
    second = builder.build()

    assert first.column("log_time").to_pylist() == [
        BASE_TIME + i * STEP for i in range(4)
    ]
    assert second.num_rows == CHUNKED_FILE_MESSAGES - 4
    expected_ids = [row_id for _, _, _, row_id in messages[4:]]
    assert second.column(ROW_ID_COLUMN).to_pylist() == expected_ids


def test_payload_buffer_starts_a_chunk_before_its_offsets_overflow(monkeypatch):
    """A ``binary`` buffer is sealed into its own chunk before its int32 offsets
    would overflow, and the chunks keep the values in order."""
    # A 10-byte limit stands in for 2 GiB.
    monkeypatch.setattr(mcap_message_rows, "_MAX_BUFFER_BYTES", 10)
    values = [b"abcd", b"efgh", b"ijk", b"", b"lmnopqrstu", b"v"]
    buffer = mcap_message_rows._VarWidthBuffer(pa.binary())
    for value in values:
        buffer.append(value)

    column = buffer.finish()

    assert column.to_pylist() == values
    assert [len(chunk) for chunk in column.chunks] == [2, 2, 1, 1]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
