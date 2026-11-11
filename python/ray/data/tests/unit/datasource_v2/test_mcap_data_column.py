"""Unit tests for planning the type of the MCAP ``data`` column.

Some tests call ``infer_data_type`` with explicit inputs, others plan and read
through the datasource. None starts a Ray cluster. ``write_files`` gives each
JSON file its own payload key, so a planned struct type names the file it was
typed from.
"""

import importlib.util
import json
import os
from typing import Dict, List, Optional

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap import mcap_data_column
from ray.data._internal.datasource_v2.formats.mcap.mcap_data_column import (
    infer_data_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    STEP,
    SeekLog,
    chunks_read,
    infer_schema,
    list_manifests,
    mixed_encoding_messages,
    read_all,
    round_robin_messages,
    summary_of,
    without_summary_records,
    write_corrupt_chunk,
    write_corrupt_chunk_header,
    write_mcap,
    write_mcap_without_summary_schemas,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

ONLY_A = MCAPSelection.create(["/a"], None, None)


def write_channel(
    path, *, topic="/a", encoding="json", times=(0,), key="seq", index=True
):
    """Write one channel with a message at each of ``times``, one chunk each.

    JSON payloads are ``{key: i}``.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=1,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL if index else IndexType.NONE,
            use_chunking=index,
            use_statistics=index,
            use_summary_offsets=index,
        )
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b"{}"
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic=topic, message_encoding=encoding
        )
        for i, t in enumerate(times):
            writer.add_message(
                channel_id=channel_id,
                log_time=BASE_TIME + t * STEP,
                publish_time=BASE_TIME + t * STEP,
                data=json.dumps({key: i}).encode() if encoding == "json" else b"\x01",
            )
        writer.finish()
    return path


def write_files(tmp_path, specs: List[dict]) -> List[str]:
    """Write one file per spec, named so that listing order is spec order."""
    return [
        write_channel(os.path.join(tmp_path, f"{i:02d}.mcap"), **spec)
        for i, spec in enumerate(specs)
    ]


def infer(
    paths: List[str],
    *,
    sampled: int = 16,
    selection: Optional[MCAPSelection] = None,
    filesystem=None,
) -> Optional[pa.DataType]:
    """Plan ``data`` for ``paths``, the first ``sampled`` of them as the sample."""
    return infer_data_type(
        selection or MCAPSelection(),
        filesystem or LocalFileSystem(),
        paths[:sampled],
        lambda: iter(paths),
    )


def struct_of(key: str) -> pa.DataType:
    """The type planned for JSON payloads ``{key: i}``."""
    return pa.struct([(key, pa.int64())])


def count_scans(monkeypatch) -> List[str]:
    """Record the path of every file planning scans whole, in order."""
    real_by_scanning = mcap_data_column._FileInspector._by_scanning
    scanned: List[str] = []

    def by_scanning(self, f, path, want_type):
        scanned.append(path)
        return real_by_scanning(self, f, path, want_type)

    monkeypatch.setattr(mcap_data_column._FileInspector, "_by_scanning", by_scanning)
    return scanned


def test_only_the_file_that_types_data_has_a_chunk_read(tmp_path):
    """Planning types ``data`` from the first file with a selected message in
    ``time_range``, reading only its first chunk that may hold one. No other
    file has a chunk read."""
    paths = write_files(
        tmp_path,
        [
            dict(topic="/b", times=(50,), key="other_topic"),
            dict(times=(0, 1), key="out_of_range"),
            dict(times=(45, 50, 55), key="in_range"),
            dict(times=(50,), key="later"),
        ],
    )
    selection = MCAPSelection.create(
        ["/a"], TimeRange(BASE_TIME + 40 * STEP, BASE_TIME + 60 * STEP), None
    )
    seek_log = SeekLog()

    assert infer(paths, selection=selection, filesystem=seek_log) == struct_of(
        "in_range"
    )
    typing_file_chunks = summary_of(paths[2]).chunk_indexes
    assert [chunks_read(seek_log, path) for path in paths] == [
        [],
        [],
        [typing_file_chunks[0].chunk_start_offset],
        [],
    ]


def test_encodings_are_checked_in_every_sampled_file(tmp_path):
    """A non-JSON channel in any sampled file plans ``binary``, even after an
    earlier file typed ``data``. A file past the sample is not checked."""
    paths = write_files(
        tmp_path, [dict(key=f"json_{i}") for i in range(6)] + [dict(encoding="cdr")]
    )
    assert infer(paths) is None
    assert infer(paths, sampled=6) == struct_of("json_0")


def test_unindexed_json_files_are_typed_by_a_scan(tmp_path):
    """Files without a chunk index are scanned, so their JSON is still decoded."""
    paths = write_files(
        tmp_path, [dict(index=False, key=f"json_{i}") for i in range(3)]
    )
    assert infer(paths) == struct_of("json_0")
    cdr = write_channel(os.path.join(tmp_path, "03.mcap"), index=False, encoding="cdr")
    assert infer(paths + [cdr]) is None


def test_at_most_max_scanned_files_are_scanned(tmp_path, monkeypatch):
    """Sampled files past the scan budget are not scanned, so ``data`` is binary.

    The only file with ``/a`` comes after the budget's worth of files without
    a chunk index.
    """
    budget = mcap_data_column._MAX_SCANNED_FILES
    paths = write_files(
        tmp_path,
        [dict(topic="/b", index=False) for _ in range(budget)] + [dict(index=False)],
    )
    scans = count_scans(monkeypatch)

    assert infer(paths, selection=ONLY_A) is None
    assert scans == paths[:budget]


def test_walk_never_scans_a_file_whole(tmp_path, monkeypatch):
    """Past the sample, a file whose summary cannot settle the selection is skipped."""
    paths = write_files(
        tmp_path, [dict(topic="/b") for _ in range(16)] + [dict(index=False)]
    )
    scans = count_scans(monkeypatch)

    assert infer(paths, selection=ONLY_A) is None
    assert scans == []


def test_walk_skips_unreadable_files_and_stops_at_its_cap(tmp_path, monkeypatch):
    """The walk passes over a file that is not MCAP and inspects at most its cap.

    ``list_paths`` is called only when the sample does not type ``data``.
    """
    paths = write_files(tmp_path, [dict(topic="/b") for _ in range(3)])
    not_mcap = os.path.join(tmp_path, "03.mcap")
    with open(not_mcap, "wb") as f:
        f.write(b"not an MCAP file")
    paths += [not_mcap, write_channel(os.path.join(tmp_path, "04.mcap"), key="walked")]

    assert infer(paths, sampled=3, selection=ONLY_A) == struct_of("walked")
    monkeypatch.setattr(mcap_data_column, "_MAX_WALKED_FILES", 1)
    assert infer(paths, sampled=3, selection=ONLY_A) is None

    def no_listing():
        raise AssertionError("listed although the sample types data")

    sampled = paths[4:]
    assert infer_data_type(ONLY_A, LocalFileSystem(), sampled, no_listing) == struct_of(
        "walked"
    )


def test_unreadable_sampled_file_is_skipped(tmp_path):
    """A sampled file that is not MCAP does not fail planning: ``data`` is typed
    from the next file, and the bad file's own read task reports the error."""
    paths = write_files(tmp_path, [dict(key="unread"), dict(key="typed")])
    with open(paths[0], "wb") as f:
        f.write(b"not an MCAP file")

    assert infer(paths, selection=ONLY_A) == struct_of("typed")


# Bytes before a message record's data: the opcode, the record length, the
# channel id, the sequence, and the log and publish times.
MESSAGE_PREFIX = 1 + 8 + 2 + 4 + 8 + 8


@pytest.mark.parametrize(
    "corrupt",
    [write_corrupt_chunk, write_corrupt_chunk_header],
    ids=["payload", "header"],
)
def test_a_sampled_chunk_that_does_not_decode_is_skipped(tmp_path, corrupt):
    """The chunk planning reads to type ``data`` may be corrupt. Planning then
    types ``data`` from the next chunk instead of failing, and the chunk's own
    read task reports the error."""
    (good,) = write_files(tmp_path, [dict(times=(0, 1), key="typed")])
    bad = corrupt(os.path.join(tmp_path, "bad.mcap"), good, 0)

    assert infer([bad], selection=ONLY_A) == struct_of("typed")


def test_a_corrupt_record_ends_the_scan_of_an_unindexed_sampled_file(tmp_path):
    """Planning scans a sampled file without a chunk index. A record that does
    not parse ends the scan instead of failing planning, so ``data`` is typed
    from the messages before it."""
    (path,) = write_files(tmp_path, [dict(times=(0, 1, 2), key="typed", index=False)])
    with open(path, "r+b") as f:
        # The last message's opcode and length become 0xff bytes, an unknown
        # record longer than mcap reads.
        f.seek(f.read().index(b'{"typed": 2}') - MESSAGE_PREFIX)
        f.write(b"\xff" * 9)

    assert infer([path], selection=ONLY_A) == struct_of("typed")


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_json_is_decoded_only_when_every_selected_channel_is_json(tmp_path, index):
    """``data`` is decoded JSON only when every selected channel is JSON-encoded.

    Otherwise every payload stays bytes, including those of JSON channels.
    """
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mcap(path, mixed_encoding_messages(), encodings={"/cdr": "cdr"}, index=index)

    # Both topics: ``/cdr`` is not JSON, so the ``/json`` payloads stay bytes too.
    mixed = MCAPDatasourceV2([path])
    schema = infer_schema(mixed)
    assert schema.field("data").type == pa.binary()
    table = read_all(mixed, list_manifests(mixed)[0])
    assert table.schema.equals(schema)
    json_rows = [
        data
        for topic, data in zip(
            table.column("topic").to_pylist(), table.column("data").to_pylist()
        )
        if topic == "/json"
    ]
    assert json_rows == [json.dumps({"seq": i}).encode() for i in (0, 2, 4)]

    # Only the JSON topic: decoded, with the sampled struct type.
    only_json = MCAPDatasourceV2([path], topics=["/json"])
    schema = infer_schema(only_json)
    assert pa.types.is_struct(schema.field("data").type)
    table = read_all(only_json, list_manifests(only_json)[0])
    assert table.column("data").to_pylist() == [{"seq": i} for i in (0, 2, 4)]

    only_cdr = MCAPDatasourceV2([path], topics=["/cdr"])
    assert infer_schema(only_cdr).field("data").type == pa.binary()


def test_non_json_channel_past_the_fourth_sampled_file_plans_binary(tmp_path):
    """Planning checks the encodings of every sampled file, not only the first four.

    The seventh file's ``/a`` is CDR, so every payload is read as bytes.
    """
    paths = []
    for i in range(6):
        path = os.path.join(tmp_path, f"json_{i}.mcap")
        write_mcap(path, round_robin_messages(2, topics=("/a",)))
        paths.append(path)
    cdr_path = os.path.join(tmp_path, "zz_cdr.mcap")
    write_mcap(
        cdr_path, round_robin_messages(2, topics=("/a",)), message_encoding="cdr"
    )
    paths.append(cdr_path)
    datasource = MCAPDatasourceV2(paths, topics=["/a"])

    assert infer_schema(datasource).field("data").type == pa.binary()
    tables = [read_all(datasource, m) for m in list_manifests(datasource)]

    assert sum(table.num_rows for table in tables) == 14
    assert all(table.schema.field("data").type == pa.binary() for table in tables)


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_planning_types_data_from_messages_in_the_time_range(tmp_path, index):
    """Planning types ``data`` from a message inside ``time_range``.

    A malformed payload before the range is not decoded, so planning succeeds.
    """
    messages = [{"topic": "/a", "log_time": BASE_TIME, "data": b"{not json"}] + [
        {"topic": "/a", "log_time": BASE_TIME + i * STEP, "data": {"seq": i}}
        for i in range(1, 4)
    ]
    path = os.path.join(tmp_path, "early_garbage.mcap")
    write_mcap(path, messages, index=index)
    datasource = MCAPDatasourceV2(
        [path], time_range=TimeRange(BASE_TIME + STEP, BASE_TIME + 10 * STEP)
    )

    schema = infer_schema(datasource)
    assert pa.types.is_struct(schema.field("data").type)
    table = read_all(datasource, list_manifests(datasource)[0])

    assert table.column("data").to_pylist() == [{"seq": 1}, {"seq": 2}, {"seq": 3}]


def write_one_chunk(path: str, payloads: list, *, index: bool = True) -> str:
    """Write ``payloads`` on ``/a``, one message each, all in one chunk."""
    messages = [
        {"topic": "/a", "log_time": BASE_TIME + i * STEP, "data": data}
        for i, data in enumerate(payloads)
    ]
    write_mcap(path, messages, chunk_size=1 << 20, index=index)
    return path


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_planning_types_data_as_the_read_task_does(tmp_path, index):
    """Planning types ``data`` from every selected message of the first chunk,
    so a field only some messages have, and an int that is a float elsewhere,
    are planned as the block holds them."""
    path = write_one_chunk(
        os.path.join(tmp_path, "mixed.mcap"),
        [{"x": 1}, {"x": 2.5, "y": "a"}, {"x": 3}],
        index=index,
    )
    datasource = MCAPDatasourceV2([path])

    planned = infer_schema(datasource).field("data").type
    table = read_all(datasource, list_manifests(datasource)[0])

    assert planned == pa.struct([("x", pa.float64()), ("y", pa.string())])
    assert table.schema.field("data").type == planned


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_a_payload_that_does_not_decode_is_left_out_of_the_type(tmp_path, index):
    """A malformed payload among the sampled messages does not fail planning.
    ``data`` is typed from the others, and the read task reports the payload."""
    path = write_one_chunk(
        os.path.join(tmp_path, "garbage.mcap"),
        [b"{not json", {"x": 1}, {"x": 2}],
        index=index,
    )

    assert infer([path], selection=ONLY_A) == pa.struct([("x", pa.int64())])


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_planning_types_data_from_at_most_max_typed_messages(
    tmp_path, monkeypatch, index
):
    """Planning decodes at most ``_MAX_TYPED_MESSAGES`` messages, so a field
    that first appears after them is not planned."""
    monkeypatch.setattr(mcap_data_column, "_MAX_TYPED_MESSAGES", 2)
    path = write_one_chunk(
        os.path.join(tmp_path, "late_field.mcap"),
        [{"x": 1}, {"x": 2}, {"x": 3, "y": "a"}],
        index=index,
    )

    assert infer([path], selection=ONLY_A) == pa.struct([("x", pa.int64())])


def test_schema_walk_does_not_scan_files_whose_summary_lacks_schemas(
    tmp_path, monkeypatch
):
    """The walk past the sample never scans a whole file.

    It skips a file whose summary lacks the schemas ``message_types`` needs, so
    ``data`` stays binary.
    """
    # Eighteen files without ``/a`` put the last file past the 16-file sample.
    paths = []
    for i in range(18):
        path = os.path.join(tmp_path, f"other_{i:02d}.mcap")
        write_mcap(path, round_robin_messages(2, topics=("/other",)))
        paths.append(path)
    last = os.path.join(tmp_path, "zz_no_summary_schemas.mcap")
    write_mcap_without_summary_schemas(last)
    paths.append(last)

    scans = count_scans(monkeypatch)
    datasource = MCAPDatasourceV2(paths, topics=["/a"], message_types=["test_schema"])

    assert infer_schema(datasource).field("data").type == pa.binary()
    assert scans == []


def test_data_type_is_inferred_when_the_summary_lists_no_channels(
    chunked_file, monkeypatch
):
    """Planning scans a file whose summary lists no channels.

    JSON payloads are still planned as decoded values.
    """
    without_summary_records(monkeypatch, "channels")
    scans = count_scans(monkeypatch)
    datasource = MCAPDatasourceV2([chunked_file], topics=["/a"])

    schema = infer_schema(datasource)
    assert pa.types.is_struct(schema.field("data").type)
    assert scans == [chunked_file]
    (manifest,) = list_manifests(datasource)
    table = read_all(datasource, manifest)

    assert table.column("data").to_pylist() == [{"seq": 0}, {"seq": 3}, {"seq": 6}]


def test_message_types_decide_the_plan_when_the_summary_lacks_schemas(tmp_path):
    """When the summary lacks the schemas, planning reads them from the chunks.

    ``message_types`` then drops the CDR channel, so ``data`` is planned as
    decoded JSON rather than ``binary``.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    path = os.path.join(tmp_path, "no_summary_schemas.mcap")
    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=1,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL,
            repeat_schemas=False,
        )
        writer.start(profile="", library="ray-test")
        keep = writer.register_channel(
            schema_id=writer.register_schema(
                name="keep", encoding="jsonschema", data=b"{}"
            ),
            topic="/keep",
            message_encoding="json",
        )
        drop = writer.register_channel(
            schema_id=writer.register_schema(
                name="drop", encoding="ros2msg", data=b"uint8 x"
            ),
            topic="/drop",
            message_encoding="cdr",
        )
        for i in range(4):
            writer.add_message(
                channel_id=keep if i % 2 == 0 else drop,
                log_time=BASE_TIME + i * STEP,
                publish_time=BASE_TIME + i * STEP,
                data=json.dumps({"seq": i}).encode() if i % 2 == 0 else bytes([i]),
            )
        writer.finish()
    summary = summary_of(path)
    assert summary.channels and not summary.schemas
    datasource = MCAPDatasourceV2([path], message_types=["keep"])

    schema = infer_schema(datasource)
    assert pa.types.is_struct(schema.field("data").type)
    (manifest,) = list_manifests(datasource)
    table = read_all(datasource, manifest)

    assert table.column("topic").to_pylist() == ["/keep", "/keep"]
    assert table.column("data").to_pylist() == [{"seq": 0}, {"seq": 2}]


def test_schema_sample_looks_past_files_without_the_selected_topic(tmp_path, caplog):
    """Planning looks past sampled files without a selected topic to type ``data``.

    If no file holds a selected topic, ``data`` is planned as ``binary`` with a
    warning.
    """
    # Eighteen files without ``/a`` put the last file past the 16-file sample.
    paths = []
    for i in range(18):
        path = os.path.join(tmp_path, f"other_{i:02d}.mcap")
        write_mcap(path, round_robin_messages(2, topics=("/other",)))
        paths.append(path)
    last = os.path.join(tmp_path, "zz_selected.mcap")
    write_mcap(last, round_robin_messages(3, topics=("/a",)))
    paths.append(last)
    datasource = MCAPDatasourceV2(paths, topics=["/a"])

    schema = infer_schema(datasource)
    assert pa.types.is_struct(schema.field("data").type)
    (manifest,) = list_manifests(datasource)
    assert read_all(datasource, manifest).column("data").to_pylist() == [
        {"seq": 0},
        {"seq": 1},
        {"seq": 2},
    ]

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_data_column.logger.addHandler(caplog.handler)
    try:
        nothing = MCAPDatasourceV2(paths, topics=["/missing"])
        schema = infer_schema(nothing)
    finally:
        mcap_data_column.logger.removeHandler(caplog.handler)
    assert schema.field("data").type == pa.binary()
    assert any("planned as binary" in record.message for record in caplog.records)


def test_schema_walk_runs_when_the_sample_has_no_message_in_the_time_range(tmp_path):
    """Planning looks past sampled files with no selected message in ``time_range``.

    A later file with one in range types ``data``, so its JSON is decoded.
    """
    # The 16 sampled files hold ``/a`` only before the range.
    paths = []
    for i in range(16):
        path = os.path.join(tmp_path, f"a_{i:02d}.mcap")
        early = [
            {"topic": "/a", "log_time": BASE_TIME + j, "data": {"seq": j}}
            for j in range(2)
        ]
        late = [{"topic": "/b", "log_time": BASE_TIME + 50 * STEP, "data": {"x": 1}}]
        write_mcap(path, early + late)
        paths.append(path)
    last = os.path.join(tmp_path, "zz_in_range.mcap")
    write_mcap(
        last,
        [{"topic": "/a", "log_time": BASE_TIME + 50 * STEP, "data": {"seq": 7}}],
    )
    paths.append(last)
    datasource = MCAPDatasourceV2(
        paths,
        topics=["/a"],
        time_range=TimeRange(BASE_TIME + 40 * STEP, BASE_TIME + 60 * STEP),
    )

    schema = infer_schema(datasource)
    assert pa.types.is_struct(schema.field("data").type)
    rows = [
        value
        for manifest in list_manifests(datasource)
        for value in read_all(datasource, manifest).column("data").to_pylist()
    ]

    assert rows == [{"seq": 7}]


def test_schema_walk_keeps_the_extension_filter(tmp_path):
    """The walk past the sample uses the read's extension filter.

    The ``.txt`` sidecar is a valid MCAP file whose ``/a`` is not JSON; were it
    read, ``data`` would be planned as binary.
    """
    from ray.data._internal.datasource_v2.common.listing_utils import _build_pruners

    folder = os.path.join(tmp_path, "recordings")
    os.makedirs(folder)
    for i in range(18):
        write_mcap(
            os.path.join(folder, f"a_other_{i:02d}.mcap"),
            round_robin_messages(2, topics=("/other",)),
        )
    write_mcap(
        os.path.join(folder, "m_notes.txt"),
        round_robin_messages(3, topics=("/a",)),
        encodings={"/a": "cdr"},
    )
    write_mcap(
        os.path.join(folder, "zz_selected.mcap"),
        round_robin_messages(3, topics=("/a",)),
    )
    datasource = MCAPDatasourceV2([folder], topics=["/a"])
    indexer = datasource._get_file_indexer()

    sample = sample_files(
        indexer,
        datasource.paths,
        datasource.filesystem,
        _build_pruners(datasource.file_extensions, None),
    )
    schema = datasource.infer_schema(sample)

    assert pa.types.is_struct(schema.field("data").type)


SAMPLED_FILES = 16  # files ``sample_files`` lists for planning by default


def write_sample_and_a_17th(tmp_path, payloads, encoding="json") -> List[str]:
    """Write the planning sample and one more file past it, all on ``/a``.

        file      00 .. 15          zz_last
        payload   {"seq": 0} JSON   ``payloads``, ``encoding``
        planning  sampled           not sampled

    The names sort in this order, so planning samples files 00 to 15.
    """
    paths = write_files(tmp_path, [dict(key="seq")] * SAMPLED_FILES)
    last = os.path.join(tmp_path, "zz_last.mcap")
    messages = [
        {"topic": "/a", "data": payload, "log_time": BASE_TIME + i * STEP}
        for i, payload in enumerate(payloads)
    ]
    write_mcap(last, messages, message_encoding=encoding)
    return paths + [last]


def read_each_file(paths) -> Dict[str, pa.Table]:
    """Plan ``paths`` together, then read each file as its own task, by path."""
    datasource = MCAPDatasourceV2(paths)
    assert infer_schema(datasource).field("data").type == struct_of("seq")
    return {
        manifest.paths.tolist()[0]: read_all(datasource, manifest)
        for manifest in list_manifests(datasource)
    }


def test_non_json_file_past_the_sample_fails_its_read_task(tmp_path):
    """A CDR channel in a 17th file fails that file's read task, naming the topic.

    The 16 sampled files planned ``data`` as decoded JSON, and every task follows
    the plan rather than mixing bytes into a JSON column.
    """
    paths = write_sample_and_a_17th(tmp_path, [b"\x01\x02"], encoding="cdr")
    datasource = MCAPDatasourceV2(paths)
    manifests = {m.paths.tolist()[0]: m for m in list_manifests(datasource)}

    for path in paths[:SAMPLED_FILES]:
        read_all(datasource, manifests[path])
    with pytest.raises(ValueError, match="topic '/a' is 'cdr'-encoded"):
        read_all(datasource, manifests[paths[-1]])


@pytest.mark.parametrize(
    "payloads, last_type, combined_type, combined_first_row",
    [
        (
            [{"seq": 0, "extra": "x"}],
            # Ray's converter orders struct fields by name; unifying keeps the
            # planned field first.
            pa.struct([("extra", pa.string()), ("seq", pa.int64())]),
            pa.struct([("seq", pa.int64()), ("extra", pa.string())]),
            {"seq": 0, "extra": None},
        ),
        (
            [{"seq": 0.5}],
            pa.struct([("seq", pa.float64())]),
            pa.struct([("seq", pa.float64())]),
            {"seq": 0.0},
        ),
    ],
    ids=["new_field", "int_to_float"],
)
def test_json_past_the_sample_is_typed_by_its_own_values(
    tmp_path, payloads, last_type, combined_type, combined_first_row
):
    """A 17th file's JSON is typed by its own values, not by the plan.

    Combining its block with a planned one, as Ray Data does, adds a new field
    as null and widens int to float.
    """
    from ray.data._internal.arrow_ops.transform_pyarrow import concat

    paths = write_sample_and_a_17th(tmp_path, payloads)

    tables = read_each_file(paths)

    assert tables[paths[-1]].schema.field("data").type == last_type
    combined = concat([tables[paths[0]], tables[paths[-1]]], promote_types=True)
    assert combined.schema.field("data").type == combined_type
    # The first row comes from a sampled file, typed to the combined struct.
    assert combined.column("data").to_pylist()[0] == combined_first_row


def test_json_past_the_sample_with_a_clashing_type_fails_where_combined(tmp_path):
    """A 17th file whose ``seq`` is a string reads alone, but its block cannot be
    combined with a planned block, whose ``seq`` is an int."""
    from ray.data._internal.arrow_ops.transform_pyarrow import concat
    from ray.data._internal.tensor_extensions.arrow import ArrowConversionError

    paths = write_sample_and_a_17th(tmp_path, [{"seq": "zero"}])

    tables = read_each_file(paths)

    assert tables[paths[-1]].column("data").to_pylist() == [{"seq": "zero"}]
    with pytest.raises(ArrowConversionError, match="Field seq has incompatible types"):
        concat([tables[paths[0]], tables[paths[-1]]], promote_types=True)


def test_mixed_json_types_in_one_task_are_refused(tmp_path):
    """JSON values Arrow cannot type together fall back to pickled objects, which
    the reader refuses."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mcap(
        path,
        round_robin_messages(1, topics=("/a",))
        + [{"topic": "/a", "data": {"seq": "one"}, "log_time": BASE_TIME + STEP}],
    )
    datasource = MCAPDatasourceV2([path])

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        read_all(datasource, list_manifests(datasource)[0])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
