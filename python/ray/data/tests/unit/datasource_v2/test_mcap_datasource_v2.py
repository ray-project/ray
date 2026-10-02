"""Unit tests for the V2 MCAP datasource: listing, splitting, reading, ids.

No Ray cluster: the indexer, scanner and reader are driven directly, the way
``_read_datasource_v2`` and the ``ListFiles`` / ``ReadFiles`` tasks do.
"""

import dataclasses
import importlib.util
import json
import os
from typing import Any, Dict, List

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ROW_ID_COLUMN,
    MCAPSelection,
    TimeRange,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    chunk_unit_id,
    read_summary,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.logical.operators import ReadFiles

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

BASE_TIME = 1_000_000_000
STEP = 1_000_000


def write_mcap(
    path,
    messages,
    *,
    chunk_size=1,
    message_encoding="json",
    index=True,
    encodings=None,
):
    """Write ``messages`` (dicts with topic, log_time, data) to ``path``.

    ``chunk_size=1`` closes a chunk after every message, so a file with N
    messages has N chunks and every chunk boundary is a potential split.
    ``encodings`` overrides ``message_encoding`` per topic.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=chunk_size,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL if index else IndexType.NONE,
            use_chunking=index,
            use_statistics=index,
            use_summary_offsets=index,
        )
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b'{"type": "object"}'
        )
        channels = {}
        for message in messages:
            topic = message["topic"]
            if topic not in channels:
                channels[topic] = writer.register_channel(
                    schema_id=schema_id,
                    topic=topic,
                    message_encoding=(encodings or {}).get(topic, message_encoding),
                    metadata={"origin": topic},
                )
            data = message["data"]
            if not isinstance(data, bytes):
                data = json.dumps(data).encode()
            writer.add_message(
                channel_id=channels[topic],
                log_time=message["log_time"],
                publish_time=message.get("publish_time", message["log_time"]),
                data=data,
            )
        writer.finish()


def round_robin_messages(n, topics=("/a", "/b", "/c")) -> List[Dict[str, Any]]:
    return [
        {
            "topic": topics[i % len(topics)],
            "data": {"seq": i},
            "log_time": BASE_TIME + i * STEP,
        }
        for i in range(n)
    ]


def list_manifests(datasource, **kwargs):
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(
            pa.array(datasource.paths), filesystem=datasource.filesystem, **kwargs
        )
    )


def infer_schema(datasource):
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    return datasource.infer_schema(sample)


def read_all(datasource, manifest, scanner=None) -> pa.Table:
    scanner = scanner or datasource.create_scanner(
        infer_schema(datasource), datasource.filesystem
    )
    tables = list(scanner.create_reader().read(manifest))
    assert tables, "expected at least one table"
    return pa.concat_tables(tables)


def unit_ids(chunk_metadata: Any) -> List[int]:
    return [int(i) for i in chunk_metadata["unit_ids"]]


def summary_of(path):
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    return summary


@pytest.fixture
def chunked_file(tmp_path):
    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(9))
    return path


def test_indexer_lists_one_row_per_chunk(chunked_file):
    datasource = MCAPDatasourceV2([chunked_file])
    (manifest,) = list_manifests(datasource)
    summary = summary_of(chunked_file)

    assert len(manifest) == len(summary.chunk_indexes) == 9
    offsets = [unit_ids(md)[0] for md in manifest.file_chunk_metadatas]
    assert offsets == [c.chunk_start_offset for c in summary.chunk_indexes]
    assert [int(md["size_bytes"]) for md in manifest.file_chunk_metadatas] == [
        c.uncompressed_size for c in summary.chunk_indexes
    ]
    # Row counts are estimates: never trusted as exact survivor counts.
    assert not any(md["fully_matched"] for md in manifest.file_chunk_metadatas)
    assert all(path == chunked_file for path in manifest.paths)


def test_indexer_prunes_chunks_by_topic_and_time(chunked_file):
    (by_topic,) = list_manifests(MCAPDatasourceV2([chunked_file], topics=["/a"]))
    assert len(by_topic) == 3  # messages 0, 3, 6

    window = TimeRange(start_time=BASE_TIME + 2 * STEP, end_time=BASE_TIME + 5 * STEP)
    (by_time,) = list_manifests(MCAPDatasourceV2([chunked_file], time_range=window))
    assert len(by_time) == 3  # messages 2, 3, 4

    # No chunk can match: the file is dropped from the listing entirely.
    assert list_manifests(MCAPDatasourceV2([chunked_file], topics=["/nope"])) == []
    assert (
        list_manifests(
            MCAPDatasourceV2(
                [chunked_file],
                time_range=TimeRange(start_time=1, end_time=2),
            )
        )
        == []
    )


def test_indexer_honors_excluded_read_unit_ids(tmp_path, chunked_file):
    other = os.path.join(tmp_path, "other.mcap")
    write_mcap(other, round_robin_messages(2))
    datasource = MCAPDatasourceV2([chunked_file, other])
    summary = summary_of(chunked_file)
    excluded = {
        chunk_unit_id(chunked_file, summary.chunk_indexes[0].chunk_start_offset),
        other,  # a whole file a checkpoint finished
    }

    manifests = list_manifests(datasource, excluded_read_unit_ids=excluded)

    assert len(manifests) == 1
    assert len(manifests[0]) == 8
    assert set(manifests[0].paths) == {chunked_file}


def test_file_without_index_is_one_whole_file_row(tmp_path):
    path = os.path.join(tmp_path, "unindexed.mcap")
    write_mcap(path, round_robin_messages(3), index=False)
    datasource = MCAPDatasourceV2([path], include_row_id=True)

    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 1
    assert manifest.file_chunk_metadatas[0] is None

    table = read_all(datasource, manifest)
    assert table.num_rows == 3
    assert table.column("data").to_pylist() == [{"seq": 0}, {"seq": 1}, {"seq": 2}]
    assert table.column(ROW_ID_COLUMN).to_pylist() == [
        f"{path}#m=0",
        f"{path}#m=1",
        f"{path}#m=2",
    ]


@pytest.mark.parametrize("include_metadata", [True, False])
def test_reader_tables_match_inferred_schema(chunked_file, include_metadata):
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
    assert table.num_rows == 9
    assert table.column("path").to_pylist() == [chunked_file] * 9
    if include_metadata:
        assert pa.types.is_dictionary(table.schema.field("schema_data").type)
        assert table.column("channel_metadata").to_pylist()[0] == [("origin", "/a")]
        assert set(table.column_names) >= {"channel_id", "schema_name"}
    else:
        assert "channel_id" not in table.column_names


def test_binary_payloads_stay_bytes(tmp_path):
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


def mixed_encoding_messages(n=6):
    messages = round_robin_messages(n, topics=("/json", "/cdr"))
    for message in messages:
        if message["topic"] == "/cdr":
            message["data"] = bytes([message["data"]["seq"]]) * 4
    return messages


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_json_is_decoded_only_when_every_selected_channel_is_json(tmp_path, index):
    """The planned ``data`` type decides for every row: values or bytes, never both."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mcap(path, mixed_encoding_messages(), encodings={"/cdr": "cdr"}, index=index)

    # Both topics selected: one is not JSON, so every payload stays bytes,
    # including the JSON topic's.
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


def test_invalid_json_fails_naming_the_message(tmp_path):
    path = os.path.join(tmp_path, "bad.mcap")
    messages = round_robin_messages(3, topics=("/json",))
    messages[1]["data"] = b"{not json"
    write_mcap(path, messages)
    datasource = MCAPDatasourceV2([path])
    assert pa.types.is_struct(infer_schema(datasource).field("data").type)

    with pytest.raises(ValueError, match=r"#c=\d+:0: message on JSON-encoded topic"):
        read_all(datasource, list_manifests(datasource)[0])


def test_non_json_channel_under_a_decoded_plan_fails(tmp_path):
    """A file the sample did not see cannot slip bytes into a decoded column."""
    json_path = os.path.join(tmp_path, "json.mcap")
    write_mcap(json_path, round_robin_messages(3, topics=("/json",)))
    cdr_path = os.path.join(tmp_path, "cdr.mcap")
    write_mcap(
        cdr_path,
        [m for m in mixed_encoding_messages() if m["topic"] == "/cdr"],
        message_encoding="cdr",
    )
    planned = MCAPDatasourceV2([json_path])
    scanner = planned.create_scanner(infer_schema(planned), planned.filesystem)
    assert scanner.decodes_json()

    other = MCAPDatasourceV2([cdr_path])
    with pytest.raises(ValueError, match="'/cdr' is 'cdr'-encoded"):
        read_all(other, list_manifests(other)[0], scanner=scanner)


def test_path_partitioning_adds_columns(tmp_path):
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


def test_splits_cover_the_file_exactly_once_with_stable_ids(chunked_file):
    datasource = MCAPDatasourceV2([chunked_file], include_row_id=True)
    (manifest,) = list_manifests(datasource)
    schema = infer_schema(datasource)
    scanner = datasource.create_scanner(schema, datasource.filesystem)

    whole = read_all(datasource, manifest, scanner)
    # Two tasks: chunks {0, 1, 2, 3} and {4, ..., 8}, as a packer might cut them.
    block = manifest.as_block()
    first = read_all(datasource, FileManifest(block.slice(0, 4)), scanner)
    second = read_all(datasource, FileManifest(block.slice(4)), scanner)

    split_ids = (
        first.column(ROW_ID_COLUMN).to_pylist()
        + second.column(ROW_ID_COLUMN).to_pylist()
    )
    assert split_ids == whole.column(ROW_ID_COLUMN).to_pylist()
    assert len(set(split_ids)) == 9
    assert (
        first.column("log_time").to_pylist() + second.column("log_time").to_pylist()
        == whole.column("log_time").to_pylist()
    )


def test_row_id_does_not_depend_on_the_selection(chunked_file):
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
    # Chunk 0 holds times 10 and 30, chunk 1 holds 20: the chunks overlap in
    # time, so file order and log-time order differ.
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


def test_reader_cuts_tables_at_target_block_size(chunked_file):
    datasource = MCAPDatasourceV2([chunked_file])
    (manifest,) = list_manifests(datasource)
    schema = infer_schema(datasource)
    scanner = datasource.create_scanner(schema, datasource.filesystem)

    one_byte = dataclasses.replace(scanner, target_block_size=1)
    tables = list(one_byte.create_reader().read(manifest))
    assert len(tables) == 9
    assert all(t.num_rows == 1 for t in tables)

    unbounded = dataclasses.replace(scanner, target_block_size=None)
    assert len(list(unbounded.create_reader().read(manifest))) == 1


def test_scanner_prunes_columns_and_pushes_limit(chunked_file):
    datasource = MCAPDatasourceV2([chunked_file], include_paths=True)
    (manifest,) = list_manifests(datasource)
    scanner = datasource.create_scanner(infer_schema(datasource), datasource.filesystem)

    assert isinstance(scanner, MCAPScanner)
    pruned = scanner.prune_columns(["log_time", "path"]).push_limit(4)
    assert pruned.read_schema().names == ["log_time", "path"]
    assert pruned.pushed_limit() == 4

    table = pa.concat_tables(list(pruned.create_reader().read(manifest)))
    assert table.column_names == ["log_time", "path"]
    assert table.num_rows == 4

    # An empty projection (``count()``) keeps the row count through a stub column.
    empty = scanner.prune_columns([])
    table = pa.concat_tables(list(empty.create_reader().read(manifest)))
    assert table.num_rows == 9
    assert empty.read_schema().names == []


def test_message_types_filter_matches_legacy_semantics(chunked_file):
    selection = MCAPSelection.create(None, None, {"other_schema"})
    summary = summary_of(chunked_file)
    assert selection.selected_channel_ids(summary.channels, summary.schemas) == set()

    selection = MCAPSelection.create(["/a"], None, {"test_schema"})
    selected = selection.selected_channel_ids(summary.channels, summary.schemas)
    assert {summary.channels[cid].topic for cid in selected} == {"/a"}


def test_bin_packer_groups_chunks_by_uncompressed_bytes(tmp_path):
    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(4))
        paths.append(path)
    datasource = MCAPDatasourceV2(paths)
    manifests = list_manifests(datasource)
    chunk_bytes = max(
        int(md["size_bytes"]) for m in manifests for md in m.file_chunk_metadatas
    )

    packer = OnlineBinPacker(max_bin_bytes=2 * chunk_bytes)
    for manifest in manifests:
        packer.add_input(manifest)
    packer.finalize()
    bins = []
    while packer.has_partition():
        bins.append(packer.next_partition())

    # Twelve chunks of roughly equal size at two per bin: about six tasks, every
    # chunk in exactly one of them.
    assert 4 <= len(bins) <= 12
    seen = []
    for manifest in bins:
        for path, md in zip(manifest.paths, manifest.file_chunk_metadatas):
            seen.extend((path, i) for i in unit_ids(md))
    assert len(seen) == 12 and len(set(seen)) == 12


def test_time_range_is_moved_and_still_exported():
    from ray.data._internal.datasource.mcap_datasource import (
        TimeRange as LegacyTimeRange,
    )
    from ray.data.datasource import TimeRange as PublicTimeRange

    assert LegacyTimeRange is TimeRange is PublicTimeRange
    with pytest.raises(ValueError, match="must be less than"):
        TimeRange(start_time=2, end_time=1)


def test_datasource_is_a_file_datasource_v2(chunked_file):
    datasource = MCAPDatasourceV2([chunked_file])
    assert datasource.name == "MCAP"
    assert datasource.file_extensions == ["mcap"]
    assert datasource.schema_needs_file_sample
    assert isinstance(datasource.get_file_partitioner(), OnlineBinPacker)
    assert ReadFiles is not None  # the op the read is planned into


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
