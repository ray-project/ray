"""Unit tests for reading MCAP chunk rows into tables.

The scanner and reader are driven directly, as a ``ReadFiles`` task does, with
no Ray cluster.
"""

import dataclasses
import importlib.util
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ROW_ID_COLUMN,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CHUNKED_FILE_MESSAGES,
    infer_schema,
    list_manifests,
    mixed_encoding_messages,
    read_all,
    round_robin_messages,
    scanner_for,
    summary_of,
    write_mcap,
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
        assert table.column("channel_metadata").to_pylist()[0] == [("origin", "/a")]
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
