"""Unit tests for the attachment and metadata granularities of the V2 MCAP reader."""

import dataclasses
import importlib.util
import json
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import TimeRange
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    attachment_unit_id,
    read_summary,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.supports_metadata import MetadataType
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data.expressions import col
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    list_manifests,
    read_rows,
    scanner_for,
    summary_of,
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
    """Each attachment is one listing row, sized by its data, and reads back with
    its fields, bytes and row id."""
    datasource = MCAPDatasourceV2(
        [records_file], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    summary = read_summary(datasource.filesystem, records_file)
    assert summary is not None
    assert len(manifest) == len(summary.attachment_indexes) == 3
    assert [int(md["size_bytes"]) for md in manifest.file_chunk_metadatas] == [
        len(data) for *_, data in ATTACHMENTS
    ]
    assert isinstance(datasource.get_file_partitioner(), OnlineBinPacker)

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

    def counting_read_record_at(f, offset):
        record = real_read_record_at(f, offset)
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
