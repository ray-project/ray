"""Unit tests for the attachment and metadata granularities of the V2 MCAP reader."""

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
from ray.data.expressions import col

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


def write_file(path, *, index=True, attachments=ATTACHMENTS, metadata=METADATA):
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


def list_manifests(datasource, **kwargs):
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(
            pa.array(datasource.paths), filesystem=datasource.filesystem, **kwargs
        )
    )


def read_rows(datasource, manifests):
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    scanner = datasource.create_scanner(
        datasource.infer_schema(sample), datasource.filesystem
    )
    reader = scanner.create_reader()
    rows = []
    for manifest in manifests:
        for table in reader.read(manifest):
            assert table.schema.equals(scanner.read_schema()), table.schema
            rows.extend(table.to_pylist())
    return rows, reader


@pytest.fixture
def recording(tmp_path):
    path = os.path.join(tmp_path, "run.mcap")
    write_file(path)
    return path


def test_attachment_rows(recording):
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    summary = read_summary(datasource.filesystem, recording)
    assert summary is not None
    assert len(manifest) == len(summary.attachment_indexes) == 3
    assert [int(md["size_bytes"]) for md in manifest.file_chunk_metadatas] == [
        len(data) for *_, data in ATTACHMENTS
    ]
    assert isinstance(datasource.get_file_partitioner(), OnlineBinPacker)

    rows, _ = read_rows(datasource, [manifest])
    assert [row["name"] for row in rows] == ["calib.yaml", "map.bin", "thumb.jpg"]
    assert [row["media_type"] for row in rows] == [a[1] for a in ATTACHMENTS]
    assert [row["log_time"] for row in rows] == [a[2] for a in ATTACHMENTS]
    assert [row["create_time"] for row in rows] == [a[3] for a in ATTACHMENTS]
    assert [row["data"] for row in rows] == [a[4] for a in ATTACHMENTS]
    assert rows[0]["path"] == recording
    assert [row["row_id"] for row in rows] == [
        attachment_unit_id(recording, idx.offset) for idx in summary.attachment_indexes
    ]


def test_attachment_rows_split_across_tasks_and_honor_time_range(recording):
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    first, _ = read_rows(datasource, [FileManifest(block.slice(0, 1))])
    rest, _ = read_rows(datasource, [FileManifest(block.slice(1))])
    assert [r["name"] for r in first + rest] == ["calib.yaml", "map.bin", "thumb.jpg"]

    ranged = MCAPDatasourceV2(
        [recording],
        read_granularity="attachment",
        time_range=TimeRange(start_time=1_500, end_time=3_000),
    )
    (pruned,) = list_manifests(ranged)
    assert len(pruned) == 1  # only map.bin at 2_000 is listed
    rows, _ = read_rows(ranged, [pruned])
    assert [row["name"] for row in rows] == ["map.bin"]

    excluded = {attachment_unit_id(recording, summary_offsets(recording)[0])}
    (remaining,) = list_manifests(datasource, excluded_read_unit_ids=excluded)
    assert len(remaining) == 2


def summary_offsets(path):
    from pyarrow.fs import LocalFileSystem

    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    return [idx.offset for idx in summary.attachment_indexes]


def test_metadata_rows(recording):
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="metadata", include_row_id=True
    )
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 2
    rows, _ = read_rows(datasource, [manifest])
    assert [row["name"] for row in rows] == ["recorder", "vehicle"]
    assert rows[0]["metadata"] == [("version", "1.2"), ("host", "rig-7")]
    assert rows[1]["metadata"] == [("id", "v42")]
    assert rows[0]["row_id"].startswith(f"{recording}#md=")


def test_unindexed_file_records_are_scanned(tmp_path):
    path = os.path.join(tmp_path, "noindex.mcap")
    write_file(path, index=False)

    attachments = MCAPDatasourceV2(
        [path], read_granularity="attachment", include_row_id=True
    )
    (manifest,) = list_manifests(attachments)
    assert len(manifest) == 1 and manifest.file_chunk_metadatas[0] is None
    rows, reader = read_rows(attachments, [manifest])
    assert [row["name"] for row in rows] == ["calib.yaml", "map.bin", "thumb.jpg"]
    assert rows[0]["row_id"] == f"{path}#a-m=0"
    assert [m.num_rows for m in reader.read_metadata(manifest)] == [3]

    metadata = MCAPDatasourceV2([path], read_granularity="metadata")
    (manifest,) = list_manifests(metadata)
    rows, reader = read_rows(metadata, [manifest])
    assert [row["name"] for row in rows] == ["recorder", "vehicle"]
    assert [m.num_rows for m in reader.read_metadata(manifest)] == [2]


def test_file_without_records_lists_nothing(tmp_path):
    path = os.path.join(tmp_path, "plain.mcap")
    write_file(path, attachments=[], metadata=[])
    assert list_manifests(MCAPDatasourceV2([path], read_granularity="attachment")) == []
    assert list_manifests(MCAPDatasourceV2([path], read_granularity="metadata")) == []


def test_record_counts_from_statistics(recording):
    for granularity, expected in (("attachment", 3), ("metadata", 2)):
        datasource = MCAPDatasourceV2([recording], read_granularity=granularity)
        indexer = datasource._get_file_indexer()
        sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
        scanner = datasource.create_scanner(
            datasource.infer_schema(sample), datasource.filesystem
        )
        reader = scanner.create_reader()
        assert scanner.metadata_row_count_is_exact()
        assert reader.available_metadata() == {MetadataType.NUM_ROWS}
        assert [m.num_rows for m in reader.read_metadata(sample)] == [expected]
        # Filters on message columns do not apply to records.
        same, residual = scanner.push_filters(col("topic") == "/t")
        assert same is scanner and residual is not None

    ranged = MCAPDatasourceV2(
        [recording],
        read_granularity="attachment",
        time_range=TimeRange(start_time=1_500, end_time=3_000),
    )
    indexer = ranged._get_file_indexer()
    sample = sample_files(indexer, ranged.paths, ranged.filesystem, [])
    scanner = ranged.create_scanner(ranged.infer_schema(sample), ranged.filesystem)
    assert not scanner.metadata_row_count_is_exact()
    assert scanner.create_reader().available_metadata() == set()


def test_record_granularity_validation(recording):
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2([recording], read_granularity="attachment", topics=["/t"])
    with pytest.raises(ValueError, match="do not apply"):
        MCAPDatasourceV2(
            [recording], read_granularity="metadata", message_types=["test_schema"]
        )
    with pytest.raises(ValueError, match="time_range does not apply"):
        MCAPDatasourceV2(
            [recording],
            read_granularity="metadata",
            time_range=TimeRange(start_time=1, end_time=2),
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
