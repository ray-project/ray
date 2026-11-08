"""Schema-only manifest protocol, layout and bounded shard control tests."""
from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.parquet import (
    footer_file_indexer as module,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
    _schema_only_manifest,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import ChunkedFile
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data.datasource.partitioning import Partitioning, PartitionStyle


def marker():
    return FileChunk(
        unit_ids=(), num_rows=0, size_bytes=0, fully_matched=False, schema_only=True
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"unit_ids": (0,)},
        {"num_rows": 1},
        {"size_bytes": 1},
        {"fully_matched": True},
        {"schema_only": "true"},
    ],
)
def test_invalid_schema_marker_is_rejected(changes):
    with pytest.raises(ValueError):
        replace(marker(), **changes)


def test_marker_roundtrip_and_legacy_zero_units_are_distinct():
    assert FileChunk.from_metadata(marker().to_metadata()) == marker()
    legacy = FileChunk(unit_ids=(), num_rows=0, size_bytes=0).to_metadata()
    legacy.pop("schema_only")
    assert not FileChunk.from_metadata(legacy).schema_only
    legacy["schema_only"] = "false"
    with pytest.raises(ValueError):
        FileChunk.from_metadata(legacy)


@pytest.mark.parametrize("with_data", [False, True])
@pytest.mark.parametrize("global_input", [False, True])
def test_schema_marker_preserves_source_size_with_zero_read_budget(
    with_data, global_input
):
    file = FileInfo("source.parquet", 10_000)
    assert file.size is not None
    control = _schema_only_manifest(file)
    assert control.file_sizes.tolist() == [file.size]
    assert control.known_file_sizes == [file.size]
    chunk = FileChunk.from_metadata(control.file_chunk_metadatas[0])
    assert chunk.schema_only and chunk.size_bytes == 0

    # The physical file exceeds the bin cap. Only actual read units consume it.
    packer = OnlineBinPacker(10, requires_global_input=global_input)
    assert packer.requires_global_input is global_input
    packer.add_input(control)
    assert not packer.has_partition()
    if with_data:
        data = FileChunk(unit_ids=(0,), num_rows=2, size_bytes=5)
        packer.add_input(
            FileManifest.construct_manifest(
                paths=[file.path],
                sizes=[file.size],
                chunk_metadatas=[data.to_metadata()],
            )
        )
        assert not packer.has_partition()
    packer.finalize()
    result = packer.next_partition()
    assert not packer.has_partition()
    assert result.file_sizes.tolist() == [file.size] * (2 if with_data else 1)
    chunks = [FileChunk.from_metadata(md) for md in result.file_chunk_metadatas]
    assert sum(chunk.schema_only for chunk in chunks) == 1
    assert sum(chunk.size_bytes for chunk in chunks) == (5 if with_data else 0)


def test_packer_preserves_markers_and_duplicate_data():
    run = FileChunk(unit_ids=(0,), num_rows=2, size_bytes=10)
    source = FileManifest.construct_manifest(
        paths=["__empty__"] * 4,
        sizes=[10] * 4,
        chunk_metadatas=[
            marker().to_metadata(),
            run.to_metadata(),
            marker().to_metadata(),
            run.to_metadata(),
        ],
    )
    packer = OnlineBinPacker(100)
    packer.add_input(source)
    packer.finalize()
    chunks = []
    while packer.has_partition():
        out = packer.next_partition()
        assert set(out.paths) == {"__empty__"}
        chunks.extend(FileChunk.from_metadata(v) for v in out.file_chunk_metadatas)
    assert sum(c.schema_only for c in chunks) == 2
    assert [c.unit_ids for c in chunks if not c.schema_only] == [(0,), (0,)]


@pytest.mark.parametrize("columns", [None, ["p", "x"], ["x"], []])
def test_reader_empty_uses_same_layout_without_opening_data(
    tmp_path, monkeypatch, columns
):
    path = tmp_path / "p=1" / "__empty__"
    path.parent.mkdir()
    table = pa.Table.from_arrays(
        [pa.array([1, 2])],
        schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]),
    )
    pq.write_table(table, path)
    schema = table.schema.append(pa.field("p", pa.int8(), nullable=False))
    reader = ParquetFileReader(
        schema=schema,
        filesystem=fs.LocalFileSystem(),
        columns=columns,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
        ),
    )
    normal = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    actual = pa.concat_tables(list(reader.read(normal)))
    control = _schema_only_manifest(FileInfo(str(path), path.stat().st_size))
    from ray.data._internal.datasource_v2.common import file_reader

    def forbidden(*args, **kwargs):
        raise AssertionError("Schema-only input must not open any data file")

    monkeypatch.setattr(file_reader.pds, "dataset", forbidden)
    empty = pa.concat_tables(list(reader.read(control)))
    assert empty.num_rows == 0
    if columns != []:
        assert actual.schema.equals(empty.schema, check_metadata=True)
    else:
        # Nonempty zero-column rows use Ray's row-count stub; empty output does not.
        assert empty.num_columns == 0


def test_reader_errors_are_not_swallowed(tmp_path, monkeypatch):
    reader = ParquetFileReader(schema=pa.schema([("x", pa.int64())]))

    def fail(table):
        raise RuntimeError("required output check")

    monkeypatch.setattr(reader, "_on_batch_read", fail)
    with pytest.raises(RuntimeError, match="required output check"):
        list(
            reader.read(
                _schema_only_manifest(FileInfo(str(tmp_path / "actual.parquet"), 1))
            )
        )


@pytest.mark.parametrize("mode", ["many", "conflict", "error"])
def test_shard_control_is_online_bounded_and_propagates_errors(monkeypatch, mode):
    indexer = FooterFileIndexer(
        ignore_missing_paths=False, footer_batch_size=32, max_inflight_batches=2
    )
    visited = []
    layout = (pa.schema([("x", pa.int64())]).serialize().to_pybytes(), ("p",), None)

    def remote(batch, **kwargs):
        def stream():
            for item in batch:
                visited.append(item.path)
                if mode == "error" and item.path == "32":
                    raise OSError("mandatory footer failure")
                key = (
                    (b"different", ("p",), None)
                    if mode == "conflict" and item.path == "32"
                    else layout
                )
                yield [ChunkedFile(item, (), schema_only_layout=key)]

        return stream()

    actor = MagicMock()
    actor.read_footers.remote.side_effect = remote
    monkeypatch.setattr(module.ray, "get", lambda value: value)

    def execute():
        return indexer._read_footers(
            [actor],
            (FileInfo(str(i), 1) for i in range(10000)),
            None,
            preserve_order=False,
        )

    output = execute()
    first = next(output)
    assert len(visited) == 1  # The control entry is emitted before the shard drains.
    assert FileChunk.from_metadata(first.file_chunk_metadatas[0]).schema_only
    if mode != "many":
        with pytest.raises(ValueError if mode == "conflict" else OSError):
            list(output)
    else:
        assert list(output) == []
        assert len(visited) == 10000  # No skipped footer work or hidden failures.
        visited.clear()
        assert len(list(execute())) == 1  # A retry/new shard has independent state.
        assert len(visited) == 10000


class RecordingLocalHandler(fs.FileSystemHandler):
    """Record operations without wrapping NativeFile or changing read_at."""

    def __init__(self):
        self.base = fs.LocalFileSystem()
        self.events = []

    def get_type_name(self):
        return "recording-local"

    def normalize_path(self, path):
        self.events.append(("normalize_path", path))
        return self.base.normalize_path(path)

    def get_file_info(self, paths):
        self.events.extend(("get_file_info", path) for path in paths)
        return self.base.get_file_info(paths)

    def get_file_info_selector(self, selector):
        self.events.append(("get_file_info_selector", selector.base_dir))
        return self.base.get_file_info(selector)

    def open_input_file(self, path):
        self.events.append(("open_input_file", path))
        return self.base.open_input_file(path)

    def open_input_stream(self, path):
        self.events.append(("open_input_stream", path))
        return self.base.open_input_stream(path)

    def create_dir(self, path, recursive):
        return self.base.create_dir(path, recursive=recursive)

    def delete_dir(self, path):
        return self.base.delete_dir(path)

    def delete_dir_contents(self, path, missing_dir_ok):
        return self.base.delete_dir_contents(path, missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        raise AssertionError("Test must never delete root contents")

    def delete_file(self, path):
        return self.base.delete_file(path)

    def move(self, source, destination):
        return self.base.move(source, destination)

    def copy_file(self, source, destination):
        return self.base.copy_file(source, destination)

    def open_output_stream(self, path, metadata):
        return self.base.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata):
        return self.base.open_append_stream(path, metadata=metadata)


@pytest.mark.parametrize("columns", [None, []])
@pytest.mark.parametrize("mode", ["marker_only", "mixed", "whole_file", "legacy_empty"])
def test_packed_marker_records_actual_filesystem_operations(
    tmp_path, monkeypatch, columns, mode
):
    partition = tmp_path / "p=1"
    partition.mkdir()
    control_path = partition / "__empty__"
    data_path = partition / "data.parquet"
    table = pa.table({"x": [1, 2]})
    for path in (control_path, data_path):
        pq.write_table(table, path, row_group_size=2)
    handler = RecordingLocalHandler()
    filesystem = fs.PyFileSystem(handler)
    schema = table.schema.append(pa.field("p", pa.string()))
    reader = ParquetFileReader(
        schema=schema,
        filesystem=filesystem,
        columns=columns,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            base_dir=str(tmp_path),
            field_names=["p"],
            filesystem=filesystem,
        ),
    )
    paths = [str(control_path)]
    chunks: list[dict[str, Any] | None] = [marker().to_metadata()]
    if mode == "mixed":
        paths += [str(data_path), str(data_path)]
        chunks += [
            FileChunk(unit_ids=(0,), num_rows=2, size_bytes=16).to_metadata()
        ] * 2
    elif mode == "whole_file":
        chunks = [None]
    elif mode == "legacy_empty":
        chunks = [FileChunk(unit_ids=(), num_rows=0, size_bytes=0).to_metadata()]
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[
            control_path.stat().st_size
            if path == str(control_path)
            else data_path.stat().st_size
            for path in paths
        ],
        chunk_metadatas=chunks,
    )
    packer = OnlineBinPacker(1024)
    packer.add_input(manifest)
    packer.finalize()
    handler.events.clear()
    from ray.data._internal.datasource_v2.common import file_reader

    native_dataset = file_reader.pds.dataset
    dataset_sources = []

    def record_dataset(*args, **kwargs):
        dataset_sources.append(kwargs["source"])
        return native_dataset(*args, **kwargs)

    monkeypatch.setattr(file_reader.pds, "dataset", record_dataset)
    output = []
    while packer.has_partition():
        output.extend(reader.read(packer.next_partition()))
    expected_sources = (
        []
        if mode == "marker_only"
        else [str(data_path) if mode == "mixed" else str(control_path)]
    )
    assert [[file.path for file in files] for files in dataset_sources] == (
        [expected_sources] if expected_sources else []
    )
    for files in dataset_sources:
        for file in files:
            assert file.size == (
                data_path.stat().st_size
                if file.path == str(data_path)
                else control_path.stat().st_size
            )
    if mode == "marker_only":
        assert handler.events == []
        assert len(output) == 1 and output[0].num_rows == 0
        assert output[0].schema.equals(schema if columns is None else pa.schema([]))
    elif mode == "mixed":
        assert sum(block.num_rows for block in output) == 4
        assert not any(path == str(control_path) for _, path in handler.events)
        assert any(
            operation == "open_input_file" and path == str(data_path)
            for operation, path in handler.events
        )
    else:
        assert sum(block.num_rows for block in output) == (
            2 if mode == "whole_file" else 0
        )
        if mode == "whole_file":
            assert any(path == str(control_path) for _, path in handler.events)
        else:
            # Known sizes avoid metadata lookups, but ordinary empty units still
            # construct a Dataset and do not yield a schema-only block.
            assert output == []
            assert handler.events == []
    # This is a direct protocol test, not a listing-pruning performance observer.
    # Real-worker pruning tests retain the admitted built-in local filesystem.
    print({"mode": mode, "columns": columns, "filesystem_operations": handler.events})


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
