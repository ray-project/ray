"""Unit tests for :class:`OrcDatasourceV2`."""

import os
from dataclasses import replace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pyarrow import orc

import ray.data.read_api as read_api
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def test_read_pruning_does_not_depend_on_listing_hook(tmp_path):
    class NoListingPruner(OrcScanner):
        def pushed_partition_pruner(self):
            return None

    scanner = NoListingPruner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.int64())]),
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_partitions(col("year") == 2024)
    paths = [
        str(tmp_path / "root.orc"),
        str(tmp_path / "year=2024" / "data.orc"),
        str(tmp_path / "year=2023" / "data.orc"),
    ]
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * 3, chunk_metadatas=[None] * 3
    )
    assert scanner.pushed_partition_pruner() is None
    assert scanner.prune_input_split(manifest).paths.tolist() == paths[:2]


def _write_orc(path, table):
    with pa.OSFile(str(path), "wb") as sink:
        orc.write_table(table, sink)


def _manifest_of(paths):
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[os.path.getsize(path) for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def test_infer_schema_unifies_files_and_hive_partitions(tmp_path):
    second_dir = tmp_path / "year=2024"
    second_dir.mkdir()
    first_path = tmp_path / "first.orc"
    second_path = second_dir / "second.orc"
    _write_orc(first_path, pa.table({"id": [1]}))
    _write_orc(second_path, pa.table({"id": [2], "name": ["two"]}))

    paths = [str(first_path), str(second_path)]
    sample = _manifest_of(paths)
    datasource = OrcDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        file_extensions=["orc"],
        include_paths=True,
    )

    schema = datasource.infer_schema(sample)

    assert schema.names == ["id", "name", "year", "path"]
    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()
    assert schema.field("year").type == pa.string()
    assert schema.field("path").type == pa.string()


@pytest.mark.parametrize("root_first", [False, True])
def test_resolve_partitioning_and_reuse_generic_file_components(tmp_path, root_first):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    file_path = partition_dir / "data.orc"
    _write_orc(file_path, pa.table({"id": [1]}))
    root = tmp_path / "root.orc"
    _write_orc(root, pa.table({"id": [0]}))
    paths = [root, file_path] if root_first else [file_path, root]
    sample = _manifest_of(paths)
    datasource = OrcDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        file_extensions=["orc"],
    )

    resolved = datasource.resolve_partitioning(sample)

    assert datasource._partitioning is not None
    assert datasource._partitioning.field_names is None
    assert resolved is not datasource._partitioning
    assert resolved is not None
    assert resolved.field_names == ["year"]
    assert isinstance(datasource._get_file_indexer(), NonSamplingFileIndexer)
    partitioner = datasource.get_file_partitioner(
        hints=PartitionHints(min_bucket_size=0, max_bucket_size=1024, num_buckets=1)
    )
    assert isinstance(partitioner, RoundRobinPartitioner)
    partitioner.add_input(sample)
    partitioner.finalize()
    assert partitioner.has_partition()
    assert partitioner.next_partition().paths.tolist() == sample.paths.tolist()


def test_list_and_read_orc_files_with_partition_columns(tmp_path):
    from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner

    for year, table in (
        ("2023", pa.table({"id": [1]})),
        ("2024", pa.table({"id": [2], "name": ["two"]})),
    ):
        partition_dir = tmp_path / f"year={year}"
        partition_dir.mkdir()
        _write_orc(partition_dir / "data.orc", table)

    datasource = OrcDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        file_extensions=["orc"],
        include_paths=True,
    )
    manifests = list(
        datasource._get_file_indexer().list_files(
            pa.array(datasource.paths, type=pa.string()),
            filesystem=datasource.filesystem,
            preserve_order=True,
        )
    )
    manifest = FileManifest.concat(manifests)
    schema = datasource.infer_schema(manifest)
    partitioning = datasource.resolve_partitioning(manifest)
    scanner = datasource.create_scanner(
        schema, filesystem=datasource.filesystem, partitioning=partitioning
    )

    assert isinstance(scanner, OrcScanner)
    result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
    rows = sorted(result.to_pylist(), key=lambda row: row["id"])
    assert [(row["id"], row["name"], row["year"]) for row in rows] == [
        (1, None, "2023"),
        (2, "two", "2024"),
    ]
    assert rows[0]["path"].endswith("year=2023/data.orc")
    assert rows[1]["path"].endswith("year=2024/data.orc")


@pytest.mark.parametrize("merge_schema", [False, True])
def test_read_orc_routes_to_v2_and_forwards_read_options(
    monkeypatch, tmp_path, merge_schema
):
    result = object()
    captured = {}

    def _read_datasource_v2(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    monkeypatch.setattr(read_api, "_read_datasource_v2", _read_datasource_v2)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", True)
    partition_filter = Mock()

    actual = read_api.read_orc(
        str(tmp_path),
        partition_filter=partition_filter,
        include_paths=True,
        concurrency=4,
        parallelism=2,
        override_num_blocks=3,
        merge_schema=merge_schema,
    )

    datasource = captured["datasource"]
    assert actual is result
    assert isinstance(datasource, OrcDatasourceV2)
    assert datasource.paths == [str(tmp_path)]
    assert datasource.file_extensions == ["orc"]
    # Keep the deprecated ``parallelism`` precedence used by ``read_datasource``.
    assert captured["kwargs"]["parallelism"] == 2
    assert captured["kwargs"]["concurrency"] == 4
    assert captured["kwargs"]["partition_filter"] is partition_filter
    assert captured["kwargs"]["sample_all_files_for_schema"] is merge_schema


def test_read_orc_rejects_full_schema_on_v1(monkeypatch, tmp_path):
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", False)
    with pytest.raises(NotImplementedError, match="requires the V2 datasource"):
        read_api.read_orc(str(tmp_path), merge_schema=True)


def test_infer_schema_empty_sample(tmp_path):
    sample = FileManifest.construct_manifest(paths=[], sizes=[], chunk_metadatas=[])
    assert OrcDatasourceV2([str(tmp_path)]).infer_schema(sample) == pa.schema([])


def test_infer_schema_unifies_across_batches(tmp_path):
    from ray.data._internal.datasource_v2.common.schema_inference import (
        _SCHEMA_READ_BATCH_SIZE,
    )

    paths = []
    for i in range(_SCHEMA_READ_BATCH_SIZE + 1):
        path = tmp_path / f"f{i:03d}.orc"
        table = (
            pa.table({"id": [i], "late": ["value"]})
            if i == _SCHEMA_READ_BATCH_SIZE
            else pa.table({"id": [i]})
        )
        _write_orc(path, table)
        paths.append(path)

    schema = OrcDatasourceV2([str(tmp_path)]).infer_schema(_manifest_of(paths))

    assert schema.names == ["id", "late"]


@pytest.mark.parametrize("target_block_size", [None, 128])
@pytest.mark.parametrize("batch_size", [None, 2])
def test_orc_batch_sizing_config_survives_pushdowns(
    tmp_path, monkeypatch, target_block_size, batch_size
):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [0, 1, 2, 3], "payload": ["wide" * 64] * 4})
    _write_orc(path, table)
    context = DataContext.get_current()
    monkeypatch.setattr(context, "target_max_block_size", target_block_size)
    datasource = OrcDatasourceV2([str(path)])
    scanner = datasource.create_scanner(table.schema)
    assert scanner.target_block_size == target_block_size

    # The scanner captures the context value and immutable pushdowns retain it.
    monkeypatch.setattr(context, "target_max_block_size", 8)
    scanner = replace(scanner, batch_size=batch_size)
    pushed, residual = scanner.push_filters(col("id") > 0)
    pushed = pushed.prune_columns(["id"]).push_limit(3)
    assert isinstance(pushed, OrcScanner)
    reader = pushed.create_reader()

    assert residual is None
    assert pushed.target_block_size == target_block_size
    assert reader._target_block_size == target_block_size
    assert reader._explicit_batch_size == batch_size
    batches = list(reader.read(_manifest_of([path])))
    assert pa.concat_tables(batches).to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
    if batch_size is not None:
        assert len(batches) > 1
        assert all(batch.num_rows <= batch_size for batch in batches)


def test_read_orc_routes_to_v1_when_v2_is_disabled(monkeypatch, tmp_path):
    result = object()
    captured = {}

    class FakeORCDatasource:
        def __init__(self, *args, **kwargs):
            captured["v1_init_args"] = args
            captured["v1_init_kwargs"] = kwargs

    def _read_datasource(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    monkeypatch.setattr(read_api, "ORCDatasource", FakeORCDatasource)
    monkeypatch.setattr(read_api, "read_datasource", _read_datasource)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", False)

    actual = read_api.read_orc(
        str(tmp_path), include_paths=True, parallelism=2, override_num_blocks=3
    )

    assert actual is result
    assert isinstance(captured["datasource"], FakeORCDatasource)
    assert captured["v1_init_args"] == (str(tmp_path),)
    assert captured["v1_init_kwargs"]["include_paths"] is True
    assert captured["v1_init_kwargs"]["file_extensions"] == ["orc"]
    assert captured["kwargs"]["parallelism"] == 2
    assert captured["kwargs"]["override_num_blocks"] == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
