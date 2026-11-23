"""Public-API checks for schema fidelity on Ray workers."""

import os
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileManifest,
    FileMetadata,
)
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.parquet_test_utils import write_map_with_empty_struct
from ray.data.tests.schema_planning_utils import (
    RecordingLocalFileSystem,
    get_prelisted_blocks,
    read_schema_records as _records,
    recording_filesystem as _filesystem,
)


@pytest.fixture
def schema_files_dir(tmp_path):
    with TemporaryDirectory(dir=tmp_path) as directory:
        yield Path(directory)


@pytest.mark.parametrize("empty_struct", [False, True])
@pytest.mark.parametrize("schema_only", [False, True])
def test_worker_parquet_map_schema_preserves_ipc_lossy_fields(
    monkeypatch,
    tmp_path,
    schema_files_dir,
    ray_start_regular_shared_2_cpus,
    empty_struct,
    schema_only,
):
    monkeypatch.delenv("RAY_SCHEMA_TEST", raising=False)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 2)
    if schema_only:
        monkeypatch.setattr(
            inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", 0
        )
    values = pa.array([[("key", "value")]], type=pa.map_(pa.string(), pa.string()))
    table = pa.table({"attrs": values}).replace_schema_metadata({b"source": b"map"})
    paths = [schema_files_dir / f"map{i}.parquet" for i in range(3)]
    for path in paths:
        if empty_struct:
            write_map_with_empty_struct(path, table)
        else:
            pq.write_table(table, path)
    expected = pq.read_schema(paths[0])
    if empty_struct:
        assert expected.field("empty").type == pa.struct([])
        assert pq.read_table(paths[0]).num_rows == 1
        with pytest.raises(pa.ArrowNotImplementedError):
            pq.write_metadata(expected, pa.BufferOutputStream())
    decoded = pa.ipc.read_schema(pa.BufferReader(expected.serialize()))
    assert not expected.equals(decoded, check_metadata=True)
    trace = tmp_path / "map-reads.jsonl"
    actual = (
        ray.data.read_parquet(
            [str(path) for path in paths],
            merge_schema=True,
            filesystem=_filesystem(trace, require_env="RAY_SCHEMA_TEST"),
            runtime_env={"env_vars": {"RAY_SCHEMA_TEST": "enabled"}},
        )
        .schema(fetch_if_missing=False)
        .base_schema
    )
    assert actual.equals(expected, check_metadata=True)
    assert (
        actual.field("attrs").type.field(0).name
        == expected.field("attrs").type.field(0).name
    )
    records = _records(trace)
    assert Counter(r["path"] for r in records if r["pid"] != os.getpid()) == {
        str(path): 1 + int(schema_only and empty_struct and index < 2)
        for index, path in enumerate(paths)
    }
    assert all(r["pid"] != os.getpid() for r in records)


@pytest.mark.parametrize("budget", [0, 32])
def test_driver_parquet_empty_struct_schema_with_metadata_budget(
    monkeypatch, tmp_path, budget
):
    path = tmp_path / "map.parquet"
    values = pa.array([[("key", "value")]], type=pa.map_(pa.string(), pa.string()))
    write_map_with_empty_struct(path, pa.table({"attrs": values}))
    expected = pq.read_schema(path)
    assert expected.field("empty").type == pa.struct([])
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", budget
    )
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", 4096
    )
    raw = RecordingLocalFileSystem(skip_instance_cache=True)
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(raw))
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    source = ParquetDatasourceV2([str(path)], filesystem=fs)
    assert source._retain_planning_metadata is bool(budget)
    result = source.infer_full_schema_with_metadata(
        sample, ray_remote_args={"num_cpus": 0}
    )
    assert result.schema.equals(expected, check_metadata=True)
    assert len(raw.opens) == 1
    metadata = result.metadata
    if budget == 0:
        assert metadata is None
    else:
        assert metadata is not None
        file_metadata = FileMetadata.from_metadata(metadata[0].as_py())
        assert file_metadata.num_rows == pq.read_metadata(path).num_rows


class _CustomType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "ray.tests.schema_reads")

    def __arrow_ext_serialize__(self):
        return b"v1"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


@pytest.mark.parametrize("case", ["custom_extension", "late_object", "map_extension"])
def test_worker_schemas_preserve_driver_unifier(
    monkeypatch, tmp_path, ray_start_regular_shared_2_cpus, case
):
    monkeypatch.delenv("RAY_SCHEMA_TEST", raising=False)
    custom = _CustomType()
    pa.register_extension_type(custom)
    try:
        if case in ("custom_extension", "map_extension"):
            array = pa.ExtensionArray.from_storage(custom, pa.array([b"value"]))
            columns = {"id": array}
            if case == "map_extension":
                columns["attrs"] = pa.array(
                    [[("key", "value")]], type=pa.map_(pa.string(), pa.string())
                )
            tables = [pa.table(columns)] * 3
        else:
            array = pa.ExtensionArray.from_storage(
                ArrowPythonObjectType(), pa.array([b"opaque"], type=pa.large_binary())
            )
            tables = [
                pa.table({"id": [1]}),
                pa.table({"id": ["one"]}),
                pa.table({"id": array}),
            ]
        paths = [tmp_path / f"f{i}.parquet" for i in range(3)]
        for path, table in zip(paths, tables):
            pq.write_table(table, path)
        file_schemas = [pq.read_schema(p) for p in paths]
        if case == "late_object":
            with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid)):
                unify_schemas_with_validation(file_schemas[:2])
        expected = unify_schemas_with_validation(file_schemas)
        assert isinstance(expected, pa.Schema)
        if case == "late_object":
            assert isinstance(expected.field("id").type, ArrowPythonObjectType)
        ctx = DataContext.get_current()
        monkeypatch.setattr(ctx, "use_datasource_v2", True)
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 2)
        trace = tmp_path / "schema-reads.jsonl"
        schema = ray.data.read_parquet(
            [str(p) for p in paths],
            merge_schema=True,
            filesystem=_filesystem(trace, require_env="RAY_SCHEMA_TEST"),
            runtime_env={"env_vars": {"RAY_SCHEMA_TEST": "enabled"}},
        ).schema(fetch_if_missing=False)
        assert schema is not None and schema.base_schema.equals(
            expected, check_metadata=True
        )
        records = _records(trace)
        assert {record["path"] for record in records} == {str(p) for p in paths}
        assert all(record["pid"] != os.getpid() for record in records)
    finally:
        pa.unregister_extension_type(custom.extension_name)


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_merge_schema_caches_each_duplicate_path(
    tmp_path, ray_start_regular_shared_2_cpus, datasource_v2
):
    path = tmp_path / "data.parquet"
    rows = list(range(1000))
    pq.write_table(pa.table({"x": rows}), path, row_group_size=100)
    trace = tmp_path / "duplicate-reads.jsonl"
    dataset = ray.data.read_parquet(
        [str(path)] * 3, merge_schema=True, filesystem=_filesystem(trace)
    )
    before = _records(trace)
    assert len(before) == 3
    assert dataset.count() == 3 * len(rows)
    assert _records(trace) == before
    assert sorted(row["x"] for row in dataset.take_all()) == sorted(rows * 3)


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_merge_schema_generated_id_binds_after_read(
    tmp_path,
    restore_data_context,
    monkeypatch,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
):
    from ray.data._internal.datasource_v2.interfaces.file_manifest import (
        FILE_METADATA_COLUMN_NAME,
    )
    from ray.data._internal.logical.operators import ReadFiles
    from ray.data._internal.logical.rules.bind_generated_id_column import (
        BindGeneratedIdColumn,
    )
    from ray.data.checkpoint import CheckpointConfig

    first, last = tmp_path / "a.parquet", tmp_path / "z.parquet"
    column = "late"
    pq.write_table(pa.table({"id": [0, 1]}), first)
    pq.write_table(pa.table({"id": [2, 3], column: [20, 30]}), last)
    restore_data_context.checkpoint_config = None
    dataset = ray.data.read_parquet(
        [str(first), str(last)], merge_schema=True, override_num_blocks=1
    )
    blocks = get_prelisted_blocks(dataset._logical_plan.dag)
    assert [
        value["num_rows"]
        for block in blocks
        for value in block[FILE_METADATA_COLUMN_NAME].to_pylist()
    ] == [2, 2]
    projected = dataset.select_columns(["id", column])
    monkeypatch.setattr(
        projected.context,
        "checkpoint_config",
        CheckpointConfig(
            checkpoint_path=str(tmp_path / "checkpoints"),
            generated_id_column="generated_id",
        ),
    )
    bound = BindGeneratedIdColumn().apply(projected._logical_plan)
    bound_read = next(
        op for op in bound.dag.post_order_iter() if isinstance(op, ReadFiles)
    )
    bound_blocks = get_prelisted_blocks(bound_read)
    assert len(bound_blocks) == len(blocks)
    assert all(a.equals(b) for a, b in zip(blocks, bound_blocks))
    rows = sorted(projected.take_all(), key=lambda row: row["id"])
    assert [row["id"] for row in rows] == list(range(4))
    assert [row["late"] for row in rows] == [None, None, 20, 30]
    assert len({tuple(sorted(r["generated_id"].items())) for r in rows}) == 4


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_count_footer_reads_preserve_runtime_env(
    tmp_path, monkeypatch, ray_start_regular_shared_2_cpus, datasource_v2
):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), path)
    trace = tmp_path / "count-reads.jsonl"
    monkeypatch.setenv("RAY_METADATA_ACCESS", "enabled")
    dataset = ray.data.read_parquet(
        str(path),
        filesystem=_filesystem(trace, require_env="RAY_METADATA_ACCESS"),
        runtime_env={"env_vars": {"RAY_METADATA_ACCESS": "enabled"}},
    )
    before = _records(trace)
    monkeypatch.delenv("RAY_METADATA_ACCESS")

    assert dataset.count() == 3
    reads = _records(trace)[len(before) :]
    assert reads and all(record["pid"] != os.getpid() for record in reads)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
