"""Public-API checks for schema fidelity on Ray workers."""

import os
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.parquet_test_utils import write_map_with_empty_struct
from ray.data.tests.schema_planning_utils import (
    read_schema_records as _records,
    recording_filesystem as _filesystem,
)


@pytest.fixture
def schema_files_dir(tmp_path):
    with TemporaryDirectory(dir=tmp_path) as directory:
        yield Path(directory)


@pytest.mark.parametrize("empty_struct", [False, True])
def test_worker_parquet_map_schema_preserves_ipc_lossy_fields(
    monkeypatch,
    tmp_path,
    schema_files_dir,
    ray_start_regular_shared_2_cpus,
    empty_struct,
):
    monkeypatch.delenv("RAY_SCHEMA_TEST", raising=False)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 2)
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
        str(path): 1 + int(empty_struct and index < 2)
        for index, path in enumerate(paths)
    }
    assert all(r["pid"] != os.getpid() for r in records)


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
