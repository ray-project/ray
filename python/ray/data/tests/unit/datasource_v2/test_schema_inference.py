"""Contracts for file schema discovery shared by Parquet and ORC."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

from ray.data._internal.datasource_v2.common import schema_inference
from ray.data._internal.datasource_v2.common.schema_inference import (
    _SCHEMA_READ_BATCH_SIZE,
    infer_schema_from_files,
)
from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import unify_schemas_with_validation


def test_schema_reads_bound_submissions_and_preserve_order(monkeypatch):
    batches = []
    workers = []

    class RecordingExecutor(ThreadPoolExecutor):
        def __init__(self, *, max_workers):
            workers.append(max_workers)
            super().__init__(max_workers=max_workers)

        def map(self, fn, *iterables, timeout=None, chunksize=1):
            batches.append(len(iterables[0]))
            return super().map(fn, *iterables, timeout=timeout, chunksize=chunksize)

    monkeypatch.setattr(schema_inference, "ThreadPoolExecutor", RecordingExecutor)
    paths = [f"field_{i}" for i in range(_SCHEMA_READ_BATCH_SIZE + 1)]

    schema = infer_schema_from_files(
        paths, lambda path: pa.schema([(path, pa.int64())]), format_name="test"
    )

    assert workers == [16]
    assert batches == [_SCHEMA_READ_BATCH_SIZE, 1]
    assert schema.names == paths


def test_schema_merge_preserves_one_shot_unifier_semantics(monkeypatch):
    first = pa.schema(
        [pa.field("id", pa.int32(), nullable=False, metadata={"origin": "first"})],
        metadata={"dataset": "first"},
    )
    second = pa.schema(
        [pa.field("id", pa.int64()), pa.field("late", pa.string(), nullable=False)],
        metadata={"dataset": "second"},
    )
    schemas = {"first": first, "second": second}
    calls = []

    def unify(values):
        calls.append(values)
        return unify_schemas_with_validation(values)

    monkeypatch.setattr(schema_inference, "unify_schemas_with_validation", unify)
    actual = infer_schema_from_files(
        list(schemas), schemas.__getitem__, format_name="test"
    )
    expected = unify_schemas_with_validation([first, second])

    assert actual.equals(expected, check_metadata=True)
    assert len(calls) == 1
    assert calls[0] == [first, second]


@pytest.mark.parametrize("format_name", ["Parquet", "ORC"])
def test_schema_conflict_preserves_cause_without_rereading(format_name):
    schemas = {
        "first": pa.schema([("id", pa.int64())]),
        "conflicting": pa.schema([("id", pa.string())]),
    }
    reads = []

    def read_schema(path):
        reads.append(path)
        return schemas[path]

    with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid)) as error:
        infer_schema_from_files(list(schemas), read_schema, format_name=format_name)

    assert f"Failed to merge {format_name} schema from 'conflicting'" in str(
        error.value
    )
    assert type(error.value.__cause__) is type(error.value)
    assert Counter(reads) == {"first": 1, "conflicting": 1}


def test_schema_merge_retains_original_batch_only_error(monkeypatch):
    original = pa.ArrowInvalid("batch-only failure")
    schema = pa.schema([("id", pa.int64())])
    calls = []

    def unify(values):
        calls.append(values)
        if len(calls) == 1:
            raise original
        return values[0]

    monkeypatch.setattr(schema_inference, "unify_schemas_with_validation", unify)
    with pytest.raises(pa.ArrowInvalid) as error:
        infer_schema_from_files(
            ["first", "second"], lambda _: schema, format_name="test"
        )

    assert error.value is original


def test_schema_read_failure_is_preserved():
    original = OSError("unreadable file")

    def read_schema(_):
        raise original

    with pytest.raises(OSError) as error:
        infer_schema_from_files(["first"], read_schema, format_name="test")

    assert error.value is original


def test_schema_inference_empty_paths_does_not_read():
    def read_schema(_):
        pytest.fail("Empty input must not open a file")

    assert infer_schema_from_files([], read_schema, format_name="test") == pa.schema([])


@pytest.mark.parametrize(
    "format_name,datasource_class,write_table,read_schema",
    [
        ("parquet", ParquetDatasourceV2, pq.write_table, pq.read_schema),
        (
            "orc",
            OrcDatasourceV2,
            orc.write_table,
            lambda path: orc.ORCFile(path).schema,
        ),
    ],
)
def test_datasources_use_existing_unifier(
    tmp_path, format_name, datasource_class, write_table, read_schema
):
    paths = [str(tmp_path / f"{i}.{format_name}") for i in range(2)]
    tables = [
        pa.table({"id": pa.array([1], type=pa.int32())}),
        pa.table({"id": [2], "late": ["value"]}),
    ]
    for path, table in zip(paths, tables):
        write_table(table, path)
    sample = FileManifest.construct_manifest(
        paths=paths,
        sizes=[(tmp_path / f"{i}.{format_name}").stat().st_size for i in range(2)],
        chunk_metadatas=[None, None],
    )

    actual = datasource_class([str(tmp_path)]).infer_schema(sample)
    expected = unify_schemas_with_validation([read_schema(path) for path in paths])

    assert actual.equals(expected, check_metadata=True)


def test_parquet_explicit_schema_bypasses_file_reads(tmp_path, monkeypatch):
    schema = pa.schema([("id", pa.int64())], metadata={"explicit": "true"})
    path = str(tmp_path / "not-opened.parquet")
    sample = FileManifest.construct_manifest(
        paths=[path], sizes=[1], chunk_metadatas=[None]
    )

    def read_schema(_):
        pytest.fail("Explicit schema must bypass footer reads")

    monkeypatch.setattr(pq, "read_schema", read_schema)
    actual = ParquetDatasourceV2([path], schema=schema).infer_schema(sample)

    assert actual.equals(schema, check_metadata=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
