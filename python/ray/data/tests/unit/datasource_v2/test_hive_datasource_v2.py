"""Unit tests for the metadata-backed HiveServer2 datasource."""

import sys

import pyarrow as pa
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.formats.hive import hive_datasource_v2
from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveConnectionOptions,
    HiveReadSpec,
)
from ray.data._internal.datasource_v2.formats.hive.hive_datasource_v2 import (
    HiveDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import DatasourceCategory
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints


def _query_spec():
    return HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"),
        query="SELECT id FROM events",
        schema=pa.schema([("id", pa.int64())]),
    )


def test_datasource_has_one_opaque_read_unit(monkeypatch):
    spec = _query_spec()
    datasource = HiveDatasourceV2(spec)
    assert datasource.category == DatasourceCategory.DATABASE
    assert datasource.paths == ["hive://read"]
    assert datasource.schema_needs_file_sample is False
    assert (
        datasource.get_file_partitioner(
            hints=PartitionHints(min_bucket_size=1, max_bucket_size=2, num_buckets=7)
        )
        is None
    )
    assert datasource.infer_schema(None) == spec.schema
    manifests = list(
        datasource._get_file_indexer().list_files(datasource.paths, filesystem=None)
    )
    assert len(manifests) == 1
    assert len(manifests[0]) == 1
    expected = pa.table({"id": [1]})
    calls = []

    def read_batches(actual_spec, actual_schema):
        calls.append((actual_spec, actual_schema))
        return iter([expected])

    monkeypatch.setattr(hive_datasource_v2, "read_hs2_batches", read_batches)
    scanner = datasource.create_scanner(spec.schema)
    assert scanner.read_schema() == spec.schema
    assert list(scanner.create_reader().read(manifests[0])) == [expected]
    assert calls == [(spec, spec.schema)]


@pytest.mark.parametrize("excluded", [set(), {"other://read"}, {"hive://read"}])
def test_indexer_honors_excluded_read_units_without_hive_io(monkeypatch, excluded):
    def unexpected_io(*args):
        pytest.fail("Listing must not connect to HiveServer2")

    monkeypatch.setattr(hive_datasource_v2, "infer_table_schema", unexpected_io)
    monkeypatch.setattr(hive_datasource_v2, "read_hs2_batches", unexpected_io)
    datasource = HiveDatasourceV2(_query_spec())
    indexer = datasource._get_file_indexer()
    manifests = list(
        indexer.list_files(
            datasource.paths, filesystem=None, excluded_read_unit_ids=excluded
        )
    )
    assert len(manifests) == (0 if "hive://read" in excluded else 1)
    infos = list(indexer.list_file_infos(datasource.paths, filesystem=None))
    assert [(info.path, info.size) for info in infos] == [("hive://read", 0)]


def test_query_schema_does_not_request_table_metadata(monkeypatch):
    monkeypatch.setattr(
        hive_datasource_v2,
        "infer_table_schema",
        lambda _: pytest.fail("Query reads must use their explicit schema"),
    )
    spec = _query_spec()
    assert HiveDatasourceV2(spec).infer_schema(None) == spec.schema


def test_table_schema_uses_hive_metadata(monkeypatch):
    spec = HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"), table="events"
    )
    expected = pa.schema([("id", pa.int64())])
    calls = []
    monkeypatch.setattr(
        hive_datasource_v2,
        "infer_table_schema",
        lambda actual_spec: calls.append(actual_spec) or expected,
    )
    assert HiveDatasourceV2(spec).infer_schema(None) == expected
    assert calls == [spec]


@pytest.mark.parametrize("paths", [[], ["other://read"], ["hive://read"] * 2])
def test_reader_rejects_invalid_work_units_before_hive_io(monkeypatch, paths):
    monkeypatch.setattr(
        hive_datasource_v2,
        "read_hs2_batches",
        lambda *_: pytest.fail("Invalid work units must not execute a query"),
    )
    spec = _query_spec()
    reader = HiveDatasourceV2(spec).create_scanner(spec.schema).create_reader()
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=[0] * len(paths), chunk_metadatas=[None] * len(paths)
    )
    with pytest.raises(ValueError, match="exactly one read unit"):
        list(reader.read(manifest))


def test_scanner_and_reader_survive_worker_serialization(monkeypatch):
    spec = _query_spec()
    expected = pa.table({"id": [1]})
    monkeypatch.setattr(
        hive_datasource_v2, "read_hs2_batches", lambda *_: iter([expected])
    )
    scanner = HiveDatasourceV2(spec).create_scanner(spec.schema)
    scanner = cloudpickle.loads(cloudpickle.dumps(scanner))
    assert scanner.read_schema() == spec.schema
    reader = cloudpickle.loads(cloudpickle.dumps(scanner.create_reader()))
    manifest = FileManifest.construct_manifest(
        paths=["hive://read"], sizes=[0], chunk_metadatas=[None]
    )
    assert list(reader.read(manifest)) == [expected]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
