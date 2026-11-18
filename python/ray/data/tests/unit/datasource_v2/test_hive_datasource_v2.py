"""Unit tests for the metadata-backed HiveServer2 datasource."""

import os
import sys
from dataclasses import asdict, is_dataclass
from typing import cast

import pyarrow as pa
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.formats.hive import hive_datasource_v2
from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveReadSpec,
)
from ray.data._internal.datasource_v2.formats.hive.hive_datasource_v2 import (
    HiveDatasourceV2,
    _HiveIndexer,
    _HiveScanner,
    _HiveTableScanner,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import DatasourceCategory
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.metadata_exporter import Topology, sanitize_for_struct

_TABLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={b"field": b"id"}),
        ("name", pa.string()),
        ("score", pa.float64()),
    ],
    metadata={b"source": b"hive"},
)


def _query_spec():
    return HiveReadSpec(
        lambda: object(),
        query="SELECT id FROM events",
        schema=pa.schema([("id", pa.int64())]),
    )


def _table_scanner(limit=None):
    return _HiveTableScanner(
        HiveReadSpec(lambda: None, table="events", limit=limit), _TABLE_SCHEMA
    )


def _read(scanner, name):
    return ReadFiles(
        datasource_name=name,
        scanner=scanner,
        schema=scanner.read_schema(),
        parallelism=1,
        input_dependencies=[InputData([])],
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

    def read_batches(actual_spec, actual_schema, **options):
        calls.append((actual_spec, actual_schema, options))
        return iter([expected])

    monkeypatch.setattr(hive_datasource_v2, "read_hs2_batches", read_batches)
    scanner = datasource.create_scanner(spec.schema)
    assert scanner.read_schema() == spec.schema
    assert isinstance(scanner, _HiveScanner)
    assert not isinstance(
        scanner, (SupportsColumnPruning, SupportsFilterPushdown, SupportsLimitPushdown)
    )
    assert list(scanner.create_reader().read(manifests[0])) == [expected]
    assert calls == [(spec, spec.schema, {})]


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
    spec = HiveReadSpec(lambda: object(), table="events")
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


def test_serialized_factory_reads_credentials_when_called(monkeypatch):
    class Cursor:
        description = [("id", "BIGINT", None, None, None, None, None)]

        def __init__(self, value):
            self._rows = [(value,)]

        def execute(self, statement):
            assert statement == "SELECT id FROM events"

        def fetchmany(self, size):
            rows, self._rows = self._rows, []
            return rows

        def cancel_operation(self):
            pass

        def close(self):
            pass

    class Connection:
        def __init__(self, credential):
            self._credential = credential

        def cursor(self, *, user):
            assert user == "session-user"
            assert self._credential == "worker-credential"
            return Cursor(1)

        def close(self):
            pass

    def connection_factory():
        return Connection(os.environ["HIVE_FACTORY_TEST_CREDENTIAL"])

    monkeypatch.setenv("HIVE_FACTORY_TEST_CREDENTIAL", "driver-credential")
    spec = HiveReadSpec(
        connection_factory,
        user="session-user",
        query="SELECT id FROM events",
        schema=pa.schema([("id", pa.int64())]),
    )
    scanner = HiveDatasourceV2(spec).create_scanner(spec.schema)
    serialized = cloudpickle.dumps(scanner)
    assert b"driver-credential" not in serialized

    monkeypatch.setenv("HIVE_FACTORY_TEST_CREDENTIAL", "worker-credential")
    scanner = cloudpickle.loads(serialized)
    reader = cloudpickle.loads(cloudpickle.dumps(scanner.create_reader()))
    manifest = FileManifest.construct_manifest(
        paths=["hive://read"], sizes=[0], chunk_metadatas=[None]
    )
    assert [table.to_pylist() for table in reader.read(manifest)] == [[{"id": 1}]]


def test_table_scanner_capabilities_clone_and_projection():
    original = _table_scanner()
    assert isinstance(original, SupportsColumnPruning)
    assert isinstance(original, SupportsLimitPushdown)
    assert not isinstance(original, SupportsFilterPushdown)
    final = cloudpickle.loads(
        cloudpickle.dumps(original.prune_columns(["name"]).push_limit(20).push_limit(5))
    )
    assert original.columns is None and original.limit is None
    assert final.pruned_column_names() == ("name",)
    assert final.pushed_limit() == 5
    assert final.read_schema().metadata == _TABLE_SCHEMA.metadata
    assert final.create_reader()._options["statement"] == (
        "SELECT `name` FROM `default`.`events` LIMIT 5"
    )
    assert (
        original.prune_columns(["id"])
        .read_schema()
        .field("id")
        .equals(_TABLE_SCHEMA.field("id"), check_metadata=True)
    )
    with pytest.raises(ValueError, match="unavailable"):
        final.prune_columns(["id"])


@pytest.mark.parametrize(
    "limit,expected",
    [(0, 0), (2**31 - 1, 2**31 - 1), (2**31, None), (2**63, None)],
)
def test_dataset_limit_sql_integer_boundary(limit, expected):
    original = _table_scanner()
    scanner = original.push_limit(limit)
    assert scanner.pushed_limit() == expected
    assert (scanner is original) == (expected is None)
    assert original.limit is None


@pytest.mark.parametrize(
    "limit,source_limit,existing_limit",
    [(True, None, None), (False, None, None), (True, 5, 2), (False, 5, 2)],
)
def test_boolean_limit_remains_in_ray(limit, source_limit, existing_limit):
    scanner = _table_scanner(source_limit)
    if existing_limit is not None:
        scanner = scanner.push_limit(existing_limit)
    previous_limit = scanner.pushed_limit()
    assert scanner.push_limit(limit) is scanner
    assert scanner.pushed_limit() == previous_limit


@pytest.mark.parametrize(
    "source_limit,limits,expected",
    [
        (None, (2**31, 10), 10),
        (None, (10, 2**31), 10),
        (100, (2**31,), 100),
        (5, (2,), 2),
    ],
)
def test_limits_preserve_smaller_source_and_accepted_limits(
    source_limit, limits, expected
):
    scanner = _table_scanner(source_limit)
    for limit in limits:
        scanner = scanner.push_limit(limit)
    assert scanner.pushed_limit() == expected


@pytest.mark.parametrize("source", ["parquet", "hive_query"])
def test_non_table_read_metadata_preserves_original_args(source):
    if source == "hive_query":
        scanner = _HiveScanner(_query_spec(), pa.schema([("id", pa.int64())]))
        name = "Hive"
    else:
        scanner = ParquetScanner(schema=_TABLE_SCHEMA)
        name = "ParquetV2"
    read = _read(scanner, name)
    original = super(ReadFiles, read)._get_args()
    current = read._get_args()
    assert current["_scanner"] is original["_scanner"] is scanner
    assert sanitize_for_struct(current) == sanitize_for_struct(original)


@pytest.mark.parametrize("source", ["query", "table"])
def test_topology_does_not_export_hive_scanner_spec(source):
    marker = "synthetic_private_value"
    schema = pa.schema([("value", pa.string())])
    if source == "query":
        spec = HiveReadSpec(
            lambda: None, query=f"SELECT '{marker}' AS value", schema=schema
        )
        scanner = HiveDatasourceV2(spec).create_scanner(schema)
        assert not is_dataclass(scanner)
    else:
        spec = HiveReadSpec(lambda: None, table="events", user=marker)
        scanner = _HiveTableScanner(spec, schema)

    read = _read(scanner, "Hive")
    listing = ListFiles(
        paths=["hive://read"],
        source_paths=["hive://read"],
        file_indexer=_HiveIndexer(),
        filesystem=None,
    )

    class PhysicalRead:
        name = "Read"
        id = "read"
        input_dependencies = []
        _logical_operators = [listing, read]
        _get_logical_args = PhysicalOperator._get_logical_args

        def post_order_iter(self):
            yield self

    physical = cast(PhysicalOperator, PhysicalRead())
    topology = Topology.create_topology_metadata(physical, {physical: "read"})
    assert marker not in str(asdict(topology))
    if source == "table":
        assert "<Hive table scanner>" in str(asdict(topology))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
