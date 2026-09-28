"""Focused tests for the HiveServer2 reader and its Ray Data contract."""

import sys
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pytest

from ray.data._internal.datasource import hive_hs2
from ray.data._internal.datasource.hive_contract import (
    HiveConnectionOptions,
    HiveReadSpec,
)
from ray.data._internal.datasource_v2.hive_datasource import HiveDatasource
from ray.data.read_api import read_hive


class _Cursor:
    description = [
        ("id", "BIGINT", None, None, None, None, None),
        ("name", "STRING", None, None, None, None, None),
    ]

    def __init__(self, batches=()):
        self.batches = list(batches)
        self.statements = []
        self.closed = False
        self.cancelled = False

    def execute(self, statement):
        self.statements.append(statement)

    def fetchmany(self, size):
        assert size == hive_hs2._FETCH_ROWS
        return self.batches.pop(0) if self.batches else []

    def get_table_schema(self, table, database):
        assert (table, database) == ("events", "analytics")
        return [("id", "BIGINT"), ("name", "varchar(20)")]

    def cancel_operation(self):
        self.cancelled = True

    def close(self):
        self.closed = True


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False
        self.cursor_user = None

    def cursor(self, *, user):
        self.cursor_user = user
        return self._cursor

    def close(self):
        self.closed = True


def _query_spec(schema=None):
    if schema is None:
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    return HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL", user="reader"),
        query="SELECT id, name FROM analytics.events",
        schema=schema,
    )


def test_connection_uses_explicit_single_attempt_and_verifies_tls(monkeypatch):
    calls = []
    impala = ModuleType("impala")
    dbapi = ModuleType("impala.dbapi")
    dbapi.connect = lambda **kwargs: calls.append(kwargs) or object()
    monkeypatch.setitem(sys.modules, "impala", impala)
    monkeypatch.setitem(sys.modules, "impala.dbapi", dbapi)

    options = HiveConnectionOptions(
        host="hs2",
        auth_mechanism="PLAIN",
        user="reader",
        password="secret",
        use_ssl=True,
    )
    hive_hs2._connect(options)
    assert calls[0]["retries"] == 1
    assert calls[0]["verify_cert"] is True
    assert calls[0]["password"] == "secret"

    hive_hs2._connect(
        HiveConnectionOptions(
            host="hs2", auth_mechanism="GSSAPI", kerberos_service_name="custom-hive"
        )
    )
    assert calls[1]["auth_mechanism"] == "GSSAPI"
    assert calls[1]["kerberos_service_name"] == "custom-hive"


def test_table_schema_uses_metadata_and_closes_session(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"),
        table="analytics.events",
    )

    schema = hive_hs2.infer_table_schema(spec)

    assert schema == pa.schema([("id", pa.int64()), ("name", pa.string())])
    assert cursor.statements == []
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    "hive_type, arrow_type",
    [
        ("BOOLEAN", pa.bool_()),
        ("TINYINT", pa.int8()),
        ("SMALLINT", pa.int16()),
        ("INT", pa.int32()),
        ("BIGINT", pa.int64()),
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
        ("STRING", pa.string()),
        ("CHAR(4)", pa.string()),
        ("BINARY", pa.binary()),
        ("DATE", pa.date32()),
        ("DECIMAL(12, 2)", pa.decimal128(12, 2)),
    ],
)
def test_hive_scalar_type_mapping(hive_type, arrow_type):
    assert hive_hs2._arrow_type(hive_type) == arrow_type


@pytest.mark.parametrize(
    "hive_type", ["array<int>", "map<string,int>", "decimal(39,0)", "TIMESTAMP"]
)
def test_unsupported_types_fail_closed(hive_type):
    with pytest.raises(ValueError, match="unsupported column type"):
        hive_hs2._arrow_type(hive_type)


@pytest.mark.parametrize(
    "column, arrow_type",
    [
        (("name", "CHAR", None, None, None, None, None), pa.string()),
        (("name", "VARCHAR", None, None, None, None, None), pa.string()),
        (("amount", "DECIMAL", None, None, 12, 2, None), pa.decimal128(12, 2)),
    ],
)
def test_result_metadata_type_mapping(column, arrow_type):
    assert hive_hs2._result_arrow_type(column) == arrow_type


@pytest.mark.parametrize(
    "column",
    [
        ("amount", "DECIMAL", None, None, None, None, None),
        ("amount", "DECIMAL", None, None, 39, 0, None),
        ("items", "ARRAY", None, None, None, None, None),
        ("created_at", "TIMESTAMP", None, None, None, None, None),
    ],
)
def test_unsupported_result_metadata_fails_closed(column):
    with pytest.raises(ValueError, match="unsupported column type"):
        hive_hs2._result_arrow_type(column)


def test_one_statement_yields_bounded_batches_and_closes(monkeypatch):
    cursor = _Cursor([[(1, "a")], [(2, "b")]])
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = _query_spec()

    tables = list(hive_hs2.read_hs2_batches(spec, spec.schema))

    assert cursor.statements == [spec.query]
    assert [table.to_pylist() for table in tables] == [
        [{"id": 1, "name": "a"}],
        [{"id": 2, "name": "b"}],
    ]
    assert cursor.cancelled and cursor.closed and connection.closed
    assert connection.cursor_user == "reader"


def test_table_limit_zero_skips_connection(monkeypatch):
    monkeypatch.setattr(
        hive_hs2, "_connect", lambda _: pytest.fail("zero limit must not connect")
    )
    spec = HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"),
        table="events",
        limit=0,
    )
    schema = pa.schema([("id", pa.int64())])
    assert list(hive_hs2.read_hs2_batches(spec, schema))[0].schema == schema


def test_table_statement_uses_validated_identifiers_and_limit():
    spec = HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"),
        table="analytics.events",
        limit=3,
    )
    assert hive_hs2._statement(spec) == "SELECT * FROM `analytics`.`events` LIMIT 3"


def test_result_schema_mismatch_closes_without_fetch(monkeypatch):
    cursor = _Cursor()
    cursor.description = [
        ("different", "BIGINT", None, None, None, None, None),
        ("name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = _query_spec()
    with pytest.raises(ValueError, match="do not match"):
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert cursor.closed and connection.closed


def test_table_result_accepts_only_its_qualified_column_names(monkeypatch):
    cursor = _Cursor([[(1, "a")]])
    cursor.description = [
        ("events.id", "BIGINT", None, None, None, None, None),
        ("events.name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = HiveReadSpec(
        HiveConnectionOptions(host="hs2", auth_mechanism="NOSASL"),
        table="analytics.events",
    )
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

    tables = list(hive_hs2.read_hs2_batches(spec, schema))

    assert tables[0].to_pylist() == [{"id": 1, "name": "a"}]
    assert cursor.closed and connection.closed
    with pytest.raises(ValueError, match="result columns do not match"):
        hive_hs2._check_result_schema(cursor.description, schema)
    with pytest.raises(ValueError, match="result columns do not match"):
        hive_hs2._check_result_schema(cursor.description, schema, "other")


def test_result_type_mismatch_fails_before_fetch(monkeypatch):
    cursor = _Cursor([[(1, "a")]])
    cursor.description = [
        ("id", "DOUBLE", None, None, None, None, None),
        ("name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = _query_spec()
    with pytest.raises(ValueError, match="result types do not match"):
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert cursor.batches == [[(1, "a")]]
    assert cursor.closed and connection.closed


def test_conversion_error_does_not_expose_row_value():
    schema = pa.schema([("id", pa.int64())])
    with pytest.raises(ValueError, match="could not be converted") as exc:
        hive_hs2._to_arrow([("private-row-value",)], schema)
    assert "private-row-value" not in str(exc.value)


def test_non_nullable_query_field_rejects_null_rows():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(ValueError, match="non-nullable"):
        hive_hs2._to_arrow([(None,)], schema)


@pytest.mark.parametrize("error_type", [RuntimeError, ValueError])
def test_fetch_error_does_not_expose_query_or_secret(monkeypatch, error_type):
    class FailingCursor(_Cursor):
        def fetchmany(self, size):
            raise error_type("private-query private-password")

    cursor = FailingCursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = _query_spec()
    with pytest.raises(RuntimeError, match="HiveServer2 read failed") as exc:
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert "private-query" not in str(exc.value)
    assert cursor.closed and connection.closed


def test_early_close_cancels_operation(monkeypatch):
    cursor = _Cursor([[(1, "a")], [(2, "b")]])
    connection = _Connection(cursor)
    monkeypatch.setattr(hive_hs2, "_connect", lambda _: connection)
    spec = _query_spec()
    batches = hive_hs2.read_hs2_batches(spec, spec.schema)
    next(batches)
    batches.close()
    assert cursor.cancelled and cursor.closed and connection.closed


def test_datasource_has_one_opaque_read_unit(monkeypatch):
    spec = _query_spec()
    datasource = HiveDatasource(spec)
    assert datasource.paths == ["hive://read"]
    assert datasource.get_file_partitioner() is None
    assert datasource.infer_schema(None) == spec.schema
    manifests = list(datasource._get_file_indexer().list_files(datasource.paths))
    assert len(manifests) == 1
    assert len(manifests[0]) == 1
    monkeypatch.setattr(
        "ray.data._internal.datasource_v2.hive_datasource.read_hs2_batches",
        lambda *_: iter([pa.table({"id": [1], "name": ["a"]})]),
    )
    scanner = datasource.create_scanner(spec.schema)
    assert scanner.read_schema() == spec.schema
    assert list(scanner.create_reader().read(manifests[0]))[0].num_rows == 1


def test_public_api_passes_task_options_and_repartitions(monkeypatch):
    from ray.data import read_api

    class FakeDataset:
        context = SimpleNamespace(max_errored_blocks=3)
        repartition_count = None

        def repartition(self, count):
            self.repartition_count = count
            return self

    dataset = FakeDataset()
    calls = []
    monkeypatch.setattr(
        read_api,
        "_read_datasource_v2",
        lambda datasource, **kwargs: calls.append((datasource, kwargs)) or dataset,
    )
    schema = pa.schema([("id", pa.int64())])
    result = read_hive(
        host="hs2",
        auth_mechanism="NOSASL",
        query="SELECT id FROM events",
        schema=schema,
        num_cpus=2,
        resources={"custom": 1},
        override_num_blocks=4,
    )
    assert result is dataset
    assert calls[0][0].infer_schema(None) == schema
    assert calls[0][1]["parallelism"] == 1
    assert calls[0][1]["ray_remote_args"] == {"max_retries": 0}
    assert calls[0][1]["num_cpus"] == 2
    assert calls[0][1]["resources"] == {"custom": 1}
    assert dataset.context.max_errored_blocks == 0
    assert dataset.repartition_count == 4


@pytest.mark.parametrize("block_count", [0, -1, True, 1.5])
def test_public_api_rejects_invalid_block_counts(block_count):
    with pytest.raises(ValueError, match="override_num_blocks"):
        read_hive(
            host="hs2",
            auth_mechanism="NOSASL",
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
            override_num_blocks=block_count,
        )


def test_public_api_requires_authentication_selection():
    with pytest.raises(TypeError, match="auth_mechanism"):
        read_hive(
            host="hs2",
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
