"""Unit tests for HiveServer2 connections, metadata, and batch conversion."""

import sys
from typing import Generator, List, Optional, Sequence, Tuple, cast

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.hive import hive_hs2
from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveReadSpec,
)


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

    def get_table_schema(self, table, database) -> Sequence[Tuple[str, Optional[str]]]:
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


def _create_connection():
    return _Connection(_Cursor())


def _query_spec(schema=None, connection_factory=_create_connection):
    if schema is None:
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    return HiveReadSpec(
        connection_factory,
        user="reader",
        query="SELECT id, name FROM analytics.events",
        schema=schema,
    )


def test_connection_factory_is_called_without_arguments():
    connections = []

    def connection_factory():
        connection = _create_connection()
        connections.append(connection)
        return connection

    first = hive_hs2._connect(connection_factory)
    second = hive_hs2._connect(connection_factory)
    assert first is connections[0]
    assert second is connections[1]
    assert first is not second


@pytest.mark.parametrize("error_type", [RuntimeError, ValueError, ImportError])
def test_connection_factory_failure_is_not_retried(error_type):
    calls = []

    def connection_factory():
        calls.append(None)
        raise error_type("factory failed")

    with pytest.raises(RuntimeError, match="HiveServer2 connection failed") as exc:
        hive_hs2._connect(connection_factory)
    assert len(calls) == 1
    assert isinstance(exc.value.__cause__, error_type)


def test_metadata_and_data_use_separate_factory_connections():
    metadata_cursor = _Cursor()
    data_cursor = _Cursor([[(1, "a")]])
    connections = [_Connection(metadata_cursor), _Connection(data_cursor)]
    calls = []

    def connection_factory():
        connection = connections[len(calls)]
        calls.append(connection)
        return connection

    spec = HiveReadSpec(
        connection_factory, user="session-user", table="analytics.events"
    )
    schema = hive_hs2.infer_table_schema(spec)
    assert calls == connections[:1]
    tables = list(hive_hs2.read_hs2_batches(spec, schema))

    assert calls == connections
    assert all(connection.closed for connection in connections)
    assert all(connection.cursor_user == "session-user" for connection in connections)
    assert metadata_cursor.statements == []
    assert data_cursor.statements == [hive_hs2._statement(spec)]
    assert tables[0].to_pylist() == [{"id": 1, "name": "a"}]


@pytest.mark.parametrize("read_mode", ["table", "query"])
def test_cursor_creation_failure_closes_factory_connection(read_mode):
    class FailingConnection(_Connection):
        def cursor(self, *, user):
            raise ValueError("cursor failed")

    connection = FailingConnection(_Cursor())
    if read_mode == "table":
        spec = HiveReadSpec(lambda: connection, table="analytics.events")
        with pytest.raises(RuntimeError, match="table schema lookup failed") as exc:
            hive_hs2.infer_table_schema(spec)
    else:
        spec = _query_spec(connection_factory=lambda: connection)
        with pytest.raises(RuntimeError, match="HiveServer2 read failed") as exc:
            list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert isinstance(exc.value.__cause__, ValueError)
    assert connection.closed


def test_table_schema_uses_metadata_and_closes_session():
    cursor = _Cursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    schema = hive_hs2.infer_table_schema(spec)

    assert schema == pa.schema([("id", pa.int64()), ("name", pa.string())])
    assert cursor.statements == []
    assert cursor.closed and connection.closed


def test_table_schema_rejects_missing_column_type_metadata():
    class MissingTypeCursor(_Cursor):
        def get_table_schema(self, table, database) -> List[Tuple[str, Optional[str]]]:
            return [("id", None)]

    cursor = MissingTypeCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    with pytest.raises(RuntimeError, match="table schema lookup failed") as exc:
        hive_hs2.infer_table_schema(spec)

    assert isinstance(exc.value.__cause__, ValueError)
    assert cursor.closed and connection.closed


def test_table_decimal_schema_recovers_precision_from_describe():
    class DecimalCursor(_Cursor):
        def get_table_schema(self, table, database):
            assert (table, database) == ("events", "analytics")
            return [("amount", "DECIMAL")]

        def fetchall(self):
            return [("amount", "decimal(12,2)", "")]

    cursor = DecimalCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    schema = hive_hs2.infer_table_schema(spec)

    assert schema == pa.schema([("amount", pa.decimal128(12, 2))])
    assert cursor.statements == ["DESCRIBE `analytics`.`events`"]
    assert cursor.closed and connection.closed


def test_metadata_lookup_escapes_identifier_wildcards():
    class UnderscoreCursor(_Cursor):
        def get_table_schema(self, table, database):
            assert (table, database) == ("events\\_2024", "sales\\_db")
            return [("amount", "DECIMAL")]

        def fetchall(self):
            return [("amount", "decimal(12,2)", "")]

    cursor = UnderscoreCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="sales_db.events_2024",
    )

    schema = hive_hs2.infer_table_schema(spec)

    assert schema == pa.schema([("amount", pa.decimal128(12, 2))])
    assert cursor.statements == ["DESCRIBE `sales_db`.`events_2024`"]
    assert cursor.closed and connection.closed


def test_decimal_describe_ignores_partition_header_rows():
    class PartitionedCursor(_Cursor):
        def get_table_schema(self, table, database):
            return [("id", "BIGINT"), ("amount", "DECIMAL"), ("day", "STRING")]

        def fetchall(self):
            return [
                ("id", "bigint", ""),
                ("amount", "decimal(12,2)", ""),
                ("day", "string", ""),
                ("", None, None),
                ("# Partition Information", "", ""),
                ("# col_name", "data_type", ""),
                ("day", "string", ""),
            ]

    cursor = PartitionedCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    schema = hive_hs2.infer_table_schema(spec)

    assert schema == pa.schema(
        [("id", pa.int64()), ("amount", pa.decimal128(12, 2)), ("day", pa.string())]
    )
    assert cursor.closed and connection.closed


def test_decimal_without_describe_precision_fails_closed():
    class SparseCursor(_Cursor):
        def get_table_schema(self, table, database):
            return [("amount", "DECIMAL")]

        def fetchall(self):
            return [("other", "bigint", "")]

    cursor = SparseCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    with pytest.raises(ValueError, match="lack precision and scale.*amount"):
        hive_hs2.infer_table_schema(spec)
    assert cursor.closed and connection.closed


def test_table_schema_rejects_duplicate_column_names():
    class DuplicateCursor(_Cursor):
        def get_table_schema(self, table, database):
            return [("id", "BIGINT"), ("ID", "bigint")]

    cursor = DuplicateCursor()
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
        table="analytics.events",
    )

    with pytest.raises(ValueError, match="duplicate column names"):
        hive_hs2.infer_table_schema(spec)
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


def test_one_statement_yields_bounded_batches_and_closes():
    cursor = _Cursor([[(1, "a")], [(2, "b")]])
    connection = _Connection(cursor)
    spec = _query_spec(connection_factory=lambda: connection)

    tables = list(hive_hs2.read_hs2_batches(spec, spec.schema))

    assert cursor.statements == [spec.query]
    assert [table.to_pylist() for table in tables] == [
        [{"id": 1, "name": "a"}],
        [{"id": 2, "name": "b"}],
    ]
    assert cursor.cancelled and cursor.closed and connection.closed
    assert connection.cursor_user == "reader"


def test_table_limit_zero_skips_connection():
    def connection_factory():
        pytest.fail("zero limit must not connect")

    spec = HiveReadSpec(connection_factory, table="events", limit=0)
    schema = pa.schema([("id", pa.int64())])
    assert list(hive_hs2.read_hs2_batches(spec, schema))[0].schema == schema


def test_table_statement_uses_validated_identifiers_and_limit():
    spec = HiveReadSpec(
        _create_connection,
        table="analytics.events",
        limit=3,
    )
    assert hive_hs2._statement(spec) == "SELECT * FROM `analytics`.`events` LIMIT 3"


def test_result_schema_mismatch_closes_without_fetch():
    cursor = _Cursor()
    cursor.description = [
        ("different", "BIGINT", None, None, None, None, None),
        ("name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    spec = _query_spec(connection_factory=lambda: connection)
    with pytest.raises(ValueError, match="do not match"):
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert cursor.closed and connection.closed


def test_table_result_accepts_only_its_qualified_column_names():
    cursor = _Cursor([[(1, "a")]])
    cursor.description = [
        ("events.id", "BIGINT", None, None, None, None, None),
        ("events.name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    spec = HiveReadSpec(
        lambda: connection,
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


@pytest.mark.parametrize(
    ("schema_names", "matches_result_labels"),
    [
        (("events.id", "events.name"), True),
        (("id", "name"), False),
    ],
)
def test_query_read_uses_hs2_result_labels_verbatim(
    schema_names, matches_result_labels
):
    cursor = _Cursor([[(1, "a")]])
    cursor.description = [
        ("events.id", "BIGINT", None, None, None, None, None),
        ("events.name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    schema = pa.schema([(schema_names[0], pa.int64()), (schema_names[1], pa.string())])
    spec = HiveReadSpec(
        lambda: connection,
        query="SELECT * FROM analytics.events",
        schema=schema,
    )

    if matches_result_labels:
        tables = list(hive_hs2.read_hs2_batches(spec, schema))
        assert tables[0].schema == schema
        assert tables[0].to_pylist() == [{"events.id": 1, "events.name": "a"}]
    else:
        with pytest.raises(ValueError, match="result columns do not match"):
            list(hive_hs2.read_hs2_batches(spec, schema))
        assert cursor.batches == [[(1, "a")]]

    assert cursor.statements == [spec.query]
    assert cursor.closed and connection.closed


def test_result_type_mismatch_fails_before_fetch():
    cursor = _Cursor([[(1, "a")]])
    cursor.description = [
        ("id", "DOUBLE", None, None, None, None, None),
        ("name", "STRING", None, None, None, None, None),
    ]
    connection = _Connection(cursor)
    spec = _query_spec(connection_factory=lambda: connection)
    with pytest.raises(ValueError, match="result types do not match"):
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert cursor.batches == [[(1, "a")]]
    assert cursor.closed and connection.closed


def test_conversion_error_preserves_underlying_arrow_error():
    schema = pa.schema([("id", pa.int64())])
    with pytest.raises(ValueError, match="could not be converted") as exc:
        hive_hs2._to_arrow([("private-row-value",)], schema)
    assert "private-row-value" in str(exc.value.__cause__)


def test_non_nullable_query_field_rejects_null_rows():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(ValueError, match="non-nullable"):
        hive_hs2._to_arrow([(None,)], schema)


@pytest.mark.parametrize("error_type", [RuntimeError, ValueError])
def test_fetch_error_preserves_original_exception(error_type):
    class FailingCursor(_Cursor):
        def fetchmany(self, size):
            raise error_type("private-query private-password")

    cursor = FailingCursor()
    connection = _Connection(cursor)
    spec = _query_spec(connection_factory=lambda: connection)
    with pytest.raises(RuntimeError, match="HiveServer2 read failed") as exc:
        list(hive_hs2.read_hs2_batches(spec, spec.schema))
    assert isinstance(exc.value.__cause__, error_type)
    assert "private-query" in str(exc.value.__cause__)
    assert "private-password" in str(exc.value.__cause__)
    assert cursor.closed and connection.closed


def test_early_close_cancels_operation():
    cursor = _Cursor([[(1, "a")], [(2, "b")]])
    connection = _Connection(cursor)
    spec = _query_spec(connection_factory=lambda: connection)
    batches = cast(
        Generator[pa.Table, None, None],
        hive_hs2.read_hs2_batches(spec, spec.schema),
    )
    next(batches)
    batches.close()
    assert cursor.cancelled and cursor.closed and connection.closed


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
