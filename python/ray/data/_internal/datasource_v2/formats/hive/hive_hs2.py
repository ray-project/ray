"""HiveServer2 connection, schema, and row conversion helpers."""

import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Callable, Iterator, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveReadSpec,
)

_FETCH_ROWS = 1024
_EMPTY_PROJECTION_COLUMN = "__ray_hive_row"
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)\Z", re.IGNORECASE)
_CHAR = re.compile(r"(?:var)?char\(\d+\)\Z", re.IGNORECASE)
_HIVE_TYPES = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "char": pa.string(),
    "varchar": pa.string(),
    "binary": pa.binary(),
    "date": pa.date32(),
    # Impyla truncates sub-microsecond digits when decoding TIMESTAMP to datetime.
    # Keep the values timezone-naive; this reader does not infer or convert time zones.
    "timestamp": pa.timestamp("us"),
}


def _arrow_type(hive_type: str) -> pa.DataType:
    normalized = hive_type.strip().lower().replace(" ", "")
    if normalized in _HIVE_TYPES:
        return _HIVE_TYPES[normalized]
    if _CHAR.fullmatch(normalized):
        return pa.string()
    decimal = _DECIMAL.fullmatch(normalized)
    if decimal:
        precision, scale = (int(part) for part in decimal.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return pa.decimal128(precision, scale)
    raise ValueError("HiveServer2 table has an unsupported column type")


def _connect(connection_factory: Callable[[], Any]):
    try:
        return connection_factory()
    except Exception as exc:
        raise RuntimeError("HiveServer2 connection failed") from exc


def _metadata_pattern(identifier: str) -> str:
    """Escape HS2 GetColumns wildcards so the identifier matches only itself.

    HiveServer2 treats ``_`` and ``%`` in GetColumns schema/table arguments as
    single- and multi-character wildcards, and ``\\`` as the escape character.
    """
    return identifier.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")


@dataclass(frozen=True)
class HiveTableMetadata:
    """A single metadata snapshot, retaining Hive types lost in Arrow mapping."""

    schema: pa.Schema
    native_types: Tuple[Tuple[str, str], ...]


def infer_table_schema(spec: HiveReadSpec) -> pa.Schema:
    """Return the Arrow schema from the same metadata lookup."""
    return infer_table_metadata(spec).schema


def infer_table_metadata(spec: HiveReadSpec) -> HiveTableMetadata:
    """Use the HS2 metadata operation; never execute a data query."""
    table_identifier = spec.table_identifier
    if table_identifier is None:
        raise ValueError("table schema inference requires a table read")
    database, table = table_identifier
    connection = _connect(spec.connection_factory)
    cursor = None
    try:
        cursor = connection.cursor(user=spec.user)
        metadata_columns = cursor.get_table_schema(
            _metadata_pattern(table), _metadata_pattern(database)
        )
        columns: List[Tuple[str, str]] = []
        for name, type_name in metadata_columns:
            if (
                not isinstance(name, str)
                or not name
                or not isinstance(type_name, str)
                or not type_name.strip()
            ):
                raise ValueError(
                    "HiveServer2 returned incomplete table column metadata"
                )
            columns.append((name, type_name))
        if any(type_name.strip().casefold() == "decimal" for _, type_name in columns):
            # Impyla's GetColumns wrapper drops DECIMAL precision and scale.
            # Hive's DESCRIBE result preserves them without reading table rows.
            cursor.execute(f"DESCRIBE `{database}`.`{table}`")
            described_types = {
                name.casefold(): type_name
                for name, type_name, *_ in cursor.fetchall()
                if name and type_name
            }
            columns = [
                (
                    name,
                    described_types.get(name.casefold(), type_name)
                    if type_name.strip().casefold() == "decimal"
                    else type_name,
                )
                for name, type_name in columns
            ]
    except Exception as exc:
        raise RuntimeError("HiveServer2 table schema lookup failed") from exc
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()

    if not columns:
        raise ValueError("HiveServer2 table has no columns")
    unresolved = [
        name for name, type_name in columns if type_name.strip().casefold() == "decimal"
    ]
    if unresolved:
        raise ValueError(
            "HiveServer2 DECIMAL columns lack precision and scale: "
            + ", ".join(sorted(unresolved))
        )
    names = [name for name, _ in columns]
    if len({name.casefold() for name in names}) != len(names):
        raise ValueError("HiveServer2 table has duplicate column names")
    return HiveTableMetadata(
        pa.schema([(name, _arrow_type(type_name)) for name, type_name in columns]),
        tuple(columns),
    )


def _statement(spec: HiveReadSpec) -> str:
    if spec.query is not None:
        return spec.query
    table_identifier = spec.table_identifier
    if table_identifier is None:
        raise ValueError("HiveServer2 statement requires a table or query")
    database, table = table_identifier
    statement = f"SELECT * FROM `{database}`.`{table}`"
    if spec.limit is not None:
        statement += f" LIMIT {spec.limit}"
    return statement


def _result_arrow_type(column) -> pa.DataType:
    """Map Impyla's result metadata to the supported Arrow scalar type."""
    try:
        type_name = column[1]
        if type_name.upper() == "DECIMAL":
            precision, scale = column[4], column[5]
            if any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (precision, scale)
            ):
                raise ValueError
            type_name = f"DECIMAL({precision},{scale})"
        return _arrow_type(type_name)
    except Exception as exc:
        raise ValueError("HiveServer2 result has an unsupported column type") from exc


def _check_result_schema(
    description, schema: pa.Schema, table_name: Optional[str] = None
) -> None:
    if description is None:
        raise ValueError("HiveServer2 statement did not return a result set")
    names = [column[0] for column in description]
    if table_name is not None:
        qualifier = f"{table_name}."
        names = [
            name[len(qualifier) :]
            if name.casefold().startswith(qualifier.casefold())
            else name
            for name in names
        ]
    if len(names) != len(schema) or any(
        actual.casefold() != expected.casefold()
        for actual, expected in zip(names, schema.names)
    ):
        raise ValueError("HiveServer2 result columns do not match the read schema")
    if any(
        _result_arrow_type(column) != field.type
        for column, field in zip(description, schema)
    ):
        raise ValueError("HiveServer2 result types do not match the read schema")


def _to_arrow(rows, schema: pa.Schema) -> pa.Table:
    try:
        arrays = [
            pa.array([row[index] for row in rows], type=field.type)
            for index, field in enumerate(schema)
        ]
    except Exception as exc:
        raise ValueError(
            "HiveServer2 rows could not be converted to the read schema"
        ) from exc
    if any(
        not field.nullable and array.null_count for field, array in zip(schema, arrays)
    ):
        raise ValueError("HiveServer2 rows violate a non-nullable schema field")
    try:
        return pa.Table.from_arrays(arrays, schema=schema)
    except Exception as exc:
        raise ValueError(
            "HiveServer2 rows could not be converted to the read schema"
        ) from exc


def read_hs2_batches(
    spec: HiveReadSpec,
    schema: pa.Schema,
    *,
    statement: Optional[str] = None,
    limit: Optional[int] = None,
    empty_projection: bool = False,
) -> Iterator[pa.Table]:
    """Execute one statement and yield bounded Arrow batches."""
    if spec.limit == 0 or limit == 0:
        yield pa.Table.from_batches([], schema=schema)
        return

    connection = _connect(spec.connection_factory)
    cursor = None
    try:
        try:
            cursor = connection.cursor(user=spec.user)
            cursor.execute(_statement(spec) if statement is None else statement)
            description = cursor.description
        except Exception as exc:
            raise RuntimeError("HiveServer2 read failed") from exc
        table_identifier = spec.table_identifier
        table_name = table_identifier[1] if table_identifier is not None else None
        result_schema = (
            pa.schema([(_EMPTY_PROJECTION_COLUMN, pa.int32())])
            if empty_projection
            else schema
        )
        _check_result_schema(description, result_schema, table_name)
        while True:
            try:
                rows = cursor.fetchmany(_FETCH_ROWS)
            except Exception as exc:
                raise RuntimeError("HiveServer2 read failed") from exc
            if not rows:
                break
            table = _to_arrow(rows, result_schema)
            if empty_projection:
                # Zero-column blocks count as zero rows in Ray. Keep its existing
                # physical stub; Scanner.read_schema() exposes the logical schema.
                table = pa.table(
                    {_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(table.num_rows)},
                    metadata=schema.metadata,
                )
            yield table
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.cancel_operation()
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()
