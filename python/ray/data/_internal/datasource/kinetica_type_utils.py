"""
Type conversion utilities for mapping between PyArrow and Kinetica types.

This module provides bidirectional type conversion between PyArrow schemas
and Kinetica column definitions.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa
except ImportError:
    pa = None


def _check_pyarrow():
    """Check if PyArrow is available."""
    if pa is None:
        raise ImportError(
            "PyArrow is required for Kinetica integration. "
            "Install it with: pip install pyarrow"
        )


def _check_gpudb():
    """Check if gpudb is available."""
    try:
        import gpudb

        return gpudb
    except ImportError:
        raise ImportError(
            "gpudb is required for Kinetica integration. "
            "Install it with: pip install gpudb"
        )


def create_gpudb_client(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
):
    """
    Create and return a GPUdb client instance.

    This is a shared factory function used by both KineticaDatasource and
    KineticaDatasink to ensure consistent client construction.

    Args:
        url: URL of the Kinetica server (e.g., "http://localhost:9191").
        username: Username for authentication.
        password: Password for authentication.
        options: Additional GPUdb client options.

    Returns:
        A configured GPUdb client instance.
    """
    gpudb = _check_gpudb()
    GPUdb = gpudb.GPUdb

    gpudb_options = GPUdb.Options()

    # Use explicit None checks to allow empty string credentials
    # (consistent with SQL connection path in KineticaConnectionFactory)
    if username is not None:
        gpudb_options.username = username
    if password is not None:
        gpudb_options.password = password

    # Apply additional options
    if options:
        for key, value in options.items():
            if hasattr(gpudb_options, key):
                setattr(gpudb_options, key, value)

    return GPUdb(host=url, options=gpudb_options)


def _column_type_to_array_inner(column_type: Any) -> str:
    """
    Convert a GPUdbRecordColumn type constant to its array inner type string.

    This helper is used when constructing array() property strings for both
    variable-length lists and fixed-size lists to avoid code duplication.

    Args:
        column_type: A GPUdbRecordColumn._ColumnType constant.

    Returns:
        The lowercase string representation for use in array() properties.

    Raises:
        TypeError: If the column type is not supported as an array element type.
            Kinetica only supports int, long, float, double, and string as
            array inner types.
    """
    gpudb = _check_gpudb()
    GPUdbRecordColumn = gpudb.GPUdbRecordColumn

    type_map = {
        GPUdbRecordColumn._ColumnType.INT: "int",
        GPUdbRecordColumn._ColumnType.LONG: "long",
        GPUdbRecordColumn._ColumnType.FLOAT: "float",
        GPUdbRecordColumn._ColumnType.DOUBLE: "double",
        GPUdbRecordColumn._ColumnType.STRING: "string",
    }
    result = type_map.get(column_type)
    if result is None:
        # Get a readable name for the unsupported type
        type_name = getattr(column_type, "name", str(column_type))
        raise TypeError(
            f"Unsupported array element type: {type_name}. "
            f"Kinetica arrays only support: int, long, float, double, string. "
            f"Consider converting binary/bytes data to base64-encoded strings."
        )
    return result


def arrow_to_kinetica_type(
    arrow_type: "pa.DataType",
    column_name: str = "",
) -> Tuple[str, List[str]]:
    """
    Convert a PyArrow data type to Kinetica column type and properties.

    Args:
        arrow_type: The PyArrow data type to convert.
        column_name: Optional column name for error messages.

    Returns:
        A tuple of (kinetica_base_type, list_of_properties).

    Raises:
        TypeError: If the PyArrow type cannot be converted.
    """
    _check_pyarrow()
    gpudb = _check_gpudb()
    GPUdbRecordColumn = gpudb.GPUdbRecordColumn
    GPUdbColumnProperty = gpudb.GPUdbColumnProperty

    # Boolean
    if pa.types.is_boolean(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, [GPUdbColumnProperty.BOOLEAN]

    # Integer types
    if pa.types.is_int8(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, [GPUdbColumnProperty.INT8]
    if pa.types.is_int16(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, [GPUdbColumnProperty.INT16]
    if pa.types.is_int32(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, []
    if pa.types.is_int64(arrow_type):
        return GPUdbRecordColumn._ColumnType.LONG, []

    # Unsigned integer types
    if pa.types.is_uint8(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, [GPUdbColumnProperty.INT16]
    if pa.types.is_uint16(arrow_type):
        return GPUdbRecordColumn._ColumnType.INT, []
    if pa.types.is_uint32(arrow_type):
        return GPUdbRecordColumn._ColumnType.LONG, []
    if pa.types.is_uint64(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.ULONG]

    # Float types
    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return GPUdbRecordColumn._ColumnType.FLOAT, []
    if pa.types.is_float64(arrow_type):
        return GPUdbRecordColumn._ColumnType.DOUBLE, []

    # Decimal types
    if pa.types.is_decimal(arrow_type):
        precision = arrow_type.precision
        scale = arrow_type.scale
        return GPUdbRecordColumn._ColumnType.STRING, [f"decimal({precision},{scale})"]

    # String types
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, []
    if hasattr(pa.types, "is_utf8"):
        if pa.types.is_utf8(arrow_type) or pa.types.is_large_utf8(arrow_type):
            return GPUdbRecordColumn._ColumnType.STRING, []

    # UUID (must come before generic fixed-size binary check)
    if pa.types.is_fixed_size_binary(arrow_type) and arrow_type.byte_width == 16:
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.UUID]

    # Binary types
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return GPUdbRecordColumn._ColumnType.BYTES, []
    if pa.types.is_fixed_size_binary(arrow_type):
        return GPUdbRecordColumn._ColumnType.BYTES, []

    # Date types
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.DATE]

    # Time types
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.TIME]

    # Timestamp types
    if pa.types.is_timestamp(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.DATETIME]

    # Duration
    if pa.types.is_duration(arrow_type):
        return GPUdbRecordColumn._ColumnType.LONG, []

    # List/Array types
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        value_type = arrow_type.value_type
        inner_type, _ = arrow_to_kinetica_type(value_type)
        array_inner = _column_type_to_array_inner(inner_type)
        return GPUdbRecordColumn._ColumnType.STRING, [f"array({array_inner})"]

    # Fixed-size list (vector)
    if pa.types.is_fixed_size_list(arrow_type):
        list_size = arrow_type.list_size
        value_type = arrow_type.value_type
        if pa.types.is_float32(value_type) or pa.types.is_float64(value_type):
            return GPUdbRecordColumn._ColumnType.BYTES, [f"vector({list_size})"]
        else:
            inner_type, _ = arrow_to_kinetica_type(value_type)
            array_inner = _column_type_to_array_inner(inner_type)
            return GPUdbRecordColumn._ColumnType.STRING, [
                f"array({array_inner},{list_size})"
            ]

    # Struct types -> JSON
    if pa.types.is_struct(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.JSON]

    # Map types -> JSON
    if pa.types.is_map(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.JSON]

    # Dictionary encoded
    if pa.types.is_dictionary(arrow_type):
        return arrow_to_kinetica_type(arrow_type.value_type, column_name)

    # Null type
    if pa.types.is_null(arrow_type):
        return GPUdbRecordColumn._ColumnType.STRING, [GPUdbColumnProperty.NULLABLE]

    raise TypeError(
        f"Cannot convert PyArrow type '{arrow_type}' to Kinetica type"
        + (f" for column '{column_name}'" if column_name else "")
    )


def kinetica_to_arrow_type(column: Any) -> "pa.DataType":
    """
    Convert a Kinetica column definition to a PyArrow data type.

    Args:
        column: The GPUdbRecordColumn to convert.

    Returns:
        The corresponding PyArrow data type.
    """
    _check_pyarrow()
    gpudb = _check_gpudb()
    GPUdbRecordColumn = gpudb.GPUdbRecordColumn
    GPUdbColumnProperty = gpudb.GPUdbColumnProperty

    base_type = column.column_type
    properties = column.column_properties
    # Lowercase both the properties and the constants for consistent comparison
    prop_set = {p.lower() for p in properties}

    # Boolean
    if GPUdbColumnProperty.BOOLEAN.lower() in prop_set:
        return pa.bool_()

    # Int8
    if GPUdbColumnProperty.INT8.lower() in prop_set:
        return pa.int8()

    # Int16
    if GPUdbColumnProperty.INT16.lower() in prop_set:
        return pa.int16()

    # UUID
    if GPUdbColumnProperty.UUID.lower() in prop_set:
        return pa.string()

    # ULong
    if GPUdbColumnProperty.ULONG.lower() in prop_set:
        return pa.uint64()

    # Date
    if GPUdbColumnProperty.DATE.lower() in prop_set:
        return pa.date32()

    # Time
    if GPUdbColumnProperty.TIME.lower() in prop_set:
        return pa.time64("us")

    # Datetime
    if GPUdbColumnProperty.DATETIME.lower() in prop_set:
        return pa.timestamp("us")

    # Timestamp
    if GPUdbColumnProperty.TIMESTAMP.lower() in prop_set:
        return pa.timestamp("ms")

    # Decimal (note: is_decimal is a @property, not a method like is_json())
    if column.is_decimal:
        # Use explicit None check to preserve valid 0 values for scale
        precision = (
            column.precision
            if column.precision is not None
            else GPUdbRecordColumn.DEFAULT_DECIMAL_PRECISION
        )
        scale = (
            column.scale
            if column.scale is not None
            else GPUdbRecordColumn.DEFAULT_DECIMAL_SCALE
        )
        return pa.decimal128(precision, scale)

    # JSON
    if column.is_json():
        return pa.string()

    # Array
    # Note: column.array_type returns _ColumnType constants for int/long/float/
    # double/string, but GPUdbColumnProperty strings for boolean/ulong.
    if column.is_array():
        array_type = column.array_type
        if array_type == GPUdbRecordColumn._ColumnType.INT:
            return pa.list_(pa.int32())
        elif array_type == GPUdbRecordColumn._ColumnType.LONG:
            return pa.list_(pa.int64())
        elif array_type == GPUdbRecordColumn._ColumnType.FLOAT:
            return pa.list_(pa.float32())
        elif array_type == GPUdbRecordColumn._ColumnType.DOUBLE:
            return pa.list_(pa.float64())
        elif array_type == GPUdbRecordColumn._ColumnType.STRING:
            return pa.list_(pa.string())
        elif array_type == GPUdbColumnProperty.BOOLEAN:
            return pa.list_(pa.bool_())
        elif array_type == GPUdbColumnProperty.ULONG:
            return pa.list_(pa.uint64())
        else:
            return pa.list_(pa.string())

    # Vector
    if column.is_vector():
        dim = column.vector_dimension
        if dim and dim > 0:
            return pa.list_(pa.float32(), dim)
        else:
            return pa.list_(pa.float32())

    # IPV4
    if GPUdbColumnProperty.IPV4.lower() in prop_set:
        return pa.string()

    # WKT
    if GPUdbColumnProperty.WKT.lower() in prop_set:
        if base_type == GPUdbRecordColumn._ColumnType.BYTES:
            return pa.binary()
        return pa.string()

    # CharN types
    for prop in properties:
        prop_lower = prop.lower()
        if prop_lower.startswith("char"):
            match = re.match(r"char(\d+)", prop_lower)
            if match:
                return pa.string()

    # Base types
    if base_type == GPUdbRecordColumn._ColumnType.INT:
        return pa.int32()
    elif base_type == GPUdbRecordColumn._ColumnType.LONG:
        return pa.int64()
    elif base_type == GPUdbRecordColumn._ColumnType.FLOAT:
        return pa.float32()
    elif base_type == GPUdbRecordColumn._ColumnType.DOUBLE:
        return pa.float64()
    elif base_type == GPUdbRecordColumn._ColumnType.STRING:
        return pa.string()
    elif base_type == GPUdbRecordColumn._ColumnType.BYTES:
        return pa.binary()

    raise TypeError(
        f"Cannot convert Kinetica type '{base_type}' with properties "
        f"{properties} to PyArrow type for column '{column.name}'"
    )


def arrow_schema_to_kinetica_columns(
    schema: "pa.Schema",
    primary_keys: Optional[List[str]] = None,
    shard_keys: Optional[List[str]] = None,
) -> List:
    """
    Convert a PyArrow schema to a list of Kinetica column definitions.

    Args:
        schema: The PyArrow schema to convert.
        primary_keys: Optional list of column names to mark as primary keys.
        shard_keys: Optional list of column names to mark as shard keys.

    Returns:
        A list of GPUdbRecordColumn objects.
    """
    _check_pyarrow()
    gpudb = _check_gpudb()
    GPUdbRecordColumn = gpudb.GPUdbRecordColumn
    GPUdbColumnProperty = gpudb.GPUdbColumnProperty

    # Handle Ray Data Schema objects
    if hasattr(schema, "base_schema"):
        schema = schema.base_schema

    primary_keys = set(primary_keys or [])
    shard_keys = set(shard_keys or [])

    # Validate primary/shard keys exist in schema
    schema_columns = {field.name for field in schema}
    invalid_primary = primary_keys - schema_columns
    invalid_shard = shard_keys - schema_columns
    if invalid_primary or invalid_shard:
        invalid_cols = invalid_primary | invalid_shard
        raise ValueError(
            f"Primary/shard keys reference non-existent columns: "
            f"{sorted(invalid_cols)}. Available columns: {sorted(schema_columns)}"
        )

    columns = []

    for field in schema:
        base_type, properties = arrow_to_kinetica_type(field.type, field.name)

        if field.name in primary_keys:
            properties.append(GPUdbColumnProperty.PRIMARY_KEY)
        if field.name in shard_keys:
            properties.append(GPUdbColumnProperty.SHARD_KEY)

        is_nullable = field.nullable

        column = GPUdbRecordColumn(
            name=field.name,
            column_type=base_type,
            column_properties=properties,
            is_nullable=is_nullable,
        )
        columns.append(column)

    return columns


def kinetica_type_to_arrow_schema(record_type: Any) -> "pa.Schema":
    """
    Convert a Kinetica record type to a PyArrow schema.

    Args:
        record_type: The GPUdbRecordType to convert.

    Returns:
        A PyArrow Schema object.
    """
    _check_pyarrow()

    fields = []
    for column in record_type.columns:
        arrow_type = kinetica_to_arrow_type(column)
        field = pa.field(column.name, arrow_type, nullable=column.is_nullable)
        fields.append(field)

    return pa.schema(fields)


def _is_date_time_column(col_def: Any) -> Optional[str]:
    """Check if a column is a date/time type and return the type name.

    Args:
        col_def: The column definition to check.

    Returns:
        The date/time type name ('date', 'time', 'datetime', 'timestamp'),
        or None if not a date/time column.
    """
    gpudb = _check_gpudb()
    GPUdbColumnProperty = gpudb.GPUdbColumnProperty

    if col_def is None:
        return None

    prop_set = {p.lower() for p in col_def.column_properties}

    if GPUdbColumnProperty.DATE.lower() in prop_set:
        return "date"
    if GPUdbColumnProperty.TIME.lower() in prop_set:
        return "time"
    if GPUdbColumnProperty.DATETIME.lower() in prop_set:
        return "datetime"
    if GPUdbColumnProperty.TIMESTAMP.lower() in prop_set:
        return "timestamp"
    return None


def convert_arrow_batch_to_records(
    batch: "pa.RecordBatch",
    columns: List,
) -> List[Dict[str, Any]]:
    """
    Convert a PyArrow RecordBatch to a list of dictionaries for Kinetica insertion.

    Args:
        batch: The PyArrow RecordBatch to convert.
        columns: The Kinetica column definitions.

    Returns:
        A list of dictionaries, each representing a record.
    """
    _check_pyarrow()
    import json
    import struct
    from datetime import date, datetime, time

    # Handle None columns - create empty map if no column definitions provided
    column_map = {col.name: col for col in columns} if columns else {}
    col_names = batch.schema.names
    num_rows = batch.num_rows

    # Pre-process columns: convert to Python lists and identify special columns
    col_data = {}
    # col_types values: 'json', 'array', 'vector', 'decimal', 'date', 'time',
    # 'datetime', 'timestamp', or None for normal columns
    col_types = {}
    for col_name in col_names:
        col_def = column_map.get(col_name)
        # Convert column to Python list directly from batch (avoids full
        # table conversion)
        col_data[col_name] = batch.column(col_name).to_pylist()

        if col_def:
            if col_def.is_json():
                col_types[col_name] = "json"
            elif col_def.is_array():
                col_types[col_name] = "array"
            elif col_def.is_vector():
                col_types[col_name] = "vector"
                # Check if Arrow source is float64 - Kinetica vectors only support
                # float32, so precision will be lost
                arrow_field = batch.schema.field(col_name)
                if pa.types.is_fixed_size_list(arrow_field.type):
                    value_type = arrow_field.type.value_type
                    if pa.types.is_float64(value_type):
                        logger.warning(
                            f"Column '{col_name}' contains float64 vectors but "
                            "Kinetica's vector type only supports float32. "
                            "Precision will be lost during conversion."
                        )
            elif col_def.is_decimal:
                col_types[col_name] = "decimal"
            else:
                # Check for date/time types
                dt_type = _is_date_time_column(col_def)
                col_types[
                    col_name
                ] = dt_type  # Could be 'date', 'time', 'datetime', 'timestamp', or None
        else:
            col_types[col_name] = None

    records = []
    for i in range(num_rows):
        record = {}
        for col_name in col_names:
            value = col_data[col_name][i]
            col_type = col_types[col_name]

            if value is None:
                record[col_name] = None
            elif col_type == "json":
                if isinstance(value, (dict, list)):
                    record[col_name] = json.dumps(value)
                else:
                    record[col_name] = value
            elif col_type == "array":
                if isinstance(value, list):
                    # Use json.dumps for proper JSON array format
                    # str() produces single-quoted strings which is invalid JSON
                    record[col_name] = json.dumps(value)
                else:
                    record[col_name] = value
            elif col_type == "vector":
                if isinstance(value, list):
                    try:
                        record[col_name] = struct.pack(f"{len(value)}f", *value)
                    except (struct.error, TypeError) as e:
                        raise ValueError(
                            f"Failed to pack vector values for column '{col_name}': "
                            f"expected list of floats, got {type(value).__name__} "
                            f"with values that cannot be converted. Error: {e}"
                        ) from e
                else:
                    record[col_name] = value
            elif col_type == "decimal":
                # Convert decimal to string representation for Kinetica
                record[col_name] = str(value)
            elif col_type == "date":
                # Convert date to ISO format string (YYYY-MM-DD)
                # Check datetime first since datetime is a subclass of date
                if isinstance(value, datetime):
                    # Extract just the date part for date-typed columns
                    record[col_name] = value.date().isoformat()
                elif isinstance(value, date):
                    record[col_name] = value.isoformat()
                else:
                    # value is not None here (handled by if block at line 569)
                    record[col_name] = str(value)
            elif col_type == "time":
                # Convert time to ISO format string (HH:MM:SS.ffffff)
                if isinstance(value, time):
                    record[col_name] = value.isoformat()
                else:
                    # value is not None here (handled by if block at line 569)
                    record[col_name] = str(value)
            elif col_type in ("datetime", "timestamp"):
                # Convert datetime to ISO format string (YYYY-MM-DDTHH:MM:SS.ffffff)
                if isinstance(value, datetime):
                    record[col_name] = value.isoformat()
                else:
                    # value is not None here (handled by if block at line 569)
                    record[col_name] = str(value)
            else:
                # Handle any remaining date/time types that weren't detected
                # by column properties
                if isinstance(value, datetime):
                    record[col_name] = value.isoformat()
                elif isinstance(value, date):
                    record[col_name] = value.isoformat()
                elif isinstance(value, time):
                    record[col_name] = value.isoformat()
                else:
                    record[col_name] = value

        records.append(record)

    return records


def convert_records_to_arrow_table(
    records: List[Dict[str, Any]],
    schema: "pa.Schema",
) -> "pa.Table":
    """
    Convert a list of record dictionaries to a PyArrow Table.

    Args:
        records: List of dictionaries representing records.
        schema: The PyArrow schema to use.

    Returns:
        A PyArrow Table.
    """
    _check_pyarrow()

    if not records:
        # Create empty arrays for each field in the schema
        empty_arrays = [pa.array([], type=field.type) for field in schema]
        return pa.Table.from_arrays(empty_arrays, schema=schema)

    columns = {field.name: [] for field in schema}

    for record in records:
        for field in schema:
            value = record.get(field.name)
            columns[field.name].append(value)

    arrays = []
    for field in schema:
        col_data = columns[field.name]
        try:
            array = pa.array(col_data, type=field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as initial_error:
            # Try creating array without explicit type, then cast
            try:
                array = pa.array(col_data)
                if array.type != field.type:
                    array = array.cast(field.type)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as cast_error:
                raise pa.ArrowTypeError(
                    f"Failed to convert column '{field.name}' to type {field.type}. "
                    f"Initial error: {initial_error}. Cast error: {cast_error}"
                ) from cast_error
        arrays.append(array)

    return pa.Table.from_arrays(arrays, schema=schema)
