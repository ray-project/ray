"""
Type conversion utilities for mapping between PyArrow and Kinetica types.

This module provides bidirectional type conversion between PyArrow schemas
and Kinetica column definitions.
"""

from typing import List, Tuple, Optional, Any, Dict
import re

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
        array_inner = inner_type
        if inner_type == GPUdbRecordColumn._ColumnType.INT:
            array_inner = "int"
        elif inner_type == GPUdbRecordColumn._ColumnType.LONG:
            array_inner = "long"
        elif inner_type == GPUdbRecordColumn._ColumnType.FLOAT:
            array_inner = "float"
        elif inner_type == GPUdbRecordColumn._ColumnType.DOUBLE:
            array_inner = "double"
        elif inner_type == GPUdbRecordColumn._ColumnType.STRING:
            array_inner = "string"
        return GPUdbRecordColumn._ColumnType.STRING, [f"array({array_inner})"]

    # Fixed-size list (vector)
    if pa.types.is_fixed_size_list(arrow_type):
        list_size = arrow_type.list_size
        value_type = arrow_type.value_type
        if pa.types.is_float32(value_type) or pa.types.is_float64(value_type):
            return GPUdbRecordColumn._ColumnType.BYTES, [f"vector({list_size})"]
        else:
            inner_type, _ = arrow_to_kinetica_type(value_type)
            return GPUdbRecordColumn._ColumnType.STRING, [
                f"array({inner_type},{list_size})"
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


def kinetica_to_arrow_type(column) -> "pa.DataType":
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
    prop_set = {p.lower() for p in properties}

    # Boolean
    if GPUdbColumnProperty.BOOLEAN in prop_set:
        return pa.bool_()

    # Int8
    if GPUdbColumnProperty.INT8 in prop_set:
        return pa.int8()

    # Int16
    if GPUdbColumnProperty.INT16 in prop_set:
        return pa.int16()

    # UUID
    if GPUdbColumnProperty.UUID in prop_set:
        return pa.string()

    # ULong
    if GPUdbColumnProperty.ULONG in prop_set:
        return pa.uint64()

    # Date
    if GPUdbColumnProperty.DATE in prop_set:
        return pa.date32()

    # Time
    if GPUdbColumnProperty.TIME in prop_set:
        return pa.time64("us")

    # Datetime
    if GPUdbColumnProperty.DATETIME in prop_set:
        return pa.timestamp("us")

    # Timestamp
    if GPUdbColumnProperty.TIMESTAMP in prop_set:
        return pa.timestamp("ms")

    # Decimal
    if column.is_decimal:
        precision = column.precision or GPUdbRecordColumn.DEFAULT_DECIMAL_PRECISION
        scale = column.scale or GPUdbRecordColumn.DEFAULT_DECIMAL_SCALE
        return pa.decimal128(precision, scale)

    # JSON
    if column.is_json():
        return pa.string()

    # Array
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
    if GPUdbColumnProperty.IPV4 in prop_set:
        return pa.string()

    # WKT
    if GPUdbColumnProperty.WKT in prop_set:
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


def kinetica_type_to_arrow_schema(record_type) -> "pa.Schema":
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

    column_map = {col.name: col for col in columns}

    records = []
    pydict = batch.to_pydict()

    num_rows = batch.num_rows
    col_names = batch.schema.names

    for i in range(num_rows):
        record = {}
        for col_name in col_names:
            value = pydict[col_name][i]
            col_def = column_map.get(col_name)

            if value is None:
                record[col_name] = None
            elif col_def and col_def.is_json():
                import json

                if isinstance(value, (dict, list)):
                    record[col_name] = json.dumps(value)
                else:
                    record[col_name] = value
            elif col_def and col_def.is_array():
                if isinstance(value, list):
                    record[col_name] = str(value)
                else:
                    record[col_name] = value
            elif col_def and col_def.is_vector():
                import struct

                if isinstance(value, list):
                    record[col_name] = struct.pack(f"{len(value)}f", *value)
                else:
                    record[col_name] = value
            elif col_def and col_def.is_decimal:
                if hasattr(value, "__str__"):
                    record[col_name] = str(value)
                else:
                    record[col_name] = value
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
        return pa.table({}, schema=schema)

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
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            array = pa.array(col_data)
            if array.type != field.type:
                try:
                    array = array.cast(field.type)
                except pa.ArrowInvalid:
                    pass
        arrays.append(array)

    return pa.Table.from_arrays(arrays, schema=schema)
