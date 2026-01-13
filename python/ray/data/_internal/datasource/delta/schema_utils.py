"""Schema utilities for Delta Lake type conversion and validation."""

import json
from typing import Any, Optional

import pyarrow as pa


def types_compatible(expected: pa.DataType, actual: pa.DataType) -> bool:
    """Check if actual type can be written to expected type column."""
    if expected == actual:
        return True

    # Integer: actual must fit within expected width
    if pa.types.is_integer(expected) and pa.types.is_integer(actual):
        if pa.types.is_signed_integer(expected) != pa.types.is_signed_integer(actual):
            return False
        return getattr(actual, "bit_width", 64) <= getattr(expected, "bit_width", 64)

    # Floating point: all compatible
    if pa.types.is_floating(expected) and pa.types.is_floating(actual):
        return True

    # String variants
    if _is_string(expected) and _is_string(actual):
        return True

    # Binary variants
    if _is_binary(expected) and _is_binary(actual):
        return True

    # Boolean
    if pa.types.is_boolean(expected) and pa.types.is_boolean(actual):
        return True

    # Date variants
    if _is_date(expected) and _is_date(actual):
        return True

    # Timestamp: must match timezone
    if pa.types.is_timestamp(expected) and pa.types.is_timestamp(actual):
        return getattr(expected, "tz", None) == getattr(actual, "tz", None)

    # Decimal: precision and scale must match
    if pa.types.is_decimal(expected) and pa.types.is_decimal(actual):
        return expected.precision == actual.precision and expected.scale == actual.scale

    return False


def schemas_compatible(existing: pa.Schema, new: pa.Schema) -> bool:
    """Check if new schema is compatible with existing schema for append."""
    existing_types = {f.name: f.type for f in existing}
    for field in new:
        if field.name not in existing_types:
            return False
        if not types_compatible(existing_types[field.name], field.type):
            return False
    return True


def convert_schema_to_delta(schema: pa.Schema) -> Any:
    """Convert PyArrow schema to Delta Lake schema.

    Uses deltalake.Schema.from_arrow() with JSON fallback for unsupported types.

    Args:
        schema: PyArrow schema to convert.

    Returns:
        Delta Lake Schema object.

    Raises:
        ValueError: If schema cannot be converted.
    """
    import logging

    # deltalake: https://delta-io.github.io/delta-rs/python/
    from deltalake import Schema as DeltaSchema

    if schema is None:
        raise ValueError("Cannot convert None schema to Delta Lake schema")

    try:
        return DeltaSchema.from_arrow(schema)
    except (ValueError, TypeError) as e:
        # Log the original error before trying fallback
        logging.getLogger(__name__).debug(
            f"Direct schema conversion failed ({e}), trying JSON fallback"
        )
        try:
            return DeltaSchema.from_json(_schema_to_json(schema))
        except Exception as json_err:
            raise ValueError(
                f"Failed to convert schema to Delta format. "
                f"Direct conversion error: {e}. JSON fallback error: {json_err}"
            ) from json_err


def pyarrow_type_to_delta_type(pa_type: pa.DataType) -> str:
    """Convert PyArrow type to Delta Lake type string.

    Args:
        pa_type: PyArrow data type.

    Returns:
        Delta Lake type string.

    Raises:
        ValueError: If type is not supported.
    """
    # Delta Lake types: https://docs.delta.io/latest/delta-batch.html#data-types
    if pa.types.is_int8(pa_type):
        return "byte"
    if pa.types.is_int16(pa_type):
        return "short"
    if pa.types.is_int32(pa_type):
        return "integer"
    if pa.types.is_int64(pa_type):
        return "long"
    if pa.types.is_uint8(pa_type):
        return "short"
    if pa.types.is_uint16(pa_type):
        return "integer"
    if pa.types.is_uint32(pa_type):
        return "long"
    if pa.types.is_uint64(pa_type):
        return "decimal(20,0)"  # uint64 can exceed int64 max
    if pa.types.is_float16(pa_type):
        return "float"  # float16 maps to float
    if pa.types.is_float32(pa_type):
        return "float"
    if pa.types.is_float64(pa_type):
        return "double"
    if _is_string(pa_type):
        return "string"
    if _is_binary(pa_type):
        return "binary"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if _is_date(pa_type):
        return "date"
    if pa.types.is_timestamp(pa_type):
        return "timestamp"
    if pa.types.is_time(pa_type):
        # Delta doesn't have native time type, use string
        return "string"
    if pa.types.is_duration(pa_type):
        # Delta doesn't have native duration type, use long (nanoseconds)
        return "long"
    if pa.types.is_decimal(pa_type):
        return f"decimal({pa_type.precision},{pa_type.scale})"
    if pa.types.is_null(pa_type):
        # Null type maps to string (least restrictive)
        return "string"

    # Unsupported types: nested types, dictionary, etc.
    raise ValueError(
        f"Unsupported PyArrow type for Delta Lake: {pa_type}. "
        "Nested types (struct, list, map) and dictionary types are not supported."
    )


def infer_partition_type(value: Any) -> pa.DataType:
    """Infer PyArrow type from partition value.

    Args:
        value: Partition value (can be string, int, float, bool, or None).

    Returns:
        Inferred PyArrow data type.

    Note:
        For string values that look like numbers (e.g., "123"), we keep them
        as strings to preserve the original type intention. Only actual
        Python numeric types are inferred as numeric.
    """
    if value is None:
        return pa.string()

    # Check actual Python types first (not string parsing)
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()

    # For string values, try to infer type but be conservative
    if isinstance(value, str):
        # Only infer bool if explicitly true/false
        if value.lower() in ("true", "false"):
            return pa.bool_()
        # Keep strings as strings - don't try to parse as numbers
        # This preserves the original type intention
        return pa.string()

    # Default to string for unknown types
    return pa.string()


# Private helpers


def _is_string(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _is_binary(t: pa.DataType) -> bool:
    return pa.types.is_binary(t) or pa.types.is_large_binary(t)


def _is_date(t: pa.DataType) -> bool:
    return pa.types.is_date32(t) or pa.types.is_date64(t)


def _schema_to_json(schema: pa.Schema) -> str:
    """Convert PyArrow schema to Delta schema JSON."""
    fields = [
        {
            "name": f.name,
            "type": pyarrow_type_to_delta_type(f.type),
            "nullable": f.nullable,
            "metadata": {},
        }
        for f in schema
    ]
    return json.dumps({"type": "struct", "fields": fields})
