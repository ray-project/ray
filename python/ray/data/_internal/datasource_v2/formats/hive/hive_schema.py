"""Hive type metadata for native file reads, without HS2 row decoding."""

import re

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import _arrow_type

_TOKEN = re.compile(r"\s*(`(?:``|[^`])*`|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[<>,:()])")


def hive_type_to_arrow(type_name: str) -> pa.DataType:
    """Convert a supported Hive type description to its Arrow metadata type."""
    tokens = []
    offset = 0
    text = type_name.strip()
    while offset < len(text):
        match = _TOKEN.match(text, offset)
        if match is None:
            raise ValueError("Unsupported Hive direct-read type description")
        tokens.append(match.group(1))
        offset = match.end()
    position = 0

    def take(expected=None):
        nonlocal position
        if position >= len(tokens):
            raise ValueError("Incomplete Hive direct-read type description")
        token = tokens[position]
        position += 1
        if expected is not None and token != expected:
            raise ValueError("Invalid Hive direct-read type description")
        return token

    def parse(depth=0):
        name = take().lower()
        if name == "array":
            take("<")
            value_type = parse(depth + 1)
            take(">")
            return pa.list_(value_type)
        if name == "map":
            take("<")
            key_type = parse(depth + 1)
            take(",")
            value_type = parse(depth + 1)
            take(">")
            if pa.types.is_nested(key_type):
                raise ValueError("Hive MAP keys must have a supported scalar type")
            return pa.map_(key_type, value_type)
        if name == "struct":
            take("<")
            fields = []
            names = set()
            while True:
                field_name = take()
                if field_name.startswith("`"):
                    field_name = field_name[1:-1].replace("``", "`")
                elif re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", field_name) is None:
                    raise ValueError("Invalid Hive STRUCT field name")
                if not field_name:
                    raise ValueError("Hive STRUCT field names must not be empty")
                if field_name.casefold() in names:
                    raise ValueError("Hive STRUCT field names must be unambiguous")
                names.add(field_name.casefold())
                take(":")
                fields.append(pa.field(field_name, parse(depth + 1)))
                separator = take()
                if separator == ">":
                    break
                if separator != ",":
                    raise ValueError("Invalid Hive STRUCT type description")
            return pa.struct(fields)
        if name in ("decimal", "char", "varchar"):
            if name in ("char", "varchar") and depth:
                raise ValueError(
                    "Hive direct-read does not support nested CHAR/VARCHAR"
                )
            take("(")
            first = take()
            if not first.isdecimal() or int(first) < 1:
                raise ValueError("Invalid Hive type parameter")
            parameter = first
            if name == "decimal":
                take(",")
                scale = take()
                if not scale.isdecimal():
                    raise ValueError("Invalid Hive DECIMAL scale")
                parameter += "," + scale
            take(")")
            if name in ("char", "varchar"):
                maximum = 255 if name == "char" else 65535
                if int(first) > maximum:
                    raise ValueError("Invalid Hive string length")
            return _arrow_type(f"{name}({parameter})")
        return _arrow_type(name)

    result = parse()
    if position != len(tokens):
        raise ValueError("Unsupported Hive direct-read type suffix")
    return result
