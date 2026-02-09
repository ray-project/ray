from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.streaming_executor_state import (
    build_schemas_mismatch_warning,
    dedupe_schemas_with_validation,
)
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data.block import Schema, _is_empty_schema


@pytest.mark.parametrize(
    "incoming_schema",
    [
        pa.schema([pa.field("uuid", pa.string())]),  # NOTE: diff from old_schema
        pa.schema([]),  # Empty Schema
        PandasBlockSchema(names=["col1"], types=[int]),
        PandasBlockSchema(names=[], types=[]),
        None,  # Null Schema
    ],
)
@pytest.mark.parametrize(
    "old_schema",
    [
        pa.schema([pa.field("id", pa.int64())]),
        pa.schema([]),  # Empty Schema
        PandasBlockSchema(names=["col2"], types=[int]),
        PandasBlockSchema(names=[], types=[]),
        None,  # Null Schema
    ],
)
def test_dedupe_schema_handle_empty(
    old_schema: Optional["Schema"],
    incoming_schema: Optional["Schema"],
):

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)
    out_bundle, diverged = dedupe_schemas_with_validation(
        old_schema, incoming_bundle, enforce_schemas=False
    )

    if _is_empty_schema(old_schema):
        # old_schema is invalid
        assert not diverged, (old_schema, incoming_schema)
        assert out_bundle.schema == incoming_schema, (old_schema, incoming_schema)
    else:
        # old_schema is valid
        assert diverged, (old_schema, incoming_schema)
        assert incoming_schema != old_schema, (old_schema, incoming_schema)
        assert old_schema == out_bundle.schema, (old_schema, incoming_schema)


@pytest.mark.parametrize("enforce_schemas", [False, True])
@pytest.mark.parametrize(
    "incoming_schema", [pa.schema([pa.field("uuid", pa.string())])]
)
@pytest.mark.parametrize("old_schema", [pa.schema([pa.field("id", pa.int64())])])
def test_dedupe_schema_divergence(
    enforce_schemas: bool,
    old_schema: Optional["Schema"],
    incoming_schema: Optional["Schema"],
):

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)
    out_bundle, diverged = dedupe_schemas_with_validation(
        old_schema, incoming_bundle, enforce_schemas=enforce_schemas
    )

    assert diverged

    if enforce_schemas:
        assert out_bundle.schema == pa.schema(list(old_schema) + list(incoming_schema))
    else:
        assert out_bundle.schema == old_schema


@pytest.mark.parametrize(
    "incoming_schema",
    [
        pa.schema([]),  # Empty Schema
        PandasBlockSchema(names=[], types=[]),
        None,  # Null Schema
    ],
)
def test_dedupe_schema_empty_warning(incoming_schema: Optional["Schema"]):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool8()),
        ]
    )

    msg = build_schemas_mismatch_warning(old_schema, incoming_schema)

    assert (
        """Operator produced a RefBundle with an empty/unknown schema. (3 total):
    foo: int32
    bar: string
    baz: extension<arrow.bool8>
This may lead to unexpected behavior."""
        == msg
    )


@pytest.mark.parametrize("truncation_length", [20, 4, 3, 2])
def test_dedupe_schema_truncation_warning(truncation_length: int):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool8()),
            pa.field("quux", pa.uint16()),
            pa.field("corge", pa.int64()),
            pa.field("grault", pa.int32()),
        ]
    )
    incoming_schema = pa.schema(
        [
            pa.field("foo", pa.int64()),
            pa.field("baz", pa.bool8()),
            pa.field("qux", pa.float64()),
            pa.field("quux", pa.uint32()),
            pa.field("corge", pa.int32()),
            pa.field("grault", pa.int64()),
        ]
    )

    msg = build_schemas_mismatch_warning(
        old_schema, incoming_schema, truncation_length=truncation_length
    )

    if truncation_length >= 4:
        assert (
            """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the incoming schema (1 total):
    qux: double
Fields exclusive to the old schema (1 total):
    bar: string
Fields that have different types across the old and the incoming schemas (4 total):
    foo: int32 => int64
    quux: uint16 => uint32
    corge: int64 => int32
    grault: int32 => int64
This may lead to unexpected behavior."""
            == msg
        )
    elif truncation_length == 3:
        assert (
            """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the incoming schema (1 total):
    qux: double
Fields exclusive to the old schema (1 total):
    bar: string
Fields that have different types across the old and the incoming schemas (4 total):
    foo: int32 => int64
    quux: uint16 => uint32
    corge: int64 => int32
    ... and 1 more
This may lead to unexpected behavior."""
            == msg
        )
    else:
        assert (
            """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the incoming schema (1 total):
    qux: double
Fields exclusive to the old schema (1 total):
    bar: string
Fields that have different types across the old and the incoming schemas (4 total):
    foo: int32 => int64
    quux: uint16 => uint32
    ... and 2 more
This may lead to unexpected behavior."""
            == msg
        )


def test_dedupe_schema_disordered_warning():
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool8()),
            pa.field("qux", pa.float64()),
        ]
    )
    incoming_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("baz", pa.bool8()),
            pa.field("bar", pa.string()),
            pa.field("qux", pa.float64()),
        ]
    )

    msg = build_schemas_mismatch_warning(old_schema, incoming_schema)

    assert (
        """Operator produced a RefBundle with a different schema than the previous one.
Some fields are ordered differently across the old and the incoming schemas.
This may lead to unexpected behavior."""
        == msg
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
