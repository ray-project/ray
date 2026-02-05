import logging
from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.streaming_executor_state import (
    _find_schemas_mismatch,
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
        old_schema, incoming_bundle, enforce_schemas=False, warn=False
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


@pytest.mark.parametrize(
    "incoming_schema",
    [
        pa.schema([]),  # Empty Schema
        PandasBlockSchema(names=[], types=[]),
        None,  # Null Schema
    ],
)
@pytest.mark.parametrize("enforce_schemas", [False, True])
def test_dedupe_schema_empty_warning(
    enforce_schemas: bool, incoming_schema: Optional["Schema"], caplog, propagate_logs
):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool8()),
        ]
    )

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)

    # Capture warnings
    with caplog.at_level(
        logging.WARNING,
    ):
        out_bundle, diverged = dedupe_schemas_with_validation(
            old_schema, incoming_bundle, enforce_schemas=enforce_schemas, warn=True
        )

    assert diverged

    if enforce_schemas:
        assert out_bundle.schema == old_schema

        msg = "\n".join(caplog.messages)
        assert (
            """Operator produced a RefBundle with an empty/unknown schema. (3 total):
    foo: int32
    bar: string
    baz: extension<arrow.bool8>
This may lead to unexpected behavior."""
            == msg
        )
    else:
        assert out_bundle.schema == old_schema
        assert not caplog.records


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
        old_schema, incoming_bundle, enforce_schemas=enforce_schemas, warn=False
    )

    assert diverged

    if enforce_schemas:
        assert out_bundle.schema == pa.schema(list(old_schema) + list(incoming_schema))
    else:
        assert out_bundle.schema == old_schema


@pytest.mark.parametrize("enforce_schemas", [False, True])
@pytest.mark.parametrize("truncation_length", [20, 3, 2])
def test_dedupe_schema_mismatch_warning(
    enforce_schemas: bool, truncation_length: int, caplog, propagate_logs
):
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

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)

    # Capture warnings
    with caplog.at_level(
        logging.WARNING,
    ):
        out_bundle, diverged = dedupe_schemas_with_validation(
            old_schema,
            incoming_bundle,
            enforce_schemas=enforce_schemas,
            warn=True,
            truncation_length=truncation_length,
        )

    assert diverged

    if enforce_schemas:
        assert out_bundle.schema == pa.schema(
            [
                pa.field("foo", pa.int64()),
                pa.field("bar", pa.string()),
                pa.field("baz", pa.bool8()),
                pa.field("quux", pa.uint32()),
                pa.field("corge", pa.int64()),
                pa.field("grault", pa.int64()),
                pa.field("qux", pa.float64()),
            ]
        )

        msg = "\n".join(caplog.messages)
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
    else:
        assert out_bundle.schema == old_schema
        assert not caplog.records


@pytest.mark.parametrize("enforce_schemas", [False, True])
def test_dedupe_schema_disordered_warning(
    enforce_schemas: bool, caplog, propagate_logs
):
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

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)

    # Capture warnings
    with caplog.at_level(
        logging.WARNING,
    ):
        out_bundle, diverged = dedupe_schemas_with_validation(
            old_schema, incoming_bundle, enforce_schemas=enforce_schemas, warn=True
        )

    assert diverged

    assert out_bundle.schema == old_schema

    if enforce_schemas:
        # Warning message should have truncated 9 schema fields
        msg = "\n".join(caplog.messages)
        assert (
            """Operator produced a RefBundle with a different schema than the previous one.
Some fields are ordered differently across the old and the incoming schemas.
This may lead to unexpected behavior."""
            == msg
        )
    else:
        assert not caplog.records


def test_find_schemas_mismatch_duplicate_field_names():
    """Duplicate field names must not be collapsed to dict keys.

    PyArrow allows duplicate field names. Converting to dict loses later
    fields, so two schemas with different field counts (one with duplicates)
    can appear identical and trigger a wrong "ordered differently" message.
    """
    # Old: 3 fields with duplicate name "a"
    old_schema = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("a", pa.string()),
            pa.field("b", pa.int64()),
        ]
    )
    # New: 2 fields, no duplicate
    new_schema = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.int64()),
        ]
    )
    msg = _find_schemas_mismatch(old_schema, new_schema)
    # Should report different field count, not "ordered differently"
    assert "different numbers of fields" in msg, msg
    assert "old: 3" in msg and "new: 2" in msg, msg
    assert "ordered differently" not in msg, msg


def test_find_schemas_mismatch_duplicate_field_names_reverse():
    """Same as above but old has fewer fields than new (new has duplicates)."""
    old_schema = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.int64()),
        ]
    )
    new_schema = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("a", pa.string()),
            pa.field("b", pa.int64()),
        ]
    )
    msg = _find_schemas_mismatch(old_schema, new_schema)
    assert "different numbers of fields" in msg, msg
    assert "old: 2" in msg and "new: 3" in msg, msg
    assert "ordered differently" not in msg, msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
