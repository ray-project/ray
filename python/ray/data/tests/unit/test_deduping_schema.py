import logging
from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.streaming_executor_state import (
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
def test_dedupe_schema_mismatch(enforce_schemas: bool, caplog, propagate_logs):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool8()),
        ]
    )
    incoming_schema = pa.schema(
        [
            pa.field("foo", pa.int64()),
            pa.field("baz", pa.bool8()),
            pa.field("qux", pa.float64()),
        ]
    )

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)

    # Capture warnings
    with caplog.at_level(
        logging.WARNING,
        logger="ray.data._internal.execution.streaming_executor_state",
    ):
        out_bundle, diverged = dedupe_schemas_with_validation(
            old_schema, incoming_bundle, enforce_schemas=enforce_schemas, warn=True
        )

    assert diverged

    if enforce_schemas:
        assert out_bundle.schema == pa.schema(
            [
                pa.field("foo", pa.int64()),
                pa.field("bar", pa.string()),
                pa.field("baz", pa.bool8()),
                pa.field("qux", pa.float64()),
            ]
        )

        msg = "\n".join(caplog.messages)
        assert (
            "Operator produced a RefBundle with a different schema than the previous one."
            in msg
        )
        assert "Fields exclusive to the incoming schema (1 total):" in msg
        assert "qux: double" in msg
        assert "Fields exclusive to the old schema (1 total):" in msg
        assert "bar: string" in msg
        assert (
            "Fields that have different types across the old and the incoming schemas (1 total):"
            in msg
        )
        assert "foo: int32 => int64" in msg
        assert "baz" not in msg
    else:
        assert out_bundle.schema == old_schema
        assert not caplog.records


def test_dedupe_schema_many_fields(caplog, propagate_logs):
    old_schema = pa.schema([pa.field(f"foo{i}", pa.int64()) for i in range(30)])
    incoming_schema = pa.schema(
        [
            pa.field("foo0", pa.int64()),
        ]
    )

    incoming_bundle = RefBundle([], owns_blocks=False, schema=incoming_schema)

    # Capture warnings
    with caplog.at_level(
        logging.WARNING,
        logger="ray.data._internal.execution.streaming_executor_state",
    ):
        out_bundle, diverged = dedupe_schemas_with_validation(
            old_schema, incoming_bundle, enforce_schemas=True, warn=True
        )

    assert diverged

    assert out_bundle.schema == pa.schema(
        [pa.field(f"foo{i}", pa.int64()) for i in range(30)]
    )

    # Warning message should have truncated 9 schema fields
    msg = "\n".join(caplog.messages)
    assert (
        "Operator produced a RefBundle with a different schema than the previous one."
        in msg
    )
    assert "Fields exclusive to the old schema (29 total):" in msg
    for i in range(1, 21):
        assert f"foo{i}: int64" in msg
    assert "... and 9 more" in msg
    assert "foo0: int64" not in msg


@pytest.mark.parametrize("enforce_schemas", [False, True])
def test_dedupe_schema_disordered(enforce_schemas: bool, caplog, propagate_logs):
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
        logger="ray.data._internal.execution.streaming_executor_state",
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
            "Operator produced a RefBundle with a different schema than the previous one."
            in msg
        )
        assert (
            "Fields ordered differently across the old and the incoming schemas (2 total):"
            in msg
        )
        assert "bar: string" in msg
        assert "baz: extension<arrow.bool8>" in msg
    else:
        assert not caplog.records


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
