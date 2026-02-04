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
def test_dedupe_schema_mismatch_warning(enforce_schemas: bool, caplog, propagate_logs):
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
            """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the incoming schema (1 total):
    qux: double
Fields exclusive to the old schema (1 total):
    bar: string
Fields that have different types across the old and the incoming schemas (1 total):
    foo: int32 => int64
This may lead to unexpected behavior."""
            == msg
        )
    else:
        assert out_bundle.schema == old_schema
        assert not caplog.records


def test_dedupe_schema_many_fields_warning(caplog, propagate_logs):
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
        """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the old schema (29 total):
    foo1: int64
    foo2: int64
    foo3: int64
    foo4: int64
    foo5: int64
    foo6: int64
    foo7: int64
    foo8: int64
    foo9: int64
    foo10: int64
    foo11: int64
    foo12: int64
    foo13: int64
    foo14: int64
    foo15: int64
    foo16: int64
    foo17: int64
    foo18: int64
    foo19: int64
    foo20: int64
    ... and 9 more
This may lead to unexpected behavior."""
        == msg
    )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
