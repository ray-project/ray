import logging
from types import SimpleNamespace
from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.streaming_executor_state import (
    OpState,
    _build_schemas_mismatch_warning,
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
def test_build_mismatch_warning_empty(incoming_schema: Optional["Schema"]):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool_()),
        ]
    )

    msg = _build_schemas_mismatch_warning(old_schema, incoming_schema)

    assert (
        """Operator produced a RefBundle with an empty/unknown schema. (3 total):
    foo: int32
    bar: string
    baz: bool
This may lead to unexpected behavior."""
        == msg
    )


@pytest.mark.parametrize("truncation_length", [20, 4, 3, 2])
def test_build_mismatch_warning_truncation(truncation_length: int):
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool_()),
            pa.field("quux", pa.uint16()),
            pa.field("corge", pa.int64()),
            pa.field("grault", pa.int32()),
        ]
    )
    incoming_schema = pa.schema(
        [
            pa.field("foo", pa.int64()),
            pa.field("baz", pa.bool_()),
            pa.field("qux", pa.float64()),
            pa.field("quux", pa.uint32()),
            pa.field("corge", pa.int32()),
            pa.field("grault", pa.int64()),
        ]
    )

    msg = _build_schemas_mismatch_warning(
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


def test_build_mismatch_warning_disordered():
    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.string()),
            pa.field("baz", pa.bool_()),
            pa.field("qux", pa.float64()),
        ]
    )
    incoming_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("baz", pa.bool_()),
            pa.field("bar", pa.string()),
            pa.field("qux", pa.float64()),
        ]
    )

    msg = _build_schemas_mismatch_warning(old_schema, incoming_schema)

    assert (
        """Operator produced a RefBundle with a different schema than the previous one.
Some fields are ordered differently across the old and the incoming schemas.
This may lead to unexpected behavior."""
        == msg
    )


class _DummyOp:
    def __init__(self, enforce_schemas=True):
        self.name = "dummy"
        self.data_context = SimpleNamespace(enforce_schemas=enforce_schemas)
        self.input_dependencies = []
        self.output_dependencies = []
        self.metrics = SimpleNamespace(
            num_alive_actors=0,
            num_restarting_actors=0,
            num_pending_actors=0,
            num_external_inqueue_blocks=0,
            num_external_inqueue_bytes=0,
            num_external_outqueue_blocks=0,
            num_external_outqueue_bytes=0,
        )

    def get_actor_info(self):
        return SimpleNamespace(running=0, restarting=0, pending=0)


@pytest.mark.parametrize("enforce_schemas", [False, True])
def test_add_output_emits_warning(enforce_schemas, caplog, propagate_logs):
    op = _DummyOp(enforce_schemas=enforce_schemas)
    state = OpState(op, [])

    old_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("bar", pa.bool_()),
        ]
    )
    new_schema = pa.schema(
        [
            pa.field("foo", pa.int32()),
            pa.field("baz", pa.uint32()),
        ]
    )

    # First output seeds schema (no warning)
    state.add_output(RefBundle([], owns_blocks=False, schema=old_schema))

    assert not caplog.messages

    # Second output diverges, should warn once
    with caplog.at_level(logging.WARNING):
        state.add_output(RefBundle([], owns_blocks=False, schema=new_schema))

    assert not (enforce_schemas ^ len(caplog.messages))

    if enforce_schemas:
        msg = "\n".join(caplog.messages)
        assert (
            msg
            == """Operator produced a RefBundle with a different schema than the previous one.
Fields exclusive to the incoming schema (1 total):
    baz: uint32
Fields exclusive to the old schema (1 total):
    bar: bool
This may lead to unexpected behavior."""
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
