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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
