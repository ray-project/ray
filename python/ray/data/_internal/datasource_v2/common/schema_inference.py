"""Ordered, bounded schema discovery shared by file datasources."""

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List

import pyarrow as pa

from ray.data._internal.util import unify_schemas_with_validation

_FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT = 10_000

# Bound submitted reads, not just executing threads, for full inference.
_SCHEMA_READ_BATCH_SIZE = 256


def infer_schema_from_files(
    paths: List[str],
    read_schema: Callable[[str], pa.Schema],
    *,
    format_name: str,
) -> pa.Schema:
    """Read file schemas in input order and apply Ray's existing unifier.

    All schemas are unified together to preserve field order, metadata and type
    promotion. Pairwise merging is used only to diagnose a failed unification;
    it does not read any file again or replace the successful-path semantics.
    """
    if not paths:
        return pa.schema([])

    per_file_schemas: List[pa.Schema] = []
    with ThreadPoolExecutor(max_workers=min(len(paths), 16)) as executor:
        for start in range(0, len(paths), _SCHEMA_READ_BATCH_SIZE):
            batch = paths[start : start + _SCHEMA_READ_BATCH_SIZE]
            per_file_schemas.extend(executor.map(read_schema, batch))
    try:
        schema = unify_schemas_with_validation(per_file_schemas) or per_file_schemas[0]
    except (pa.ArrowTypeError, pa.ArrowInvalid):
        merged = per_file_schemas[0]
        for path, file_schema in zip(paths[1:], per_file_schemas[1:]):
            try:
                merged = unify_schemas_with_validation([merged, file_schema]) or merged
            except (pa.ArrowTypeError, pa.ArrowInvalid) as cause:
                raise type(cause)(
                    f"Failed to merge {format_name} schema from {path!r}: {cause}"
                ) from cause
        raise
    assert isinstance(schema, pa.Schema)
    return schema
