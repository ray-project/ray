"""Schema reconciliation and evolution policies for the Delta Lake datasink.

The framework's ``reconcile_schema`` hook lets the adapter decide what to do
when worker schemas disagree with each other or with the existing table.
Delta evolves at commit time (this module), in contrast with Iceberg which
evolves pre-write.

PyArrow schemas: https://arrow.apache.org/docs/python/api/datatypes.html
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

import pyarrow as pa

if TYPE_CHECKING:
    from deltalake import DeltaTable

from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas
from ray.data._internal.datasource.delta.utils import (
    to_pyarrow_schema,
    types_compatible,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaPolicy:
    """Schema evolution policy.

    * ``"error"`` (default) -- reject incoming data with new columns.
    * ``"merge"`` -- add new columns to the existing table via
      ``DeltaTable.alter.add_columns``.
    """

    mode: str = "error"


def reconcile_worker_schemas(
    worker_schemas: Sequence[pa.Schema],
    existing_table_schema: Optional[pa.Schema],
) -> Optional[pa.Schema]:
    """Type-promoted union of every worker schema with the existing table."""
    if not worker_schemas and not existing_table_schema:
        return None
    if worker_schemas and all(s == worker_schemas[0] for s in worker_schemas):
        if existing_table_schema in (None, worker_schemas[0]):
            return existing_table_schema or worker_schemas[0]
    schemas = list(worker_schemas)
    if existing_table_schema is not None:
        schemas = [existing_table_schema] + schemas
    return unify_schemas(schemas, promote_types=True) if schemas else None


def validate_and_plan_evolution(
    policy: SchemaPolicy,
    existing_schema: pa.Schema,
    incoming_schema: pa.Schema,
) -> List[Tuple[str, pa.DataType, bool]]:
    """Validate compatibility and return new fields to add (if any).

    Raises ``ValueError`` if a column type mismatch is detected, or if the
    incoming schema adds new columns and the policy is ``"error"``.
    """
    existing_cols = {f.name: f.type for f in existing_schema}
    incoming_cols = {f.name: (f.type, f.nullable) for f in incoming_schema}

    mismatches = []
    for c, t in existing_cols.items():
        if c in incoming_cols:
            incoming_type = incoming_cols[c][0]
            if pa.types.is_null(incoming_type):
                existing_field = existing_schema.field(c)
                if not existing_field.nullable:
                    mismatches.append(c)
            elif not types_compatible(t, incoming_type):
                mismatches.append(c)

    if mismatches:
        raise ValueError(
            f"Schema mismatch: type mismatches for existing columns: {mismatches}"
        )

    new_cols = [c for c in incoming_cols.keys() if c not in existing_cols]
    if not new_cols:
        return []

    if policy.mode == "error":
        raise ValueError(
            f"New columns detected: {new_cols}. Schema evolution is disabled "
            "by default. Set schema_mode='merge' to allow."
        )
    if policy.mode != "merge":
        raise ValueError(
            f"Invalid schema_mode '{policy.mode}'. Must be 'error' or 'merge'."
        )
    return [(c, incoming_cols[c][0], incoming_cols[c][1]) for c in new_cols]


def evolve_schema(
    existing_table: "DeltaTable", new_fields: List[Tuple[str, pa.DataType, bool]]
) -> None:
    """Add new columns to an existing Delta table."""
    if not new_fields:
        return
    from ray.data._internal.datasource.delta.utils import convert_schema_to_delta

    delta_fields = []
    for name, pa_type, nullable in new_fields:
        temp = pa.schema([pa.field(name, pa_type, nullable)])
        delta_schema = convert_schema_to_delta(temp)
        delta_fields.append(delta_schema.fields[0])

    existing_table.alter.add_columns(delta_fields)
    logger.info(
        "Schema evolution: added %d columns: %s",
        len(new_fields),
        ", ".join(f[0] for f in new_fields),
    )


def existing_table_pyarrow_schema(existing_table: "DeltaTable") -> pa.Schema:
    """Return the PyArrow schema of an existing Delta table."""
    return to_pyarrow_schema(existing_table.schema())
