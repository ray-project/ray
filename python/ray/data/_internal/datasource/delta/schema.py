"""Schema reconciliation and evolution policies for Delta Lake datasink.

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
    """Schema evolution policy configuration."""

    mode: str = "error"  # "error" or "merge"


def reconcile_worker_schemas(
    worker_schemas: Sequence[pa.Schema],
    existing_table_schema: Optional[pa.Schema],
) -> Optional[pa.Schema]:
    """Reconcile schemas from workers with existing table schema.

    Args:
        worker_schemas: Schemas from all worker tasks.
        existing_table_schema: Schema from existing table (if any).

    Returns:
        Unified schema with type promotion, or None if no schemas.
    """
    if not worker_schemas and not existing_table_schema:
        return None

    # Early-exit optimization: if all schemas are identical, skip unification
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
    """Validate schema compatibility and plan evolution.

    Args:
        policy: Schema evolution policy.
        existing_schema: Schema from existing table.
        incoming_schema: Schema from incoming data.

    Returns:
        List of (name, type, nullable) tuples for new fields to add.

    Raises:
        ValueError: If schema_mode="error" and new columns detected,
            or if type mismatches found in existing columns.
    """
    existing_cols = {f.name: f.type for f in existing_schema}
    incoming_cols = {f.name: (f.type, f.nullable) for f in incoming_schema}

    mismatches = []
    for c, t in existing_cols.items():
        if c in incoming_cols:
            incoming_type = incoming_cols[c][0]
            # Handle null type: if incoming column is all NULL (null type),
            # use existing type for compatibility check (NULL can be written to any nullable column)
            if pa.types.is_null(incoming_type):
                # If existing column is nullable, NULL values are compatible
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
            f"New columns detected: {new_cols}. "
            "Schema evolution is disabled by default. Set schema_mode='merge' to allow."
        )

    if policy.mode != "merge":
        raise ValueError(
            f"Invalid schema_mode '{policy.mode}'. Must be 'error' or 'merge'."
        )

    # merge mode
    return [(c, incoming_cols[c][0], incoming_cols[c][1]) for c in new_cols]


def evolve_schema(
    existing_table: "DeltaTable", new_fields: List[Tuple[str, pa.DataType, bool]]
) -> None:
    """Evolve table schema by adding new columns.

    Args:
        existing_table: DeltaTable to evolve.
        new_fields: List of (name, type, nullable) tuples for new columns.
    """
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
        f"Schema evolution: added {len(new_fields)} columns: {', '.join(f[0] for f in new_fields)}"
    )


def existing_table_pyarrow_schema(existing_table: "DeltaTable") -> pa.Schema:
    """Extract PyArrow schema from existing Delta table.

    Args:
        existing_table: DeltaTable instance.

    Returns:
        PyArrow schema.
    """
    return to_pyarrow_schema(existing_table.schema())
