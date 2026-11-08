"""Private Parquet/Hive layout and admission for residual listing pruning."""

from dataclasses import dataclass
from typing import Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.listing_pruning import (
    _ExpressionSnapshot,
    _restore_expression,
    _snapshot_expression,
)
from ray.data.expressions import Expr


@dataclass(frozen=True)
class HivePartitioningSnapshot:
    """Built-in path configuration, without a mutable parser or filesystem.

    base_dir is the original value, not independently normalized. Recreating
    the actual parser's resolution semantics is a separate admission gate.
    """

    base_dir: str
    field_names: Tuple[str, ...]
    field_types: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class ListingPruningContext:
    """Fixed reader layout; not evidence of physical-schema compatibility."""

    schema: pa.Schema
    file_schema: pa.Schema
    file_schema_signature: Tuple[Tuple[str, str], ...]
    partitioning: HivePartitioningSnapshot


@dataclass(frozen=True)
class ListingPruningHint:
    """Immutable residual candidate, not a query/file admission certificate.

    Q stays on the original scanner: snapshotting it here would unnecessarily
    constrain already accepted predicates. The complete R stays in its Filter;
    this detached value is used only to propose optional listing work.
    """

    context: ListingPruningContext
    _residual: _ExpressionSnapshot

    @classmethod
    def from_residual(
        cls, context: ListingPruningContext, residual: Expr
    ) -> Optional["ListingPruningHint"]:
        try:
            return cls(context, _snapshot_expression(residual))
        except (ValueError, TypeError, RecursionError):
            return None

    @property
    def residual(self) -> Expr:
        return _restore_expression(self._residual)


def logical_schema_signature(schema):
    """Return a primitive signature, declining before extension field wrapping.

    Only fresh native-writer bytes provide this extension certificate. Never
    apply this marker check to an external/on-disk IPC payload. Buffer.size
    limits the Python copy, not native serialization allocation or recursion.
    """
    if type(schema) is not pa.Schema or not 0 < len(schema) <= 1024:
        return None
    try:
        names = schema.names
        if any(
            type(name) is not str
            or not name
            or len(name) > 4096
            or "." in name
            or len(name.encode("utf-8")) > 4096
            for name in names
        ):
            return None
        if len(set(names)) != len(names):
            return None
        buffer = schema.serialize()
        if buffer.size > 1024 * 1024:
            return None
        if b"ARROW:extension:name" in buffer.to_pybytes():
            return None
        if schema.metadata:
            return None
        primitive_tags = {
            pa.float64().id: "float64",
            pa.int8().id: "int8",
            pa.int16().id: "int16",
            pa.int32().id: "int32",
            pa.int64().id: "int64",
            pa.bool_().id: "bool",
            pa.string().id: "string",
        }
        signature = []
        for field in schema:
            kind = field.type
            if type(kind) is not pa.DataType or field.metadata:
                return None
            tag = primitive_tags.get(kind.id)
            if tag is None:
                return None
            signature.append((field.name, tag))
        return tuple(signature)
    except Exception:  # noqa: BLE001 - optional admission must decline native errors
        return None
