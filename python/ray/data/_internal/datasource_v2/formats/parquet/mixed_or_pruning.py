"""Conservative, additive row-group hints; the complete Filter stays upstream."""
import operator
from typing import Collection, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.dataset as pads

from ray.data.datasource.partitioning import (
    PathPartitionParser,
    _broadcast_partition_value,
)
from ray.data.expressions import BinaryExpr, ColumnExpr, Expr, LiteralExpr, Operation

_COMPARISONS = {
    Operation.LT: operator.lt,
    Operation.LE: operator.le,
    Operation.EQ: operator.eq,
    Operation.GE: operator.ge,
    Operation.GT: operator.gt,
}


def split_mixed_or(
    expr: Expr, partition_names: Collection[str]
) -> Optional[Tuple[BinaryExpr, BinaryExpr]]:
    """Recognize a partition comparison OR a physical integer comparison.

    This is only a pruning hint, not a replacement predicate. Nested Boolean
    expressions, casts, arithmetic and untyped literals are deliberately excluded.
    """
    if not isinstance(expr, BinaryExpr) or expr.op != Operation.OR:
        return None
    for path, data in ((expr.left, expr.right), (expr.right, expr.left)):
        if not (
            isinstance(path, BinaryExpr)
            and isinstance(data, BinaryExpr)
            and isinstance(path.left, ColumnExpr)
            and isinstance(path.right, LiteralExpr)
            and isinstance(data.left, ColumnExpr)
            and isinstance(data.right, LiteralExpr)
        ):
            continue
        if (
            path.op in _COMPARISONS
            and path.left.name in partition_names
            and data.left.name not in partition_names
            and data.op in _COMPARISONS
            and type(data.right.value) is int
            and (
                type(path.right.value) is int
                or (path.op == Operation.EQ and type(path.right.value) is str)
            )
        ):
            return path, data
    return None


def specialize_fragment(
    expr: Expr,
    parser: Optional[PathPartitionParser],
    schema: Optional[pa.Schema],
    fragment: pads.ParquetFileFragment,
    columns: Optional[Sequence[str]],
) -> Tuple[bool, Optional[pads.Expression]]:
    """Return (eligible, physical filter); None filter means keep all groups."""
    if parser is None or schema is None:
        return False, None
    names = set(parser.scheme.field_names or [])
    if parser.scheme.style != "hive" or not names:
        return False, None
    leaves = split_mixed_or(expr, names)
    if leaves is None or any(isinstance(f.type, pa.ExtensionType) for f in schema):
        return False, None
    path, data = leaves
    # Re-establish the shapes checked by split_mixed_or for static typing.
    assert isinstance(path.left, ColumnExpr) and isinstance(path.right, LiteralExpr)
    assert isinstance(data.left, ColumnExpr) and isinstance(data.right, LiteralExpr)
    try:
        values = parser(fragment.path)
        physical = fragment.physical_schema
        physical_names = set(physical.names)
        if not names.issubset(values) or names.intersection(physical_names):
            return False, None
        # A skipped group must not hide a conversion failure in another column.
        # Stay outside all physical-to-logical casts, including projected fields.
        for field in schema:
            if (
                field.name in physical_names
                and physical.field(field.name).type != field.type
            ):
                return False, None
        if columns is not None and path.left.name not in columns:
            return False, None
        path_type = schema.field(path.left.name).type
        data_type = schema.field(data.left.name).type
        if not (pa.types.is_string(path_type) or pa.types.is_signed_integer(path_type)):
            return False, None
        # Ordered partition comparisons are restricted to signed integers.
        # String equality retains its existing behavior; no implicit casts.
        if path.op != Operation.EQ and not pa.types.is_signed_integer(path_type):
            return False, None
        if not pa.types.is_signed_integer(data_type):
            return False, None
        if physical.field(data.left.name).type != data_type:
            return False, None
        if values[path.left.name] is None:
            return False, None
        # Materialization converts every partition field, not only the one in
        # this predicate. Validate them before allowing an empty pruned scan.
        typed_values = {
            name: _broadcast_partition_value(name, value, 1, schema)[0].as_py()
            for name, value in values.items()
        }
        value = typed_values[path.left.name]
        if type(value) is not type(path.right.value):
            return False, None
        # Validate both literals before pruning. An empty scan must not hide
        # conversion errors that the retained Filter would otherwise raise.
        pa.scalar(path.right.value, type=path_type)
        pa.scalar(data.right.value, type=data_type)
        if _COMPARISONS[path.op](value, path.right.value):
            return True, None
        return True, data.to_pyarrow()
    except (pa.ArrowException, OSError, KeyError, ValueError, TypeError, OverflowError):
        # Metadata access is speculative here. Leave failed I/O to the reader's
        # normal retry path, which still propagates persistent failures.
        return False, None
