"""Format-independent expression snapshots and typed partition substitution.

Callers own reader-layout admission and provide already converted partition
values. This module does not parse paths, inspect schemas, or evaluate rows.
"""

from typing import Dict, Optional, Tuple

from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnSubstitutionVisitor,
)
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UnaryExpr,
)

# The tuples retain no mutable DataType, literal container, or user callback.
# They are only serialization-safe values, not another expression evaluator.
_ExpressionSnapshot = tuple
_SIGNED_INTEGER_BITS = {"int8": 8, "int16": 16, "int32": 32, "int64": 64}


def _integer_fits(value: object, tag: str) -> bool:
    bits = _SIGNED_INTEGER_BITS[tag]
    return type(value) is int and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1)


_COMPARISONS = (
    Operation.EQ,
    Operation.NE,
    Operation.LT,
    Operation.LE,
    Operation.GT,
    Operation.GE,
)


def _snapshot_expression(expr: Expr) -> _ExpressionSnapshot:
    if type(expr) is ColumnExpr and type(expr.name) is str:
        expr.name.encode("utf-8")
        return ("column", expr.name)
    if type(expr) is LiteralExpr:
        value = expr.value
        if type(value) is bool:
            return ("literal", "bool", value)
        if type(value) is int and -(2**63) <= value < 2**63:
            return ("literal", "int64", value)
        if type(value) is str:
            value.encode("utf-8")
            return ("literal", "string", value)
        raise ValueError("Unsupported listing-hint literal")
    if type(expr) is BinaryExpr and expr.op is Operation.IN:
        if type(expr.left) not in (ColumnExpr, LiteralExpr):
            raise ValueError("Unsupported IN input")
        if type(expr.right) is not LiteralExpr or type(expr.right.value) is not list:
            raise ValueError("IN requires an exact literal list")
        values = expr.right.value
        if not 0 < len(values) <= 256:
            raise ValueError("IN list size outside supported bounds")
        items = tuple(
            _snapshot_expression(LiteralExpr(v))
            for v in values
            if type(v) in (bool, int, str)
        )
        if len(items) != len(values) or len({item[1] for item in items}) != 1:
            raise ValueError("IN requires homogeneous non-null primitive literals")
        return ("in", _snapshot_expression(expr.left), items)
    if type(expr) is BinaryExpr and type(expr.op) is Operation:
        if expr.op in (Operation.AND, Operation.OR):
            return (
                "binary",
                expr.op.value,
                _snapshot_expression(expr.left),
                _snapshot_expression(expr.right),
            )
        if expr.op in _COMPARISONS:
            shapes = (type(expr.left), type(expr.right))
            if shapes not in (
                (ColumnExpr, ColumnExpr),
                (ColumnExpr, LiteralExpr),
                (LiteralExpr, ColumnExpr),
                (LiteralExpr, LiteralExpr),
            ):
                raise ValueError("Unsupported listing-hint comparison operands")
            return (
                "binary",
                expr.op.value,
                _snapshot_expression(expr.left),
                _snapshot_expression(expr.right),
            )
    if type(expr) is UnaryExpr and type(expr.op) is Operation:
        if expr.op is Operation.NOT or (
            expr.op in (Operation.IS_NULL, Operation.IS_NOT_NULL)
            and type(expr.operand) in (ColumnExpr, LiteralExpr)
        ):
            return ("unary", expr.op.value, _snapshot_expression(expr.operand))
    raise ValueError("Unsupported listing-hint expression")


def _restore_expression(snapshot: _ExpressionSnapshot) -> Expr:
    tag = snapshot[0]
    if tag == "in":
        return BinaryExpr(
            Operation.IN,
            _restore_expression(snapshot[1]),
            LiteralExpr([item[2] for item in snapshot[2]]),
        )
    if tag == "column":
        return ColumnExpr(snapshot[1])
    if tag == "literal":
        return LiteralExpr(snapshot[2])
    if tag == "unary":
        return UnaryExpr(Operation(snapshot[1]), _restore_expression(snapshot[2]))
    return BinaryExpr(
        Operation(snapshot[1]),
        _restore_expression(snapshot[2]),
        _restore_expression(snapshot[3]),
    )


def substitute_partition_values(
    residual: Expr,
    partition_values: Dict[str, object],
    field_types: Tuple[Tuple[str, str], ...],
) -> Optional[Expr]:
    """Substitute validated primitive values without evaluating or folding R.

    Values must already represent the actual reader's logical partition values.
    This format-independent function neither parses paths nor casts values. The
    caller supplies a primitive field signature after separate schema admission.
    None means decline; it never means the predicate is false. Unprovided columns
    remain columns. Q is deliberately absent from this interface.

    Only signed-integer comparisons, bool/string equality, boolean nesting and leaf
    null checks and bounded homogeneous non-null literal IN lists are admitted.
    Integer comparison literals must fit the exact field width. IN retains its
    existing int64/bool/string domain; narrow integer IN is not admitted. This is a
    structural/type check, not a proof about physical statistics or reader layout.
    """
    if type(partition_values) is not dict or type(field_types) is not tuple:
        return None
    try:
        types = {}
        for entry in field_types:
            if type(entry) is not tuple or len(entry) != 2:
                return None
            name, tag = entry
            if type(name) is not str or type(tag) is not str:
                return None
            if (
                not name
                or name in types
                or tag not in (*_SIGNED_INTEGER_BITS, "bool", "string", "float64")
            ):
                return None
            name.encode("utf-8")
            types[name] = tag
        snapshot = _snapshot_expression(residual)

        def infer(node):
            if node[0] == "in":
                if infer(node[1]) != node[2][0][1]:
                    raise ValueError("IN input and literal types must match")
                return "bool"
            if node[0] == "column":
                tag = types[node[1]]
                if tag == "float64":
                    raise ValueError("Float payload cannot participate in the hint")
                return tag
            if node[0] == "literal":
                return node[1]
            op = Operation(node[1])
            if node[0] == "unary":
                operand_type = infer(node[2])
                if op is Operation.NOT and operand_type != "bool":
                    raise ValueError("NOT requires boolean operand")
                return "bool"
            left_type, right_type = infer(node[2]), infer(node[3])
            if op in (Operation.AND, Operation.OR):
                if left_type != "bool" or right_type != "bool":
                    raise ValueError("Boolean operands required")
            else:
                same_type = left_type == right_type
                integer_literal = not same_type and any(
                    field[0] == "column"
                    and field_type in _SIGNED_INTEGER_BITS
                    and literal[0] == "literal"
                    and literal[1] == "int64"
                    and _integer_fits(literal[2], field_type)
                    for field, field_type, literal in (
                        (node[2], left_type, node[3]),
                        (node[3], right_type, node[2]),
                    )
                )
                if not (same_type or integer_literal) or (
                    op not in (Operation.EQ, Operation.NE)
                    and left_type not in _SIGNED_INTEGER_BITS
                ):
                    raise ValueError("Unsupported comparison types")
            return "bool"

        if infer(snapshot) != "bool":
            return None
        replacements = {}
        for name, value in partition_values.items():
            if type(name) is not str or name not in types:
                return None
            # Check Python types before LiteralExpr can inspect an arbitrary value.
            if type(value) not in (bool, int, str):
                return None
            literal = LiteralExpr(value)
            if types[name] in _SIGNED_INTEGER_BITS:
                if not _integer_fits(value, types[name]):
                    return None
            elif _snapshot_expression(literal)[1] != types[name]:
                return None
            replacements[name] = literal
        if not replacements:
            return None
        # Restore first: the visitor may return unchanged leaves by identity.
        # Detaching them avoids sharing mutable Expr.data_type with R.
        substituted = _ColumnSubstitutionVisitor(replacements).visit(
            _restore_expression(snapshot)
        )

        def resolved_comparisons(node):
            if type(node) is BinaryExpr:
                if (
                    node.op in _COMPARISONS
                    and type(node.left) is ColumnExpr
                    and type(node.right) is ColumnExpr
                ):
                    return False
                return resolved_comparisons(node.left) and resolved_comparisons(
                    node.right
                )
            if type(node) is UnaryExpr:
                return resolved_comparisons(node.operand)
            return True

        return substituted if resolved_comparisons(substituted) else None
    except (KeyError, ValueError, TypeError, RecursionError):
        return None
