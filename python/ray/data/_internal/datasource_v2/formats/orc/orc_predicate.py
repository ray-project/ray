"""Conservative possible-value evaluation of bound ORC statistics predicates.

Like ORC's SearchArgument evaluation, this tracks possible TRUE/FALSE/NULL
values. Only a set without TRUE permits skipping; unknown leaves are never
negated into a proof. The ordinary Arrow filter remains authoritative.
"""

from itertools import product

import pyarrow as pa

from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    LiteralExpr,
    Operation as Op,
    UnaryExpr,
)

_UNKNOWN = frozenset((True, False, None))
_COMPARISONS = (Op.EQ, Op.NE, Op.LT, Op.LE, Op.GT, Op.GE)
_REVERSE = {
    Op.EQ: Op.EQ,
    Op.NE: Op.NE,
    Op.LT: Op.GT,
    Op.LE: Op.GE,
    Op.GT: Op.LT,
    Op.GE: Op.LE,
}
_MAX_NODES = 256
_MAX_SET_VALUES = 1024


def safe_scan_types(physical, logical, names):
    """Do not let skipping hide data-dependent failures from schema casts."""
    for name in names:
        src_index, dst_index = physical.get_field_index(name), logical.get_field_index(
            name
        )
        if src_index == -1 or dst_index == -1:
            # Missing physical fields are logical NULLs; they cannot supply stats.
            continue
        src, dst = physical.field(src_index).type, logical.field(dst_index).type
        if src == dst or pa.types.is_null(src):
            continue
        if (
            pa.types.is_signed_integer(src)
            and pa.types.is_signed_integer(dst)
            and src.bit_width <= dst.bit_width
        ):
            continue
        if (pa.types.is_string(src) or pa.types.is_large_string(src)) and (
            pa.types.is_string(dst) or pa.types.is_large_string(dst)
        ):
            continue
        return False
    return True


def _kind(data_type):
    if pa.types.is_signed_integer(data_type):
        return {8: 1, 16: 2, 32: 3, 64: 4}[data_type.bit_width]
    if pa.types.is_boolean(data_type):
        return 0
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return 7
    # Scalar counts are useful independently of range support. Nested values,
    # extensions and dictionaries are deliberately not assigned a count domain.
    if pa.types.is_floating(data_type):
        return 5 if data_type.bit_width == 32 else 6
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return 8
    if pa.types.is_timestamp(data_type):
        return 9
    if pa.types.is_decimal(data_type):
        return 14
    if pa.types.is_date(data_type):
        return 15
    return None


class OrcPredicate:
    """A small predicate bound to one file's physical and logical schema."""

    def __init__(self, expression, physical_schema, logical_schema):
        self._columns = {}
        for field in physical_schema:
            index = logical_schema.get_field_index(field.name)
            if index != -1:
                logical_kind = _kind(logical_schema.field(index).type)
                physical_kind = _kind(field.type)
                if logical_kind is not None and physical_kind is not None:
                    self._columns[field.name] = (physical_kind, logical_kind)
        self.eligible = False
        budget = [_MAX_NODES]
        self._tree = self._bind(expression, budget, 0)

    def _bind(self, expr, budget, depth):
        if expr is None or budget[0] <= 0 or depth > 64:
            return None
        budget[0] -= 1
        if isinstance(expr, AliasExpr):
            return self._bind(expr.expr, budget, depth + 1)
        if isinstance(expr, BinaryExpr) and expr.op in (Op.AND, Op.OR):
            return (
                expr.op,
                self._bind(expr.left, budget, depth + 1),
                self._bind(expr.right, budget, depth + 1),
            )
        if isinstance(expr, UnaryExpr) and expr.op == Op.NOT:
            return (Op.NOT, self._bind(expr.operand, budget, depth + 1))
        if (
            isinstance(expr, UnaryExpr)
            and expr.op in (Op.IS_NULL, Op.IS_NOT_NULL)
            and isinstance(expr.operand, ColumnExpr)
            and expr.operand.name in self._columns
        ):
            self.eligible = True
            return (expr.op, expr.operand.name, None)
        if not isinstance(expr, BinaryExpr):
            return None
        left, right, op = expr.left, expr.right, expr.op
        if (
            op in _COMPARISONS
            and isinstance(right, ColumnExpr)
            and isinstance(left, LiteralExpr)
        ):
            left, right, op = right, left, _REVERSE[op]
        if not isinstance(left, ColumnExpr) or not isinstance(right, LiteralExpr):
            return None
        if left.name not in self._columns:
            return None
        _, kind = self._columns[left.name]
        value = right.value
        if op in (Op.IN, Op.NOT_IN):
            if kind not in (1, 2, 3, 4) or not isinstance(value, (list, tuple)):
                return None
            if len(value) > _MAX_SET_VALUES or not all(
                item is None or type(item) is int and -(1 << 63) <= item < 1 << 63
                for item in value
            ):
                return None
            value = frozenset(value)
        elif op not in _COMPARISONS:
            return None
        elif value is None:
            pass
        elif kind in (1, 2, 3, 4):
            if type(value) is not int or not -(1 << 63) <= value < 1 << 63:
                return None
        elif kind == 0:
            if type(value) is not bool or op not in (Op.EQ, Op.NE):
                return None
        elif kind == 7:
            if type(value) is not str:
                return None
        else:
            return None
        self.eligible = True
        return (op, left.name, value)

    def can_match(self, statistics, rows):
        return rows != 0 and True in self._evaluate(self._tree, statistics, rows)

    def _evaluate(self, tree, statistics, rows):
        if tree is None:
            return _UNKNOWN
        op = tree[0]
        if op == Op.NOT:
            return frozenset(
                None if value is None else not value
                for value in self._evaluate(tree[1], statistics, rows)
            )
        if op in (Op.AND, Op.OR):
            left = self._evaluate(tree[1], statistics, rows)
            right = self._evaluate(tree[2], statistics, rows)
            values = set()
            for a, b in product(left, right):
                if op == Op.AND:
                    value = (
                        False
                        if a is False or b is False
                        else (None if a is None or b is None else True)
                    )
                else:
                    value = (
                        True
                        if a is True or b is True
                        else (None if a is None or b is None else False)
                    )
                values.add(value)
            return frozenset(values)
        _, name, value = tree
        stat = statistics.get(name)
        if stat is None or stat.values is None:
            return _UNKNOWN
        physical_kind = self._columns[name][0]
        if op in (Op.IS_NULL, Op.IS_NOT_NULL):
            compatible = stat.kind == physical_kind or (
                physical_kind == 7
                and stat.kind in (16, 17)
                or physical_kind == 9
                and stat.kind == 18
            )
        else:
            compatible = stat.kind == physical_kind
        if not compatible:
            return _UNKNOWN
        count = stat.values
        if op in (Op.IS_NULL, Op.IS_NOT_NULL):
            values = set()
            if count:
                values.add(op == Op.IS_NOT_NULL)
            if count < rows:
                values.add(op == Op.IS_NULL)
            return values
        values = set()
        if op in (Op.IN, Op.NOT_IN):
            if count < rows:
                values.add(None in value)
            if count:
                members = value - {None}
                if not members:
                    values.add(False)
                elif stat.minimum is None:
                    values.update((False, True))
                else:
                    if any(stat.minimum <= item <= stat.maximum for item in members):
                        values.add(True)
                    covered = sum(
                        stat.minimum <= item <= stat.maximum for item in members
                    )
                    if covered != stat.maximum - stat.minimum + 1:
                        values.add(False)
            # Arrow is_in(skip_nulls=False) produces a boolean for NULL too.
            return {not item for item in values} if op == Op.NOT_IN else values
        if value is None:
            return {None}
        if count < rows:
            values.add(None)
        if not count:
            return values
        if stat.kind == 0:
            if stat.true_count is None:
                return _UNKNOWN
            for actual, frequency in (
                (True, stat.true_count),
                (False, count - stat.true_count),
            ):
                if frequency:
                    values.add((actual == value) if op == Op.EQ else (actual != value))
            return values
        if stat.minimum is None or stat.maximum is None:
            return _UNKNOWN
        low, high = stat.minimum, stat.maximum
        if op == Op.EQ:
            possible_true = low <= value <= high
            possible_false = low != high or low != value
        elif op == Op.NE:
            possible_true = low != high or low != value
            possible_false = low <= value <= high
        elif op == Op.LT:
            possible_true, possible_false = low < value, high >= value
        elif op == Op.LE:
            possible_true, possible_false = low <= value, high > value
        elif op == Op.GT:
            possible_true, possible_false = high > value, low <= value
        else:
            possible_true, possible_false = high >= value, low < value
        if possible_true:
            values.add(True)
        if possible_false:
            values.add(False)
        return values
