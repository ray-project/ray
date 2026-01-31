"""Constant folding visitor for Ray Data expressions.

This module provides multi-pass expression tree optimization including:
- Pure constant folding: lit(3) + lit(5) → lit(8)
- Algebraic simplification: col("x") * 1 → col("x")
- Short-circuit evaluation: False & col("x") → lit(False)
- Dead code elimination: True | col("x") → lit(True)
"""

import operator
from typing import Any, Optional

from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    Expr,
    LiteralExpr,
    Operation,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    _ExprVisitor,
)


class _ConstantFoldingVisitor(_ExprVisitor[Expr]):
    """Visitor that folds constant expressions with short-circuit evaluation.

    This visitor performs a single pass over an expression tree, applying:
    1. Constant folding for pure literal operations
    2. Algebraic simplification rules
    3. Short-circuit evaluation for boolean operations
    4. Double negation elimination

    Example:
        >>> from ray.data.expressions import col, lit
        >>> expr = (lit(2) + lit(3)) * col("x")
        >>> folder = _ConstantFoldingVisitor()
        >>> result = folder.visit(expr)
        >>> # Result: lit(5) * col("x")
    """

    def visit_column(self, expr: ColumnExpr) -> Expr:
        """Column references cannot be folded."""
        return expr

    def visit_literal(self, expr: LiteralExpr) -> Expr:
        """Literals are already in canonical form."""
        return expr

    def visit_binary(self, expr: BinaryExpr) -> Expr:
        """Fold binary expressions with short-circuit evaluation.

        Process:
        1. Fold left operand recursively
        2. Check for short-circuit conditions (e.g., False & x)
        3. Fold right operand if not short-circuited
        4. Apply constant folding if both operands are literals
        5. Apply algebraic simplification rules
        """
        # Step 1: Recursively fold left operand
        left = self.visit(expr.left)

        # Step 2: Short-circuit evaluation
        short_circuit_result = self._try_short_circuit(expr.op, left, expr.right)
        if short_circuit_result is not None:
            return short_circuit_result

        # Step 3: Fold right operand (not short-circuited)
        right = self.visit(expr.right)

        # Step 4: Pure constant folding
        if isinstance(left, LiteralExpr) and isinstance(right, LiteralExpr):
            folded = self._try_fold_constants(expr.op, left, right)
            if folded is not None:
                return folded

        # Step 5: Algebraic simplification
        simplified = self._try_algebraic_simplification(expr.op, left, right)
        if simplified is not None:
            return simplified

        # No optimization applicable
        return BinaryExpr(expr.op, left, right)

    def _try_short_circuit(
        self, op: Operation, left: Expr, right_original: Expr
    ) -> Optional[Expr]:
        """Attempt short-circuit evaluation.

        Args:
            op: The binary operation to evaluate
            left: The left operand (already folded)
            right_original: The right operand (not yet folded)

        Returns:
            Folded expression if short-circuit applies, None otherwise

        Short-circuit rules:
            False & x → False  (don't evaluate x)
            True | x → True    (don't evaluate x)
            True & x → x       (evaluate x)
            False | x → x      (evaluate x)
        """
        if not isinstance(left, LiteralExpr):
            return None

        if op == Operation.AND:
            if left.value is False:
                return LiteralExpr(False)  # False & x → False
            elif left.value is True:
                return self.visit(right_original)  # True & x → x

        elif op == Operation.OR:
            if left.value is True:
                return LiteralExpr(True)  # True | x → True
            elif left.value is False:
                return self.visit(right_original)  # False | x → x

        return None

    def _try_fold_constants(
        self, op: Operation, left: LiteralExpr, right: LiteralExpr
    ) -> Optional[Expr]:
        """Fold two constant literals into a single literal.

        Args:
            op: The binary operation to perform
            left: Left literal operand
            right: Right literal operand

        Returns:
            LiteralExpr with computed result if successful, None if folding
            failed (e.g., division by zero)
        """
        try:
            result = _eval_constant_binary_op(op, left.value, right.value)
            return LiteralExpr(result)
        except (ZeroDivisionError, TypeError, ValueError, OverflowError):
            # Cannot fold (e.g., division by zero, incompatible types)
            return None

    def _try_algebraic_simplification(
        self, op: Operation, left: Expr, right: Expr
    ) -> Optional[Expr]:
        """Apply algebraic simplification rules.

        Args:
            op: The binary operation
            left: Left operand (already folded)
            right: Right operand (already folded)

        Returns:
            Simplified expression if a rule applies, None if no simplification
            is possible
        """
        # === Arithmetic Identity Elements ===

        if op == Operation.ADD:
            # x + 0 → x
            if isinstance(right, LiteralExpr) and right.value == 0:
                return left
            # 0 + x → x
            if isinstance(left, LiteralExpr) and left.value == 0:
                return right

        elif op == Operation.SUB:
            # x - 0 → x
            if isinstance(right, LiteralExpr) and right.value == 0:
                return left
            # x - x → 0 (only if x has no side effects, i.e., is a column)
            if isinstance(left, ColumnExpr) and isinstance(right, ColumnExpr):
                if left.name == right.name:
                    return LiteralExpr(0)

        elif op == Operation.MUL:
            # x * 1 → x
            if isinstance(right, LiteralExpr) and right.value == 1:
                return left
            # 1 * x → x
            if isinstance(left, LiteralExpr) and left.value == 1:
                return right
            # x * 0 → 0 (annihilator)
            if isinstance(right, LiteralExpr) and right.value == 0:
                return LiteralExpr(0)
            # 0 * x → 0
            if isinstance(left, LiteralExpr) and left.value == 0:
                return LiteralExpr(0)

        elif op == Operation.DIV:
            # x / 1 → x
            if isinstance(right, LiteralExpr) and right.value == 1:
                return left
            # 0 / x → 0 (where x != 0, but we can't verify at compile time)
            if isinstance(left, LiteralExpr) and left.value == 0:
                # Only safe if right is also a literal and non-zero
                if isinstance(right, LiteralExpr) and right.value != 0:
                    return LiteralExpr(0)

        elif op == Operation.FLOORDIV:
            # x // 1 → x (for integers)
            if isinstance(right, LiteralExpr) and right.value == 1:
                return left

        elif op == Operation.MOD:
            # x % 1 → 0
            if isinstance(right, LiteralExpr) and right.value == 1:
                return LiteralExpr(0)

        # === Boolean Identity Elements ===

        elif op == Operation.AND:
            # x & True → x
            if isinstance(right, LiteralExpr) and right.value is True:
                return left
            # True & x → x
            if isinstance(left, LiteralExpr) and left.value is True:
                return right
            # x & False → False (annihilator)
            if isinstance(right, LiteralExpr) and right.value is False:
                return LiteralExpr(False)
            # False & x → False
            if isinstance(left, LiteralExpr) and left.value is False:
                return LiteralExpr(False)
            # x & x → x (idempotent)
            if isinstance(left, ColumnExpr) and isinstance(right, ColumnExpr):
                if left.name == right.name:
                    return left

        elif op == Operation.OR:
            # x | False → x
            if isinstance(right, LiteralExpr) and right.value is False:
                return left
            # False | x → x
            if isinstance(left, LiteralExpr) and left.value is False:
                return right
            # x | True → True (annihilator)
            if isinstance(right, LiteralExpr) and right.value is True:
                return LiteralExpr(True)
            # True | x → True
            if isinstance(left, LiteralExpr) and left.value is True:
                return LiteralExpr(True)
            # x | x → x (idempotent)
            if isinstance(left, ColumnExpr) and isinstance(right, ColumnExpr):
                if left.name == right.name:
                    return left

        # === Comparison Simplifications ===

        elif op == Operation.EQ:
            # x == x → True
            if isinstance(left, ColumnExpr) and isinstance(right, ColumnExpr):
                if left.name == right.name:
                    return LiteralExpr(True)

        elif op == Operation.NE:
            # x != x → False
            if isinstance(left, ColumnExpr) and isinstance(right, ColumnExpr):
                if left.name == right.name:
                    return LiteralExpr(False)

        return None

    def visit_unary(self, expr: UnaryExpr) -> Expr:
        """Fold unary expressions.

        Handles:
        1. Constant folding: NOT(lit(True)) → lit(False)
        2. Double negation: NOT(NOT(x)) → x
        """
        # Recursively fold operand
        operand = self.visit(expr.operand)

        # Pure constant folding
        if isinstance(operand, LiteralExpr):
            try:
                result = _eval_constant_unary_op(expr.op, operand.value)
                return LiteralExpr(result)
            except (TypeError, ValueError):
                return UnaryExpr(expr.op, operand)

        # Double negation elimination: NOT(NOT(x)) → x
        if expr.op == Operation.NOT and isinstance(operand, UnaryExpr):
            if operand.op == Operation.NOT:
                return operand.operand

        return UnaryExpr(expr.op, operand)

    def visit_alias(self, expr: AliasExpr) -> Expr:
        """Fold the inner expression of an alias."""
        folded_inner = self.visit(expr.expr)

        # Preserve alias if inner expression changed
        if folded_inner is expr.expr:
            return expr

        return AliasExpr(
            folded_inner.data_type,
            folded_inner,
            _name=expr._name,
            _is_rename=expr._is_rename,
        )

    def visit_udf(self, expr: UDFExpr) -> Expr:
        """Fold arguments of UDF expressions.

        Note: UDFs themselves cannot be folded as they may have side effects.
        """
        folded_args = [self.visit(arg) for arg in expr.args]
        folded_kwargs = {k: self.visit(v) for k, v in expr.kwargs.items()}

        # Check if anything changed
        args_changed = any(new is not old for new, old in zip(folded_args, expr.args))
        kwargs_changed = any(
            folded_kwargs[k] is not expr.kwargs[k] for k in expr.kwargs
        )

        if not args_changed and not kwargs_changed:
            return expr

        return UDFExpr(expr.fn, *folded_args, **folded_kwargs)

    def visit_star(self, expr: StarExpr) -> Expr:
        """Star expressions cannot be folded."""
        return expr

    def visit_download(self, expr: DownloadExpr) -> Expr:
        """Download expressions cannot be folded."""
        return expr


def _eval_constant_binary_op(op: Operation, left: Any, right: Any) -> Any:
    """Evaluate a binary operation on constant values at compile time.

    Args:
        op: The operation to perform
        left: Left operand value
        right: Right operand value

    Returns:
        The computed result

    Raises:
        ZeroDivisionError: For division by zero
        TypeError: For incompatible operand types
        ValueError: For invalid operations
        OverflowError: For arithmetic overflow
    """
    op_map = {
        Operation.ADD: operator.add,
        Operation.SUB: operator.sub,
        Operation.MUL: operator.mul,
        Operation.DIV: operator.truediv,
        Operation.MOD: operator.mod,
        Operation.FLOORDIV: operator.floordiv,
        Operation.GT: operator.gt,
        Operation.LT: operator.lt,
        Operation.GE: operator.ge,
        Operation.LE: operator.le,
        Operation.EQ: operator.eq,
        Operation.NE: operator.ne,
        Operation.AND: operator.and_,
        Operation.OR: operator.or_,
    }

    if op not in op_map:
        raise ValueError(f"Unsupported binary operation for constant folding: {op}")

    return op_map[op](left, right)


def _eval_constant_unary_op(op: Operation, operand: Any) -> Any:
    """Evaluate a unary operation on a constant value at compile time.

    Args:
        op: The operation to perform
        operand: Operand value

    Returns:
        The computed result

    Raises:
        TypeError: For incompatible operand type
        ValueError: For invalid operations
    """
    if op == Operation.NOT:
        return not operand
    elif op == Operation.IS_NULL:
        return operand is None
    elif op == Operation.IS_NOT_NULL:
        return operand is not None
    else:
        raise ValueError(f"Unsupported unary operation for constant folding: {op}")


def fold_constant_expressions(expr: Expr, max_iterations: int = 10) -> Expr:
    """Iteratively fold constants until fixpoint is reached.

    This function applies the constant folding visitor multiple times until
    the expression stops changing. This is necessary for complex nested
    expressions that require multiple passes to fully simplify.

    Args:
        expr: The expression to optimize
        max_iterations: Maximum number of optimization passes (default: 10)

    Returns:
        Fully optimized expression

    Example:
        >>> from ray.data.expressions import col, lit
        >>> # Simple case - single pass
        >>> expr1 = lit(3) + lit(5)
        >>> fold_constant_expressions(expr1)
        >>> # Result: lit(8)

        >>> # Complex nested case - multiple passes
        >>> expr2 = ((lit(True) & col("a")) | lit(False)) & lit(True)
        >>> fold_constant_expressions(expr2)
        >>> # Pass 1: (col("a") | lit(False)) & lit(True)
        >>> # Pass 2: col("a") & lit(True)
        >>> # Pass 3: col("a")
    """
    folder = _ConstantFoldingVisitor()

    for iteration in range(max_iterations):
        new_expr = folder.visit(expr)

        # Check if reached fixpoint (no more changes)
        if new_expr.structurally_equals(expr):
            break

        expr = new_expr

    return expr
