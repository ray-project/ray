from dataclasses import replace
from typing import Dict, List, TypeVar

from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    _ExprVisitor,
)

T = TypeVar("T")


class _ExprVisitorBase(_ExprVisitor[None]):
    """Base visitor that provides automatic recursive traversal.

    This class extends _ExprVisitor and provides default implementations
    for composite nodes that automatically traverse child expressions.
    """

    def visit_binary(self, expr: "BinaryExpr") -> None:
        """Default implementation: recursively visit both operands."""
        super().visit(expr.left)
        super().visit(expr.right)

    def visit_unary(self, expr: "UnaryExpr") -> None:
        """Default implementation: recursively visit the operand."""
        super().visit(expr.operand)

    def visit_alias(self, expr: "AliasExpr") -> None:
        """Default implementation: recursively visit the inner expression."""
        super().visit(expr.expr)

    def visit_udf(self, expr: "UDFExpr") -> None:
        """Default implementation: recursively visit all arguments."""
        for arg in expr.args:
            super().visit(arg)
        for value in expr.kwargs.values():
            super().visit(value)

    def visit_literal(self, expr: LiteralExpr) -> None:
        """Visit a literal expression (no columns to collect)."""
        pass

    def visit_star(self, expr: StarExpr) -> None:
        """Visit a star expression (no columns to collect)."""
        pass

    def visit_download(self, expr: "Expr") -> None:
        """Visit a download expression (no columns to collect)."""
        pass


class _ColumnReferenceCollector(_ExprVisitorBase):
    """Visitor that collects all column references from expression trees.

    This visitor traverses expression trees and accumulates column names
    referenced in ColumnExpr nodes.
    """

    def __init__(self):
        """Initialize with an empty set of referenced columns."""

        # NOTE: We're using dict to maintain insertion ordering
        self._col_refs: Dict[str, None] = dict()

    def get_column_refs(self) -> List[str]:
        return list(self._col_refs.keys())

    def visit_column(self, expr: ColumnExpr) -> None:
        """Visit a column expression and collect its name.

        Args:
            expr: The column expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self._col_refs[expr.name] = None

    def visit_alias(self, expr: AliasExpr) -> None:
        """Visit an alias expression and collect from its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.expr)


class _ColumnRefRebindingVisitor(_ExprVisitor[Expr]):
    """Visitor rebinding column references in ``Expression``s.

    This visitor traverses given ``Expression`` trees and substitutes column references
    according to a provided substitution map.
    """

    def __init__(self, column_ref_substitutions: Dict[str, Expr]):
        """Initialize with a column substitution map.

        Args:
            column_ref_substitutions: Mapping from column names to replacement expressions.
        """
        self._col_ref_substitutions = column_ref_substitutions

    def visit_column(self, expr: ColumnExpr) -> Expr:
        """Visit a column expression and substitute it.

        Args:
            expr: The column expression.

        Returns:
            The substituted expression or the original if no substitution exists.
        """
        substitution = self._col_ref_substitutions.get(expr.name)

        return substitution if substitution is not None else expr

    def visit_literal(self, expr: LiteralExpr) -> Expr:
        """Visit a literal expression (no rewriting needed).

        Args:
            expr: The literal expression.

        Returns:
            The original literal expression.
        """
        return expr

    def visit_binary(self, expr: BinaryExpr) -> Expr:
        """Visit a binary expression and rewrite its operands.

        Args:
            expr: The binary expression.

        Returns:
            A new binary expression with rewritten operands.
        """
        return BinaryExpr(
            expr.op,
            self.visit(expr.left),
            self.visit(expr.right),
        )

    def visit_unary(self, expr: UnaryExpr) -> Expr:
        """Visit a unary expression and rewrite its operand.

        Args:
            expr: The unary expression.

        Returns:
            A new unary expression with rewritten operand.
        """
        return UnaryExpr(expr.op, self.visit(expr.operand))

    def visit_udf(self, expr: UDFExpr) -> Expr:
        """Visit a UDF expression and rewrite its arguments.

        Args:
            expr: The UDF expression.

        Returns:
            A new UDF expression with rewritten arguments.
        """
        new_args = [self.visit(arg) for arg in expr.args]
        new_kwargs = {key: self.visit(value) for key, value in expr.kwargs.items()}
        return UDFExpr(
            fn=expr.fn, data_type=expr.data_type, args=new_args, kwargs=new_kwargs
        )

    def visit_alias(self, expr: AliasExpr) -> Expr:
        """Visit an alias expression and rewrite its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            A new alias expression with rewritten inner expression and preserved name.
        """
        # We unalias returned expression to avoid nested aliasing
        visited = self.visit(expr.expr)._unalias()
        # NOTE: We're carrying over all of the other aspects of the alias
        #       only replacing inner expre
        return replace(
            expr,
            expr=visited,
            # Alias expression will remain a renaming one (ie replacing source column)
            # so long as it's referencing another column (and not otherwise)
            #
            # TODO replace w/ standalone rename expr
            _is_rename=expr._is_rename and _is_col_expr(visited),
        )

    def visit_download(self, expr: "Expr") -> Expr:
        """Visit a download expression (no rewriting needed).

        Args:
            expr: The download expression.

        Returns:
            The original download expression.
        """
        return expr

    def visit_star(self, expr: StarExpr) -> Expr:
        """Visit a star expression (no rewriting needed).

        Args:
            expr: The star expression.

        Returns:
            The original star expression.
        """
        return expr


def _is_col_expr(expr: Expr) -> bool:
    return isinstance(expr, ColumnExpr) or (
        isinstance(expr, AliasExpr) and isinstance(expr.expr, ColumnExpr)
    )
