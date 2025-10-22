from typing import Set, TypeVar

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


class _ColumnReferenceCollector(_ExprVisitorBase):
    """Visitor that collects all column references from expression trees.

    This visitor traverses expression trees and accumulates column names
    referenced in ColumnExpr nodes.
    """

    def __init__(self):
        """Initialize with an empty set of referenced columns."""
        self.referenced_columns: Set[str] = set()

    def visit_column(self, expr: ColumnExpr) -> None:
        """Visit a column expression and collect its name.

        Args:
            expr: The column expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.referenced_columns.add(expr.name)

    def visit_alias(self, expr: AliasExpr) -> None:
        """Visit an alias expression and collect from its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.expr)

    def visit_literal(self, expr: LiteralExpr) -> None:
        """Visit a literal expression (no columns to collect)."""
        pass

    def visit_star(self, expr: StarExpr) -> None:
        """Visit a star expression (no columns to collect)."""
        pass

    def visit_download(self, expr: "Expr") -> None:
        """Visit a download expression (no columns to collect)."""
        pass


class _ColumnRewriter(_ExprVisitor[Expr]):
    """Visitor that rewrites column references in expression trees.

    This visitor traverses expression trees and substitutes column references
    according to a provided substitution map, preserving the structure of the tree.
    """

    def __init__(self, column_substitutions: dict[str, Expr]):
        """Initialize with a column substitution map.

        Args:
            column_substitutions: Mapping from column names to replacement expressions.
        """
        self.column_substitutions = column_substitutions
        self._currently_substituting: Set[
            str
        ] = set()  # Track columns being substituted to prevent cycles

    def visit(self, expr: Expr) -> Expr:
        """Visit an expression node and return the rewritten expression.

        Args:
            expr: The expression to visit.

        Returns:
            The rewritten expression.
        """
        return super().visit(expr)

    def visit_column(self, expr: ColumnExpr) -> Expr:
        """Visit a column expression and substitute it.

        Args:
            expr: The column expression.

        Returns:
            The substituted expression or the original if no substitution exists.
        """
        # Check for cycles: if we're already substituting this column, stop
        if expr.name in self._currently_substituting:
            return expr

        substitution = self.column_substitutions.get(expr.name)
        if substitution is None:
            return expr

        # Mark this column as being substituted
        self._currently_substituting.add(expr.name)

        try:
            if not isinstance(substitution, AliasExpr):
                # Non-aliased expression: recursively rewrite
                return self.visit(substitution)

            inner = substitution.expr
            if isinstance(inner, ColumnExpr):
                inner_def = self.column_substitutions.get(inner.name)
                if isinstance(inner_def, AliasExpr) and isinstance(
                    inner_def.expr, ColumnExpr
                ):
                    # Preserve simple rename chain (swap semantics -> Example: [col("a").alias("b"), col("b").alias("a")])
                    return substitution

            # Aliased expression: rewrite inner and preserve alias (unless preserved above)
            return self.visit(inner).alias(substitution.name)
        finally:
            # Remove from tracking when done
            self._currently_substituting.discard(expr.name)

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
        return self.visit(expr.expr).alias(expr.name)

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
