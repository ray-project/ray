from typing import Set

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


class _ColumnReferenceCollector(_ExprVisitor[None]):
    """Visitor that collects all column references from expression trees.

    This visitor traverses expression trees and accumulates column names
    referenced in ColumnExpr nodes.
    """

    def __init__(self):
        """Initialize with an empty set of referenced columns."""
        self.referenced_columns: Set[str] = set()

    def visit(self, expr: Expr) -> None:
        """Visit an expression node and dispatch to the appropriate method.

        Extends the base visitor to handle StarExpr which is not
        part of the base _ExprVisitor interface.

        Args:
            expr: The expression to visit.

        Returns:
            None (only collects columns as a side effect).
        """
        super().visit(expr)

    def visit_column(self, expr: ColumnExpr) -> None:
        """Visit a column expression and collect its name.

        Args:
            expr: The column expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.referenced_columns.add(expr.name)

    def visit_literal(self, expr: LiteralExpr) -> None:
        """Visit a literal expression (no columns to collect).

        Args:
            expr: The literal expression.

        Returns:
            None.
        """
        # Literals don't reference any columns
        pass

    def visit_binary(self, expr: BinaryExpr) -> None:
        """Visit a binary expression and collect from both operands.

        Args:
            expr: The binary expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_unary(self, expr: UnaryExpr) -> None:
        """Visit a unary expression and collect from its operand.

        Args:
            expr: The unary expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.operand)

    def visit_udf(self, expr: UDFExpr) -> None:
        """Visit a UDF expression and collect from all arguments.

        Args:
            expr: The UDF expression.

        Returns:
            None (only collects columns as a side effect).
        """
        for arg in expr.args:
            self.visit(arg)
        for value in expr.kwargs.values():
            self.visit(value)

    def visit_alias(self, expr: AliasExpr) -> None:
        """Visit an alias expression and collect from its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.expr)

    def visit_download(self, expr: "Expr") -> None:
        """Visit a download expression (no columns to collect).

        Args:
            expr: The download expression.

        Returns:
            None.
        """
        # DownloadExpr doesn't reference any columns in the projection pushdown context
        pass

    def visit_star(self, expr: StarExpr) -> None:
        """Visit a star expression (no columns to collect).

        Args:
            expr: The star expression.

        Returns:
            None.
        """
        # StarExpr doesn't reference any columns
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

    def visit(self, expr: Expr) -> Expr:
        """Visit an expression node and return the rewritten expression.

        Args:
            expr: The expression to visit.

        Returns:
            The rewritten expression.
        """
        return super().visit(expr)

    def visit_column(self, expr: ColumnExpr) -> Expr:
        """Visit a column expression and potentially substitute it.

        Args:
            expr: The column expression.

        Returns:
            The substituted expression or the original if no substitution exists.
        """
        substitution = self.column_substitutions.get(expr.name)
        if substitution is not None:
            # Unwrap aliases to get the actual expression
            return (
                substitution.expr
                if isinstance(substitution, AliasExpr)
                else substitution
            )
        return expr

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
        return type(expr)(
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
        return type(expr)(expr.op, self.visit(expr.operand))

    def visit_udf(self, expr: UDFExpr) -> Expr:
        """Visit a UDF expression and rewrite its arguments.

        Args:
            expr: The UDF expression.

        Returns:
            A new UDF expression with rewritten arguments.
        """
        new_args = [self.visit(arg) for arg in expr.args]
        new_kwargs = {key: self.visit(value) for key, value in expr.kwargs.items()}
        return type(expr)(
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
