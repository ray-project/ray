from dataclasses import replace
from typing import Dict, List, TypeVar

from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
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


class _ColumnSubstitutionVisitor(_ExprVisitor[Expr]):
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


class _TreeReprVisitor(_ExprVisitor[str]):
    """Visitor that generates a readable tree representation of expressions. Returns in pre-order traversal."""

    def __init__(self, prefix: str = "", is_last: bool = True):
        """
        Initialize the tree representation visitor.

        Args:
            prefix: The prefix string for indentation (accumulated from parent nodes)
            is_last: Whether this node is the last child of its parent
        """
        self.prefix = prefix
        self.is_last = is_last
        self._max_length = 50  # Maximum length of the node label

    def _make_tree_lines(
        self,
        node_label: str,
        children: List[tuple[str, "Expr"]] = None,
        expr: "Expr" = None,
    ) -> str:
        """
        Format a node and its children with tree box-drawing characters.

        Args:
            node_label: The label for this node (e.g., "ADD")
            children: List of (label, child_expr) tuples to render as children
            expr: The expression node (used to extract datatype)

        Returns:
            Multi-line string representation of the tree
        """
        lines = [node_label]

        if children:
            for i, (label, child_expr) in enumerate(children):
                is_last_child = i == len(children) - 1

                # Build prefix for the child based on whether current node is last
                child_prefix = self.prefix + ("    " if self.is_last else "│   ")

                # Choose connector: └── for last child, ├── for others
                connector = "└── " if is_last_child else "├── "

                # Recursively visit the child with updated prefix
                child_visitor = _TreeReprVisitor(child_prefix, is_last_child)
                child_lines = child_visitor.visit(child_expr).split("\n")

                # Add the first line with label and connector
                if label:
                    lines.append(f"{child_prefix}{connector}{label}: {child_lines[0]}")
                else:
                    lines.append(f"{child_prefix}{connector}{child_lines[0]}")

                # Add remaining lines from child with proper indentation
                for line in child_lines[1:]:
                    lines.append(line)

        return "\n".join(lines)

    def visit_column(self, expr: "ColumnExpr") -> str:
        return self._make_tree_lines(f"COL({expr.name!r})", expr=expr)

    def visit_literal(self, expr: "LiteralExpr") -> str:
        # Truncate long values for readability
        value_repr = repr(expr.value)
        if len(value_repr) > self._max_length:
            value_repr = value_repr[: self._max_length - 3] + "..."
        return self._make_tree_lines(f"LIT({value_repr})", expr=expr)

    def visit_binary(self, expr: "BinaryExpr") -> str:
        return self._make_tree_lines(
            f"{expr.op.name}",
            children=[
                ("left", expr.left),
                ("right", expr.right),
            ],
            expr=expr,
        )

    def visit_unary(self, expr: "UnaryExpr") -> str:
        return self._make_tree_lines(
            f"{expr.op.name}",
            children=[("operand", expr.operand)],
            expr=expr,
        )

    def visit_alias(self, expr: "AliasExpr") -> str:
        rename_marker = " [rename]" if expr._is_rename else ""
        return self._make_tree_lines(
            f"ALIAS({expr.name!r}){rename_marker}",
            children=[("", expr.expr)],
            expr=expr,
        )

    def visit_udf(self, expr: "UDFExpr") -> str:
        # Get function name for better readability
        fn_name = getattr(expr.fn, "__name__", str(expr.fn))

        children = []
        # Add positional arguments
        for i, arg in enumerate(expr.args):
            children.append((f"arg[{i}]", arg))

        # Add keyword arguments
        for key, value in expr.kwargs.items():
            children.append((f"kwarg[{key!r}]", value))

        return self._make_tree_lines(
            f"UDF({fn_name})",
            children=children if children else None,
            expr=expr,
        )

    def visit_download(self, expr: "DownloadExpr") -> str:
        return self._make_tree_lines(f"DOWNLOAD({expr.uri_column_name!r})", expr=expr)

    def visit_star(self, expr: "StarExpr") -> str:
        return self._make_tree_lines("COL(*)", expr=expr)
