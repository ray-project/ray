from collections import Counter
from dataclasses import dataclass, replace
from typing import Dict, Hashable, List, TypeVar

from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    Expr,
    LiteralExpr,
    MonotonicallyIncreasingIdExpr,
    Operation,
    RandomExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    UUIDExpr,
    _CallableClassUDF,
    _ExprVisitor,
)
from ray.data.util.expression_utils import (
    _alias_fingerprint_key,
    _binary_fingerprint_key,
    _column_fingerprint_key,
    _download_fingerprint_key,
    _literal_fingerprint_key,
    _monotonically_increasing_id_fingerprint_key,
    _random_fingerprint_key,
    _star_fingerprint_key,
    _udf_fingerprint_key,
    _unary_fingerprint_key,
    _uuid_fingerprint_key,
)

T = TypeVar("T")

# Mapping of operations to their string symbols for inline representation
_INLINE_OP_SYMBOLS = {
    Operation.ADD: "+",
    Operation.SUB: "-",
    Operation.MUL: "*",
    Operation.DIV: "/",
    Operation.MOD: "%",
    Operation.FLOORDIV: "//",
    Operation.GT: ">",
    Operation.LT: "<",
    Operation.GE: ">=",
    Operation.LE: "<=",
    Operation.EQ: "==",
    Operation.NE: "!=",
    Operation.AND: "&",
    Operation.OR: "|",
    Operation.IN: "in",
    Operation.NOT_IN: "not in",
}


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

    def visit_monotonically_increasing_id(
        self, expr: "MonotonicallyIncreasingIdExpr"
    ) -> None:
        """Visit a monotonically_increasing_id expression (no columns to collect)."""
        pass

    def visit_random(self, expr: "RandomExpr") -> None:
        """Visit a synthetic expression (no columns to collect)."""
        pass

    def visit_uuid(self, expr: "UUIDExpr") -> None:
        """Visit a uuid expression (no columns to collect)."""
        pass


class _ColumnReferenceCollector(_ExprVisitorBase):
    """Visitor that collects all column references from expression trees.

    Backed by a ``Counter`` so callers can take either:
    - ``get_column_refs()`` -> ordered, de-duplicated column names, or
    - ``get_counts()``      -> per-name reference multiplicity, counting repeats
      *within* a single expression (``x + x`` -> ``{"x": 2}``).

    ``Counter`` preserves first-insertion order, so ``get_column_refs()`` returns the
    same ordered, de-duplicated list as a plain insertion-ordered ``dict`` would.
    """

    def __init__(self):
        """Initialize with an empty reference counter."""
        self._col_refs: Counter = Counter()

    def get_column_refs(self) -> List[str]:
        return list(self._col_refs.keys())

    def get_counts(self) -> Counter:
        return self._col_refs

    def visit_column(self, expr: ColumnExpr) -> None:
        """Visit a column expression and count its name.

        Args:
            expr: The column expression.

        Returns:
            None (only counts columns as a side effect).
        """
        self._col_refs[expr.name] += 1

    def visit_alias(self, expr: AliasExpr) -> None:
        """Visit an alias expression and collect from its inner expression.

        Args:
            expr: The alias expression.

        Returns:
            None (only collects columns as a side effect).
        """
        self.visit(expr.expr)


class _IdempotencyVisitor(_ExprVisitor[bool]):
    """Reports whether an expression is safe to duplicate, reorder, or move.

    Returns ``True`` only when every node in the tree is idempotent. The three
    non-idempotent leaf types (``RandomExpr``, ``UUIDExpr``,
    ``MonotonicallyIncreasingIdExpr``) return ``False`` and propagate upward: a
    composite is idempotent iff all of its children are.

    Optimizer rules consult this (via :func:`is_idempotent`) before any rewrite that
    would change an expression's evaluation count, row set, or position.
    """

    # --- non-idempotent leaves ---
    def visit_random(self, expr: RandomExpr) -> bool:
        # Conservatively non-idempotent even when seeded: CSE matches structurally and
        # ignores ``_instance_id``, while the runtime RNG counter keys on it, so a
        # seeded RandomExpr cannot be safely de-duplicated in general.
        return False

    def visit_uuid(self, expr: UUIDExpr) -> bool:
        return False

    def visit_monotonically_increasing_id(
        self, expr: MonotonicallyIncreasingIdExpr
    ) -> bool:
        return False

    # --- idempotent leaves ---
    def visit_column(self, expr: ColumnExpr) -> bool:
        return True

    def visit_literal(self, expr: LiteralExpr) -> bool:
        return True

    def visit_star(self, expr: StarExpr) -> bool:
        return True

    def visit_download(self, expr: DownloadExpr) -> bool:
        # ``DownloadExpr`` is a leaf with no Expr children. It is idempotent (same URI
        # yields the same bytes); CSE avoids re-fetching it for *cost* reasons, which
        # is a separate concern from this correctness contract.
        return True

    # --- composites: idempotent iff all children are ---
    #
    # Children are visited via ``child.is_idempotent()`` (not ``self.visit(child)``)
    # so each node's result is read from / written to its per-instance cache. This
    # keeps an all-nodes query (e.g. CSE visiting every occurrence) linear overall
    # instead of re-walking each subtree.
    def visit_alias(self, expr: AliasExpr) -> bool:
        return expr.expr.is_idempotent()

    def visit_unary(self, expr: UnaryExpr) -> bool:
        return expr.operand.is_idempotent()

    def visit_binary(self, expr: BinaryExpr) -> bool:
        return expr.left.is_idempotent() and expr.right.is_idempotent()

    def visit_udf(self, expr: UDFExpr) -> bool:
        # FUTURE EXTENSION POINT: today UDFs are assumed idempotent and we only recurse
        # into their argument expressions. When per-UDF non-determinism is supported,
        # gate this on the UDF's declared determinism as well.
        return all(arg.is_idempotent() for arg in expr.args) and all(
            value.is_idempotent() for value in expr.kwargs.values()
        )


# Stateless singleton: ``Expr.is_idempotent`` reuses this rather than allocating a
# visitor per node during the initial (uncached) computation.
_IDEMPOTENCY_VISITOR = _IdempotencyVisitor()


class _CallableClassUDFCollector(_ExprVisitorBase):
    """Visitor that collects all callable class UDFs from expression trees.

    This visitor traverses expression trees and collects _CallableClassUDF instances
    that wrap callable classes (as opposed to regular functions).
    """

    def __init__(self):
        """Initialize with an empty list of _CallableClassUDF instances."""
        self._expr_udfs: List[_CallableClassUDF] = []

    def get_callable_class_udfs(self) -> List[_CallableClassUDF]:
        """Get the list of collected _CallableClassUDF instances.

        Returns:
            List of _CallableClassUDF instances that wrap callable classes.
        """
        return self._expr_udfs

    def visit_column(self, expr: ColumnExpr) -> None:
        """Visit a column expression (no UDFs to collect)."""
        pass

    def visit_udf(self, expr: UDFExpr) -> None:
        """Visit a UDF expression and collect it if it's a callable class.

        Args:
            expr: The UDF expression.

        Returns:
            None (only collects UDFs as a side effect).
        """
        # Check if fn is an _CallableClassUDF (indicates callable class)
        if isinstance(expr.fn, _CallableClassUDF):
            self._expr_udfs.append(expr.fn)

        # Continue visiting child expressions
        super().visit_udf(expr)


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
        return replace(expr, args=new_args, kwargs=new_kwargs)

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

    def visit_monotonically_increasing_id(
        self, expr: MonotonicallyIncreasingIdExpr
    ) -> Expr:
        """Visit a monotonically_increasing_id expression (no rewriting needed).

        Args:
            expr: The monotonically_increasing_id expression.

        Returns:
            The original expression.
        """
        return expr

    def visit_random(self, expr: "RandomExpr") -> Expr:
        """Visit a random expression (no rewriting needed).

        Args:
            expr: The random expression.

        Returns:
            The original random expression.
        """
        return expr

    def visit_uuid(self, expr: "UUIDExpr") -> Expr:
        """Visit a uuid expression (no rewriting needed).

        Args:
            expr: The uuid expression.

        Returns:
            The original uuid expression.
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

    def visit_monotonically_increasing_id(
        self, expr: "MonotonicallyIncreasingIdExpr"
    ) -> str:
        return self._make_tree_lines("MONOTONICALLY_INCREASING_ID()", expr=expr)

    def visit_random(self, expr: "RandomExpr") -> str:
        if expr.seed is None:
            label = "RANDOM()"
        else:
            label = f"RANDOM(seed={expr.seed}, reseed_after_execution={expr.reseed_after_execution})"
        return self._make_tree_lines(label, expr=expr)

    def visit_uuid(self, expr: "UUIDExpr") -> str:
        return self._make_tree_lines("UUID()", expr=expr)


class _InlineExprReprVisitor(_ExprVisitor[str]):
    """Visitor that generates concise inline string representations of expressions.

    This visitor creates single-line string representations suitable for displaying
    in operator names, log messages, etc. It aims to be human-readable while keeping
    the representation compact.
    """

    def __init__(self, max_literal_length: int = 20):
        """Initialize the inline representation visitor.

        Args:
            max_literal_length: Maximum length for literal value representations
        """
        self._max_literal_length = max_literal_length

    def visit_column(self, expr: "ColumnExpr") -> str:
        """Visit a column expression and return its inline representation."""
        return f"col({expr.name!r})"

    def visit_literal(self, expr: "LiteralExpr") -> str:
        """Visit a literal expression and return its inline representation."""
        value_repr = repr(expr.value)
        if len(value_repr) > self._max_literal_length:
            value_repr = value_repr[: self._max_literal_length - 3] + "..."
        return value_repr

    def visit_binary(self, expr: "BinaryExpr") -> str:
        """Visit a binary expression and return its inline representation."""
        left_str = self.visit(expr.left)
        right_str = self.visit(expr.right)

        # Add parentheses around child binary expressions to avoid ambiguity
        if isinstance(expr.left, BinaryExpr):
            left_str = f"({left_str})"
        if isinstance(expr.right, BinaryExpr):
            right_str = f"({right_str})"

        op_str = _INLINE_OP_SYMBOLS.get(expr.op, expr.op.name.lower())
        return f"{left_str} {op_str} {right_str}"

    def visit_unary(self, expr: "UnaryExpr") -> str:
        """Visit a unary expression and return its inline representation."""
        operand_str = self.visit(expr.operand)

        # Add parentheses around binary expression operands to avoid ambiguity
        if isinstance(expr.operand, BinaryExpr):
            operand_str = f"({operand_str})"

        # Map operations to symbols/functions
        if expr.op == Operation.NOT:
            return f"~{operand_str}"
        elif expr.op == Operation.IS_NULL:
            return f"{operand_str}.is_null()"
        elif expr.op == Operation.IS_NOT_NULL:
            return f"{operand_str}.is_not_null()"
        else:
            return f"{expr.op.name.lower()}({operand_str})"

    def visit_alias(self, expr: "AliasExpr") -> str:
        """Visit an alias expression and return its inline representation."""
        inner_str = self.visit(expr.expr)
        return f"{inner_str}.alias({expr.name!r})"

    def visit_udf(self, expr: "UDFExpr") -> str:
        """Visit a UDF expression and return its inline representation."""
        # Get function name for better readability
        # For callable objects (instances with __call__), use the class name
        fn_name = getattr(expr.fn, "__name__", expr.fn.__class__.__name__)

        # Build argument list
        args_str = []
        for arg in expr.args:
            args_str.append(self.visit(arg))
        for key, value in expr.kwargs.items():
            args_str.append(f"{key}={self.visit(value)}")

        args_repr = ", ".join(args_str) if args_str else ""
        return f"{fn_name}({args_repr})"

    def visit_download(self, expr: "DownloadExpr") -> str:
        """Visit a download expression and return its inline representation."""
        return f"download({expr.uri_column_name!r})"

    def visit_star(self, expr: "StarExpr") -> str:
        """Visit a star expression and return its inline representation."""
        return "col(*)"

    def visit_monotonically_increasing_id(
        self, expr: "MonotonicallyIncreasingIdExpr"
    ) -> str:
        """Visit a monotonically_increasing_id expression and return its inline representation."""
        return "monotonically_increasing_id()"

    def visit_random(self, expr: "RandomExpr") -> str:
        """Visit a random expression and return its inline representation."""
        return "random()"

    def visit_uuid(self, expr: "UUIDExpr") -> str:
        """Visit a uuid expression and return its inline representation."""
        return "uuid()"


class _StructuralFingerprintVisitor(_ExprVisitor[Hashable]):
    """Visitor that computes a hashable structural fingerprint for an expression.

    Two expressions that are structurally equivalent produce equal fingerprints,
    so the fingerprint can be used as a cheap bucketing key before falling back to
    full ``structurally_equals`` comparison (e.g. for common sub-expression
    elimination).
    """

    def visit_column(self, expr: ColumnExpr) -> Hashable:
        return _column_fingerprint_key(expr)

    def visit_literal(self, expr: LiteralExpr) -> Hashable:
        return _literal_fingerprint_key(expr)

    def visit_binary(self, expr: BinaryExpr) -> Hashable:
        return _binary_fingerprint_key(
            expr,
            self.visit(expr.left),
            self.visit(expr.right),
        )

    def visit_unary(self, expr: UnaryExpr) -> Hashable:
        return _unary_fingerprint_key(expr, self.visit(expr.operand))

    def visit_udf(self, expr: UDFExpr) -> Hashable:
        return _udf_fingerprint_key(
            expr,
            tuple(self.visit(arg) for arg in expr.args),
            tuple(
                (k, self.visit(v))
                for k, v in sorted(expr.kwargs.items(), key=lambda item: item[0])
            ),
        )

    def visit_alias(self, expr: AliasExpr) -> Hashable:
        return _alias_fingerprint_key(expr, self.visit(expr.expr))

    def visit_download(self, expr: DownloadExpr) -> Hashable:
        return _download_fingerprint_key(expr)

    def visit_star(self, expr: StarExpr) -> Hashable:
        return _star_fingerprint_key()

    def visit_monotonically_increasing_id(
        self, expr: MonotonicallyIncreasingIdExpr
    ) -> Hashable:
        return _monotonically_increasing_id_fingerprint_key(expr)

    def visit_random(self, expr: RandomExpr) -> Hashable:
        return _random_fingerprint_key(expr)

    def visit_uuid(self, expr: UUIDExpr) -> Hashable:
        return _uuid_fingerprint_key(expr)


@dataclass(frozen=True)
class _ExpressionOccurrence:
    expr: Expr
    key: Hashable
    depth: int


class _StructuralFingerprintOccurrenceCollector(_ExprVisitor[Hashable]):
    """Collect expression occurrences while computing structural keys bottom-up."""

    def __init__(self):
        self._occurrences: List[_ExpressionOccurrence] = []
        self._depth = 0

    def get_occurrences(self) -> List[_ExpressionOccurrence]:
        return self._occurrences

    def _visit_child(self, expr: Expr) -> Hashable:
        self._depth += 1
        try:
            return self.visit(expr)
        finally:
            self._depth -= 1

    def _record(self, expr: Expr, key: Hashable) -> Hashable:
        self._occurrences.append(
            _ExpressionOccurrence(
                expr=expr,
                key=key,
                depth=self._depth,
            )
        )
        return key

    def visit_column(self, expr: ColumnExpr) -> Hashable:
        return self._record(expr, _column_fingerprint_key(expr))

    def visit_literal(self, expr: LiteralExpr) -> Hashable:
        return self._record(expr, _literal_fingerprint_key(expr))

    def visit_binary(self, expr: BinaryExpr) -> Hashable:
        return self._record(
            expr,
            _binary_fingerprint_key(
                expr,
                self._visit_child(expr.left),
                self._visit_child(expr.right),
            ),
        )

    def visit_unary(self, expr: UnaryExpr) -> Hashable:
        return self._record(
            expr,
            _unary_fingerprint_key(expr, self._visit_child(expr.operand)),
        )

    def visit_udf(self, expr: UDFExpr) -> Hashable:
        return self._record(
            expr,
            _udf_fingerprint_key(
                expr,
                tuple(self._visit_child(arg) for arg in expr.args),
                tuple(
                    (k, self._visit_child(v))
                    for k, v in sorted(expr.kwargs.items(), key=lambda item: item[0])
                ),
            ),
        )

    def visit_alias(self, expr: AliasExpr) -> Hashable:
        return self._record(
            expr,
            _alias_fingerprint_key(expr, self._visit_child(expr.expr)),
        )

    def visit_download(self, expr: DownloadExpr) -> Hashable:
        return self._record(expr, _download_fingerprint_key(expr))

    def visit_star(self, expr: StarExpr) -> Hashable:
        return self._record(expr, _star_fingerprint_key())

    def visit_monotonically_increasing_id(
        self, expr: MonotonicallyIncreasingIdExpr
    ) -> Hashable:
        return self._record(expr, _monotonically_increasing_id_fingerprint_key(expr))

    def visit_random(self, expr: RandomExpr) -> Hashable:
        return self._record(expr, _random_fingerprint_key(expr))

    def visit_uuid(self, expr: UUIDExpr) -> Hashable:
        return self._record(expr, _uuid_fingerprint_key(expr))


def get_column_references(expr: Expr) -> List[str]:
    """Extract all column references from an expression.

    This is a convenience function that creates a _ColumnReferenceCollector,
    visits the expression tree, and returns the list of referenced column names.

    Args:
        expr: The expression to extract column references from.

    Returns:
        List of column names referenced in the expression, in order of appearance.

    Example:
        >>> from ray.data.expressions import col
        >>> expr = (col("a") > 5) & (col("b") == "test")
        >>> get_column_references(expr)
        ['a', 'b']
    """
    collector = _ColumnReferenceCollector()
    collector.visit(expr)
    return collector.get_column_refs()
