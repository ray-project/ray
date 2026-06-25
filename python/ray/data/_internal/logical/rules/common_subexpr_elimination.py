import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Dict, Hashable, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators import CSE_TEMP_COLUMN_PREFIX, Project
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ExpressionOccurrence,
    _StructuralFingerprintOccurrenceCollector,
    _StructuralFingerprintVisitor,
)
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    Expr,
    LiteralExpr,
    MonotonicallyIncreasingIdExpr,
    RandomExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    UUIDExpr,
)

__all__ = ["CommonSubExprElimination"]


@dataclass
class _Candidate:
    """A structurally duplicated expression that CSE may materialize once."""

    expr: Expr
    key: Hashable
    occurrences: List[_ExpressionOccurrence]
    temp_name: Optional[str] = None

    @property
    def max_depth(self) -> int:
        """Return the deepest AST depth at which this candidate appears."""
        return max(o.depth for o in self.occurrences)


_IGNORED_CSE_ROOT_TYPES = (ColumnExpr, LiteralExpr, AliasExpr, StarExpr)


def _is_ignored_cse_root(expr: Expr) -> bool:
    return isinstance(expr, _IGNORED_CSE_ROOT_TYPES)


def _collect_occurrences(exprs: List[Expr]) -> List[_ExpressionOccurrence]:
    """Collect structural occurrences from projection expressions.

    Args:
        exprs: Visible expression list from a ``Project`` operator.

    Returns:
        A flat list of occurrences, one per visited AST node, with each node's
        structural key and depth computed by
        ``_StructuralFingerprintOccurrenceCollector``.
    """
    collector = _StructuralFingerprintOccurrenceCollector()
    for expr in exprs:
        collector.visit(expr)

    return collector.get_occurrences()


def _add_to_structural_group(
    groups: List[List[_ExpressionOccurrence]], occurrence: _ExpressionOccurrence
) -> None:
    """Add one occurrence to the first structurally equivalent group.

    Args:
        groups: Existing structural groups for one fingerprint key. This list is
            mutated in place.
        occurrence: Occurrence with the same fingerprint key to place.

    Returns:
        ``None``. The occurrence is appended to the first group whose
        representative passes ``structurally_equals``; otherwise, a new
        singleton group is appended.
    """
    for group in groups:
        if occurrence.expr.structurally_equals(group[0].expr):
            group.append(occurrence)
            return
    groups.append([occurrence])


def _find_candidates(exprs: List[Expr]) -> List[_Candidate]:
    """Find duplicated, CSE-eligible sub-expressions in a projection.

    Args:
        exprs: Visible expression list from a ``Project`` operator.

    Returns:
        Candidates whose non-ignored root expressions occur more than once.
        Ignored roots such as columns, literals, aliases, and stars are skipped
        before structural grouping so wide projections do not pay unnecessary
        exact-comparison cost for leaves that will never be materialized.
    """
    occurrences = _collect_occurrences(exprs)

    buckets: Dict[Hashable, List[List[_ExpressionOccurrence]]] = defaultdict(list)
    for occurrence in occurrences:
        if not _is_ignored_cse_root(occurrence.expr):
            _add_to_structural_group(buckets[occurrence.key], occurrence)

    candidates: List[_Candidate] = []
    for key, structural_groups in buckets.items():
        for group in structural_groups:
            if len(group) <= 1:
                continue

            candidates.append(
                _Candidate(
                    expr=group[0].expr,
                    key=key,
                    occurrences=group,
                )
            )

    return candidates


def _assign_temp_names(candidates: List[_Candidate]) -> None:
    """Assign hidden temporary column names to selected CSE candidates.

    Args:
        candidates: Candidate list returned by ``_find_candidates``. Each
            candidate is mutated in place.
    """
    project_token = uuid.uuid4().hex[:6]

    for i, candidate in enumerate(candidates):
        temp_name = f"{CSE_TEMP_COLUMN_PREFIX}{project_token}_{i}"
        candidate.temp_name = temp_name


@dataclass(frozen=True)
class _Replacement:
    expr: Expr
    key: Hashable
    temp_name: str


class _ReplacementIndex:
    """Lookup structure for exact replacement matches during expression rewrite."""

    def __init__(self, replacements: List[_Replacement]):
        """Build a fingerprint bucket index from replacement records.

        Args:
            replacements: Zero or more replacements that are already safe to
                reference.
        """
        self._fingerprint = _StructuralFingerprintVisitor()
        self._replacements_by_key: Dict[Hashable, List[_Replacement]] = defaultdict(
            list
        )
        for replacement in replacements:
            self._replacements_by_key[replacement.key].append(replacement)

    def find(self, expr: Expr) -> Optional[_Replacement]:
        """Return the replacement for an exactly matching expression, if any.

        Args:
            expr: Expression node being considered for replacement.

        Returns:
            The matching replacement when both the structural fingerprint and
            ``structurally_equals`` match; otherwise, ``None``.
        """
        key = self._fingerprint.visit(expr)
        for replacement in self._replacements_by_key.get(key, []):
            if expr.structurally_equals(replacement.expr):
                return replacement
        return None


def _candidate_to_replacement(candidate: _Candidate) -> _Replacement:
    """Convert a temp-named candidate into a rewriter replacement.

    Args:
        candidate: Candidate after temp-name assignment.

    Returns:
        An immutable replacement carrying the representative expression,
        fingerprint key, and temp column name.
    """
    assert candidate.temp_name is not None
    return _Replacement(
        expr=candidate.expr,
        key=candidate.key,
        temp_name=candidate.temp_name,
    )


class _CSEExpressionRewriter:
    """Rewrite expression trees to read already-materialized CSE temp columns."""

    def __init__(self, replacements: List[_Replacement]):
        """Create a rewriter over the replacements available in this phase.

        Args:
            replacements: Replacement list that may be referenced while
                rewriting.
        """
        self._replacement_index = _ReplacementIndex(replacements)

    def rewrite_visible_expr(self, expr: Expr) -> Expr:
        """Rewrite a user-visible projection expression.

        Args:
            expr: One expression from ``Project.exprs`` after CSE candidates
                have been selected.

        Returns:
            A semantically equivalent expression where the root itself may be
            replaced by a temp column when the entire visible expression is
            common.
        """
        return self._rewrite(expr, allow_root_replacement=True)

    def rewrite_materialization_expr(self, expr: Expr) -> Expr:
        """Rewrite the right-hand side for a hidden materialization expression.

        Args:
            expr: Representative expression for a candidate that will be emitted
                as ``rhs.alias(temp_name)``.

        Returns:
            An expression that may replace child sub-expressions with previously
            materialized temp columns, but never replaces the root candidate
            itself.
        """
        return self._rewrite(expr, allow_root_replacement=False)

    def _maybe_replace(self, expr: Expr) -> Optional[ColumnExpr]:
        """Return a temp-column reference for a matching expression node.

        Args:
            expr: Expression node encountered by the recursive rewriter.

        Returns:
            ``ColumnExpr(temp_name)`` when the node matches a replacement
            exactly; otherwise, ``None`` so traversal can continue.
        """
        replacement = self._replacement_index.find(expr)
        if replacement is None:
            return None
        return ColumnExpr(replacement.temp_name)

    def _rewrite(self, expr: Expr, *, allow_root_replacement: bool) -> Expr:
        """Recursively rewrite one expression node.

        Args:
            expr: Expression tree to rewrite.
            allow_root_replacement: Whether the current root may become a temp
                column reference.

        Returns:
            The original immutable leaf/synthetic expression, a ``ColumnExpr``
            replacement, or a rebuilt expression whose children have been
            rewritten.

        Raises:
            TypeError: If ``expr`` has an expression node type that CSE does not
                know how to rewrite.
        """
        if allow_root_replacement:
            replacement = self._maybe_replace(expr)
            if replacement is not None:
                return replacement

        if isinstance(expr, (ColumnExpr, LiteralExpr, StarExpr)):
            return expr

        if isinstance(expr, BinaryExpr):
            return BinaryExpr(
                expr.op,
                self._rewrite(expr.left, allow_root_replacement=True),
                self._rewrite(expr.right, allow_root_replacement=True),
            )

        if isinstance(expr, UnaryExpr):
            return UnaryExpr(
                expr.op,
                self._rewrite(expr.operand, allow_root_replacement=True),
            )

        if isinstance(expr, AliasExpr):
            visited = self._rewrite(
                expr.expr,
                allow_root_replacement=True,
            )._unalias()
            return replace(
                expr,
                expr=visited,
                _is_rename=expr._is_rename and isinstance(visited, ColumnExpr),
            )

        if isinstance(expr, UDFExpr):
            return replace(
                expr,
                args=[
                    self._rewrite(arg, allow_root_replacement=True) for arg in expr.args
                ],
                kwargs={
                    key: self._rewrite(value, allow_root_replacement=True)
                    for key, value in expr.kwargs.items()
                },
            )

        if isinstance(
            expr,
            (
                DownloadExpr,
                MonotonicallyIncreasingIdExpr,
                RandomExpr,
                UUIDExpr,
            ),
        ):
            return expr

        raise TypeError(f"Unsupported expression type for CSE rewrite: {type(expr)}")


def _build_cse_materialization_plan(candidates: List[_Candidate]) -> List[Expr]:
    """Build hidden expressions that compute common sub-expressions first.

    Args:
        candidates: Temp-named candidates selected for a ``Project``.

    Returns:
        Hidden alias expressions ordered deepest-first, so nested common
        expressions are materialized before parent expressions that can
        reference their temporary columns.
    """
    ordered_candidates = sorted(
        candidates,
        key=lambda candidate: candidate.max_depth,
        reverse=True,
    )

    materializations: List[Expr] = []
    replacements: List[_Replacement] = []

    for candidate in ordered_candidates:
        assert candidate.temp_name is not None
        rewriter = _CSEExpressionRewriter(replacements)
        rhs = rewriter.rewrite_materialization_expr(candidate.expr)
        materializations.append(rhs.alias(candidate.temp_name))
        replacements.append(_candidate_to_replacement(candidate))

    return materializations


def _rewrite_visible_exprs(
    exprs: List[Expr],
    candidates: List[_Candidate],
) -> List[Expr]:
    """Rewrite all user-visible projection expressions to use CSE temps.

    Args:
        exprs: Original visible ``Project.exprs``.
        candidates: Temp-named candidates selected for that project.

    Returns:
        A new visible expression list where any matching common sub-expression,
        including an entire root expression, reads from the corresponding hidden
        temp column.
    """
    replacements = [_candidate_to_replacement(candidate) for candidate in candidates]
    rewriter = _CSEExpressionRewriter(replacements)
    return [rewriter.rewrite_visible_expr(expr) for expr in exprs]


class CommonSubExprElimination(Rule):
    """Logical optimizer rule that materializes duplicated projection expressions.

    This rule rewrites eligible ``Project`` operators so hidden common
    sub-expression aliases are evaluated before the visible projection
    expressions that read them.
    """

    # TODO: Add a general optimizer volatility contract before treating
    # potentially volatile expressions specially. This should apply
    # across CSE, ProjectionPushdown, PredicatePushdown, LimitPushdown,
    # constant folding, and any other rule that can change expression
    # evaluation count, timing, or placement.

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply CSE to every eligible project in a logical plan.

        Args:
            plan: Logical plan that may contain repeated projection
                sub-expressions.

        Returns:
            The original plan when no project changes, or a new logical plan
            with transformed project nodes and the original context preserved.
        """
        dag = plan.dag
        new_dag = dag._apply_transform(self._try_optimize_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _try_optimize_project(cls, op: LogicalOperator) -> LogicalOperator:
        """Rewrite one logical operator if it is an eligible ``Project``.

        Args:
            op: Logical operator visited during DAG traversal.

        Returns:
            The original operator for non-projects, already-CSE-optimized
            projects, or projects with no candidates. Otherwise, returns a new
            ``Project`` containing hidden materialization expressions and
            rewritten visible expressions while preserving the original project
            execution settings.
        """
        if not isinstance(op, Project):
            return op

        if op.get_common_sub_exprs():
            return op

        candidates = _find_candidates(op.exprs)
        if not candidates:
            return op

        _assign_temp_names(candidates)
        common_exprs = _build_cse_materialization_plan(candidates)
        rewritten_exprs = _rewrite_visible_exprs(op.exprs, candidates)

        return Project(
            exprs=rewritten_exprs,
            input_dependencies=op.input_dependencies,
            compute=op.compute,
            ray_remote_args=op.ray_remote_args,
            ray_remote_args_fn=op.ray_remote_args_fn,
            per_block_limit=op.per_block_limit,
            _common_sub_exprs=common_exprs,
        )
