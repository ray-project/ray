import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any, Dict, Hashable, Iterable, List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators import CSE_TEMP_COLUMN_PREFIX, Project
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    Expr,
    LiteralExpr,
    MonotonicallyIncreasingIdExpr,
    PyArrowComputeUDFExpr,
    RandomExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    UUIDExpr,
    _CallableClassUDF,
    _ExprVisitor,
)

__all__ = ["CommonSubExprElimination"]


@dataclass(frozen=True)
class _Occurrence:
    expr: Expr
    key: Hashable
    depth: int
    is_ignored_root: bool


@dataclass
class _Candidate:
    expr: Expr
    key: Hashable
    occurrences: List[_Occurrence]
    temp_name: Optional[str] = None

    @property
    def max_depth(self) -> int:
        return max(o.depth for o in self.occurrences)


def _make_hashable(value: Any) -> Hashable:
    try:
        hash(value)
        return value
    except TypeError:
        pass

    if isinstance(value, list):
        return tuple(_make_hashable(v) for v in value)
    if isinstance(value, tuple):
        return tuple(_make_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(
            sorted(
                ((k, _make_hashable(v)) for k, v in value.items()),
                key=lambda item: repr(item[0]),
            )
        )
    if isinstance(value, set):
        return frozenset(_make_hashable(v) for v in value)

    return repr(value)


def _data_type_key(expr: Expr) -> Hashable:
    return repr(getattr(expr, "data_type", None))


def _udf_function_key(fn: Any) -> Hashable:
    if isinstance(fn, _CallableClassUDF):
        return ("callable_class", fn.callable_class_spec.make_key())
    return ("function", _make_hashable(fn))


class _StructuralFingerprintVisitor(_ExprVisitor[Hashable]):
    def visit_column(self, expr: ColumnExpr) -> Hashable:
        return ("column", expr.name)

    def visit_literal(self, expr: LiteralExpr) -> Hashable:
        return ("literal", type(expr.value), _make_hashable(expr.value))

    def visit_binary(self, expr: BinaryExpr) -> Hashable:
        return (
            "binary",
            expr.op,
            self.visit(expr.left),
            self.visit(expr.right),
        )

    def visit_unary(self, expr: UnaryExpr) -> Hashable:
        return ("unary", expr.op, self.visit(expr.operand))

    def visit_udf(self, expr: UDFExpr) -> Hashable:
        if isinstance(expr, PyArrowComputeUDFExpr):
            return (
                "pyarrow_compute_udf",
                _make_hashable(expr.pc_func),
                _make_hashable(expr.pc_positional),
                _make_hashable(expr.pc_kwargs),
                tuple(self.visit(arg) for arg in expr.args),
                tuple(
                    (k, self.visit(v))
                    for k, v in sorted(expr.kwargs.items(), key=lambda item: item[0])
                ),
                _data_type_key(expr),
            )

        return (
            "udf",
            _udf_function_key(expr.fn),
            tuple(self.visit(arg) for arg in expr.args),
            tuple(
                (k, self.visit(v))
                for k, v in sorted(expr.kwargs.items(), key=lambda item: item[0])
            ),
            _data_type_key(expr),
        )

    def visit_alias(self, expr: AliasExpr) -> Hashable:
        return (
            "alias",
            expr.name,
            expr._is_rename,
            self.visit(expr.expr),
            _data_type_key(expr),
        )

    def visit_download(self, expr: DownloadExpr) -> Hashable:
        return ("download", expr.uri_column_name)

    def visit_star(self, expr: StarExpr) -> Hashable:
        return ("star",)

    def visit_monotonically_increasing_id(
        self, expr: MonotonicallyIncreasingIdExpr
    ) -> Hashable:
        return ("monotonically_increasing_id", expr._instance_id)

    def visit_random(self, expr: RandomExpr) -> Hashable:
        return (
            "random",
            expr.seed,
            expr.reseed_after_execution,
            _data_type_key(expr),
        )

    def visit_uuid(self, expr: UUIDExpr) -> Hashable:
        return ("uuid", _data_type_key(expr))


_IGNORED_CSE_ROOT_TYPES = (ColumnExpr, LiteralExpr, AliasExpr, StarExpr)


def _is_ignored_cse_root(expr: Expr) -> bool:
    return isinstance(expr, _IGNORED_CSE_ROOT_TYPES)


def _iter_children(expr: Expr) -> Iterable[Expr]:
    if isinstance(expr, BinaryExpr):
        yield expr.left
        yield expr.right
    elif isinstance(expr, UnaryExpr):
        yield expr.operand
    elif isinstance(expr, AliasExpr):
        yield expr.expr
    elif isinstance(expr, UDFExpr):
        yield from expr.args
        yield from expr.kwargs.values()


def _collect_occurrences(exprs: List[Expr]) -> List[_Occurrence]:
    fingerprint = _StructuralFingerprintVisitor()
    occurrences: List[_Occurrence] = []

    def visit(expr: Expr, *, depth: int) -> None:
        key = fingerprint.visit(expr)
        occurrences.append(
            _Occurrence(
                expr=expr,
                key=key,
                depth=depth,
                is_ignored_root=_is_ignored_cse_root(expr),
            )
        )
        for child in _iter_children(expr):
            visit(child, depth=depth + 1)

    for expr in exprs:
        visit(expr, depth=0)

    return occurrences


def _add_to_structural_group(
    groups: List[List[_Occurrence]], occurrence: _Occurrence
) -> None:
    for group in groups:
        if occurrence.expr.structurally_equals(group[0].expr):
            group.append(occurrence)
            return
    groups.append([occurrence])


def _find_candidates(exprs: List[Expr]) -> List[_Candidate]:
    occurrences = _collect_occurrences(exprs)

    buckets: Dict[Hashable, List[List[_Occurrence]]] = defaultdict(list)
    for occurrence in occurrences:
        if not occurrence.is_ignored_root:
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


def _assign_temp_names(project: Project, candidates: List[_Candidate]) -> None:
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
    def __init__(self, replacements: List[_Replacement]):
        self._fingerprint = _StructuralFingerprintVisitor()
        self._replacements_by_key: Dict[Hashable, List[_Replacement]] = defaultdict(
            list
        )
        for replacement in replacements:
            self._replacements_by_key[replacement.key].append(replacement)

    def find(self, expr: Expr) -> Optional[_Replacement]:
        key = self._fingerprint.visit(expr)
        for replacement in self._replacements_by_key.get(key, []):
            if expr.structurally_equals(replacement.expr):
                return replacement
        return None


def _candidate_to_replacement(candidate: _Candidate) -> _Replacement:
    assert candidate.temp_name is not None
    return _Replacement(
        expr=candidate.expr,
        key=candidate.key,
        temp_name=candidate.temp_name,
    )


class _CSEExpressionRewriter:
    def __init__(self, replacements: List[_Replacement]):
        self._replacement_index = _ReplacementIndex(replacements)

    def rewrite_visible_expr(self, expr: Expr) -> Expr:
        return self._rewrite(expr, allow_root_replacement=True)

    def rewrite_materialization_expr(self, expr: Expr) -> Expr:
        return self._rewrite(expr, allow_root_replacement=False)

    def _maybe_replace(self, expr: Expr) -> Optional[ColumnExpr]:
        replacement = self._replacement_index.find(expr)
        if replacement is None:
            return None
        return ColumnExpr(replacement.temp_name)

    def _rewrite(self, expr: Expr, *, allow_root_replacement: bool) -> Expr:
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
    replacements = [_candidate_to_replacement(candidate) for candidate in candidates]
    rewriter = _CSEExpressionRewriter(replacements)
    return [rewriter.rewrite_visible_expr(expr) for expr in exprs]


class CommonSubExprElimination(Rule):
    """
    Optimization rule to execute duplicated sub-expressions only once. This rule
    analyzes the fully optimized expressions list in a single operator, identifies
    duplicate sub-expressions, and allows the duplicated expressions to run once.
    """

    # TODO: Add a general optimizer volatility contract before treating
    # potentially volatile expressions specially. This should apply
    # across CSE, ProjectionPushdown, PredicatePushdown, LimitPushdown,
    # constant folding, and any other rule that can change expression
    # evaluation count, timing, or placement.

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._try_optimize_project)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _try_optimize_project(cls, op: LogicalOperator) -> LogicalOperator:
        if not isinstance(op, Project):
            return op

        assert len(op.get_cse_common_exprs()) == 0, "CSE already applied."

        candidates = _find_candidates(op.exprs)
        if not candidates:
            return op

        _assign_temp_names(op, candidates)
        common_exprs = _build_cse_materialization_plan(candidates)
        rewritten_exprs = _rewrite_visible_exprs(op.exprs, candidates)

        return Project(
            exprs=rewritten_exprs,
            input_dependencies=op.input_dependencies,
            compute=op.compute,
            ray_remote_args=op.ray_remote_args,
            ray_remote_args_fn=op.ray_remote_args_fn,
            per_block_limit=op.per_block_limit,
            _cse_common_exprs=common_exprs,
        )
