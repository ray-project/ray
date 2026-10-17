import copy
import functools
from dataclasses import dataclass, is_dataclass, replace
from typing import List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalPlan,
    PredicatePassThroughBehavior,
    Rule,
)
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    AbstractMap,
    Filter,
    Join,
    JoinSide,
    Limit,
    Project,
    RandomShuffle,
    Repartition,
    Union,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnSubstitutionVisitor,
)
from ray.data.expressions import BinaryExpr, Expr, Operation, col

__all__ = [
    "DeriveJoinSidePredicates",
    "PredicatePushdown",
]


@dataclass(frozen=True)
class _ConvertibilitySplit:
    """Result of splitting a predicate by PyArrow convertibility.

    Attributes:
        convertible: The conjuncts that can be lowered to PyArrow and pushed
            into the datasource. ``None`` if nothing is convertible.
        residual: The conjuncts that cannot be lowered and must remain as a
            ``Filter``. ``None`` if everything is convertible.
    """

    convertible: Optional[Expr]
    residual: Optional[Expr]


class PredicatePushdown(Rule):
    """Pushes down predicates across the graph.

    This rule performs the following optimizations:
    1. Combines chained Filter operators with compatible expressions
    2. Pushes filter expressions through eligible operators using trait-based rules
    3. Pushes filters into data sources that support predicate pushdown

    Eligibility is determined by the LogicalOperatorSupportsPredicatePassThrough trait, which operators
    implement to declare their pushdown behavior:
    - PASSTHROUGH: Filter passes through unchanged (Sort, Repartition, Shuffle)
    - PASSTHROUGH_WITH_SUBSTITUTION: Filter passes through with column rebinding (Project)
    - PUSH_INTO_BRANCHES: Filter is pushed into each branch (Union)
    - CONDITIONAL: Filter may be pushed based on analysis (Join - splits the
      predicate's top level ``AND`` chain, pushes each conjunct to the side it
      references if that is safe for the join type, and keeps the rest above)

    Filters are fused before being pushed. A fused predicate is never harder to
    push than its parts: the conditional path splits it back into conjuncts, so
    ``.filter(left_only).filter(right_only)`` above a join still pushes both.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply predicate pushdown optimization to the logical plan."""
        dag = plan.dag
        new_dag = dag._apply_transform(self._try_fuse_filters)
        new_dag = new_dag._apply_transform(self._try_push_down_predicate)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _is_valid_filter_operator(cls, op: LogicalOperator) -> bool:
        return isinstance(op, Filter) and op.is_expression_based()

    @classmethod
    def _try_fuse_filters(cls, op: LogicalOperator) -> LogicalOperator:
        """Fuse consecutive Filter operators with compatible expressions."""
        if not cls._is_valid_filter_operator(op):
            return op

        input_op = op.input_dependencies[0]
        if not cls._is_valid_filter_operator(input_op):
            return op

        # Do not fuse across a non-idempotent predicate (random/uuid/
        # monotonically_increasing_id): combining moves where a predicate is
        # evaluated (and thus which rows it sees), changing results.
        if (
            not op.predicate_expr.is_idempotent()
            or not input_op.predicate_expr.is_idempotent()
        ):
            return op

        # Combine predicates
        combined_predicate = op.predicate_expr & input_op.predicate_expr

        # Create new filter on the input of the lower filter
        return Filter(
            predicate_expr=combined_predicate,
            input_dependencies=[input_op.input_dependencies[0]],
        )

    @classmethod
    def _can_push_filter_through_projection(
        cls, filter_op: "Filter", projection_op: Project
    ) -> bool:
        """Check if a filter can be pushed through a projection operator.

        Returns False (blocks pushdown) if filter references:
        - Columns removed by select: select(['a']).filter(col('b'))
        - Computed columns: with_column('d', 4).filter(col('d'))
        - Old column names after rename: rename({'b': 'B'}).filter(col('b'))

        Returns True (allows pushdown) for:
        - Columns present in output: select(['a', 'b']).filter(col('a'))
        - New column names after rename: rename({'b': 'B'}).filter(col('B'))
        - Rename chains with name reuse: rename({'a': 'b', 'b': 'c'}).filter(col('b'))
          (where 'b' is valid output created by a->b)
        """
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _ColumnReferenceCollector,
        )
        from ray.data.expressions import AliasExpr, is_rename_expr

        # Do not push a filter below a projection that produces a non-idempotent
        # column (random/uuid/monotonically_increasing_id): reordering changes the row
        # set / position the expression is evaluated over (e.g.
        # monotonically_increasing_id reassigned over the filtered subset).
        if not projection_op.is_idempotent():
            return False

        collector = _ColumnReferenceCollector()
        collector.visit(filter_op.predicate_expr)
        predicate_columns = set(collector.get_column_refs() or [])

        output_columns = set()
        new_names = set()
        original_columns_being_renamed = set()

        for expr in projection_op.exprs:
            if expr.name is not None:
                # Collect output column names
                output_columns.add(expr.name)

            # Process AliasExpr (computed columns or renames)
            if isinstance(expr, AliasExpr):
                new_names.add(expr.name)

                # Check computed column: with_column('d', 4) creates AliasExpr(lit(4), 'd')
                if expr.name in predicate_columns and not is_rename_expr(expr):
                    return False  # Computed column

                # Track old names being renamed for later check
                if is_rename_expr(expr):
                    original_columns_being_renamed.add(expr.expr.name)

        # Check if filter references columns removed by explicit select.
        # Valid if: projection includes all columns (star, UDF-fallback path)
        # OR predicate columns exist in the explicit output set (typed path,
        # where ``StarExpr`` is expanded into explicit ``col()`` refs in
        # ``Project.__post_init__`` when the input schema is known).
        has_required_columns = (
            projection_op.has_star_expr() or predicate_columns.issubset(output_columns)
        )
        if not has_required_columns:
            return False

        # Find old names that are:
        # 1. Being renamed away (in original_columns_being_renamed), AND
        # 2. Referenced in predicate (in predicate_columns), AND
        # 3. NOT recreated as new names (not in new_names)
        #
        # Examples:
        #   rename({'b': 'B'}).filter(col('b'))
        #     → {'b'} & {'b'} - {'B'} = {'b'} → BLOCKS (old name 'b' no longer exists)
        #
        #   rename({'a': 'b', 'b': 'c'}).filter(col('b'))
        #     → {'a','b'} & {'b'} - {'b','c'} = {} → ALLOWS (new 'b' created by a->b)
        #
        #   rename({'b': 'B'}).filter(col('B'))
        #     → {'b'} & {'B'} - {'B'} = {} → ALLOWS (using new name 'B')
        invalid_old_names = (
            original_columns_being_renamed & predicate_columns
        ) - new_names
        if invalid_old_names:
            return False  # Old name after rename

        return True

    @classmethod
    def _substitute_predicate_columns(
        cls, predicate_expr: Expr, column_rename_map: dict[str, str]
    ) -> Expr:
        """Rebind column references in a predicate expression.

        When pushing a predicate through a projection with column renames,
        we need to rewrite column references from new names to old names.

        Args:
            predicate_expr: The predicate with new column names
            column_rename_map: Mapping from old_name -> new_name

        Returns:
            The predicate rewritten to use old column names
        """
        # Invert the mapping: new_name -> old_name (as col expression)
        # This is because the predicate uses new names and we need to map
        # them back to old names
        column_mapping = {
            new_col: col(old_col) for old_col, new_col in column_rename_map.items()
        }

        visitor = _ColumnSubstitutionVisitor(column_mapping)
        return visitor.visit(predicate_expr)

    @classmethod
    def _combine_with_and(
        cls, left: Optional[Expr], right: Optional[Expr]
    ) -> Optional[Expr]:
        """Combine two optional predicates with ``AND``, ignoring ``None``."""
        if left is not None and right is not None:
            return left & right
        return left if left is not None else right

    @classmethod
    def _flatten(cls, predicate: Expr, op: Operation) -> List[Expr]:
        """Flatten the top level chain of ``op`` (``AND`` or ``OR``) in ``predicate``
        into its operands, left to right."""
        if isinstance(predicate, BinaryExpr) and predicate.op == op:
            return cls._flatten(predicate.left, op) + cls._flatten(predicate.right, op)
        return [predicate]

    @classmethod
    def _combine(cls, predicates: List[Expr], op: Operation) -> Optional[Expr]:
        """Fold ``predicates`` with ``op`` left to right; ``None`` if there are none."""
        if not predicates:
            return None
        return functools.reduce(
            lambda left, right: BinaryExpr(op, left, right), predicates
        )

    @classmethod
    def _append_unique(cls, predicates: List[Expr], predicate: Expr) -> None:
        """Append ``predicate`` unless a structurally equal one is already present."""
        if not any(predicate.structurally_equals(existing) for existing in predicates):
            predicates.append(predicate)

    @classmethod
    def _split_by_convertibility(cls, predicate: Expr) -> _ConvertibilitySplit:
        """Split a predicate into PyArrow convertible and residual parts.

        Walks the top level ``AND`` chain and buckets each conjunct by whether
        it can be lowered to PyArrow. The convertible part can be pushed into
        the datasource while the residual part stays as a ``Filter`` above it.

        Args:
            predicate: The predicate expression to split.

        Returns:
            A ``_ConvertibilitySplit`` whose ``convertible`` and ``residual``
            fields hold the two parts. Both are optional.
        """
        if isinstance(predicate, BinaryExpr) and predicate.op == Operation.AND:
            left = cls._split_by_convertibility(predicate.left)
            right = cls._split_by_convertibility(predicate.right)
            return _ConvertibilitySplit(
                convertible=cls._combine_with_and(left.convertible, right.convertible),
                residual=cls._combine_with_and(left.residual, right.residual),
            )

        if predicate._is_pyarrow_convertible():
            return _ConvertibilitySplit(convertible=predicate, residual=None)
        return _ConvertibilitySplit(convertible=None, residual=predicate)

    @classmethod
    def _try_push_down_predicate(
        cls, op: LogicalOperator, derive_implied: bool = False
    ) -> LogicalOperator:
        """Push Filter down through the operator tree.

        Args:
            op: The operator to transform. Anything but an expression based
                ``Filter`` is returned unchanged.
            derive_implied: Whether a conjunct that cannot be pushed through a
                join as is should also push the weaker one-sided predicates it
                implies. Only :class:`DeriveJoinSidePredicates` sets this; see
                there for why it is not done in the regular optimizer loop.

        Returns:
            The transformed operator, or ``op`` itself if nothing was pushed.
        """
        if not cls._is_valid_filter_operator(op):
            return op
        filter_op: Filter = op
        input_op = filter_op.input_dependencies[0]
        predicate_expr = filter_op.predicate_expr

        # Case 1: Check if operator supports predicate pushdown (e.g., Read).
        # The read stage never renames columns (renaming is always carried
        # by an ``AliasExpr`` in a ``Project`` operator above the read), so
        # the predicate above the read is already in the same column
        # namespace the scanner sees — no rebinding is required here.
        if (
            isinstance(input_op, LogicalOperatorSupportsPredicatePushdown)
            and input_op.supports_predicate_pushdown()
        ):
            # Datasources evaluate pushed predicates via PyArrow. A predicate
            # that can't be lowered to PyArrow (e.g. it contains a UDF) must
            # stay as a Filter. Split the top level AND chain so the convertible
            # conjuncts can still be pushed while the residual ones are kept as
            # a Filter above the read.
            split = cls._split_by_convertibility(predicate_expr)

            if split.convertible is None:
                return filter_op

            result_op = input_op.apply_predicate(split.convertible)

            # If the operator is unchanged (e.g., predicate references partition columns
            # that can't be pushed down), keep the Filter operator
            if result_op is input_op:
                return filter_op

            # Convertible conjuncts were pushed into the read. Re-apply any
            # residual (non-convertible) conjuncts as a Filter above it.
            if split.residual is None:
                return result_op
            return Filter(predicate_expr=split.residual, input_dependencies=[result_op])

        # Datasource pushdown (Case 1) only lowers PyArrow-convertible (hence
        # idempotent) conjuncts. Beyond that, do not relocate a filter whose predicate
        # is non-idempotent (random/uuid/monotonically_increasing_id): pushing it
        # through a pass-through operator, into Union branches, or to a Join side
        # changes the row set / position the expression is evaluated over.
        if not predicate_expr.is_idempotent():
            return filter_op

        # Case 2: Check if operator allows predicates to pass through
        if isinstance(input_op, LogicalOperatorSupportsPredicatePassThrough):
            behavior = input_op.predicate_passthrough_behavior()

            if behavior in (
                PredicatePassThroughBehavior.PASSTHROUGH,
                PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION,
            ):
                # Both cases push through a single input with optional column rebinding
                assert len(input_op.input_dependencies) == 1, (
                    f"{behavior.value} operators must have exactly 1 input, "
                    f"got {len(input_op.input_dependencies)}"
                )

                # Apply column substitution if needed
                if (
                    behavior
                    == PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION
                ):
                    # Check if we can safely push the filter through this projection
                    if isinstance(
                        input_op, Project
                    ) and not cls._can_push_filter_through_projection(
                        filter_op, input_op
                    ):
                        return filter_op

                    rename_map = input_op.get_column_substitutions()
                    if rename_map:
                        predicate_expr = cls._substitute_predicate_columns(
                            predicate_expr, rename_map
                        )

                # Push filter through and recursively try to push further
                new_filter = Filter(
                    predicate_expr=predicate_expr,
                    input_dependencies=[input_op.input_dependencies[0]],
                )
                pushed_filter = cls._try_push_down_predicate(
                    new_filter, derive_implied=derive_implied
                )

                # Return input_op with the pushed filter as its input
                return cls._clone_op_with_new_inputs(input_op, [pushed_filter])

            elif behavior == PredicatePassThroughBehavior.PUSH_INTO_BRANCHES:
                # Push into each branch (e.g., Union)
                # Apply filter to each branch and recursively push down
                new_inputs = []
                for branch_op in input_op.input_dependencies:
                    branch_filter = Filter(
                        predicate_expr=predicate_expr, input_dependencies=[branch_op]
                    )
                    pushed_branch = cls._try_push_down_predicate(
                        branch_filter, derive_implied=derive_implied
                    )
                    new_inputs.append(pushed_branch)

                # Return operator with filtered branches
                return cls._clone_op_with_new_inputs(input_op, new_inputs)

            elif behavior == PredicatePassThroughBehavior.CONDITIONAL:
                # Handle conditional pushdown (e.g., Join)
                return cls._push_filter_through_conditionally(
                    filter_op, input_op, derive_implied=derive_implied
                )

        return filter_op

    @classmethod
    def _push_filter_through_conditionally(
        cls,
        filter_op: Filter,
        conditional_op: LogicalOperator,
        derive_implied: bool = False,
    ) -> LogicalOperator:
        """Handle conditional pushdown for operators like Join.

        The predicate's top level ``AND`` chain is split and each conjunct is
        routed on its own: one that references a single side of the join (and
        is safe to move for the join type) is pushed to that side (to both, for
        a same-named key of an inner join), the others are recombined into a
        residual ``Filter`` kept above the operator. So
        ``filter(left_pred & right_pred)`` pushes both halves and leaves no
        residual, and ``filter(left_pred & cross_pred)`` pushes ``left_pred``
        and keeps ``cross_pred`` above the join.

        With ``derive_implied``, a conjunct that cannot be pushed as is (for
        example an ``OR`` whose branches each reference both sides) also pushes
        the weaker one-sided predicates it implies, while the conjunct itself
        still stays in the residual. See :meth:`_derive_one_sided_predicate`.
        """
        # Check if operator supports conditional pushdown by having the required method
        if not hasattr(conditional_op, "can_push_predicate_to_side"):
            return filter_op

        inputs = conditional_op.input_dependencies
        pushed: List[List[Expr]] = [[] for _ in inputs]
        residual: List[Expr] = []
        for conjunct in cls._flatten(filter_op.predicate_expr, Operation.AND):
            # A conjunct over a same-named join key can go to both sides of an
            # inner join; every accepting side gets it. The enum value is the
            # branch index.
            push_sides = cls._sides_accepting(conditional_op, conjunct)
            if push_sides:
                for side in push_sides:
                    cls._append_unique(pushed[side.value], conjunct)
                continue

            residual.append(conjunct)
            if not derive_implied:
                continue
            for side in JoinSide:
                derived = cls._derive_one_sided_predicate(
                    conjunct, conditional_op, side
                )
                # The conjunct stays in the residual, so on a later pass over
                # this plan (execution rewires the dataset's DAG to the
                # optimized one) it would derive the same predicate again.
                # Skip what the side already enforces.
                if derived is not None and not cls._is_enforced_below(
                    inputs[side.value], derived
                ):
                    cls._append_unique(pushed[side.value], derived)

        if not any(pushed):
            # Nothing can move; keep the Filter where it is.
            return filter_op

        new_inputs = list(inputs)
        for branch_idx, conjuncts in enumerate(pushed):
            if not conjuncts:
                continue
            branch_filter = Filter(
                predicate_expr=cls._combine(conjuncts, Operation.AND),
                input_dependencies=[new_inputs[branch_idx]],
            )
            # The side may already start with a Filter (an earlier pushed
            # conjunct that could go no further). Fuse into it now rather than
            # stacking a second Filter the next optimization would fuse,
            # so an optimized plan re-optimizes to itself.
            branch_filter = cls._try_fuse_filters(branch_filter)
            new_inputs[branch_idx] = cls._try_push_down_predicate(
                branch_filter, derive_implied=derive_implied
            )

        new_op = cls._clone_op_with_new_inputs(conditional_op, new_inputs)
        residual_predicate = cls._combine(residual, Operation.AND)
        if residual_predicate is None:
            return new_op
        return Filter(predicate_expr=residual_predicate, input_dependencies=[new_op])

    @classmethod
    def _sides_accepting(
        cls, conditional_op: LogicalOperator, predicate: Expr
    ) -> List[JoinSide]:
        """The join sides ``predicate`` can be pushed to as is."""
        return [
            side
            for side in JoinSide
            if conditional_op.can_push_predicate_to_side(predicate, side)
        ]

    @classmethod
    def _is_enforced_below(cls, op: LogicalOperator, predicate: Expr) -> bool:
        """Whether every conjunct of ``predicate`` already filters ``op``'s output.

        Follows the same paths :meth:`_try_push_down_predicate` would take, so
        a predicate it pushed earlier is found wherever it ended up: a
        ``Filter`` (possibly fused with others), below pass-through operators
        (rebinding columns through a ``Project`` like the push did), on every
        ``Union`` branch, on the join side it routes to, or in the predicate a
        datasource reports via ``get_current_predicate``. Conservative: when
        unsure it answers ``False`` and the predicate is pushed again, which
        is redundant but correct.
        """
        return all(
            cls._is_conjunct_enforced_below(op, conjunct)
            for conjunct in cls._flatten(predicate, Operation.AND)
        )

    @classmethod
    def _is_conjunct_enforced_below(cls, op: LogicalOperator, conjunct: Expr) -> bool:
        while True:
            if cls._is_valid_filter_operator(op):
                if any(
                    conjunct.structurally_equals(existing)
                    for existing in cls._flatten(op.predicate_expr, Operation.AND)
                ):
                    return True
                op = op.input_dependencies[0]
                continue

            if (
                isinstance(op, LogicalOperatorSupportsPredicatePushdown)
                and op.supports_predicate_pushdown()
            ):
                current = op.get_current_predicate()
                return current is not None and any(
                    conjunct.structurally_equals(existing)
                    for existing in cls._flatten(current, Operation.AND)
                )

            if not isinstance(op, LogicalOperatorSupportsPredicatePassThrough):
                return False
            behavior = op.predicate_passthrough_behavior()
            if behavior == PredicatePassThroughBehavior.PASSTHROUGH:
                op = op.input_dependencies[0]
            elif behavior == PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION:
                # A conjunct that could not pass this operator would have been
                # left as a Filter above it and found already, so just rebind
                # the way the push did and keep looking below.
                rename_map = op.get_column_substitutions()
                if rename_map:
                    conjunct = cls._substitute_predicate_columns(conjunct, rename_map)
                op = op.input_dependencies[0]
            elif behavior == PredicatePassThroughBehavior.PUSH_INTO_BRANCHES:
                return all(
                    cls._is_conjunct_enforced_below(branch, conjunct)
                    for branch in op.input_dependencies
                )
            elif behavior == PredicatePassThroughBehavior.CONDITIONAL:
                if not hasattr(op, "can_push_predicate_to_side"):
                    return False
                # On either accepting input the conjunct holds for the join's
                # output too (an inner join on a same-named key is the only
                # case with two).
                return any(
                    cls._is_conjunct_enforced_below(
                        op.input_dependencies[s.value], conjunct
                    )
                    for s in cls._sides_accepting(op, conjunct)
                )
            else:
                return False

    @classmethod
    def _derive_one_sided_predicate(
        cls, predicate: Expr, conditional_op: LogicalOperator, side: JoinSide
    ) -> Optional[Expr]:
        """Derive a predicate implied by ``predicate`` that can be pushed to ``side``.

        The result is weaker than ``predicate`` (every row that satisfies
        ``predicate`` satisfies it), so it can filter ``side`` ahead of the join
        while ``predicate`` itself stays in place to reject the remaining rows:

        - A predicate that is already pushable to ``side`` is returned as is.
        - ``a & b`` implies whatever each conjunct implies, so the conjuncts
          that derive something are kept and the others dropped.
        - ``a | b`` implies ``a' | b'`` where ``a'`` and ``b'`` are derived from
          the branches. A branch that implies nothing about ``side`` makes the
          whole disjunction underivable.

        For example TPC-H q7's
        ``(n1 == 'FRANCE' & n2 == 'GERMANY') | (n1 == 'GERMANY' & n2 == 'FRANCE')``
        yields ``n1 == 'FRANCE' | n1 == 'GERMANY'`` for the ``n1`` side and
        ``n2 == 'GERMANY' | n2 == 'FRANCE'`` for the ``n2`` side, pruning both
        scans to two rows before the shuffle.

        Args:
            predicate: The predicate to derive from.
            conditional_op: The join whose ``can_push_predicate_to_side``
                decides what is pushable.
            side: The join side to derive a predicate for.

        Returns:
            The derived predicate, or ``None`` if nothing can be derived for
            ``side`` (including when the join type does not allow pushing to it).
        """
        if conditional_op.can_push_predicate_to_side(predicate, side):
            return predicate
        if not isinstance(predicate, BinaryExpr):
            return None

        if predicate.op == Operation.AND:
            derived_conjuncts = [
                cls._derive_one_sided_predicate(conjunct, conditional_op, side)
                for conjunct in cls._flatten(predicate, Operation.AND)
            ]
            return cls._combine(
                [d for d in derived_conjuncts if d is not None], Operation.AND
            )

        if predicate.op == Operation.OR:
            derived_branches: List[Expr] = []
            for branch in cls._flatten(predicate, Operation.OR):
                derived_branch = cls._derive_one_sided_predicate(
                    branch, conditional_op, side
                )
                if derived_branch is None:
                    return None
                cls._append_unique(derived_branches, derived_branch)
            return cls._combine(derived_branches, Operation.OR)

        return None

    @classmethod
    def _clone_op_with_new_inputs(
        cls, op: LogicalOperator, new_inputs: List[LogicalOperator]
    ) -> LogicalOperator:
        """Clone an operator with new inputs.

        Args:
            op: The operator to clone
            new_inputs: List of new input operators (can be single element list)

        Returns:
            A shallow copy of the operator with updated input dependencies
        """
        if isinstance(op, Limit):
            assert len(new_inputs) == 1, len(new_inputs)
            return Limit(op.limit, input_dependencies=[new_inputs[0]])
        if isinstance(op, AbstractMap) and is_dataclass(op):
            assert len(new_inputs) == 1, len(new_inputs)
            return replace(op, input_dependencies=[new_inputs[0]])
        if isinstance(op, AbstractAllToAll) and is_dataclass(op):
            assert len(new_inputs) == 1, len(new_inputs)
            kwargs = {"input_dependencies": [new_inputs[0]]}
            if isinstance(op, Repartition):
                kwargs["num_outputs"] = op.num_outputs
            if isinstance(op, RandomShuffle):
                kwargs["name"] = op.name
            return replace(op, **kwargs)
        if isinstance(op, Join) and is_dataclass(op):
            assert len(new_inputs) == 2, len(new_inputs)
            return Join(
                new_inputs[0],
                new_inputs[1],
                op.join_type,
                op.left_key_columns,
                op.right_key_columns,
                num_partitions=op.num_outputs,
                left_columns_suffix=op.left_columns_suffix,
                right_columns_suffix=op.right_columns_suffix,
                partition_size_hint=op.partition_size_hint,
                aggregator_ray_remote_args=op.aggregator_ray_remote_args,
            )
        if isinstance(op, Union) and is_dataclass(op):
            return Union(new_inputs)
        new_op = copy.copy(op)
        new_op.input_dependencies = new_inputs
        return new_op


class DeriveJoinSidePredicates(Rule):
    """Push the one-sided predicates implied by a residual filter above a join.

    :class:`PredicatePushdown` only relocates conjuncts that reference a single
    join side. A conjunct such as TPC-H q7's
    ``(n1 == 'FRANCE' & n2 == 'GERMANY') | (n1 == 'GERMANY' & n2 == 'FRANCE')``
    references both sides and stays above the join, although every row it can
    accept has ``n1 in ('FRANCE', 'GERMANY')`` and ``n2 in ('FRANCE',
    'GERMANY')``, each of which could prune one side (down to its scan) before
    the shuffle. This rule derives those implied predicates and pushes them
    with the regular pushdown machinery; the original conjunct stays where it
    is, since the derived ones are weaker.

    Because the residual filter is left in place, this is a derivation rather
    than a rewrite. It runs once per optimization, over the already pushed-down
    plan, from ``LogicalOptimizer._post_optimize`` rather than in the rule loop,
    and before pushing a derived predicate it checks that the join side does
    not already enforce it (:meth:`PredicatePushdown._is_enforced_below`). The
    check is what makes re-optimizing an optimized plan a no-op, which matters
    because executing a dataset rewires its logical DAG to the optimized one.
    Staying out of the loop additionally bounds the cost of a datasource that
    accepts a predicate but does not report it back through
    ``get_current_predicate``: one redundant push per optimization instead of
    a loop that never converges.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        dag = plan.dag
        new_dag = dag._apply_transform(self._derive_and_push)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @staticmethod
    def _derive_and_push(op: LogicalOperator) -> LogicalOperator:
        return PredicatePushdown._try_push_down_predicate(op, derive_implied=True)
