import copy
from dataclasses import dataclass, is_dataclass, replace
from typing import Dict, List, Optional, Tuple

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    append_unique_predicate,
    combine_predicate_chain,
    flatten_predicate_chain,
)
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
    get_column_references,
)
from ray.data.expressions import BinaryExpr, Expr, Operation, col

__all__ = [
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
      references if that is safe for the join type, and keeps the rest above;
      a kept conjunct still pushes the weaker one-sided predicates it implies,
      see ``_derive_one_sided_predicate``)
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
            processed_join_conjuncts=op.processed_join_conjuncts
            + input_op.processed_join_conjuncts,
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
    def _split_by_convertibility(cls, predicate: Expr) -> _ConvertibilitySplit:
        """Split the top level ``AND`` chain into PyArrow-convertible conjuncts and the rest."""
        convertible: List[Expr] = []
        residual: List[Expr] = []
        for conjunct in flatten_predicate_chain(predicate, Operation.AND):
            if conjunct._is_pyarrow_convertible():
                convertible.append(conjunct)
            else:
                residual.append(conjunct)
        return _ConvertibilitySplit(
            convertible=combine_predicate_chain(convertible, Operation.AND),
            residual=combine_predicate_chain(residual, Operation.AND),
        )

    @classmethod
    def _try_push_down_predicate(cls, op: LogicalOperator) -> LogicalOperator:
        """Push Filter down through the operator tree."""
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
                pushed_filter = cls._try_push_down_predicate(new_filter)

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
                    pushed_branch = cls._try_push_down_predicate(branch_filter)
                    new_inputs.append(pushed_branch)

                # Return operator with filtered branches
                return cls._clone_op_with_new_inputs(input_op, new_inputs)

            elif behavior == PredicatePassThroughBehavior.CONDITIONAL:
                # Handle conditional pushdown (e.g., Join)
                return cls._push_filter_through_conditionally(filter_op, input_op)

        return filter_op

    @classmethod
    def _push_filter_through_conditionally(
        cls,
        filter_op: Filter,
        conditional_op: LogicalOperator,
    ) -> LogicalOperator:
        """Push each ``AND`` conjunct of ``filter_op`` to the join side that accepts it
        (rebound to that input's column names); the rest stays as a residual Filter.
        """
        if not isinstance(conditional_op, Join):
            return filter_op
        # Step 1: Build the mapping from output to input column names.
        output_to_input_by_side = conditional_op.output_to_input_columns()
        if output_to_input_by_side is None:
            return filter_op

        inputs = conditional_op.input_dependencies
        pushed: List[List[Expr]] = [[] for _ in inputs]
        residual: List[Expr] = []

        def push(side: JoinSide, predicate: Expr) -> None:
            append_unique_predicate(
                pushed[side.value],
                cls._rename_to_input_columns(
                    predicate, output_to_input_by_side[side.value]
                ),
            )

        # Step 2: Sort the AND conjuncts. A side that can evaluate one as is
        # takes it and the rest stays above the join, but each side still gets the
        # weaker predicate such a conjunct implies about that side alone.
        for conjunct in flatten_predicate_chain(
            filter_op.predicate_expr, Operation.AND
        ):
            movable = any(
                cls._can_push_to_side(
                    conditional_op, output_to_input_by_side, side, conjunct
                )
                for side in JoinSide
            )
            if not movable:
                residual.append(conjunct)
                if any(
                    conjunct.structurally_equals(done)
                    for done in filter_op.processed_join_conjuncts
                ):
                    continue  # Its implications were pushed by an earlier pass.
            for side in JoinSide:
                # The conjunct itself if this side accepts it, else the weaker
                # predicate it implies about that side alone, else nothing.
                weaker_one_side_conjunct = cls._derive_one_sided_predicate(
                    conjunct, conditional_op, output_to_input_by_side, side
                )
                if weaker_one_side_conjunct is not None:
                    push(side, weaker_one_side_conjunct)

        if not any(pushed):
            return filter_op

        # Step 3: Push each side's conjuncts as one Filter into that input.
        new_inputs = list(inputs)
        for branch_idx, conjuncts in enumerate(pushed):
            if not conjuncts:
                continue
            # `replace` copies every field of `filter_op`; reset the two that
            # only make sense above the join(`per_block_limit` and `processed_join_conjuncts`).
            branch_filter = replace(
                filter_op,
                predicate_expr=combine_predicate_chain(conjuncts, Operation.AND),
                input_dependencies=[new_inputs[branch_idx]],
                per_block_limit=None,
                processed_join_conjuncts=(),
            )
            branch_filter = cls._try_fuse_filters(branch_filter)
            new_inputs[branch_idx] = cls._try_push_down_predicate(branch_filter)

        # Step 4: Rebuild the join; the residual stays above it, marked as
        # processed so no later pass derives from it again.
        new_op = cls._clone_op_with_new_inputs(conditional_op, new_inputs)
        residual_predicate = combine_predicate_chain(residual, Operation.AND)
        if residual_predicate is None:
            return new_op
        return replace(
            filter_op,
            predicate_expr=residual_predicate,
            input_dependencies=[new_op],
            processed_join_conjuncts=tuple(residual),
        )

    @classmethod
    def _can_push_to_side(
        cls,
        join: Join,
        output_to_input_by_side: Tuple[Dict[str, str], Dict[str, str]],
        side: JoinSide,
        predicate: Expr,
    ) -> bool:
        """Whether predicate (over the join's output) can be evaluated on
        side's input: the join type allows filtering that side early and the
        input produces every column the predicate references.
        """
        if not join.allows_predicate_pushdown_to(side):
            return False
        side_columns = output_to_input_by_side[side.value]
        return all(c in side_columns for c in get_column_references(predicate))

    @classmethod
    def _rename_to_input_columns(
        cls, predicate: Expr, output_to_input: Dict[str, str]
    ) -> Expr:
        """Rewrite predicate from the join's output column names to the input's."""
        substitutions = {
            output_name: col(input_name)
            for output_name, input_name in output_to_input.items()
            if input_name != output_name
        }
        if not substitutions:
            return predicate
        return _ColumnSubstitutionVisitor(substitutions).visit(predicate)

    @classmethod
    def _derive_one_sided_predicate(
        cls,
        predicate: Expr,
        join: Join,
        output_to_input_by_side: Tuple[Dict[str, str], Dict[str, str]],
        side: JoinSide,
    ) -> Optional[Expr]:
        """Derives the weaker predicate for each side.

        A predicate side can evaluate is returned as is. Otherwise there are
        2 cases here:
        1. If the predicate op is AND: keep the conjuncts that derive something
           and AND them.

        2. If the predicate op is OR: derive each branch for this side and OR
        them; if any branch derives nothing, return None.

        Anything else (NOT, a comparison across both sides) yields None.
        """
        if cls._can_push_to_side(join, output_to_input_by_side, side, predicate):
            return predicate
        if not isinstance(predicate, BinaryExpr):
            return None

        if predicate.op == Operation.AND:
            derived_conjuncts = [
                cls._derive_one_sided_predicate(
                    conjunct, join, output_to_input_by_side, side
                )
                for conjunct in flatten_predicate_chain(predicate, Operation.AND)
            ]
            return combine_predicate_chain(
                [d for d in derived_conjuncts if d is not None], Operation.AND
            )

        if predicate.op == Operation.OR:
            derived_branches: List[Expr] = []
            for branch in flatten_predicate_chain(predicate, Operation.OR):
                derived_branch = cls._derive_one_sided_predicate(
                    branch, join, output_to_input_by_side, side
                )
                if derived_branch is None:
                    return None
                append_unique_predicate(derived_branches, derived_branch)
            return combine_predicate_chain(derived_branches, Operation.OR)

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
