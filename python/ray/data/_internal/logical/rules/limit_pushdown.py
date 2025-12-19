import copy
import logging
from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data._internal.logical.operators.n_ary_operator import Union
from ray.data._internal.logical.operators.one_to_one_operator import (
    AbstractOneToOne,
    Limit,
)

logger = logging.getLogger(__name__)


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. We are conservative and only
    push through operators that we know for certain do not modify row counts:
    - Project operations (column selection)
    - MapRows operations (row-wise transformations that preserve row count)
    - Union operations (limits are prepended to each branch)

    We stop at:
    - Any operator that can modify the number of output rows (Sort, Shuffle, Aggregate, Read etc.)

    For per-block limiting, we also set per-block limits on Read operators to optimize
    I/O while keeping the Limit operator for exact row count control.

    In addition, we also fuse consecutive Limit operators into a single
    Limit operator, i.e. `Limit[n] -> Limit[m]` becomes `Limit[min(n, m)]`.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        # The DAG's root is the most downstream operator.
        def transform(node: LogicalOperator) -> LogicalOperator:
            if isinstance(node, Limit):
                # First, try to fuse with upstream Limit if possible (reuse fusion logic)
                upstream_op = node.input_dependency
                if isinstance(upstream_op, Limit):
                    # Fuse consecutive Limits: Limit[n] -> Limit[m] becomes Limit[min(n,m)]
                    new_limit = min(node._limit, upstream_op._limit)
                    return Limit(upstream_op.input_dependency, new_limit)

                # If no fusion, apply pushdown logic
                if isinstance(upstream_op, Union):
                    return self._push_limit_into_union(node)
                else:
                    return self._push_limit_down(node)

            return node

        optimized_dag = plan.dag._apply_transform(transform)
        return LogicalPlan(dag=optimized_dag, context=plan.context)

    def _apply_limit_pushdown(self, op: LogicalOperator) -> LogicalOperator:
        """Push down Limit operators in the given operator DAG.

        This implementation uses ``LogicalOperator._apply_transform`` to
        post-order-traverse the DAG and rewrite each ``Limit`` node via
        :py:meth:`_push_limit_down`.
        """

        def transform(node: LogicalOperator) -> LogicalOperator:
            if isinstance(node, Limit):
                if isinstance(node.input_dependency, Union):
                    return self._push_limit_into_union(node)
                return self._push_limit_down(node)
            return node

        # ``_apply_transform`` returns the (potentially new) root of the DAG.
        return op._apply_transform(transform)

    def _push_limit_into_union(self, limit_op: Limit) -> Limit:
        """Push `limit_op` INTO every branch of its upstream Union
        and preserve the global limit.

        Existing topology:
            child₁ , child₂ , …  ->  Union  ->  Limit

        New topology:
            child₁ -> Limit ->│
                               │
            child₂ -> Limit ->┤ Union ──► Limit   (original)
                               │
            …    -> Limit  ->│
        """
        union_op = limit_op.input_dependency
        assert isinstance(union_op, Union)

        # 1. Detach the original Union from its children.
        original_children = list(union_op.input_dependencies)
        for child in original_children:
            if union_op in child._output_dependencies:
                child._output_dependencies.remove(union_op)

        # 2. Insert a branch-local Limit and push it further upstream.
        branch_tails: List[LogicalOperator] = []
        for child in original_children:
            raw_limit = Limit(child, limit_op._limit)  # child → limit
            if isinstance(child, Union):
                # This represents the limit operator appended after the union.
                pushed_tail = self._push_limit_into_union(raw_limit)
            else:
                # This represents the operator that takes place of the original limit position.
                pushed_tail = self._push_limit_down(raw_limit)
            branch_tails.append(pushed_tail)

        # 3. Re-attach the Union so that it consumes the *tails*.
        new_union = Union(*branch_tails)
        for tail in branch_tails:
            tail._output_dependencies.append(new_union)

        # 4. Re-wire the original (global) Limit to consume the *new* Union.
        limit_op._input_dependencies = [new_union]
        new_union._output_dependencies = [limit_op]

        return limit_op

    def _push_limit_down(self, limit_op: Limit) -> LogicalOperator:
        """Push a single limit down through compatible operators conservatively.

        Creates entirely new operators instead of mutating existing ones.
        """
        # Traverse up the DAG until we reach the first operator that meets
        # one of the stopping conditions
        current_op = limit_op.input_dependency
        num_rows_preserving_ops: List[LogicalOperator] = []
        while (
            isinstance(current_op, AbstractOneToOne)
            and not current_op.can_modify_num_rows()
        ):
            if isinstance(current_op, AbstractMap):
                min_rows = current_op._min_rows_per_bundled_input
                if min_rows is not None and min_rows > limit_op._limit:
                    # Avoid pushing the limit past batch-based maps that require more
                    # rows than the limit to produce stable outputs (e.g. schema).
                    logger.info(
                        f"Skipping push down of limit {limit_op._limit} through map {current_op} because it requires {min_rows} rows to produce stable outputs"
                    )
                    break
            num_rows_preserving_ops.append(current_op)
            current_op = current_op.input_dependency

        # If we couldn't push through any operators, return original
        if not num_rows_preserving_ops:
            return limit_op
        # Apply per-block limit to the deepest operator if it supports it
        limit_input = self._apply_per_block_limit_if_supported(
            current_op, limit_op._limit
        )

        # Build the new operator chain: Chain non-preserving number of rows -> Limit -> Operators preserving number of rows
        new_limit = Limit(limit_input, limit_op._limit)
        result_op = new_limit

        # Recreate the intermediate operators and apply per-block limits
        for op_to_recreate in reversed(num_rows_preserving_ops):
            recreated_op = self._recreate_operator_with_new_input(
                op_to_recreate, result_op
            )
            result_op = recreated_op

        return result_op

    def _apply_per_block_limit_if_supported(
        self, op: LogicalOperator, limit: int
    ) -> LogicalOperator:
        """Apply per-block limit to operators that support it."""
        if isinstance(op, AbstractMap):
            new_op = copy.copy(op)
            new_op.set_per_block_limit(limit)
            return new_op
        return op

    def _recreate_operator_with_new_input(
        self, original_op: LogicalOperator, new_input: LogicalOperator
    ) -> LogicalOperator:
        """Create a new operator of the same type as original_op but with new_input as its input."""

        if isinstance(original_op, Limit):
            return Limit(new_input, original_op._limit)

        # Use copy and replace input dependencies approach
        new_op = copy.copy(original_op)
        new_op._input_dependencies = [new_input]
        new_op._output_dependencies = []

        return new_op
