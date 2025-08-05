import copy
from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.n_ary_operator import Union
from ray.data._internal.logical.operators.one_to_one_operator import (
    AbstractOneToOne,
    Limit,
)
from ray.data._internal.logical.operators.read_operator import Read


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

        Similar to the original algorithm but more conservative in what we push through.
        Now also supports setting per-block limits on Read operators for I/O optimization.
        """
        limit_op_copy = copy.copy(limit_op)

        # Traverse up the DAG until we reach the first operator that meets
        # one of the stopping conditions
        new_input_into_limit = limit_op.input_dependency
        ops_between_new_input_and_limit: List[LogicalOperator] = []

        while (
            isinstance(new_input_into_limit, AbstractOneToOne)
            and not new_input_into_limit.can_modify_num_rows()
            and not isinstance(new_input_into_limit, MapBatches)
            # We should push past MapBatches, but MapBatches can modify the row count TODO: add a flag in map_batches that allows the user to opt in ensure row preservation
        ):
            new_input_into_limit_copy = copy.copy(new_input_into_limit)
            ops_between_new_input_and_limit.append(new_input_into_limit_copy)
            new_input_into_limit = new_input_into_limit.input_dependency

        # Special case: if we reached a Read operator, enable per-block limiting
        # but still keep the limit operator for exact control
        if isinstance(new_input_into_limit, Read):
            # Apply per-block limiting to the Read operator as an optimization
            read_op_with_limit = copy.copy(new_input_into_limit)
            read_op_with_limit.set_per_block_limit(limit_op._limit)

            # Always keep the limit operator, just update its input to the read op with per-block limit
            # This maintains backward compatibility while adding the I/O optimization
            limit_op_copy._input_dependencies = [read_op_with_limit]
            read_op_with_limit._output_dependencies = [limit_op_copy]

            # Wire through any operations between read and limit
            current_op = limit_op_copy
            for next_op in reversed(ops_between_new_input_and_limit):
                current_op._output_dependencies = [next_op]
                next_op._input_dependencies = [current_op]
                current_op = next_op

            # Link up to downstream operations
            last_op = current_op
            for downstream_op in limit_op.output_dependencies:
                downstream_op._input_dependencies = [last_op]
            last_op._output_dependencies = limit_op.output_dependencies
            return last_op

        # If we couldn't push through any operators, return original
        if not ops_between_new_input_and_limit:
            return limit_op

        # Link the Limit operator and its newly designated input op from above.
        limit_op_copy._input_dependencies = [new_input_into_limit]
        new_input_into_limit._output_dependencies = [limit_op_copy]

        # Wire limit_op_copy to the first operator that should come after it
        # (which is the last one we added to the list). Going from up upstream to downstream.
        current_op = limit_op_copy
        for next_op in reversed(ops_between_new_input_and_limit):
            current_op._output_dependencies = [next_op]
            next_op._input_dependencies = [current_op]
            current_op = next_op

        # Link up all operations from last downstream op to post old limit location (further downstream)
        last_op = current_op
        for downstream_op in limit_op.output_dependencies:
            downstream_op._input_dependencies = [last_op]
        last_op._output_dependencies = limit_op.output_dependencies
        return last_op
