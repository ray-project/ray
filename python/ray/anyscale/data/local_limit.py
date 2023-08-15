from typing import Iterator

from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces.optimizer import Rule
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
from ray.data._internal.logical.operators.map_operator import (
    AbstractMap,
    AbstractUDFMap,
)
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data.block import Block, BlockAccessor


class ApplyLocalLimitRule(Rule):
    """Rule for applying local limit for Map->Limit operator pairs.

    When Map[transform_fn]->Limit[N] is present, we modify the Map operator's
    transform_fn to return only N rows. The two operators remain separate and
    are not fused into a single operator, but are instead replaced by new copies
    of the original operators with the modified transform_fn.
    """

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        new_dag = self._apply_local_limit_on_map_in_dag(plan.dag)
        return PhysicalPlan(new_dag, self._op_map)

    def is_map_and_limit_ops(
        self,
        down_op: PhysicalOperator,
        up_op: PhysicalOperator,
    ) -> bool:
        """Returns True if the operators are MapOperator->LimitOperator,
        and therefore can apply local limit on the MapOperator; False otherwise.
        """
        if not (isinstance(down_op, LimitOperator) and isinstance(up_op, MapOperator)):
            return False

        down_logical_op = self._op_map[down_op]
        up_logical_op = self._op_map[up_op]
        if not (
            isinstance(up_logical_op, AbstractMap)
            and isinstance(down_logical_op, Limit)
        ):
            return False
        return True

    def _apply_local_limit_on_map_in_dag(self, dag: LimitOperator) -> LimitOperator:
        """Starting at the given operator, traverses up the DAG of operators
        and recursively modifies compatible MapOperator -> LimitOperator pairs.
        Returns the current (root) operator after processing upstream operator pairs.
        """
        upstream_ops = dag.input_dependencies
        if len(upstream_ops) == 1 and self.is_map_and_limit_ops(dag, upstream_ops[0]):
            dag = self._apply_local_limit_on_map(dag, upstream_ops[0])
            upstream_ops = dag.input_dependencies

        # Done applying local limit here, move up the DAG to find the next pair of ops.
        dag._input_dependencies = [
            self._apply_local_limit_on_map_in_dag(upstream_op)
            for upstream_op in upstream_ops
        ]
        return dag

    def _apply_local_limit_on_map(
        self,
        down_op: LimitOperator,
        up_op: MapOperator,
    ) -> LimitOperator:
        assert self.is_map_and_limit_ops(down_op, up_op), (
            "Current rule only applies to MapOperator->LimitOperator, but received: "
            f"{type(up_op).__name__} -> {type(down_op).__name__}"
        )

        def _limit_n_rows(blocks: Iterator[Block], n: int):
            """Yields n rows from the input blocks."""
            if n <= 0:
                return
            consumed_rows = 0
            for b in blocks:
                # For n > 0, we yield at least one complete block.
                # The subsequent Limit operator takes care of slicing for
                # the case where n < block_size.
                yield b

                ba = BlockAccessor.for_block(b)
                num_rows = ba.num_rows()
                assert num_rows is not None, "Block has unknown number of rows."
                consumed_rows += num_rows

                # Once we have consumed n rows, stop yielding more blocks.
                if consumed_rows >= n:
                    break

        n_limit = down_op._limit
        map_transform_fn = up_op.get_transformation_fn()

        # Wrap the map fn with _limit_n_rows, and use the result
        # as the new map fn for the replacement MapOperator.
        def map_transform_fn_limit_n(
            blocks: Iterator[Block], ctx: TaskContext
        ) -> Iterator[Block]:
            blocks = map_transform_fn(blocks, ctx)
            return _limit_n_rows(blocks, n_limit)

        # Create the new logical and physical MapOperators.
        up_logical_op = self._op_map.pop(up_op)
        down_logical_op = self._op_map.pop(down_op)
        new_map_op_name = f"{up_op._name[:-1]}->Limit[{n_limit}])"
        init_fn = (
            up_op.get_init_fn() if isinstance(up_op, ActorPoolMapOperator) else None
        )
        compute = None
        target_block_size = None
        ray_remote_args = None

        if isinstance(up_logical_op, AbstractUDFMap):
            compute = get_compute(up_logical_op._compute)
            target_block_size = up_logical_op._target_block_size
            ray_remote_args = up_logical_op._ray_remote_args

        up_op = MapOperator.create(
            map_transform_fn_limit_n,
            up_op.input_dependency,
            init_fn=init_fn,
            name=new_map_op_name,
            compute_strategy=compute,
            min_rows_per_bundle=target_block_size,
            ray_remote_args=ray_remote_args,
        )

        if isinstance(up_logical_op, AbstractUDFMap):
            up_logical_op = AbstractUDFMap(
                up_logical_op.name,
                up_logical_op.input_dependency,
                up_logical_op._fn,
                up_logical_op._fn_args,
                up_logical_op._fn_kwargs,
                up_logical_op._fn_constructor_args,
                up_logical_op._fn_constructor_kwargs,
                target_block_size,
                compute,
                ray_remote_args,
            )
            up_logical_op._name = new_map_op_name
        else:
            from ray.data._internal.logical.operators.map_operator import AbstractMap

            # The downstream op is AbstractMap instead of AbstractUDFMap.
            up_logical_op = AbstractMap(
                new_map_op_name,
                up_logical_op,
                ray_remote_args,
            )

        # Create the new logical and physical LimitOperators.
        down_op = LimitOperator(
            n_limit,
            up_op,
        )
        down_logical_op = Limit(
            up_logical_op,
            n_limit,
        )

        # Add both new operators to the op map.
        self._op_map[up_op] = up_logical_op
        self._op_map[down_op] = down_logical_op

        # Return the new Limit operator.
        return down_op
