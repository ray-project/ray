import collections
from typing import Iterable

from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    MapTransformFn,
    MapTransformFnData,
    MapTransformFnDataType,
)
from ray.data._internal.logical.interfaces.optimizer import Rule
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
from ray.data._internal.logical.operators.map_operator import (
    AbstractMap,
    AbstractUDFMap,
)
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data.block import BlockAccessor


class LimitNRowsMapTransformFn(MapTransformFn):
    """A MapTransformFn that limits the number of rows in the output."""

    def __init__(self, n: int, data_type: MapTransformFnDataType):
        self._n = n
        self._data_type = data_type

    def _num_rows(self, data: MapTransformFnData) -> int:
        """Gets the number of rows in the input data."""
        if self._data_type == MapTransformFnDataType.Block:
            return BlockAccessor.for_block(data).num_rows()
        elif self._data_type == MapTransformFnDataType.Batch:
            # TODO(hchen): We should have a better abstraction for batch.
            # to get rid of this type check.
            if isinstance(data, collections.abc.Mapping):
                # If the batch is a dict.
                first_key = list(data.keys())[0]
                return len(data[first_key])
            else:
                # If the batch is not a dict, it must be a block.
                return BlockAccessor.for_block(data).num_rows()
        else:
            assert self._data_type == MapTransformFnDataType.Row
            return 1

    def __call__(
        self, input: Iterable[MapTransformFnData], _: TaskContext
    ) -> Iterable[MapTransformFnData]:
        """Yields at least n rows from the input data."""
        if self._n <= 0:
            return

        consumed_rows = 0
        for data in input:
            # For n > 0, we yield at least one row/batch/block.
            # The subsequent Limit operator takes care of slicing for
            # the case where n < block_size.
            yield data

            num_rows = self._num_rows(data)
            assert num_rows is not None, "Block has unknown number of rows."
            consumed_rows += num_rows

            # Once we have consumed n rows, stop yielding more blocks.
            if consumed_rows >= self._n:
                break

    @property
    def input_type(self) -> MapTransformFnDataType:
        return self._data_type

    @property
    def output_type(self) -> MapTransformFnDataType:
        return self._data_type

    def __repr__(self) -> str:
        return f"LimitNRowsMapTransformFn(n={self._n}, data_type={self._data_type})"


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

        n_limit = down_op._limit
        map_transform_fn = up_op.get_map_transformer()
        # Create a new list. Do not modify the original list.
        transform_fns = list(map_transform_fn._transform_fns)
        init_fn = map_transform_fn._init_fn

        insert_index = len(transform_fns)
        if len(transform_fns) > 1 and isinstance(
            transform_fns[-1], BuildOutputBlocksMapTransformFn
        ):
            # If the last transform_fn is a BuildOutputBlocksMapTransformFn,
            # we insert the limit transform_fn before it to avoid
            # building unncessary blocks.
            insert_index -= 1

        transform_fns.insert(
            insert_index,
            LimitNRowsMapTransformFn(
                n=n_limit,
                data_type=transform_fns[insert_index - 1].output_type,
            ),
        )

        new_map_transfomer = MapTransformer(
            transform_fns,
            init_fn,
        )

        # Create the new logical and physical MapOperators.
        up_logical_op = self._op_map.pop(up_op)
        assert isinstance(up_logical_op, AbstractMap)

        down_logical_op = self._op_map.pop(down_op)
        new_map_op_name = f"{up_op._name[:-1]}->Limit[{n_limit}])"
        compute = None
        target_block_size = None
        ray_remote_args = up_logical_op._ray_remote_args

        if isinstance(up_logical_op, AbstractUDFMap):
            compute = get_compute(up_logical_op._compute)
            target_block_size = up_logical_op._target_block_size

        up_op = MapOperator.create(
            new_map_transfomer,
            up_op.input_dependency,
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
