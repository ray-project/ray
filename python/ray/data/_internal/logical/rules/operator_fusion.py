from typing import Iterator, List, Tuple
from ray.data._internal.logical.operators.all_to_all_operator import Repartition
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.all_to_all_operator import (
    AbstractAllToAll,
    RandomShuffle,
)
from ray.data._internal.stats import StatsDict

from ray.data.block import Block

# TODO(Clark): Remove compute dependency once we delete the legacy compute.
from ray.data._internal.compute import is_task_compute, CallableClass, get_compute
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.logical.interfaces import Rule, PhysicalPlan
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap


# Scheduling strategy can be inherited from upstream operator if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


class OperatorFusionRule(Rule):
    """Fuses linear chains of compatible physical operators."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        # Do DFS fusion on compatible pairwise operators in two passes.
        # In the first pass, only fuse back-to-back map operators together.
        fused_dag = self._fuse_map_operators_in_dag(plan.dag)

        # Now that we have fused together all back-to-back map operators,
        # we fuse together MapOperator -> AllToAllOperator pairs.
        fused_dag = self._fuse_all_to_all_operators_in_dag(fused_dag)

        return PhysicalPlan(fused_dag, self._op_map)

    def _fuse_map_operators_in_dag(self, dag: PhysicalOperator) -> MapOperator:
        """Starting at the given operator, traverses up the DAG of operators
        and recursively fuses compatible MapOperator -> MapOperator pairs.
        Returns the current (root) operator after completing upstream operator fusions.
        """
        upstream_ops = dag.input_dependencies
        while (
            len(upstream_ops) == 1
            and isinstance(dag, MapOperator)
            and isinstance(upstream_ops[0], MapOperator)
            and self._can_fuse(dag, upstream_ops[0])
        ):
            # Fuse operator with its upstream op.
            dag = self._get_fused_map_operator(dag, upstream_ops[0])
            upstream_ops = dag.input_dependencies

        # Done fusing back-to-back map operators together here,
        # move up the DAG to find the next map operators to fuse.
        dag._input_dependencies = [
            self._fuse_map_operators_in_dag(upstream_op) for upstream_op in upstream_ops
        ]
        return dag

    def _fuse_all_to_all_operators_in_dag(
        self, dag: AllToAllOperator
    ) -> AllToAllOperator:
        """Starting at the given operator, traverses up the DAG of operators
        and recursively fuses compatible MapOperator -> AllToAllOperator pairs.
        Returns the current (root) operator after completing upstream operator fusions.
        """
        upstream_ops = dag.input_dependencies
        while (
            len(upstream_ops) == 1
            and isinstance(dag, AllToAllOperator)
            and isinstance(upstream_ops[0], MapOperator)
            and self._can_fuse(dag, upstream_ops[0])
        ):
            # Fuse operator with its upstream op.
            dag = self._get_fused_all_to_all_operator(dag, upstream_ops[0])
            upstream_ops = dag.input_dependencies

        # Done fusing MapOperator -> AllToAllOperator together here,
        # move up the DAG to find the next pair of operators to fuse.
        dag._input_dependencies = [
            self._fuse_all_to_all_operators_in_dag(upstream_op)
            for upstream_op in upstream_ops
        ]
        return dag

    def _can_fuse(self, down_op: PhysicalOperator, up_op: PhysicalOperator) -> bool:
        """Returns whether the provided downstream operator can be fused with the given
        upstream operator.

        We currently support fusing two operators if the following are all true:
            * We are fusing either MapOperator -> MapOperator or
              MapOperator -> AllToAllOperator.
            * They either use the same compute configuration, or the upstream operator
              uses a task pool while the downstream operator uses an actor pool.
            * If both operators involve callable classes, the callable classes are
              the same class AND constructor args are the same for both.
            * They have compatible remote arguments.
        """
        from ray.data._internal.execution.operators.map_operator import MapOperator
        from ray.data._internal.logical.operators.map_operator import AbstractMap
        from ray.data._internal.logical.operators.map_operator import AbstractUDFMap

        # We currently only support fusing for the following cases:
        # - MapOperator -> MapOperator
        # - MapOperator -> AllToAllOperator
        # (only RandomShuffle and Repartition LogicalOperators are currently supported)
        if not isinstance(down_op, (MapOperator, AllToAllOperator)) or not isinstance(
            up_op, MapOperator
        ):
            return False

        down_logical_op = self._op_map[down_op]
        up_logical_op = self._op_map[up_op]

        # If the downstream operator takes no input, it cannot be fused with
        # the upstream operator.
        if not down_logical_op._input_dependencies:
            return False

        # We currently only support fusing for the following cases:
        # - AbstractMap -> AbstractMap
        # - AbstractMap -> RandomShuffle
        # - AbstractMap -> Repartition (shuffle=True)
        if not isinstance(
            down_logical_op, (AbstractMap, RandomShuffle, Repartition)
        ) or not isinstance(up_logical_op, AbstractMap):
            return False

        # Do not fuse Repartition operator if shuffle is disabled
        # (i.e. using split shuffle).
        if isinstance(down_logical_op, Repartition) and not down_logical_op._shuffle:
            return False

        # Allow fusing tasks->actors if the resources are compatible (read->map), but
        # not the other way around. The latter (downstream op) will be used as the
        # compute if fused.
        if (
            isinstance(down_logical_op, AbstractUDFMap)
            and is_task_compute(down_logical_op._compute)
            and isinstance(up_logical_op, AbstractUDFMap)
            and get_compute(up_logical_op._compute)
            != get_compute(down_logical_op._compute)
        ):
            return False

        # Fusing callable classes is only supported if they are the same function AND
        # their construction arguments are the same. Note the Write can be compatbile
        # with any UDF as Write itself doesn't have UDF.
        # TODO(Clark): Support multiple callable classes instantiating in the same actor
        # worker.
        if (
            isinstance(down_logical_op, AbstractUDFMap)
            and isinstance(down_logical_op._fn, CallableClass)
            and isinstance(up_logical_op, AbstractUDFMap)
            and isinstance(up_logical_op._fn, CallableClass)
            and (
                up_logical_op._fn != down_logical_op._fn
                or (
                    up_logical_op._fn_constructor_args
                    != down_logical_op._fn_constructor_args
                    or up_logical_op._fn_constructor_kwargs
                    != down_logical_op._fn_constructor_kwargs
                )
            )
        ):
            return False

        # Only fuse if the ops' remote arguments are compatible.
        if not _are_remote_args_compatible(
            up_logical_op._ray_remote_args or {}, down_logical_op._ray_remote_args or {}
        ):
            return False

        # Otherwise, ops are compatible for fusion.
        return True

    def _get_fused_map_operator(
        self, down_op: MapOperator, up_op: MapOperator
    ) -> MapOperator:
        assert self._can_fuse(down_op, up_op), (
            "Current rule supports fusing MapOperator->MapOperator, but received: "
            f"{type(up_op).__name__} -> {type(down_op).__name__}"
        )

        # Fuse operator names.
        name = up_op.name + "->" + down_op.name

        down_logical_op = self._op_map.pop(down_op)
        up_logical_op = self._op_map.pop(up_op)

        # Merge target block sizes.
        down_target_block_size = down_logical_op._target_block_size
        up_target_block_size = (
            up_logical_op._target_block_size
            if isinstance(up_logical_op, AbstractUDFMap)
            else None
        )
        if down_target_block_size is not None and up_target_block_size is not None:
            target_block_size = max(down_target_block_size, up_target_block_size)
        elif up_target_block_size is not None:
            target_block_size = up_target_block_size
        else:
            target_block_size = down_target_block_size

        # Fuse transformation functions.
        down_transform_fn = down_op.get_transformation_fn()
        up_transform_fn = up_op.get_transformation_fn()

        def fused_map_transform_fn(
            blocks: Iterator[Block], ctx: TaskContext
        ) -> Iterator[Block]:
            blocks = up_transform_fn(blocks, ctx)
            # TODO(Scott): Add zero-copy batching between transform functions.
            return down_transform_fn(blocks, ctx)

        # We take the downstream op's compute in case we're fusing upstream tasks with a
        # downstream actor pool (e.g. read->map).
        compute = get_compute(down_logical_op._compute)
        ray_remote_args = down_logical_op._ray_remote_args
        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        # Fused physical map operator.
        op = MapOperator.create(
            fused_map_transform_fn,
            input_op,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=target_block_size,
            ray_remote_args=ray_remote_args,
        )

        # Build a map logical operator to be used as a reference for further fusion.
        # TODO(Scott): This is hacky, remove this once we push fusion to be purely based
        # on a lower-level operator spec.
        if isinstance(up_logical_op, AbstractUDFMap):
            input_op = up_logical_op.input_dependencies[0]
        else:
            # Bottom out at the source logical op (e.g. Read()).
            input_op = up_logical_op
        if isinstance(down_logical_op, AbstractUDFMap):
            logical_op = AbstractUDFMap(
                name,
                input_op,
                down_logical_op._fn,
                down_logical_op._fn_args,
                down_logical_op._fn_kwargs,
                down_logical_op._fn_constructor_args,
                down_logical_op._fn_constructor_kwargs,
                target_block_size,
                compute,
                ray_remote_args,
            )
        else:
            from ray.data._internal.logical.operators.map_operator import AbstractMap

            # The downstream op is AbstractMap instead of AbstractUDFMap.
            logical_op = AbstractMap(
                name,
                input_op,
                ray_remote_args,
            )
        self._op_map[op] = logical_op
        # Return the fused physical operator.
        return op

    def _get_fused_all_to_all_operator(
        self, down_op: AllToAllOperator, up_op: MapOperator
    ) -> AllToAllOperator:
        assert self._can_fuse(down_op, up_op), (
            "Current rule supports fusing MapOperator -> AllToAllOperator"
            f", but received: {type(up_op).__name__} -> {type(down_op).__name__}"
        )

        # Fuse operator names.
        name = up_op.name + "->" + down_op.name

        down_logical_op: AbstractAllToAll = self._op_map.pop(down_op)
        up_logical_op: AbstractUDFMap = self._op_map.pop(up_op)

        # Fuse transformation functions.
        down_transform_fn = down_op.get_transformation_fn()
        up_transform_fn = up_op.get_transformation_fn()

        def fused_all_to_all_transform_fn(
            blocks: List[RefBundle], ctx: TaskContext
        ) -> Tuple[List[RefBundle], StatsDict]:
            """To fuse MapOperator->AllToAllOperator, we store the map function
            in the TaskContext so that it may be used by the downstream
            AllToAllOperator's transform function."""
            ctx.upstream_map_transform_fn = up_transform_fn
            return down_transform_fn(blocks, ctx)

        ray_remote_args = down_logical_op._ray_remote_args
        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        op = AllToAllOperator(
            fused_all_to_all_transform_fn,
            input_op,
            name=name,
        )
        # Bottom out at the source logical op (e.g. Read()).
        input_op = up_logical_op

        if isinstance(down_logical_op, RandomShuffle):
            logical_op = RandomShuffle(
                input_op,
                name=name,
                ray_remote_args=ray_remote_args,
            )
        elif isinstance(down_logical_op, Repartition):
            logical_op = Repartition(
                input_op,
                num_outputs=down_logical_op._num_outputs,
                shuffle=down_logical_op._shuffle,
            )
        self._op_map[op] = logical_op
        # Return the fused physical operator.
        return op


def _are_remote_args_compatible(up_args, down_args):
    """Check if Ray remote arguments are compatible for merging."""
    from ray.data._internal.execution.operators.map_operator import (
        _canonicalize_ray_remote_args,
    )

    up_args = _canonicalize_ray_remote_args(up_args)
    down_args = _canonicalize_ray_remote_args(down_args)
    remote_args = down_args.copy()
    for key in INHERITABLE_REMOTE_ARGS:
        if key in up_args:
            remote_args[key] = up_args[key]
    if up_args != remote_args:
        return False
    return True
