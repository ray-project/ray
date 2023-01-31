from typing import Iterator

from ray.data.block import Block

# TODO(Clark): Remove compute dependency once we delete the legacy compute.
from ray.data._internal.compute import is_task_compute, CallableClass, get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.logical.interfaces import Rule, PhysicalPlan


# Scheduling strategy can be inherited from upstream operator if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


class OperatorFusionRule(Rule):
    """Fuses linear chains of compatible physical operators."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        # Do DFS fusion.
        root = self._apply(plan.dag)
        return PhysicalPlan(root, self._op_map)

    def _apply(self, op: PhysicalOperator) -> PhysicalOperator:
        """Performs DFS fusion of linear chains of physical map operators, provided that
        they are pairwise-compatible.

        Args:
            op: The op that we're trying to fuse with its input.
        """
        upstream_ops = op.input_dependencies
        # Fuse with upstream ops while possible.
        while len(upstream_ops) == 1 and self._can_fuse(op, upstream_ops[0]):
            # Fuse operator with its upstream op.
            op = self._fuse(op, upstream_ops[0])
            upstream_ops = op.input_dependencies
        # Can no longer fuse with upstream ops, proceed up the DAG.
        op._input_dependencies = [
            self._apply(upstream_op) for upstream_op in upstream_ops
        ]
        return op

    def _can_fuse(self, down_op: PhysicalOperator, up_op: PhysicalOperator) -> bool:
        """Returns whether the provided downstream operator can be fused with the given
        upstream operator.

        We currently support fusing two operators if the following are all true:
            * They are both MapOperators.
            * They either use the same compute configuration, or the upstream operator
              uses a task pool while the downstream operator uses an actor pool.
            * If both operators involve callable classes, the callable classes are
              the same class AND constructor args are the same for both.
            * They have compatible remote arguments.
        """
        from ray.data._internal.execution.operators.map_operator import MapOperator
        from ray.data._internal.logical.operators.map_operator import AbstractMap
        from ray.data._internal.logical.operators.read_operator import Read

        # We only support fusing MapOperators.
        if not isinstance(down_op, MapOperator) or not isinstance(up_op, MapOperator):
            return False

        down_logical_op = self._op_map[down_op]
        up_logical_op = self._op_map[up_op]

        # We only support fusing upstream reads and maps with downstream maps.
        if not isinstance(down_logical_op, AbstractMap) or not isinstance(
            up_logical_op, (Read, AbstractMap)
        ):
            return False

        # Allow fusing tasks->actors if the resources are compatible (read->map), but
        # not the other way around. The latter (downstream op) will be used as the
        # compute if fused.
        if (
            is_task_compute(down_logical_op._compute)
            and isinstance(up_logical_op, AbstractMap)
            and get_compute(up_logical_op._compute)
            != get_compute(down_logical_op._compute)
        ):
            return False

        # Fusing callable classes is only supported if they are the same function AND
        # their construction arguments are the same.
        # TODO(Clark): Support multiple callable classes instantiating in the same actor
        # worker.
        if (
            isinstance(down_logical_op._fn, CallableClass)
            and isinstance(up_logical_op, AbstractMap)
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

    def _fuse(self, down_op: PhysicalOperator, up_op: PhysicalOperator):
        """Fuse the downstream operator with its upstream operator."""
        from ray.data._internal.execution.operators.map_operator import MapOperator
        from ray.data._internal.logical.operators.map_operator import AbstractMap

        assert self._can_fuse(down_op, up_op)

        # Fuse operator names.
        name = up_op.name + "->" + down_op.name

        down_logical_op = self._op_map.pop(down_op)
        up_logical_op = self._op_map.pop(up_op)

        # Merge target block sizes.
        down_target_block_size = down_logical_op._target_block_size
        up_target_block_size = (
            up_logical_op._target_block_size
            if isinstance(up_logical_op, AbstractMap)
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

        def transform_fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
            blocks = up_transform_fn(blocks, ctx)
            # TODO(Clark): Add zero-copy batching between transform functions.
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
            transform_fn,
            input_op,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=target_block_size,
            ray_remote_args=ray_remote_args,
        )

        # Build a map logical operator to be used as a reference for further fusion.
        # TODO(Clark): This is hacky, remove this once we push fusion to be purely based
        # on a lower-level operator spec.
        if isinstance(up_logical_op, AbstractMap):
            input_op = up_logical_op.input_dependencies[0]
        else:
            # Bottom out at the source logical op (e.g. Read()).
            input_op = up_logical_op
        logical_op = AbstractMap(
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
