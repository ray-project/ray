from typing import List

# TODO(Clark): Remove compute dependency once we delete the legacy compute.
from ray.data._internal.compute import is_task_compute, CallableClass, get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator, MapTransformFn
from ray.data._internal.logical.interfaces import Rule, PhysicalPlan, LogicalOperator
from ray.data._internal.planner.transforms import (
    generate_data_transform_for_op,
    generate_adapted_transform,
    InputAdapter,
    OutputAdapter,
    OpAdapter,
)
from ray.data._internal.planner.transforms.utils import _pairwise


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
        # Gather ops to fuse.
        ops_to_fuse = [op]
        while len(upstream_ops) == 1 and self._can_fuse(op, upstream_ops[0]):
            op = upstream_ops[0]
            ops_to_fuse.append(op)
            upstream_ops = op.input_dependencies
        if len(ops_to_fuse) >= 2:
            # Fuse all fusable ops together in a single pass.
            op = self._fuse(ops_to_fuse)
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
        from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
        from ray.data._internal.logical.operators.read_operator import Read

        # We only support fusing MapOperators.
        if not isinstance(down_op, MapOperator) or not isinstance(up_op, MapOperator):
            return False

        down_logical_op = self._op_map[down_op]
        up_logical_op = self._op_map[up_op]

        # We only support fusing upstream reads and maps with downstream maps.
        if not isinstance(down_logical_op, AbstractUDFMap) or not isinstance(
            up_logical_op, (Read, AbstractUDFMap)
        ):
            return False

        # Allow fusing tasks->actors if the resources are compatible (read->map), but
        # not the other way around. The latter (downstream op) will be used as the
        # compute if fused.
        if (
            is_task_compute(down_logical_op._compute)
            and isinstance(up_logical_op, AbstractUDFMap)
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

    def _fuse(self, ops: List[PhysicalOperator]):
        """Fuse the provided fusable ops together into a single op."""
        from ray.data._internal.execution.operators.map_operator import MapOperator
        from ray.data._internal.logical.operators.map_operator import AbstractUDFMap

        assert len(ops) >= 2

        for down_op, up_op in _pairwise(ops):
            assert self._can_fuse(down_op, up_op)

        # Set op order from source to sink.
        ops = list(reversed(ops))
        # Fuse operator names.
        name = "->".join([op.name for op in ops])
        # We pop the logical ops from the map since they should no longer be needed.
        logical_ops = [self._op_map.pop(op) for op in ops]

        # Merge target block sizes.
        target_block_sizes = [
            op._target_block_size
            if isinstance(op, AbstractUDFMap) and op._target_block_size is not None
            else 0
            for op in logical_ops
        ]
        target_block_size = max(target_block_sizes) or None

        # We take the downstream op's compute in case we're fusing upstream tasks with a
        # downstream actor pool (e.g. read->map).
        logical_output_op = logical_ops[-1]
        assert isinstance(logical_output_op, AbstractUDFMap)
        compute = get_compute(logical_output_op._compute)

        # TODO(Clark): Support merging compatible remote args, e.g. taking the max of
        # the num_cpus requests.
        ray_remote_args = logical_output_op._ray_remote_args

        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = ops[0].input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        # Create fused block transform.
        fused_transform_fn = _ops_to_fused_transform(logical_ops)

        # Fused physical map operator.
        op = MapOperator.create(
            fused_transform_fn,
            input_op,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=target_block_size,
            ray_remote_args=ray_remote_args,
        )

        # TODO(Clark): Build a map logical operator to be used as a reference for other
        # optimization rules.
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


def _ops_to_fused_transform(ops: List[LogicalOperator]) -> MapTransformFn:
    """Create a fused block transform function from a list of fusable logical
    operators.
    """
    from ray.data._internal.logical.operators.map_operator import AbstractUDFMap

    adapters = []
    transforms = []
    # Get input adapter and op transform.
    input_adapter = InputAdapter.from_downstream_op(ops[0])
    adapters.append(input_adapter)
    input_transform = generate_data_transform_for_op(ops[0])
    transforms.append(input_transform)
    # Get all intermediate op adapters and all op transforms.
    for up_op, down_op in _pairwise(ops):
        op_adapter = OpAdapter.from_ops(up_op, down_op)
        adapters.append(op_adapter)
        assert isinstance(down_op, AbstractUDFMap)
        op_transform = generate_data_transform_for_op(down_op)
        transforms.append(op_transform)

    # Get output adapter.
    # NOTE: Output op transform should have already been added by the above for
    # loop.
    output_adapter = OutputAdapter.from_upstream_op(ops[-1])
    adapters.append(output_adapter)

    assert len(transforms) == len(ops)
    assert len(adapters) == len(ops) + 1

    return generate_adapted_transform(transforms, adapters)
