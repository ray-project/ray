from typing import List, Optional, Tuple

# TODO(Clark): Remove compute dependency once we delete the legacy compute.
from ray.data._internal.compute import get_compute, is_task_compute
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import (
    AbstractAllToAll,
    RandomShuffle,
    Repartition,
)
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext

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

        # Update output dependencies after fusion.
        # TODO(hchen): Instead of updating the depdencies manually,
        # we need a better abstraction for manipulating the DAG.
        self._remove_output_depes(fused_dag)
        self._update_output_depes(fused_dag)

        return PhysicalPlan(fused_dag, self._op_map)

    def _remove_output_depes(self, op: PhysicalOperator) -> None:
        for input in op._input_dependencies:
            input._output_dependencies = []
            self._remove_output_depes(input)

    def _update_output_depes(self, op: PhysicalOperator) -> None:
        for input in op._input_dependencies:
            input._output_dependencies.append(op)
            self._update_output_depes(input)

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

        Also, sets the target block size of the immediately upstream map op to
        match the shuffle block size. We use a larger block size for shuffles
        because tiny blocks are bad for I/O performance.

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
        from ray.data._internal.logical.operators.map_operator import (
            AbstractMap,
            AbstractUDFMap,
        )

        # We currently only support fusing for the following cases:
        # - TaskPoolMapOperator -> TaskPoolMapOperator/ActorPoolMapOperator
        # - TaskPoolMapOperator -> AllToAllOperator
        # (only RandomShuffle and Repartition LogicalOperators are currently supported)
        if not (
            (
                isinstance(up_op, TaskPoolMapOperator)
                and isinstance(down_op, (TaskPoolMapOperator, ActorPoolMapOperator))
            )
            or (
                isinstance(up_op, TaskPoolMapOperator)
                and isinstance(down_op, AllToAllOperator)
            )
        ):
            return False

        down_logical_op = self._op_map[down_op]
        up_logical_op = self._op_map[up_op]

        if up_op.get_additional_split_factor() > 1:
            return False

        # If the downstream operator takes no input, it cannot be fused with
        # the upstream operator.
        if not down_logical_op._input_dependencies:
            return False

        # We currently only support fusing for the following cases:
        # - AbstractMap -> AbstractMap
        # - AbstractMap -> RandomShuffle
        # - AbstractMap -> Repartition (shuffle=True)
        if not (
            (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, AbstractMap)
            )
            or (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, RandomShuffle)
            )
            or (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, Repartition)
            )
        ):
            return False

        # Do not fuse Repartition operator if shuffle is disabled
        # (i.e. using split shuffle).
        if isinstance(down_logical_op, Repartition) and not down_logical_op._shuffle:
            return False

        if isinstance(down_logical_op, AbstractUDFMap) and isinstance(
            up_logical_op, AbstractUDFMap
        ):
            # Allow fusing tasks->actors if the resources are compatible (read->map),
            # but not the other way around. The latter (downstream op) will be used as
            # the compute if fused.
            if is_task_compute(down_logical_op._compute) and get_compute(
                up_logical_op._compute
            ) != get_compute(down_logical_op._compute):
                return False

        # Only fuse if the ops' remote arguments are compatible.
        if not _are_remote_args_compatible(
            getattr(up_logical_op, "_ray_remote_args", {}),
            getattr(down_logical_op, "_ray_remote_args", {}),
        ):
            return False

        if not self._can_merge_target_max_block_size(
            up_op.target_max_block_size, down_op.target_max_block_size
        ):
            return False

        # Otherwise, ops are compatible for fusion.
        return True

    def _can_merge_target_max_block_size(
        self,
        up_target_max_block_size: Optional[int],
        down_target_max_block_size: Optional[int],
    ):
        # If the upstream op overrode the target max block size, only fuse if
        # they are equal.
        if up_target_max_block_size is not None:
            if down_target_max_block_size is None:
                down_target_max_block_size = (
                    DataContext.get_current().target_max_block_size
                )
            if up_target_max_block_size != down_target_max_block_size:
                return False
        return True

    def _get_merged_target_max_block_size(
        self,
        up_target_max_block_size: Optional[int],
        down_target_max_block_size: Optional[int],
    ):
        if up_target_max_block_size is not None:
            # If the upstream op overrode the target max block size, we can
            # only merge if the downstream op matches or uses the default.
            assert (
                down_target_max_block_size is None
                or down_target_max_block_size == up_target_max_block_size
            )
            return up_target_max_block_size
        else:
            # Upstream op inherits the downstream op's target max block size,
            # because the downstream op is the one that outputs the final
            # blocks.
            return down_target_max_block_size

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

        # Merge minimum block sizes.
        down_min_rows_per_block = (
            down_logical_op._min_rows_per_block
            if isinstance(down_logical_op, AbstractUDFMap)
            else None
        )
        up_min_rows_per_block = (
            up_logical_op._min_rows_per_block
            if isinstance(up_logical_op, AbstractUDFMap)
            else None
        )
        if down_min_rows_per_block is not None and up_min_rows_per_block is not None:
            min_rows_per_block = max(down_min_rows_per_block, up_min_rows_per_block)
        elif up_min_rows_per_block is not None:
            min_rows_per_block = up_min_rows_per_block
        else:
            min_rows_per_block = down_min_rows_per_block

        target_max_block_size = self._get_merged_target_max_block_size(
            up_op.target_max_block_size, down_op.target_max_block_size
        )

        # We take the downstream op's compute in case we're fusing upstream tasks with a
        # downstream actor pool (e.g. read->map).
        compute = None
        if isinstance(down_logical_op, AbstractUDFMap):
            compute = get_compute(down_logical_op._compute)
        ray_remote_args = up_logical_op._ray_remote_args
        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        # Fused physical map operator.
        op = MapOperator.create(
            up_op.get_map_transformer().fuse(down_op.get_map_transformer()),
            input_op,
            target_max_block_size=target_max_block_size,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=min_rows_per_block,
            ray_remote_args=ray_remote_args,
        )

        # Build a map logical operator to be used as a reference for further fusion.
        # TODO(Scott): This is hacky, remove this once we push fusion to be purely based
        # on a lower-level operator spec.
        if isinstance(up_logical_op, AbstractUDFMap):
            input_op = up_logical_op.input_dependency
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
                min_rows_per_block,
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
        ray_remote_args = up_logical_op._ray_remote_args
        down_transform_fn = down_op.get_transformation_fn()
        up_map_transformer = up_op.get_map_transformer()

        def fused_all_to_all_transform_fn(
            blocks: List[RefBundle], ctx: TaskContext
        ) -> Tuple[List[RefBundle], StatsDict]:
            """To fuse MapOperator->AllToAllOperator, we store the map function
            in the TaskContext so that it may be used by the downstream
            AllToAllOperator's transform function."""
            ctx.upstream_map_transformer = up_map_transformer
            ctx.upstream_map_ray_remote_args = ray_remote_args
            return down_transform_fn(blocks, ctx)

        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        target_max_block_size = self._get_merged_target_max_block_size(
            up_op.target_max_block_size, down_op.target_max_block_size
        )

        op = AllToAllOperator(
            fused_all_to_all_transform_fn,
            input_op,
            target_max_block_size=target_max_block_size,
            num_outputs=down_op._num_outputs,
            # Transfer over the existing sub-progress bars from
            # the AllToAllOperator (if any) into the fused operator.
            sub_progress_bar_names=down_op._sub_progress_bar_names,
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


def _are_remote_args_compatible(prev_args, next_args):
    """Check if Ray remote arguments are compatible for merging."""
    prev_args = _canonicalize(prev_args)
    next_args = _canonicalize(next_args)
    remote_args = next_args.copy()
    for key in INHERITABLE_REMOTE_ARGS:
        if key in prev_args:
            remote_args[key] = prev_args[key]
    if prev_args != remote_args:
        return False
    return True


def _canonicalize(remote_args: dict) -> dict:
    """Returns canonical form of given remote args."""
    remote_args = remote_args.copy()
    if "num_cpus" not in remote_args or remote_args["num_cpus"] is None:
        remote_args["num_cpus"] = 1
    if "num_gpus" not in remote_args or remote_args["num_gpus"] is None:
        remote_args["num_gpus"] = 0
    resources = remote_args.get("resources", {})
    for k, v in list(resources.items()):
        if v is None or v == 0.0:
            del resources[k]
    remote_args["resources"] = resources
    return remote_args
