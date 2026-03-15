import itertools
import logging
import time
from typing import Any, Dict, List, Optional

from ray.data._internal.compute import (
    ActorPoolStrategy,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    MapTransformer,
    ShuffleMapTransformFn,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    AbstractMap,
    AbstractUDFMap,
    MapBatches,
    RandomShuffle,
    Repartition,
    StreamingRepartition,
)
from ray.data._internal.planner.exchange.map_shuffle_task_spec import (
    MapTransformerShuffleTaskSpec,
)
from ray.data._internal.planner.random_shuffle import _execute_sort_shuffle
from ray.data._internal.streaming_repartition import StreamingRepartitionRefBundler
from ray.util.annotations import DeveloperAPI
from ray.util.common import INT32_MAX

__all__ = [
    "FuseOperators",
    "are_remote_args_compatible",
]


# Scheduling strategy can be inherited from upstream operator if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


logger = logging.getLogger(__name__)


class FuseOperators(Rule):
    """Fuses linear chains of compatible physical operators."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        # TODO(xgui): Currently we have to fuse streaming_repartition before map fusion
        # because the result of map fusion loses the batch_size information.
        # We should fix this by not losing the batch_size information when fusing map operators.
        fused_dag = self._fuse_streaming_repartition_operators_in_dag(plan.dag)
        # Do DFS fusion on compatible pairwise operators in two passes.
        # In the first pass, only fuse back-to-back map operators together.
        fused_dag = self._fuse_map_operators_in_dag(fused_dag)

        # Now that we have fused together all back-to-back map operators,
        # we fuse together MapOperator -> AllToAllOperator pairs.
        fused_dag = self._fuse_all_to_all_operators_in_dag(fused_dag)

        # Update output dependencies after fusion.
        # TODO(hchen): Instead of updating the depdencies manually,
        # we need a better abstraction for manipulating the DAG.
        self._remove_output_deps(fused_dag)
        self._update_output_deps(fused_dag)

        new_plan = PhysicalPlan(fused_dag, self._op_map, plan.context)
        return new_plan

    def _remove_output_deps(self, op: PhysicalOperator) -> None:
        for input in op.input_dependencies:
            input._output_dependencies = []
            self._remove_output_deps(input)

    def _update_output_deps(self, op: PhysicalOperator) -> None:
        for input in op.input_dependencies:
            input._output_dependencies.append(op)
            self._update_output_deps(input)

    def _fuse_streaming_repartition_operators_in_dag(
        self, dag: PhysicalOperator
    ) -> PhysicalOperator:
        """Fuse (MapBatches -> StreamingRepartition) pair.

        This will ensure the map_batch's function receive the correct number of rows.
        We also ensure the output rows is `batch_size`.

        Why don't we fuse `StreamingRepartition -> MapBatches`?

        ----------------------------------------------------------------------------------------------------
        |                      | Number of `map_batches` tasks                                             |
        |----------------------|---------------------------------------------------------------------------|
        | Fused                | num_input_blocks (which is <= num output blocks of StreamingRepartition) |
        | Not fused            | num output blocks of StreamingRepartition                                 |
        ----------------------------------------------------------------------------------------------------

        When fused, the number of tasks equals the number of input blocks, which is
        <= the number of output blocks of StreamingRepartition. If StreamingRepartition
        is supposed to break down blocks to increase parallelism, that won't happen
        when fused. So we don't fuse.

        Why do we fuse `MapBatches -> StreamingRepartition` (when `batch_size % target_num_rows == 0`)?
        ----------------------------------------------------------
        |                      | Number of `map_batches` tasks  |
        |----------------------|--------------------------------|
        | Fused                | total_rows / batch_size        |
        | Not fused            | total_rows / batch_size        |
        ----------------------------------------------------------

        Parallelism is unchanged, so we fuse to avoid intermediate materialization.
        """
        upstream_ops = dag.input_dependencies
        while (
            len(upstream_ops) == 1
            and isinstance(self._op_map[dag], StreamingRepartition)
            and isinstance(self._op_map[upstream_ops[0]], MapBatches)
            and self._can_fuse(dag, upstream_ops[0])
        ):
            dag = self._get_fused_streaming_repartition_operator(dag, upstream_ops[0])
            upstream_ops = dag.input_dependencies

        dag._input_dependencies = [
            self._fuse_streaming_repartition_operators_in_dag(upstream_op)
            for upstream_op in upstream_ops
        ]
        return dag

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
        if not up_op.supports_fusion() or not down_op.supports_fusion():
            return False

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
        if not down_logical_op.input_dependencies:
            return False

        # We currently only support fusing for the following cases:
        # - AbstractMap -> AbstractMap
        # - AbstractMap -> RandomShuffle
        # - AbstractMap -> Repartition (shuffle=True)
        if not (
            (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, AbstractMap)
                and self._can_fuse_map_ops(up_logical_op, down_logical_op)
            )
            or (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, RandomShuffle)
            )
            # Do not fuse Repartition operator if shuffle is disabled
            # (i.e. using split shuffle).
            or (
                isinstance(up_logical_op, AbstractMap)
                and isinstance(down_logical_op, Repartition)
                and down_logical_op.shuffle
            )
        ):
            return False

        # Only fuse if the ops' remote arguments are compatible.
        if not are_remote_args_compatible(
            getattr(up_logical_op, "ray_remote_args", {}),
            getattr(down_logical_op, "ray_remote_args", {}),
        ):
            return False

        # Do not fuse if either op specifies a `ray_remote_args_fn`,
        # since it is not known whether the generated args will be compatible.
        if getattr(up_logical_op, "ray_remote_args_fn", None) or getattr(
            down_logical_op, "ray_remote_args_fn", None
        ):
            return False

        if not self._can_merge_target_max_block_size(
            up_op.target_max_block_size_override,
            down_op.target_max_block_size_override,
        ):
            return False

        # only allow fusion of MapBatches -> StreamingRepartition
        if isinstance(down_logical_op, StreamingRepartition):
            if not (
                isinstance(up_logical_op, MapBatches)
                and down_logical_op.target_num_rows_per_block is not None
                and down_logical_op.target_num_rows_per_block > 0
            ):
                return False

            # Non-strict mode: can always fuse, no matter what batch_size is.
            # This allows fusion without cross-task buffering by using default bundler.
            if not down_logical_op._strict:
                return True

            # Strict mode: only fuse when batch_size is a multiple of target_num_rows_per_block.
            # When batch_size % target == 0, each batch can be perfectly sliced into chunks
            # without cross-task buffering. See `_fuse_streaming_repartition_operators_in_dag`
            # docstring for details.
            return (
                up_logical_op.batch_size is not None
                and up_logical_op.batch_size % down_logical_op.target_num_rows_per_block
                == 0
            )
        # Other operators cannot fuse with StreamingRepartition.
        if isinstance(up_logical_op, StreamingRepartition):
            return False

        # Otherwise, ops are compatible for fusion.
        return True

    def _get_fused_streaming_repartition_operator(
        self, down_op: PhysicalOperator, up_op: PhysicalOperator
    ) -> PhysicalOperator:
        assert self._can_fuse(down_op, up_op), (
            "Current rule supports fusing MapBatches->StreamingRepartition, but received: "
            f"{type(up_op).__name__} -> {type(down_op).__name__}"
        )

        name = up_op.name + "->" + down_op.name

        down_logical_op = self._op_map.pop(down_op)
        up_logical_op = self._op_map.pop(up_op)
        assert isinstance(up_logical_op, MapBatches)
        assert isinstance(down_logical_op, StreamingRepartition)

        batch_size = up_logical_op.batch_size

        # Choose ref_bundler and fusion behavior based on strict mode
        if down_logical_op._strict:
            # Strict mode: use StreamingRepartitionRefBundler for stitching.
            # Only works when batch_size % target == 0 (verified in _can_fuse).
            assert batch_size % down_logical_op.target_num_rows_per_block == 0, (
                f"Strict mode fusion requires batch_size ({batch_size}) to be "
                f"a multiple of target_num_rows_per_block "
                f"({down_logical_op.target_num_rows_per_block})"
            )
            ref_bundler = StreamingRepartitionRefBundler(batch_size)
        else:
            # Non-strict mode: use default pass-through bundler.
            # Works with any batch_size without cross-task buffering.
            ref_bundler = None

        compute = self._fuse_compute_strategy(
            up_logical_op.compute, down_logical_op.compute
        )
        assert compute is not None

        map_task_kwargs = {**up_op._map_task_kwargs, **down_op._map_task_kwargs}

        ray_remote_args = up_logical_op.ray_remote_args
        ray_remote_args_fn = (
            up_logical_op.ray_remote_args_fn or down_logical_op.ray_remote_args_fn
        )
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        assert up_op.data_context is down_op.data_context

        # In non-strict mode, use min_rows_per_bundle to ensure creating batches with batch_size.
        # In strict mode, ref_bundler handles bundling, so do not set min_rows_per_bundle.
        min_rows = None if down_logical_op._strict else batch_size

        op = MapOperator.create(
            up_op.get_map_transformer().fuse(down_op.get_map_transformer()),
            input_op,
            up_op.data_context,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=min_rows,
            ref_bundler=ref_bundler,
            map_task_kwargs=map_task_kwargs,
            ray_remote_args=ray_remote_args,
            ray_remote_args_fn=ray_remote_args_fn,
            supports_fusion=True,
        )
        op.set_logical_operators(*up_op._logical_operators, *down_op._logical_operators)
        for map_task_kwargs_fn in itertools.chain(
            up_op._map_task_kwargs_fns, down_op._map_task_kwargs_fns
        ):
            op.add_map_task_kwargs_fn(map_task_kwargs_fn)

        input_op = up_logical_op.input_dependency
        logical_op = AbstractUDFMap(
            name,
            input_op,
            up_logical_op.fn,
            can_modify_num_rows=up_logical_op.can_modify_num_rows,
            fn_args=up_logical_op.fn_args,
            fn_kwargs=up_logical_op.fn_kwargs,
            fn_constructor_args=up_logical_op.fn_constructor_args,
            fn_constructor_kwargs=up_logical_op.fn_constructor_kwargs,
            min_rows_per_bundled_input=batch_size,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )
        self._op_map[op] = logical_op
        return op

    @classmethod
    def _fuse_compute_strategy(
        cls, up_compute: ComputeStrategy, down_compute: ComputeStrategy
    ) -> Optional[ComputeStrategy]:
        """Fuse the compute strategies of the upstream and downstream operators.
        Returns None if they are not compatible.

        Task->Task and Task->Actor are allowed.
        Actor->Actor and Actor->Task are not allowed.
        """
        if isinstance(up_compute, ActorPoolStrategy):
            return None
        assert isinstance(up_compute, TaskPoolStrategy)
        if isinstance(down_compute, TaskPoolStrategy):
            # For Task->Task, the sizes must match.
            if up_compute.size != down_compute.size:
                return None
            return down_compute
        else:
            assert isinstance(down_compute, ActorPoolStrategy)
            # For Task->Actor, if Task's size is set, it must match Actor's max_size.
            if up_compute.size is not None and up_compute.size != down_compute.max_size:
                return None
            return down_compute

    def _can_merge_target_max_block_size(
        self,
        up_target_max_block_size: Optional[int],
        down_target_max_block_size: Optional[int],
    ) -> bool:
        if (
            up_target_max_block_size is not None
            and down_target_max_block_size is not None
        ):
            # NOTE: In case of both ops overriding `target_max_block_size` only
            #       merge them if settings are equal
            return down_target_max_block_size == up_target_max_block_size

        return True

    def _get_merged_target_max_block_size(
        self,
        up_target_max_block_size: Optional[int],
        down_target_max_block_size: Optional[int],
    ) -> Optional[int]:
        assert self._can_merge_target_max_block_size(
            up_target_max_block_size, down_target_max_block_size
        )

        return up_target_max_block_size or down_target_max_block_size

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
        assert isinstance(down_logical_op, AbstractMap)
        assert isinstance(up_logical_op, AbstractMap)

        # Derive min num rows per input bundle
        min_rows_per_bundled_input = self._derive_bundle_min_num_rows(
            down_logical_op, up_logical_op
        )

        target_max_block_size = self._get_merged_target_max_block_size(
            up_op.target_max_block_size_override, down_op.target_max_block_size_override
        )

        compute = self._fuse_compute_strategy(
            up_logical_op.compute, down_logical_op.compute
        )
        assert compute is not None

        # Merge map task kwargs
        map_task_kwargs = {**up_op._map_task_kwargs, **down_op._map_task_kwargs}

        ray_remote_args = up_logical_op.ray_remote_args
        ray_remote_args_fn = (
            up_logical_op.ray_remote_args_fn or down_logical_op.ray_remote_args_fn
        )
        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        # Fuse on_start callbacks from both operators.
        # This preserves deferred initialization (e.g., on_write_start for Write ops).
        up_on_start = up_op._on_start
        down_on_start = down_op._on_start
        if up_on_start is not None and down_on_start is not None:

            def fused_on_start(schema):
                up_on_start(schema)
                down_on_start(schema)

            on_start = fused_on_start
        else:
            on_start = up_on_start or down_on_start

        # Preserve StreamingRepartitionRefBundler if either operator has one.
        # This is critical for strict-mode streaming repartition to maintain
        # exact block size guarantees during further fusion.
        ref_bundler = None
        if isinstance(up_op._block_ref_bundler, StreamingRepartitionRefBundler):
            ref_bundler = up_op._block_ref_bundler
        elif isinstance(down_op._block_ref_bundler, StreamingRepartitionRefBundler):
            ref_bundler = down_op._block_ref_bundler

        # Fused physical map operator.
        assert up_op.data_context is down_op.data_context
        op = MapOperator.create(
            up_op.get_map_transformer().fuse(down_op.get_map_transformer()),
            input_op,
            up_op.data_context,
            target_max_block_size_override=target_max_block_size,
            name=name,
            compute_strategy=compute,
            min_rows_per_bundle=min_rows_per_bundled_input
            if ref_bundler is None
            else None,
            ref_bundler=ref_bundler,
            map_task_kwargs=map_task_kwargs,
            ray_remote_args=ray_remote_args,
            ray_remote_args_fn=ray_remote_args_fn,
            on_start=on_start,
        )
        op.set_logical_operators(*up_op._logical_operators, *down_op._logical_operators)
        for map_task_kwargs_fn in itertools.chain(
            up_op._map_task_kwargs_fns, down_op._map_task_kwargs_fns
        ):
            op.add_map_task_kwargs_fn(map_task_kwargs_fn)

        # Build a map logical operator to be used as a reference for further fusion.
        # TODO(Scott): This is hacky, remove this once we push fusion to be purely based
        # on a lower-level operator spec.
        if isinstance(up_logical_op, AbstractUDFMap):
            input_op = up_logical_op.input_dependency
        else:
            # Bottom out at the source logical op (e.g. Read()).
            input_op = up_logical_op

        can_modify_num_rows = (
            up_logical_op.can_modify_num_rows or down_logical_op.can_modify_num_rows
        )
        if isinstance(down_logical_op, AbstractUDFMap):
            logical_op = AbstractUDFMap(
                name,
                input_op,
                down_logical_op.fn,
                fn_args=down_logical_op.fn_args,
                fn_kwargs=down_logical_op.fn_kwargs,
                fn_constructor_args=down_logical_op.fn_constructor_args,
                fn_constructor_kwargs=down_logical_op.fn_constructor_kwargs,
                min_rows_per_bundled_input=min_rows_per_bundled_input,
                compute=compute,
                can_modify_num_rows=can_modify_num_rows,
                ray_remote_args_fn=ray_remote_args_fn,
                ray_remote_args=ray_remote_args,
            )
        else:
            # The downstream op is AbstractMap instead of AbstractUDFMap.
            logical_op = AbstractMap(
                name,
                input_op,
                can_modify_num_rows=can_modify_num_rows,
                min_rows_per_bundled_input=min_rows_per_bundled_input,
                ray_remote_args_fn=ray_remote_args_fn,
                ray_remote_args=ray_remote_args,
            )
        self._op_map[op] = logical_op
        # Return the fused physical operator.
        return op

    @classmethod
    def _derive_bundle_min_num_rows(
        cls,
        down_logical_op: AbstractMap,
        up_logical_op: AbstractMap,
    ) -> Optional[int]:
        us_bundle_min_rows_req = up_logical_op.min_rows_per_bundled_input
        ds_bundle_min_rows_req = down_logical_op.min_rows_per_bundled_input

        # In case neither of the ops specify `min_rows_per_bundled_input`,
        # return None
        if us_bundle_min_rows_req is None and ds_bundle_min_rows_req is None:
            return None

        # Target min bundle size is selected as max of upstream and downstream ones
        # such that it could satisfy both of their requirements
        return max(
            ds_bundle_min_rows_req or 0,
            us_bundle_min_rows_req or 0,
        )

    def _get_fused_all_to_all_operator(
        self, down_op: AllToAllOperator, up_op: MapOperator
    ) -> AllToAllOperator:
        assert self._can_fuse(down_op, up_op), (
            "Current rule supports fusing MapOperator -> AllToAllOperator"
            f", but received: {type(up_op).__name__} -> {type(down_op).__name__}"
        )

        # Fuse operator names.
        name = up_op.name + "->" + down_op.name

        down_logical_op = self._op_map.pop(down_op)
        up_logical_op = self._op_map.pop(up_op)
        assert isinstance(down_logical_op, AbstractAllToAll)
        assert isinstance(up_logical_op, AbstractMap)

        # Fuse transformation functions.
        # Fusion owns the composition: fuse via MapTransformer.fuse() and run shuffle.
        ray_remote_args = up_logical_op.ray_remote_args
        up_map_transformer = up_op.get_map_transformer()
        data_context = down_op.data_context
        target_max_block_size = self._get_merged_target_max_block_size(
            up_op.target_max_block_size_override, down_op.target_max_block_size_override
        )

        is_random_shuffle = isinstance(down_logical_op, RandomShuffle)
        shuffle_seed = down_logical_op.seed if is_random_shuffle else None

        # Override target max-block sizing to avoid unnecessary block shaping.
        up_map_transformer.override_target_max_block_size(None)

        # Fuse via MapTransformer.fuse() for consistency with MapOp->MapOp fusion.
        shuffle_map_transformer = MapTransformer([ShuffleMapTransformFn()])
        fused_map_transformer = up_map_transformer.fuse(shuffle_map_transformer)

        def fused_all_to_all_transform_fn(
            blocks: List[RefBundle],
            ctx: TaskContext,
        ) -> AllToAllTransformFnResult:
            num_input_blocks = sum(len(r.blocks) for r in blocks)
            num_outputs = (
                down_logical_op.num_outputs
                if down_logical_op.num_outputs is not None
                else num_input_blocks
            )
            target_size = (
                ctx.target_max_block_size_override
                or target_max_block_size
                or data_context.target_max_block_size
            )
            # Pin timestamp-based seed when not specified (for retry safety)
            effective_seed = (
                shuffle_seed
                if shuffle_seed is not None
                else (time.time_ns() % INT32_MAX)
            )
            shuffle_spec = MapTransformerShuffleTaskSpec(
                map_transformer=fused_map_transformer,
                target_shuffle_max_block_size=target_size,
                random_shuffle=is_random_shuffle,
                random_seed=effective_seed,
            )
            return _execute_sort_shuffle(
                blocks,
                ctx,
                shuffle_spec,
                num_outputs,
                data_context,
                map_ray_remote_args=ray_remote_args,
                reduce_ray_remote_args=ray_remote_args,
            )

        # Make the upstream operator's inputs the new, fused operator's inputs.
        input_deps = up_op.input_dependencies
        assert len(input_deps) == 1
        input_op = input_deps[0]

        assert up_op.data_context is down_op.data_context
        op = AllToAllOperator(
            fused_all_to_all_transform_fn,
            input_op,
            up_op.data_context,
            target_max_block_size_override=target_max_block_size,
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
                num_outputs=down_logical_op.num_outputs,
                shuffle=down_logical_op.shuffle,
            )
        self._op_map[op] = logical_op
        # Return the fused physical operator.
        return op

    @classmethod
    def _can_fuse_map_ops(
        cls,
        upstream_op: AbstractMap,
        downstream_op: AbstractMap,
    ) -> bool:
        if (
            cls._fuse_compute_strategy(
                upstream_op.compute,
                downstream_op.compute,
            )
            is None
        ):
            return False

        # Do not fuse Map operators in case:
        #
        #   - Upstream could (potentially) drastically modify number of rows, while
        #   - Downstream has `min_rows_per_input_bundle` specified
        #
        # Fusing such transformations is not desirable as it could
        #
        #   - Drastically reduce parallelism for the upstream up (for ex, if
        #   fusing ``Read->MapBatches(batch_size=...)`` with large enough batch-size
        #   could drastically reduce parallelism level of the Read op)
        #
        #   - Potentially violate batching semantic by fusing
        #   ``Filter->MapBatches(batch_size=...)``
        #
        if (
            upstream_op.can_modify_num_rows
            # For historical consistency, we allow fusing `MapBatches` even if it
            # can modify the number of rows. Before #60448, `MapBatches` was
            # incorrectly marked as not modifying row counts, so it was always
            # fused. We preserve that behavior here to avoid regressions.
            #
            # For the full history, see https://github.com/ray-project/ray/pull/60756.
            and not isinstance(upstream_op, MapBatches)
        ) and downstream_op.min_rows_per_bundled_input is not None:
            logger.debug(
                f"Upstream operator '{upstream_op}' could be modifying # of input "
                f"rows, while downstream operator '{downstream_op}' expects at least "
                f"{downstream_op.min_rows_per_bundled_input} rows in a batch. "
                f"Skipping fusion"
            )

            return False

        return True


@DeveloperAPI
def are_remote_args_compatible(
    prev_args: Dict[str, Any], next_args: Dict[str, Any]
) -> bool:
    """Check if Ray remote arguments are compatible for merging."""
    prev_args = _canonicalize(prev_args)
    next_args = _canonicalize(next_args)
    remote_args = next_args.copy()
    for key in INHERITABLE_REMOTE_ARGS:
        # NOTE: We only carry over inheritable value in case
        #       of it not being provided in the remote args
        if key in prev_args and key not in remote_args:
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
