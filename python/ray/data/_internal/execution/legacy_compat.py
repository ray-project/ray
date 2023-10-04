"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""

from typing import Any, Iterator, Tuple

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import ActorPoolStrategy, get_compute
from ray.data._internal.execution.interfaces import (
    Executor,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.util import make_callable_class_concurrent
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.util import record_operators_usage
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.plan import AllToAllStage, ExecutionPlan, OneToOneStage, Stage
from ray.data._internal.stage_impl import LimitStage, RandomizeBlocksStage
from ray.data._internal.stats import DatasetStats, StatsDict
from ray.data._internal.util import validate_compute
from ray.data.block import Block, BlockMetadata, CallableClass, List
from ray.data.context import DataContext
from ray.data.datasource import ReadTask
from ray.types import ObjectRef

# Warn about tasks larger than this.
TASK_SIZE_WARN_THRESHOLD_BYTES = 100000


def execute_to_legacy_block_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
    """Same as execute_to_legacy_bundle_iterator but returning blocks and metadata."""
    bundle_iter = execute_to_legacy_bundle_iterator(
        executor, plan, allow_clear_input_blocks, dataset_uuid
    )
    for bundle in bundle_iter:
        for block, metadata in bundle.blocks:
            yield block, metadata


def execute_to_legacy_bundle_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
    dag_rewrite=None,
) -> Iterator[RefBundle]:
    """Execute a plan with the new executor and return a bundle iterator.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
        dataset_uuid: UUID of the dataset for this execution.
        dag_rewrite: Callback that can be used to mutate the DAG prior to execution.
            This is currently used as a legacy hack to inject the OutputSplit operator
            for `Dataset.streaming_split()`.

    Returns:
        The output as a bundle iterator.
    """
    dag, stats = _get_execution_dag(
        executor,
        plan,
        allow_clear_input_blocks,
        preserve_order=False,
    )
    if dag_rewrite:
        dag = dag_rewrite(dag)

    bundle_iter = executor.execute(dag, initial_stats=stats)
    return bundle_iter


def execute_to_legacy_block_list(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
    preserve_order: bool,
) -> BlockList:
    """Execute a plan with the new executor and translate it into a legacy block list.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
        dataset_uuid: UUID of the dataset for this execution.
        preserve_order: Whether to preserve order in execution.

    Returns:
        The output as a legacy block list.
    """
    dag, stats = _get_execution_dag(
        executor,
        plan,
        allow_clear_input_blocks,
        preserve_order,
    )
    bundles = executor.execute(dag, initial_stats=stats)
    block_list = _bundles_to_block_list(bundles)
    # Set the stats UUID after execution finishes.
    _set_stats_uuid_recursive(executor.get_stats(), dataset_uuid)
    return block_list


def _get_execution_dag(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    preserve_order: bool,
) -> Tuple[PhysicalOperator, DatasetStats]:
    """Get the physical operators DAG from a plan."""
    # Record usage of logical operators if available.
    if hasattr(plan, "_logical_plan") and plan._logical_plan is not None:
        record_operators_usage(plan._logical_plan.dag)

    # Get DAG of physical operators and input statistics.
    if (
        DataContext.get_current().optimizer_enabled
        # TODO(scottjlee): remove this once we remove DatasetPipeline.
        and not plan._generated_from_pipeline
    ):
        dag = get_execution_plan(plan._logical_plan).dag
        stats = _get_initial_stats_from_plan(plan)
    else:
        dag, stats = _to_operator_dag(plan, allow_clear_input_blocks)

    # Enforce to preserve ordering if the plan has stages required to do so, such as
    # Zip and Sort.
    # TODO(chengsu): implement this for operator as well.
    if preserve_order or plan.require_preserve_order():
        executor._options.preserve_order = True

    return dag, stats


def _get_initial_stats_from_plan(plan: ExecutionPlan) -> DatasetStats:
    assert DataContext.get_current().optimizer_enabled
    if plan._snapshot_blocks is not None and not plan._snapshot_blocks.is_cleared():
        return plan._snapshot_stats
    # For Datasets created from "read_xxx", `plan._in_blocks` is a LazyBlockList,
    # and `plan._in_stats` contains useless data.
    # For Datasets created from "from_xxx", we need to use `plan._in_stats` as
    # the initial stats. Because the `FromXxx` logical operators will be translated to
    # "InputDataBuffer" physical operators, which will be ignored when generating
    # stats, see `StreamingExecutor._generate_stats`.
    # TODO(hchen): Unify the logic by saving the initial stats in `InputDataBuffer
    if isinstance(plan._in_blocks, LazyBlockList):
        return DatasetStats(stages={}, parent=None)
    else:
        return plan._in_stats


def _to_operator_dag(
    plan: ExecutionPlan, allow_clear_input_blocks: bool
) -> Tuple[PhysicalOperator, DatasetStats]:
    """Translate a plan into an operator DAG for the new execution backend."""

    blocks, stats, stages = plan._optimize()
    if allow_clear_input_blocks:
        if isinstance(blocks, LazyBlockList):
            # Always clear lazy input blocks since they can be recomputed.
            owns_blocks = True
        else:
            # Otherwise, defer to the block's ownership status.
            owns_blocks = blocks._owned_by_consumer
    else:
        owns_blocks = False
    operator = _blocks_to_input_buffer(blocks, owns_blocks)
    for stage in stages:
        operator = _stage_to_operator(stage, operator)
    return operator, stats


def _blocks_to_input_buffer(blocks: BlockList, owns_blocks: bool) -> PhysicalOperator:
    """Translate a block list into an InputBuffer operator.

    Args:
        blocks: The block list to translate.
        owns_blocks: Whether we can take ownership of the input blocks.

    Returns:
        The physical operator representing the input block list.
    """

    if hasattr(blocks, "_tasks"):
        read_tasks = blocks._tasks
        remote_args = blocks._remote_args
        assert all(isinstance(t, ReadTask) for t in read_tasks), read_tasks

        from ray.data._internal.planner.plan_read_op import cleaned_metadata

        inputs = InputDataBuffer(
            [
                RefBundle(
                    [
                        (
                            # This isn't a proper block, but it's what we are doing
                            # in the legacy code.
                            ray.put(read_task),
                            cleaned_metadata(read_task),
                        )
                    ],
                    # `owns_blocks` is False, because these refs are the root of the
                    # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
                    # be reconstructed.
                    owns_blocks=False,
                )
                for read_task in read_tasks
            ]
        )

        for i in inputs._input_data:
            for b in i.blocks:
                trace_allocation(b[0], "legacy_compat.blocks_to_input_buf[0]")

        def do_read(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
            for read_task in blocks:
                yield from read_task()

        # If the BlockList's read stage name is available, we assign it
        # as the operator's name, which is used as the task name.
        task_name = "Read"
        if isinstance(blocks, LazyBlockList):
            task_name = getattr(blocks, "_read_stage_name", task_name)
        return MapOperator.create(
            create_map_transformer_from_block_fn(do_read),
            inputs,
            name=task_name,
            ray_remote_args=remote_args,
        )
    else:
        output = _block_list_to_bundles(blocks, owns_blocks=owns_blocks)
        for i in output:
            for b in i.blocks:
                trace_allocation(b[0], "legacy_compat.blocks_to_input_buf[1]")
        return InputDataBuffer(output)


def _stage_to_operator(stage: Stage, input_op: PhysicalOperator) -> PhysicalOperator:
    """Translate a stage into a PhysicalOperator.

    Args:
        stage: The stage to translate.
        input_op: The upstream operator (already translated).

    Returns:
        The translated operator that depends on the input data.
    """

    if isinstance(stage, OneToOneStage):
        compute = get_compute(stage.compute)
        validate_compute(stage.fn, compute)

        block_fn = stage.block_fn
        if stage.fn:
            if isinstance(stage.fn, CallableClass):
                assert isinstance(compute, ActorPoolStrategy)

                fn_constructor_args = stage.fn_constructor_args or ()
                fn_constructor_kwargs = stage.fn_constructor_kwargs or {}

                fn_ = make_callable_class_concurrent(stage.fn)

                def fn(item: Any) -> Any:
                    assert ray.data._cached_fn is not None
                    assert ray.data._cached_cls == fn_
                    return ray.data._cached_fn(item)

                def init_fn():
                    if ray.data._cached_fn is None:
                        ray.data._cached_cls = fn_
                        ray.data._cached_fn = fn_(
                            *fn_constructor_args, **fn_constructor_kwargs
                        )

            else:
                fn = stage.fn
                init_fn = None
            fn_args = (fn,)
        else:
            fn_args = ()
            init_fn = None
        if stage.fn_args:
            fn_args += stage.fn_args
        fn_kwargs = stage.fn_kwargs or {}

        def do_map(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
            yield from block_fn(blocks, ctx, *fn_args, **fn_kwargs)

        return MapOperator.create(
            create_map_transformer_from_block_fn(do_map, init_fn),
            input_op,
            name=stage.name,
            compute_strategy=compute,
            min_rows_per_bundle=stage.target_block_size,
            ray_remote_args=stage.ray_remote_args,
        )
    elif isinstance(stage, LimitStage):
        return LimitOperator(stage.limit, input_op)
    elif isinstance(stage, AllToAllStage):
        fn = stage.fn
        block_udf = stage.block_udf
        remote_args = stage.ray_remote_args
        stage_name = stage.name

        def bulk_fn(
            refs: List[RefBundle], ctx: TaskContext
        ) -> Tuple[List[RefBundle], StatsDict]:
            input_owned = all(b.owns_blocks for b in refs)
            if isinstance(stage, RandomizeBlocksStage):
                output_owned = input_owned  # Passthrough ownership hack.
            else:
                output_owned = True
            block_list = _bundles_to_block_list(refs)
            block_list, stats_dict = fn(
                block_list, ctx, input_owned, block_udf, remote_args
            )
            output = _block_list_to_bundles(block_list, owns_blocks=output_owned)
            if not stats_dict:
                stats_dict = {stage_name: block_list.get_metadata()}
            return output, stats_dict

        return AllToAllOperator(
            bulk_fn,
            input_op,
            name=stage.name,
            num_outputs=stage.num_blocks,
            sub_progress_bar_names=stage.sub_stage_names,
        )
    else:
        raise NotImplementedError


def _bundles_to_block_list(bundles: Iterator[RefBundle]) -> BlockList:
    blocks, metadata = [], []
    owns_blocks = True
    for ref_bundle in bundles:
        if not ref_bundle.owns_blocks:
            owns_blocks = False
        for block, meta in ref_bundle.blocks:
            blocks.append(block)
            metadata.append(meta)
    return BlockList(blocks, metadata, owned_by_consumer=owns_blocks)


def _block_list_to_bundles(blocks: BlockList, owns_blocks: bool) -> List[RefBundle]:
    output = []
    for block, meta in blocks.iter_blocks_with_metadata():
        output.append(
            RefBundle(
                [
                    (
                        block,
                        meta,
                    )
                ],
                owns_blocks=owns_blocks,
            )
        )
    return output


def _set_stats_uuid_recursive(stats: DatasetStats, dataset_uuid: str) -> None:
    if not stats.dataset_uuid:
        stats.dataset_uuid = dataset_uuid
    for parent in stats.parents or []:
        _set_stats_uuid_recursive(parent, dataset_uuid)
