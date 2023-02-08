"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""

import ray.cloudpickle as cloudpickle
from typing import Iterator, Tuple, Any

import ray
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.context import DatasetContext
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, List
from ray.data.datasource import ReadTask
from ray.data._internal.stats import StatsDict, DatasetStats
from ray.data._internal.stage_impl import RandomizeBlocksStage
from ray.data._internal.block_list import BlockList
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.compute import (
    get_compute,
    CallableClass,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.plan import ExecutionPlan, OneToOneStage, AllToAllStage, Stage
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.interfaces import (
    Executor,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)


def execute_to_legacy_block_iterator(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
) -> Iterator[ObjectRef[Block]]:
    """Execute a plan with the new executor and return a block iterator.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
        dataset_uuid: UUID of the dataset for this execution.

    Returns:
        The output as a block iterator.
    """
    dag, stats = _to_operator_dag(plan, allow_clear_input_blocks)
    bundle_iter = executor.execute(dag, initial_stats=stats)

    for bundle in bundle_iter:
        for block, _ in bundle.blocks:
            yield block


def execute_to_legacy_block_list(
    executor: Executor,
    plan: ExecutionPlan,
    allow_clear_input_blocks: bool,
    dataset_uuid: str,
) -> BlockList:
    """Execute a plan with the new executor and translate it into a legacy block list.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.
        dataset_uuid: UUID of the dataset for this execution.

    Returns:
        The output as a legacy block list.
    """
    if DatasetContext.get_current().optimizer_enabled:
        dag, stats = get_execution_plan(plan._logical_plan).dag, None
    else:
        dag, stats = _to_operator_dag(plan, allow_clear_input_blocks)
    bundles = executor.execute(dag, initial_stats=stats)
    _set_stats_uuid_recursive(executor.get_stats(), dataset_uuid)
    return _bundles_to_block_list(bundles)


def _to_operator_dag(
    plan: ExecutionPlan, allow_clear_input_blocks: bool
) -> (PhysicalOperator, DatasetStats):
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
        assert all(isinstance(t, ReadTask) for t in read_tasks), read_tasks
        inputs = InputDataBuffer(
            [
                RefBundle(
                    [
                        (
                            # This isn't a proper block, but it's what we are doing
                            # in the legacy code.
                            ray.put(read_task),
                            BlockMetadata(
                                num_rows=1,
                                size_bytes=len(cloudpickle.dumps(read_task)),
                                schema=None,
                                input_files=[],
                                exec_stats=None,
                            ),
                        )
                    ],
                    owns_blocks=True,
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

        return MapOperator.create(do_read, inputs, name="DoRead")
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

        block_fn = stage.block_fn
        if stage.fn:
            if isinstance(stage.fn, CallableClass):
                if isinstance(compute, TaskPoolStrategy):
                    raise ValueError(
                        "``compute`` must be specified when using a callable class, "
                        "and must specify the actor compute strategy. "
                        'For example, use ``compute="actors"`` or '
                        "``compute=ActorPoolStrategy(min, max)``."
                    )
                assert isinstance(compute, ActorPoolStrategy)

                fn_constructor_args = stage.fn_constructor_args or ()
                fn_constructor_kwargs = stage.fn_constructor_kwargs or {}
                fn_ = stage.fn

                def fn(item: Any) -> Any:
                    # Wrapper providing cached instantiation of stateful callable class
                    # UDFs.
                    if ray.data._cached_fn is None:
                        ray.data._cached_cls = fn_
                        ray.data._cached_fn = fn_(
                            *fn_constructor_args, **fn_constructor_kwargs
                        )
                    else:
                        # A worker is destroyed when its actor is killed, so we
                        # shouldn't have any worker reuse across different UDF
                        # applications (i.e. different map operators).
                        assert ray.data._cached_cls == fn_
                    return ray.data._cached_fn(item)

            else:
                fn = stage.fn
            fn_args = (fn,)
        else:
            fn_args = ()
        if stage.fn_args:
            fn_args += stage.fn_args
        fn_kwargs = stage.fn_kwargs or {}

        def do_map(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
            yield from block_fn(blocks, ctx, *fn_args, **fn_kwargs)

        return MapOperator.create(
            do_map,
            input_op,
            name=stage.name,
            compute_strategy=compute,
            min_rows_per_bundle=stage.target_block_size,
            ray_remote_args=stage.ray_remote_args,
        )
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
        )
    else:
        raise NotImplementedError


def _bundles_to_block_list(bundles: Iterator[RefBundle]) -> BlockList:
    blocks, metadata = [], []
    for ref_bundle in bundles:
        for block, meta in ref_bundle.blocks:
            blocks.append(block)
            metadata.append(meta)
    owns_blocks = all(b.owns_blocks for b in bundles)
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
