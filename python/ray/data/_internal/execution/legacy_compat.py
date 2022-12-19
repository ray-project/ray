"""This file contains temporary helper functions for legacy plan/executor interaction.

It should be deleted once we fully move to the new executor backend.
"""

import ray.cloudpickle as cloudpickle
from typing import Iterator

import ray
from ray.data.block import Block, BlockMetadata, List
from ray.data._internal.stats import StatsDict, DatasetStats
from ray.data._internal.block_list import BlockList
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.compute import get_compute
from ray.data._internal.util import _trace_allocation
from ray.data._internal.plan import ExecutionPlan, OneToOneStage, AllToAllStage, Stage
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.interfaces import (
    Executor,
    PhysicalOperator,
    RefBundle,
)


def execute_to_legacy_block_list(
    executor: Executor, plan: ExecutionPlan, allow_clear_input_blocks: bool
) -> BlockList:
    """Execute a plan with the new executor and translate it into a legacy block list.

    Args:
        executor: The executor to use.
        plan: The legacy plan to execute.
        allow_clear_input_blocks: Whether the executor may consider clearing blocks.

    Returns:
        The output as a legacy block list.
    """
    dag, stats = _to_operator_dag(plan, allow_clear_input_blocks)
    bundles = executor.execute(dag, initial_stats=stats)
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
            # Otherwise, we have non-lazy input blocks that's the source of this
            # execution plan, so we don't clear these.
            owns_blocks = False
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
                _trace_allocation(b[0], "legacy_compat.blocks_to_input_buf[0]")

        def do_read(blocks: Iterator[Block], _) -> Iterator[Block]:
            for read_task in blocks:
                for output_block in read_task():
                    yield output_block

        return MapOperator(do_read, inputs, name="DoRead")
    else:
        output = _block_list_to_bundles(blocks, owns_blocks=owns_blocks)
        for i in output:
            for b in i.blocks:
                _trace_allocation(b[0], "legacy_compat.blocks_to_input_buf[1]")
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
        if stage.fn_constructor_args or stage.fn_constructor_kwargs:
            raise NotImplementedError
        if stage.compute != "tasks":
            raise NotImplementedError

        block_fn = stage.block_fn
        # TODO: pass the following via object store instead of closure capture
        # TODO: implement arg packing and passing for test_map_batches_extra_args
        fn_args = (stage.fn,) if stage.fn else ()
        fn_args = fn_args + (stage.fn_args or ())
        fn_kwargs = stage.fn_kwargs or {}

        def do_map(blocks: Iterator[Block], _) -> Iterator[Block]:
            for output_block in block_fn(blocks, *fn_args, **fn_kwargs):
                yield output_block

        return MapOperator(
            do_map,
            input_op,
            name=stage.name,
            compute_strategy=get_compute(stage.compute),
            ray_remote_args=stage.ray_remote_args,
        )
    elif isinstance(stage, AllToAllStage):
        fn = stage.fn
        block_udf = stage.block_udf
        remote_args = stage.ray_remote_args
        stage_name = stage.name

        def bulk_fn(refs: List[RefBundle]) -> (List[RefBundle], StatsDict):
            owns_blocks = all(b.owns_blocks for b in refs)
            block_list = _bundles_to_block_list(refs)
            block_list, stats_dict = fn(block_list, owns_blocks, block_udf, remote_args)
            output = _block_list_to_bundles(block_list, owns_blocks=True)
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
