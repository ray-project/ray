import itertools
from typing import Iterable, List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
)
from ray.data._internal.logical.operators import Write
from ray.data._internal.planner.plan_write_op import (
    _plan_write_op_internal,
    generate_collect_write_stats_fn,
)
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.checkpoint_writer import CheckpointWriter
from ray.data.checkpoint.interfaces import (
    InvalidCheckpointingOperators,
)
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink


def plan_write_op_with_checkpoint_writer(
    op: Write, physical_children: List[PhysicalOperator], data_context: DataContext
) -> PhysicalOperator:
    assert data_context.checkpoint_config is not None

    collect_stats_fn = generate_collect_write_stats_fn()
    write_checkpoint_for_block_fn = _generate_checkpoint_writing_transform(
        data_context, op
    )

    physical_op = _plan_write_op_internal(
        op,
        physical_children,
        data_context,
        extra_transformations=[
            write_checkpoint_for_block_fn,
            collect_stats_fn,
        ],
    )

    return physical_op


def _generate_checkpoint_writing_transform(
    data_context: DataContext, logical_op: Write
) -> BlockMapTransformFn:
    datasink = logical_op._datasink_or_legacy_datasource
    if not isinstance(datasink, Datasink):
        raise InvalidCheckpointingOperators(
            f"To enable checkpointing, Write operation must use a "
            f"Datasink and not a legacy Datasource, but got: "
            f"{type(datasink)}"
        )

    checkpoint_writer = CheckpointWriter.create(data_context.checkpoint_config)

    # MapTransformFn for writing checkpoint files after write completes.
    def write_checkpoint_for_block(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        it1, it2 = itertools.tee(blocks, 2)
        for block in it1:
            ba = BlockAccessor.for_block(block)
            if ba.num_rows() > 0:
                if data_context.checkpoint_config.id_column not in ba.column_names():
                    raise ValueError(
                        f"ID column {data_context.checkpoint_config.id_column} is "
                        f"absent in the block to be written. Do not drop or rename "
                        f"this column."
                    )
            checkpoint_writer.write_block_checkpoint(ba)

        return list(it2)

    return BlockMapTransformFn(
        write_checkpoint_for_block,
        is_udf=False,
        # NOTE: No need for block-shaping
        disable_block_shaping=True,
    )
