import itertools
import uuid
from typing import TYPE_CHECKING, Callable, Iterator, List, Optional, Union

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

if TYPE_CHECKING:
    from ray.data._internal.logical.operators import Write

WRITE_UUID_KWARG_NAME = "write_uuid"
# Key for storing pending checkpoint paths for commit phase
PENDING_CHECKPOINTS_KWARG_NAME = "_pending_checkpoints"
# Keys for stats recorded by ``generate_write_fn`` while the input blocks
# flow through the writer. These are consumed by
# ``generate_collect_write_stats_fn`` so that stats stay accurate even if
# the post-write iterator is no longer safe / cheap to re-iterate (e.g.
# the writer mutated the blocks in place).
_TOTAL_NUM_ROWS_KWARG_NAME = "_total_num_rows"
_TOTAL_SIZE_BYTES_KWARG_NAME = "_total_size_bytes"


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written."""
        # Record per-block stats inline so the downstream stats collector
        # can read totals from ``ctx.kwargs`` without re-iterating blocks
        # that the writer may have already mutated in place.
        total_num_rows = 0
        total_size_bytes = 0

        def _record_stats_passthrough(blocks: Iterator[Block]) -> Iterator[Block]:
            nonlocal total_num_rows, total_size_bytes
            for block in blocks:
                ba = BlockAccessor.for_block(block)
                total_num_rows += ba.num_rows()
                total_size_bytes += ba.size_bytes()
                yield block

        # ``tee`` lets us hand the blocks to the writer while still yielding
        # them downstream for post-transformations (e.g. checkpoint commit)
        # that pass blocks through without inspecting their contents.
        it1, it2 = itertools.tee(_record_stats_passthrough(blocks), 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            ctx.kwargs["_datasink_write_return"] = datasink_or_legacy_datasource.write(
                it1, ctx
            )
        else:
            datasink_or_legacy_datasource.write(it1, ctx, **write_args)

        # Drain ``it1`` so the stats generator runs for every block, even
        # when the datasink ignores the iterator (e.g. test datasinks that
        # only inspect the runtime context). Without this drain,
        # ``total_num_rows`` / ``total_size_bytes`` would stay 0 and be
        # propagated to ``WriteResult.num_rows`` via ``ctx.kwargs``. If the
        # datasink already exhausted ``it1``, this loop is a no-op; if it
        # didn't, ``tee`` buffers the yielded blocks for ``it2`` to consume
        # next.
        for _ in it1:
            pass

        ctx.kwargs[_TOTAL_NUM_ROWS_KWARG_NAME] = total_num_rows
        ctx.kwargs[_TOTAL_SIZE_BYTES_KWARG_NAME] = total_size_bytes

        return it2

    return fn


def generate_collect_write_stats_fn() -> BlockMapTransformFn:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Handles stats collection for block writes."""
        # Stats are recorded inline by ``generate_write_fn``. Totals come from
        # ``ctx.kwargs``; we still exhaust ``blocks`` to clear ``tee`` buffers
        # (see ``generate_write_fn``) without re-reading block metadata.
        if (
            _TOTAL_NUM_ROWS_KWARG_NAME in ctx.kwargs
            and _TOTAL_SIZE_BYTES_KWARG_NAME in ctx.kwargs
        ):
            total_num_rows = ctx.kwargs[_TOTAL_NUM_ROWS_KWARG_NAME]
            total_size_bytes = ctx.kwargs[_TOTAL_SIZE_BYTES_KWARG_NAME]
            # ``generate_write_fn`` tees the block stream: after ``it1`` is
            # drained, tee still holds each value until ``it2`` is consumed.
            for _ in blocks:
                pass
        else:
            # Fallback for callers that invoke this transform without going
            # through ``generate_write_fn`` first (e.g. some unit tests).
            block_accessors = [BlockAccessor.for_block(block) for block in blocks]
            total_num_rows = sum(ba.num_rows() for ba in block_accessors)
            total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame(
            {
                "num_rows": [total_num_rows],
                "size_bytes": [total_size_bytes],
                "write_return": [ctx.kwargs.get("_datasink_write_return", None)],
            }
        )
        return iter([block])

    return BlockMapTransformFn(
        fn,
        is_udf=False,
        disable_block_shaping=True,
    )


def plan_write_op(
    op: "Write",
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    collect_stats_fn = generate_collect_write_stats_fn()

    return _plan_write_op_internal(
        op,
        physical_children,
        data_context,
        post_transformations=[collect_stats_fn],
    )


def _plan_write_op_internal(
    op: "Write",
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    post_transformations: List[BlockMapTransformFn],
    pre_transformations: Optional[List[BlockMapTransformFn]] = None,
) -> PhysicalOperator:
    """Plan a write operation with optional pre and post write transformations.

    Args:
        op: The write operator.
        physical_children: The physical children operators.
        data_context: The data context.
        post_transformations: Transformations to run AFTER the write.
        pre_transformations: Transformations to run BEFORE the write.
            Useful for 2-phase commit where pending checkpoint is written first.

    Returns:
        The physical operator for the write operation.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    datasink = op.datasink_or_legacy_datasource
    write_fn = generate_write_fn(datasink, **op.write_args)

    # Build transform chain: pre_write -> write -> post_write
    pre_transforms = pre_transformations or []
    write_transform = BlockMapTransformFn(
        write_fn,
        is_udf=False,
        # NOTE: No need for block-shaping
        disable_block_shaping=True,
    )
    transform_fns = pre_transforms + [write_transform] + post_transformations

    map_transformer = MapTransformer(transform_fns)

    # Set up on_start callback for datasinks.
    # This allows on_write_start to receive the schema from the first input bundle,
    # enabling schema-dependent initialization (e.g., Iceberg schema evolution).
    on_start = None
    if isinstance(datasink, Datasink):
        on_start = datasink.on_write_start

    map_op = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="Write",
        # Add a UUID to write tasks to prevent filename collisions. This a UUID for the
        # overall write operation, not the individual write tasks.
        map_task_kwargs={WRITE_UUID_KWARG_NAME: uuid.uuid4().hex},
        ray_remote_args=op.ray_remote_args,
        min_rows_per_bundle=op.min_rows_per_bundled_input,
        compute_strategy=op.compute,
        on_start=on_start,
    )

    return map_op
