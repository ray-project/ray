import uuid
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, List, Optional, Union

from ray.data._internal.execution.bundle_queue import EstimateBytes, RebundleQueue
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
# Key for storing the write stats accumulator on `TaskContext.kwargs`
WRITE_STATS_KWARG_NAME = "_write_stats"


class _WriteStats:
    """Row and byte totals for one write task's input.

    This is all the output stats block needs from the input, so it is
    accumulated while blocks are on their way to the sink rather than by
    holding the blocks and measuring them afterwards.
    """

    __slots__ = ("num_rows", "size_bytes")

    def __init__(self) -> None:
        self.num_rows = 0
        self.size_bytes = 0

    def add(self, block: Block) -> None:
        accessor = BlockAccessor.for_block(block)
        self.num_rows += accessor.num_rows()
        self.size_bytes += accessor.size_bytes()


class _StatsCollectingBlocks(Iterator[Block]):
    """Block pass-through that accumulates :class:`_WriteStats` as blocks flow.

    ``itertools.tee`` used to give the write and the stats collection their own
    view of the same blocks, but the two views are read one after the other:
    the sink drains its copy first, so tee buffers the whole input for the
    later reader. The stats are two integers, so count blocks on the way past
    and hold none of them.
    """

    __slots__ = ("_blocks", "_stats")

    def __init__(self, blocks: Iterable[Block], stats: _WriteStats) -> None:
        self._blocks = iter(blocks)
        self._stats = stats

    def __iter__(self) -> "_StatsCollectingBlocks":
        return self

    def __next__(self) -> Block:
        block = next(self._blocks)
        self._stats.add(block)
        return block


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs no blocks: the write stats accumulate on `ctx.kwargs`, and
        `generate_collect_write_stats_fn` turns them into the operator's
        single output block. Returning the written blocks would mean holding
        every one of them until the stats stage read them."""
        stats = _WriteStats()
        ctx.kwargs[WRITE_STATS_KWARG_NAME] = stats
        blocks = _StatsCollectingBlocks(blocks, stats)

        if isinstance(datasink_or_legacy_datasource, Datasink):
            ctx.kwargs["_datasink_write_return"] = datasink_or_legacy_datasource.write(
                blocks, ctx
            )
        else:
            datasink_or_legacy_datasource.write(blocks, ctx, **write_args)

        # The stats cover the whole input, not just what the sink pulled: a
        # sink may stop early, and the blocks it did not read still arrived at
        # this task. Drain the rest through the counter, dropping each block.
        for _ in blocks:
            pass

        return iter(())

    return fn


def generate_collect_write_stats_fn() -> BlockMapTransformFn:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Handles stats collection for block writes."""
        # Drain before reading `ctx` below: consuming the input is what runs
        # the write stage, and the write is what sets `_datasink_write_return`
        # and the stats. Count the blocks as they drain, so callers that pass
        # the blocks straight in (rather than driving this from
        # `generate_write_fn`) still get their stats without holding them.
        drained = _WriteStats()
        for block in blocks:
            drained.add(block)

        stats = ctx.kwargs.pop(WRITE_STATS_KWARG_NAME, drained)

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame(
            {
                "num_rows": [stats.num_rows],
                "size_bytes": [stats.size_bytes],
                "write_return": [ctx.kwargs.pop("_datasink_write_return", None)],
            }
        )
        return iter([block])

    return BlockMapTransformFn(
        fn,
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

    min_bytes_per_bundle = (
        datasink.min_bytes_per_write if isinstance(datasink, Datasink) else None
    )
    ref_bundler = None
    supports_fusion = True
    if min_bytes_per_bundle is not None:
        ref_bundler = RebundleQueue(EstimateBytes(min_bytes_per_bundle))
        supports_fusion = False

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
        ref_bundler=ref_bundler,
        supports_fusion=supports_fusion,
        compute_strategy=op.compute,
        on_start=on_start,
    )

    return map_op
