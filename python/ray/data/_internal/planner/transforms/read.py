from typing import Callable, Iterator

from ray.data.block import Block
from ray.data.context import DatasetContext
from ray.data.datasource.datasource import ReadTask
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.transforms.adapters import BlocksToBlocksAdapter


def generate_read_transform() -> Callable[[Iterator[ReadTask]], Iterator[Block]]:
    """Generate function to transform input read tasks into blocks."""

    def fn(blocks: Iterator[ReadTask], ctx: TaskContext) -> Iterator[Block]:
        for read_task in blocks:
            yield from read_task()

    return fn


def generate_read_legacy_transform() -> Callable[[Iterator[ReadTask]], Iterator[Block]]:
    """
    Generate function to transform input read tasks into blocks, with output buffering
    up to the target max block size.
    """
    read_transform = generate_read_transform()

    ctx = DatasetContext.get_current()
    buffer_blocks_adapter = BlocksToBlocksAdapter(ctx.target_max_block_size)

    def fn(blocks: Iterator[ReadTask], ctx: TaskContext) -> Iterator[Block]:
        blocks = read_transform(blocks, ctx)
        yield from buffer_blocks_adapter.adapt(blocks)

    return fn
