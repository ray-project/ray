from typing import Callable, Iterator

from ray.data.block import Block
from ray.data.datasource import Datasource
from ray.data._internal.execution.interfaces import TaskContext


def generate_write_transform(
    datasource: Datasource, **write_args
) -> Callable[[Iterator[Block]], Iterator[Block]]:
    """Generate function to transform data blocks into write result blocks."""

    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        yield [datasource.write(blocks, ctx, **write_args)]

    return fn


def generate_write_legacy_transform(
    datasource: Datasource, **write_args
) -> Callable[[Iterator[Block]], Iterator[Block]]:
    """Generate function to transform data blocks into write result blocks."""
    return generate_write_transform(datasource, **write_args)
