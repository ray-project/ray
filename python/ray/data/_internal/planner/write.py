from typing import Callable, Iterator

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Datasource


def generate_write_fn(
    datasource: Datasource, **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    # If the write op succeeds, the resulting Dataset is a list of
    # WriteResult (one element per write task). Otherwise, an error will
    # be raised. The Datasource can handle execution outcomes with the
    # on_write_complete() and on_write_failed().
    def fn(blocks: Iterator[Block], ctx) -> Iterator[Block]:
        # NOTE: `WriteResult` isn't a valid block type, so we need to wrap it up.
        import pandas as pd

        block = pd.DataFrame(
            {"write_result": [datasource.write(blocks, ctx, **write_args)]}
        )
        return [block]

    return fn
