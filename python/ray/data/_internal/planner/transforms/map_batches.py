import sys
from typing import Callable, Iterator, Optional, Iterable, Any, Dict

from ray.data.block import BatchUDF, Block, DataBatch
from ray.data.context import DEFAULT_BATCH_SIZE, DatasetContext
from ray.data._internal.compute import UDF
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.transforms.adapters import (
    BlocksToBatchesAdapter,
    BatchesToBlocksAdapter,
    BlocksToBlocksAdapter,
)


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


def generate_map_batches_transform(
    fn: UDF,
    fn_args: Optional[Iterable[Any]] = None,
    fn_kwargs: Optional[Dict[str, Any]] = None,
) -> Callable[[Iterator[DataBatch]], Iterator[DataBatch]]:
    """Generate function to apply the batch UDF to blocks."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    fn_args = fn_args or ()
    fn_kwargs = fn_kwargs or {}

    def fn_(batches: Iterator[DataBatch], ctx: TaskContext) -> Iterator[DataBatch]:
        def validate_batch(batch: Block) -> None:
            if not isinstance(
                batch, (list, pa.Table, np.ndarray, dict, pd.core.frame.DataFrame)
            ):
                raise ValueError(
                    "The `fn` you passed to `map_batches` returned a value of type "
                    f"{type(batch)}. This isn't allowed -- `map_batches` expects "
                    "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
                    "`numpy.ndarray`, `list`, or `dict[str, numpy.ndarray]`."
                )

            if isinstance(batch, dict):
                for key, value in batch.items():
                    if not isinstance(value, np.ndarray):
                        raise ValueError(
                            "The `fn` you passed to `map_batches` returned a "
                            f"`dict`. `map_batches` expects all `dict` values "
                            f"to be of type `numpy.ndarray`, but the value "
                            f"corresponding to key {key!r} is of type "
                            f"{type(value)}. To fix this issue, convert "
                            f"the {type(value)} to a `numpy.ndarray`."
                        )

        def process_next_batch(batch: DataBatch) -> DataBatch:
            # Apply UDF.
            try:
                batch = fn(batch, *fn_args, **fn_kwargs)
            except ValueError as e:
                read_only_msgs = [
                    "assignment destination is read-only",
                    "buffer source array is read-only",
                ]
                err_msg = str(e)
                if any(msg in err_msg for msg in read_only_msgs):
                    raise ValueError(
                        f"Batch mapper function {fn.__name__} tried to mutate a "
                        "zero-copy read-only batch. To be able to mutate the "
                        "batch, pass zero_copy_batch=False to map_batches(); "
                        "this will create a writable copy of the batch before "
                        "giving it to fn. To elide this copy, modify your mapper "
                        "function so it doesn't try to mutate its input."
                    ) from e
                else:
                    raise e from None

            validate_batch(batch)
            return batch

        for batch in batches:
            yield process_next_batch(batch)

    return fn_


def generate_map_batches_legacy_transform(
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
    prefetch_batches: int = 0,
    zero_copy_batch: bool = False,
) -> Callable[[Iterator[Block], TaskContext, BatchUDF], Iterator[Block]]:
    """Generate function to apply the batch UDF to blocks."""
    # Get batching adapter transform.
    blocks_to_batches_adapter = BlocksToBatchesAdapter(
        batch_size, batch_format, prefetch_batches, zero_copy_batch
    )
    batches_to_blocks_adapter = BatchesToBlocksAdapter()
    # Get a block-buffering transform for dynamic block splitting.
    target_max_block_size = DatasetContext.get_current().target_max_block_size
    buffer_blocks_adapter = BlocksToBlocksAdapter(target_max_block_size)

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        batch_fn: BatchUDF,
        *fn_args,
        **fn_kwargs,
    ) -> Iterator[Block]:
        # Generate the map_batches transform with bound fn_args and fn_kwargs.
        transform = generate_map_batches_transform(batch_fn, fn_args, fn_kwargs)

        batches = blocks_to_batches_adapter.adapt(blocks)
        batches = transform(batches, ctx)
        blocks = batches_to_blocks_adapter.adapt(batches)
        yield from buffer_blocks_adapter.adapt(blocks)

    return fn
