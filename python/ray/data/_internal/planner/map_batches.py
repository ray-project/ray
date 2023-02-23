import sys
from typing import Callable, Iterator, Optional

from ray.data._internal.block_batching import batch_blocks
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import BatchUDF, Block, DataBatch
from ray.data.context import DEFAULT_BATCH_SIZE, DatasetContext


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


def generate_map_batches_fn(
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
    prefetch_batches: int = 0,
    zero_copy_batch: bool = False,
) -> Callable[[Iterator[Block], TaskContext, BatchUDF], Iterator[Block]]:
    """Generate function to apply the batch UDF to blocks."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    context = DatasetContext.get_current()

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        batch_fn: BatchUDF,
        *fn_args,
        **fn_kwargs,
    ) -> Iterator[Block]:
        DatasetContext._set_current(context)
        output_buffer = BlockOutputBuffer(None, context.target_max_block_size)

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

        def process_next_batch(batch: DataBatch) -> Iterator[Block]:
            # Apply UDF.
            try:
                batch = batch_fn(batch, *fn_args, **fn_kwargs)
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
            # Add output batch to output buffer.
            output_buffer.add_batch(batch)
            if output_buffer.has_next():
                yield output_buffer.next()

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=batch_size,
            batch_format=batch_format,
            ensure_copy=not zero_copy_batch and batch_size is not None,
            prefetch_batches=prefetch_batches,
        )

        for batch in formatted_batch_iter:
            yield from process_next_batch(batch)

        # Yield remainder block from output buffer.
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()

    return fn
