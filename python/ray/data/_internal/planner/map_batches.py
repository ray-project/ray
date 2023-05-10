import collections
from types import GeneratorType
from typing import Callable, Iterator, Optional

from ray.data._internal.block_batching import batch_blocks
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _truncated_repr
from ray.data.block import UserDefinedFunction, Block, DataBatch
from ray.data.context import DEFAULT_BATCH_SIZE, DataContext


def generate_map_batches_fn(
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    batch_format: Optional[str] = "default",
    zero_copy_batch: bool = False,
) -> Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]:
    """Generate function to apply the batch UDF to blocks."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        batch_fn: UserDefinedFunction,
        *fn_args,
        **fn_kwargs,
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        output_buffer = BlockOutputBuffer(None, context.target_max_block_size)

        def validate_batch(batch: Block) -> None:
            if not isinstance(
                batch,
                (
                    list,
                    pa.Table,
                    np.ndarray,
                    collections.abc.Mapping,
                    pd.core.frame.DataFrame,
                ),
            ):
                raise ValueError(
                    "The `fn` you passed to `map_batches` returned a value of type "
                    f"{type(batch)}. This isn't allowed -- `map_batches` expects "
                    "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
                    "`numpy.ndarray`, `list`, or `dict[str, numpy.ndarray]`."
                )

            if isinstance(batch, collections.abc.Mapping):
                for key, value in list(batch.items()):
                    if not isinstance(value, (np.ndarray, list)):
                        raise ValueError(
                            f"Error validating {_truncated_repr(batch)}: "
                            "The `fn` you passed to `map_batches` returned a "
                            f"`dict`. `map_batches` expects all `dict` values "
                            f"to be `list` or `np.ndarray` type, but the value "
                            f"corresponding to key {key!r} is of type "
                            f"{type(value)}. To fix this issue, convert "
                            f"the {type(value)} to a `np.ndarray`."
                        )

        def process_next_batch(batch: DataBatch) -> Iterator[Block]:
            # Apply UDF.
            try:
                batch = batch_fn(batch, *fn_args, **fn_kwargs)

                if not isinstance(batch, GeneratorType):
                    batch = [batch]

                for b in batch:
                    validate_batch(b)
                    # Add output batch to output buffer.
                    output_buffer.add_batch(b)
                    if output_buffer.has_next():
                        yield output_buffer.next()
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

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=batch_size,
            batch_format=batch_format,
            ensure_copy=not zero_copy_batch and batch_size is not None,
        )

        for batch in formatted_batch_iter:
            yield from process_next_batch(batch)

        # Yield remainder block from output buffer.
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()

    return fn
