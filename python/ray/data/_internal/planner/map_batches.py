import itertools
from types import GeneratorType
from typing import Callable, Iterable, Iterator, Optional

from ray.data._internal.block_batching import batch_blocks
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.planner.plan_udf_map_op import validate_batch
from ray.data.block import Block, BlockAccessor, DataBatch, UserDefinedFunction
from ray.data.context import DEFAULT_BATCH_SIZE, DataContext


def generate_map_batches_fn(
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    batch_format: Optional[str] = "default",
    zero_copy_batch: bool = False,
) -> Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]:
    """Generate function to apply the batch UDF to blocks."""
    context = DataContext.get_current()

    def fn(
        blocks: Iterable[Block],
        task_context: TaskContext,
        batch_fn: UserDefinedFunction,
        *fn_args,
        **fn_kwargs,
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        output_buffer = BlockOutputBuffer(None, context.target_max_block_size)

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

        try:
            block_iter = iter(blocks)
            first_block = next(block_iter)
            blocks = itertools.chain([first_block], block_iter)
            empty_block = BlockAccessor.for_block(first_block).builder().build()
            # Don't hold the first block in memory, so we reset the reference.
            first_block = None
        except StopIteration:
            first_block = None

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=batch_size,
            batch_format=batch_format,
            ensure_copy=not zero_copy_batch and batch_size is not None,
        )

        has_batches = False
        for batch in formatted_batch_iter:
            has_batches = True
            yield from process_next_batch(batch)

        if not has_batches:
            # If the input blocks are all empty, then yield an empty block with same
            # format as the input blocks.
            yield empty_block
        else:
            # Yield remainder block from output buffer.
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

    return fn
