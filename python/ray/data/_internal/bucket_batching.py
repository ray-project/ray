from numbers import Integral
from typing import Any, Callable, Dict, Iterator, Optional

from ray.data.block import Block, BlockAccessor, DataBatch


def validate_bucket_batching(max_tokens: int, buffer_size: int, length_fn: Callable):
    for name, value in (("max_tokens", max_tokens), ("buffer_size", buffer_size)):
        if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
            raise ValueError(f"{name} must be a positive integer, got {value!r}.")
    if not callable(length_fn):
        raise TypeError("length_fn must be callable.")


def bucket_batches(
    block: Block,
    max_tokens: int,
    length_fn: Callable[[Dict[str, Any]], int],
    batch_format: Optional[str],
) -> Iterator[DataBatch]:
    """Sort one bounded window by length and greedily pack it into batches."""
    accessor = BlockAccessor.for_block(block)
    lengths = []
    for index, row in enumerate(accessor.iter_rows(public_row_format=True)):
        length = length_fn(row)
        if isinstance(length, bool) or not isinstance(length, Integral) or length < 0:
            raise ValueError(
                "length_fn must return a non-negative integer, "
                f"got {length!r} for row {index} in the buffer."
            )
        if length > max_tokens:
            raise ValueError(
                f"Row {index} in the buffer has length {length}, "
                f"which exceeds max_tokens={max_tokens}."
            )
        lengths.append((index, int(length)))
    lengths.sort(key=lambda item: item[1])

    indices = []
    tokens = 0
    for index, length in lengths:
        if indices and tokens + length > max_tokens:
            yield BlockAccessor.for_block(accessor.take(indices)).to_batch_format(
                batch_format
            )
            indices = []
            tokens = 0
        indices.append(index)
        tokens += length
    if indices:
        yield BlockAccessor.for_block(accessor.take(indices)).to_batch_format(
            batch_format
        )
