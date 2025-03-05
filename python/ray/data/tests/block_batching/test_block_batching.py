from unittest import mock

import pyarrow as pa
import pytest

from ray.data._internal.block_batching.block_batching import batch_blocks


def block_generator(num_rows: int, num_blocks: int):
    for _ in range(num_blocks):
        yield pa.table({"foo": [1] * num_rows})


def test_batch_blocks():
    with mock.patch(
        "ray.data._internal.block_batching.block_batching.blocks_to_batches"
    ) as mock_batch, mock.patch(
        "ray.data._internal.block_batching.block_batching.format_batches"
    ) as mock_format:
        block_iter = block_generator(2, 2)
        batch_iter = batch_blocks(block_iter)
        for _ in batch_iter:
            pass
        assert mock_batch.call_count == 1
        assert mock_format.call_count == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
