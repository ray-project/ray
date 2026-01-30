import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ray.data._internal.block_batching.block_batching import batch_blocks


def block_generator(num_rows: int, num_blocks: int):
    for i in range(num_blocks):
        yield pa.table({"foo": list(range(i * num_rows, (i + 1) * num_rows))})


class TestBatchBlocks:
    """Tests for batch_blocks function."""

    @pytest.mark.parametrize("batch_format", ["pandas", "numpy", "pyarrow"])
    def test_basic(self, batch_format):
        """Test that batch_blocks yields all data in the requested format."""
        blocks = block_generator(num_rows=3, num_blocks=2)
        batches = list(batch_blocks(blocks, batch_format=batch_format))

        assert len(batches) == 2

        if batch_format == "pandas":
            assert isinstance(batches[0], pd.DataFrame)
            assert isinstance(batches[1], pd.DataFrame)
            pd.testing.assert_frame_equal(
                batches[0], pd.DataFrame({"foo": [0, 1, 2]})
            )
            pd.testing.assert_frame_equal(
                batches[1], pd.DataFrame({"foo": [3, 4, 5]})
            )
        elif batch_format == "numpy":
            assert isinstance(batches[0], dict)
            assert isinstance(batches[1], dict)
            np.testing.assert_array_equal(batches[0]["foo"], np.array([0, 1, 2]))
            np.testing.assert_array_equal(batches[1]["foo"], np.array([3, 4, 5]))
        elif batch_format == "pyarrow":
            assert batches == [pa.table({"foo": [0, 1, 2]}), pa.table({"foo": [3, 4, 5]})]
        else:
            pytest.fail(f"Unsupported batch format {batch_format}")

    @pytest.mark.parametrize(
        "batch_size,drop_last,expected_values",
        [
            # 6 rows, batch_size=2: yields 3 full batches
            (2, False, [[0, 1], [2, 3], [4, 5]]),
            # 6 rows, batch_size=4: yields 1 full + 1 partial batch
            (4, False, [[0, 1, 2, 3], [4, 5]]),
            # 6 rows, batch_size=4, drop_last: drops partial batch
            (4, True, [[0, 1, 2, 3]]),
            # 6 rows, batch_size=10, drop_last: no batches (all dropped)
            (10, True, []),
        ],
    )
    def test_batch_size(self, batch_size, drop_last, expected_values):
        """Test batch_size and drop_last parameters."""
        blocks = block_generator(num_rows=3, num_blocks=2)
        batches = list(
            batch_blocks(
                blocks,
                batch_size=batch_size,
                drop_last=drop_last,
                batch_format="numpy",
            )
        )

        assert len(batches) == len(expected_values)
        for batch, expected in zip(batches, expected_values):
            np.testing.assert_array_equal(batch["foo"], np.array(expected))

    def test_collate_fn(self):
        """Test that collate_fn transforms batches."""

        def double_values(batch):
            return {"foo": [x * 2 for x in batch["foo"].tolist()]}

        blocks = block_generator(num_rows=3, num_blocks=2)
        batches = list(batch_blocks(blocks, collate_fn=double_values))

        assert len(batches) == 2
        assert batches[0]["foo"] == [0, 2, 4]
        assert batches[1]["foo"] == [6, 8, 10]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))