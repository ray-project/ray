from unittest.mock import patch

import pytest

from ray.data._internal.size_estimator import SizeEstimator


class _StubSizeEstimator(SizeEstimator):
    """SizeEstimator that uses deterministic integer item values as byte sizes."""

    def __init__(self):
        super().__init__()
        self.sampled_items = []

    def _real_size(self, item: int) -> int:
        self.sampled_items.append(item)
        return item


@pytest.mark.parametrize(
    "block_sizes",
    [
        [10],
        [100],
        [100, 100, 100],
        [50, 50, 50, 50, 50],
        [250],
        [3, 7, 10, 15, 65, 100, 100],
        [1, 9, 1, 89, 1, 99, 1],
        [13, 27, 60, 100, 45, 55, 120],
        [0, 10, 0, 90, 0, 100, 0],
    ],
)
def test_add_block_matches_item_by_item_add(block_sizes):
    total_items = sum(block_sizes)
    items = [(i + 1) * 17 for i in range(total_items)]

    item_estimator = _StubSizeEstimator()
    for item in items:
        item_estimator.add(item)

    block_estimator = _StubSizeEstimator()
    cursor = 0
    for size in block_sizes:
        block_estimator.add_block(items[cursor : cursor + size])
        cursor += size

    assert block_estimator.sampled_items == item_estimator.sampled_items
    assert block_estimator._count == item_estimator._count
    assert block_estimator._running_mean.n == item_estimator._running_mean.n
    assert block_estimator._running_mean.mean == pytest.approx(
        item_estimator._running_mean.mean
    )
    assert block_estimator.size_bytes() == item_estimator.size_bytes()


def test_add_block_100_row_blocks_sample_every_100th_item():
    estimator = _StubSizeEstimator()
    # First 100 items: samples 1..10 (weight 1) and 20, 30, ..., 100 (weight 10)
    estimator.add_block(list(range(1, 101)))
    assert estimator.sampled_items == list(range(1, 11)) + list(range(20, 101, 10))

    # Next 100-item block (items 101..200): must sample item 200 (index 99) with weight 100
    estimator.add_block(list(range(101, 201)))
    assert estimator.sampled_items[-1] == 200
    assert estimator._running_mean.n == 10 * 1 + 9 * 10 + 1 * 100


def test_add_block_uses_cloudpickle_in_client_mode():
    with patch("ray.util.client.ray.is_connected", return_value=True):
        item_est = SizeEstimator()
        block_est = SizeEstimator()
        rows = [{"k": "x" * (i + 1)} for i in range(220)]
        for row in rows:
            item_est.add(row)
        block_est.add_block(rows[:100])
        block_est.add_block(rows[100:200])
        block_est.add_block(rows[200:])
        assert block_est.size_bytes() == item_est.size_bytes()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
