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
        [100, 100, 100],
        [3, 7, 10, 15, 65, 100, 100],
        [250],
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
    assert block_estimator.size_bytes() == item_estimator.size_bytes()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
