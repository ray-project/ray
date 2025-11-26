import pytest

import ray
from ray.data.context import DataContext
from ray.tests.conftest import *  # noqa


class TestPreserveHashShuffleBlocks:
    """Test that hash shuffle repartition preserves block structure."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test context with small target_max_block_size."""
        ctx = DataContext.get_current()
        # Very small to force splitting if enabled
        ctx.target_max_block_size = 1
        yield

    def test_repartition_preserves_blocks(
        self, ray_start_regular_shared_2_cpus, shutdown_only
    ):
        """Test that repartition with keys preserves block count."""
        # Create a dataset with multiple blocks
        ds = ray.data.range(10, override_num_blocks=10)

        # Repartition using hash shuffle with keys
        result = ds.repartition(2, keys=["id"]).materialize()

        # Should have exactly 2 blocks (one per partition)
        assert result.num_blocks() == 2

    def test_map_groups_works(self, ray_start_regular_shared_2_cpus, shutdown_only):
        """Test that map_groups works correctly."""
        ds = ray.data.from_items(
            [
                {"group": 1, "value": 1},
                {"group": 1, "value": 2},
                {"group": 2, "value": 3},
                {"group": 2, "value": 4},
            ]
        )

        # map_groups should work correctly
        result = (
            ds.groupby("group")
            .map_groups(lambda g: {"count": [len(g["value"])]})
            .take_all()
        )

        assert len(result) == 2
        counts = sorted([r["count"] for r in result])
        assert counts == [2, 2]

    def test_join_does_not_preserve_blocks(
        self, ray_start_regular_shared_2_cpus, shutdown_only
    ):
        """Test that join does not preserve block structure (default behavior)."""
        # Create a dataset with one large block
        ds = ray.data.range(10, override_num_blocks=2)

        # Join operation uses hash shuffle but doesn't set disallow_block_splitting
        result = ds.join(
            ds, on=("id",), join_type="inner", num_partitions=2
        ).materialize()

        # Should have more than 2 blocks due to block splitting
        assert result.num_blocks() >= 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
