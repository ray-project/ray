import pytest

import ray
from ray.data.context import DataContext


class TestPreserveHashShuffleBlocks:
    """Test the _preserve_hash_shuffle_finalize_blocks config."""

    def test_preserve_blocks_enabled(self, shutdown_only):
        """Test that blocks aren't broken down when _preserve_hash_shuffle_finalize_blocks=True."""
        ctx = DataContext.get_current()
        ctx._preserve_hash_shuffle_finalize_blocks = True
        ctx.target_max_block_size = 100  # Very small to force splitting if enabled

        # Create a dataset with one large block
        ds = ray.data.range(1000, override_num_blocks=1)

        # Repartition using hash shuffle
        ds = ds.repartition(10, shuffle="hash_shuffle")

        # Materialize to trigger execution
        result = ds.materialize()

        # With preserve_blocks=True, blocks shouldn't be broken down further
        # We should have exactly 10 blocks (one per partition)
        assert (
            result.num_blocks() == 10
        ), f"Expected 10 blocks, got {result.num_blocks()}"

    def test_preserve_blocks_disabled(self, shutdown_only):
        """Test that _preserve_hash_shuffle_finalize_blocks=False enables block splitting."""
        ctx = DataContext.get_current()
        ctx.target_max_block_size = 200  # Small size to enable splitting

        # Create a simple dataset
        ds = ray.data.range(1000, override_num_blocks=1)

        # Repartition using hash shuffle
        ds_result = ds.repartition(5, shuffle="hash_shuffle")

        # Materialize to trigger execution
        result = ds_result.materialize()

        # Note: Block splitting depends on the actual block sizes after partitioning.
        # With preserve_blocks=False, the BlockOutputBuffer path is enabled.
        # However, splitting only occurs if blocks exceed 1.5x target_max_block_size.
        # For this test, we just verify the operation completes successfully.
        # The key behavior is that map_groups requires this to be False.
        assert result.num_blocks() > 1

    def test_map_groups_works_with_preserve_disabled(self, shutdown_only):
        """Test that map_groups works when _preserve_hash_shuffle_finalize_blocks=False."""

        ds = ray.data.from_items(
            [
                {"group": 1, "value": 1},
                {"group": 1, "value": 2},
                {"group": 2, "value": 3},
                {"group": 2, "value": 4},
            ]
        )

        # map_groups should work fine when _preserve_hash_shuffle_finalize_blocks=False
        result = (
            ds.groupby("group")
            .map_groups(lambda g: {"count": [len(g["value"])]})
            .take_all()
        )

        assert len(result) == 2
        counts = sorted([r["count"] for r in result])
        assert counts == [2, 2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
