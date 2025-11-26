import pytest

import ray
from ray.data.context import DataContext
from ray.tests.conftest import *  # noqa


class TestPreserveHashShuffleBlocks:
    """Test the _preserve_hash_shuffle_finalize_blocks config."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test context with small target_max_block_size."""
        ctx = DataContext.get_current()
        # Very small to force splitting if enabled
        ctx.target_max_block_size = 1
        yield

    @pytest.mark.parametrize("preserve_block_size", [True, False])
    def test_preserve_blocks_enabled(
        self, ray_start_regular_shared_2_cpus, preserve_block_size: bool, shutdown_only
    ):
        """Test that blocks aren't broken down when _preserve_hash_shuffle_finalize_blocks=True."""
        ctx = DataContext.get_current()
        ctx._preserve_hash_shuffle_finalize_blocks = preserve_block_size

        # Create a dataset with one large block
        ds = ray.data.range(10, override_num_blocks=2)

        # Materialize to trigger execution
        result = ds.join(
            ds, on=("id",), join_type="inner", num_partitions=2
        ).materialize()

        if preserve_block_size:
            assert result.num_blocks() == 2
        else:
            assert result.num_blocks() > 2

    def test_map_groups_works_with_preserve_disabled(
        self, ray_start_regular_shared_2_cpus, shutdown_only
    ):
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

    def test_repartition_works_with_num_blocks(
        self, ray_start_regular_shared_2_cpus, shutdown_only
    ):
        """Test that blocks aren't broken down when _preserve_hash_shuffle_finalize_blocks=True."""
        # Create a dataset with one large block
        ds = ray.data.range(10, override_num_blocks=10)

        # Materialize to trigger execution for hash-shuffle
        result = ds.repartition(2, keys=["id"]).materialize()

        assert result.num_blocks() == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
