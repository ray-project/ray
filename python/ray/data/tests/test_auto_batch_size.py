"""Tests for batch_size="auto" in BatchMapTransformFn and map_batches."""
import pytest

import ray


def test_map_batches_auto_correctness(ray_start_regular_shared):
    """batch_size='auto' preserves all rows and values in an end-to-end pipeline."""
    n_rows = 100
    rows = (
        ray.data.range(n_rows)
        .map_batches(lambda batch: batch, batch_size="auto")
        .take_all()
    )
    assert len(rows) == n_rows
    assert sorted(r["id"] for r in rows) == list(range(n_rows))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
