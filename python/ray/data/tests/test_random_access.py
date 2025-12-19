import pyarrow
import pytest

import ray
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("pandas", [False, True])
def test_basic(ray_start_regular_shared, pandas):
    ds = ray.data.range(100, override_num_blocks=10)
    ds = ds.add_column("key", lambda b: b["id"] * 2)
    ds = ds.add_column("embedding", lambda b: b["id"] ** 2)
    if not pandas:
        ds = ds.map_batches(
            lambda df: pyarrow.Table.from_pandas(df), batch_format="pandas"
        )

    rad = ds.to_random_access_dataset("key", num_workers=1)

    def expected(i):
        return {"id": i, "key": i * 2, "embedding": i**2}

    # Test get.
    assert ray.get(rad.get_async(-1)) is None
    assert ray.get(rad.get_async(200)) is None
    for i in range(100):
        assert ray.get(rad.get_async(i * 2 + 1)) is None
        assert ray.get(rad.get_async(i * 2)) == expected(i)

    # Test multiget.
    results = rad.multiget([-1] + list(range(0, 20, 2)) + list(range(1, 21, 2)) + [200])
    assert results == [None] + [expected(i) for i in range(10)] + [None] * 10 + [None]


def test_empty_blocks(ray_start_regular_shared):
    ds = ray.data.range(10).repartition(20)
    assert ds._plan.initial_num_blocks() == 20
    rad = ds.to_random_access_dataset("id")
    for i in range(10):
        assert ray.get(rad.get_async(i)) == {"id": i}


def test_errors(ray_start_regular_shared):
    ds = ray.data.range(10)
    with pytest.raises(ValueError):
        ds.to_random_access_dataset("invalid")


def test_stats(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=10)
    rad = ds.to_random_access_dataset("id", num_workers=1)
    stats = rad.stats()
    assert "Accesses per worker: 0 min, 0 max, 0 mean" in stats, stats
    ray.get(rad.get_async(0))
    stats = rad.stats()
    assert "Accesses per worker: 1 min, 1 max, 1 mean" in stats, stats
    rad.multiget([1, 2, 3])
    stats = rad.stats()
    assert "Accesses per worker: 2 min, 2 max, 2 mean" in stats, stats


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
