import pytest
import pyarrow

import ray

from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("pandas", [False, True])
def test_basic(ray_start_regular_shared, pandas):
    ds = ray.data.range_arrow(100, parallelism=10)
    ds = ds.add_column("embedding", lambda b: b["value"] ** 2)
    if pandas:
        assert ds._dataset_format() == "pandas"
    else:
        ds = ds.map_batches(lambda df: pyarrow.Table.from_pandas(df))
        assert ds._dataset_format() == "arrow"

    rad = ds.to_random_access_dataset("value", num_workers=1)

    # Test get.
    assert ray.get(rad.get_async(-1)) is None
    assert ray.get(rad.get_async(100)) is None
    for i in range(100):
        assert ray.get(rad.get_async(i)) == {"value": i, "embedding": i ** 2}

    def expected(i):
        return {"value": i, "embedding": i ** 2}

    # Test multiget.
    results = rad.multiget([-1] + list(range(10)) + [100])
    assert results == [None] + [expected(i) for i in range(10)] + [None]


def test_empty_blocks(ray_start_regular_shared):
    ds = ray.data.range_arrow(10).repartition(20)
    assert ds.num_blocks() == 20
    rad = ds.to_random_access_dataset("value")
    for i in range(10):
        assert ray.get(rad.get_async(i)) == {"value": i}


def test_errors(ray_start_regular_shared):
    ds = ray.data.range(10)
    with pytest.raises(ValueError):
        ds.to_random_access_dataset("value")

    ds = ray.data.range_arrow(10)
    with pytest.raises(ValueError):
        ds.to_random_access_dataset("invalid")


def test_stats(ray_start_regular_shared):
    ds = ray.data.range_arrow(100, parallelism=10)
    rad = ds.to_random_access_dataset("value", num_workers=1)
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
