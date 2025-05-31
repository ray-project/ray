import pytest
import sys
import time
import ray
from ray.util import as_completed, map_unordered


@pytest.fixture(scope="module")
def ray_cluster_10_cpus():
    """Shared Ray cluster with 10 CPUs for all tests in this module"""
    ray.init(num_cpus=10, object_store_memory=150 * 1024 * 1024)
    yield
    ray.shutdown()


@pytest.mark.parametrize("chunk_size", [1, 2])
@pytest.mark.parametrize("return_objrefs", [True, False])
def test_as_completed(ray_cluster_10_cpus, chunk_size, return_objrefs):
    # With 10 cpus, the tasks are all running at the same time.

    @ray.remote
    def f(x):
        time.sleep(x / 50)
        return x

    refs = [f.remote(i) for i in [10, 8, 6, 4, 2]]

    results = list(
        as_completed(refs, chunk_size=chunk_size, return_objrefs=return_objrefs)
    )

    if chunk_size == 1:
        expected = [2, 4, 6, 8, 10]
    else:
        # We collect every 2 results at a time. Each pair is ordered by
        # the submission order.
        expected = [4, 2, 8, 6, 10]

    if return_objrefs:
        results = ray.get(results)

    assert results == expected


@pytest.mark.parametrize("chunk_size", [1, 2])
@pytest.mark.parametrize("return_objrefs", [True, False])
def test_map_unordered(ray_cluster_10_cpus, chunk_size, return_objrefs):
    # With 10 cpus, the tasks are all running at the same time.

    @ray.remote
    def f(x):
        time.sleep(x / 50)
        return x

    inputs = [10, 8, 6, 4, 2]

    results = list(
        map_unordered(f, inputs, chunk_size=chunk_size, return_objrefs=return_objrefs)
    )

    # Tasks complete in order of their sleep time (shortest first)
    if chunk_size == 1:
        expected = [2, 4, 6, 8, 10]
    else:
        # We collect every 2 results at a time. Each pair is ordered by
        # the submission order.
        expected = [4, 2, 8, 6, 10]

    if return_objrefs:
        results = ray.get(results)

    assert results == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
