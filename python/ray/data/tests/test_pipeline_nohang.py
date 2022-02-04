import pytest

import ray
from ray.tests.conftest import *  # noqa

NUM_REPEATS = 10
NUM_TASKS = 10


# This test can be flaky if there is resource deadlock between the pipeline
# stages. Run it a lot to ensure no regressions.
def test_basic_actors(shutdown_only):
    ray.init(num_cpus=2)
    for _ in range(NUM_REPEATS):
        ds = ray.data.range(NUM_TASKS)
        ds = ds.window(blocks_per_window=1)
        assert sorted(ds.map(lambda x: x + 1, compute="actors").take()) == list(
            range(1, NUM_TASKS + 1)
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
