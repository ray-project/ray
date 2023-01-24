import ray

from ray.tests.conftest import *  # noqa


def test_incremental_take(shutdown_only):
    ray.init(num_cpus=2)

    # Can read incrementally even if future results are delayed.
    def block_on_ones(x: int) -> int:
        if x == 1:
            time.sleep(999999)
        return x

    pipe = ray.data.range(2).window(blocks_per_window=1)
    pipe = pipe.map(block_on_ones)
    assert pipe.take(1) == [0]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
