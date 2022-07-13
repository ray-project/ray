import pytest

import ray
from ray.data import read_api, dataset
from ray.tests.conftest import *  # noqa


class MockLogger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(msg)

    def info(self, msg):
        print("info:", msg)

    def debug(self, msg):
        print("debug:", msg)


def test_map(shutdown_only):
    dataset.logger = MockLogger()
    ray.init(num_cpus=2)
    ray.data.range(3).map(lambda x: x)
    assert dataset.logger.warnings == [
        "The `map`, `flat_map`, and `filter` operations are unvectorized and can "
        "be very slow. Consider using `.map_batches()` instead."
    ]
    dataset.logger.warnings.clear()
    ray.data.range(3).map(lambda x: x)

    # Logged once only.
    assert dataset.logger.warnings == []


def test_limited_parallelism(shutdown_only):
    read_api.logger = MockLogger()
    ray.init(num_cpus=10)
    ray.data.range(3)
    assert read_api.logger.warnings == [
        "This dataset will be auto-repartitioned from 3 to 20 blocks to "
        "increase its available parallelism. Specify the `parallelism` "
        "arg to disable automatic repartitioning."
    ]

    read_api.logger = MockLogger()
    ray.data.range(10)
    assert read_api.logger.warnings == []

    read_api.logger = MockLogger()
    ray.data.range(10, parallelism=3)
    assert read_api.logger.warnings == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
