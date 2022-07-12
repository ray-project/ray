import pytest

import ray
from ray.data import read_api
from ray.tests.conftest import *  # noqa


class MockLogger:
    def __init__(self):
        self.buffer = []

    def warning(self, msg):
        self.buffer.append(msg)


def test_limited_parallelism(shutdown_only):
    read_api.logger = MockLogger()
    ray.init(num_cpus=10)
    ds = ray.data.range(3)
    assert read_api.logger.buffer == [
        "The number of blocks in this dataset (3) limits its parallelism to "
        "3 concurrent tasks. This is much less than the number of available "
        "CPU slots in the cluster. Use `.repartition(n)` to increase the number "
        "of dataset blocks."
    ]

    read_api.logger = MockLogger()
    ds = ray.data.range(8)
    assert read_api.logger.buffer == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
