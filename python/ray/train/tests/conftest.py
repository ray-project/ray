# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa


import pytest

import ray


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_runtime_env():
    # Requires at least torch 1.11 to pass
    # TODO update torch version in requirements instead
    runtime_env = {"pip": ["torch==1.11.0"]}
    address_info = ray.init(runtime_env=runtime_env)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
