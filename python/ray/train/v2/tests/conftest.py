import logging

import pytest

import ray


@pytest.fixture()
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture(autouse=True)
def setup_logging():
    logger = logging.getLogger("ray.train")
    orig_level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(orig_level)


@pytest.fixture
def shutdown_only():
    yield None
    ray.shutdown()
