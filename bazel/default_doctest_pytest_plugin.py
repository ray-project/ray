"""This file is injected for all doctest targets in the repo by default."""
import pytest

import ray


@pytest.fixture(autouse=True, scope="module")
def shutdown_ray():
    ray.shutdown()
    yield
