import os

import pytest

import ray
from ray import serve
from ray.serve.tests.conftest import serve_instance  # noqa

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture
def ray_shutdown():
    yield
    serve.shutdown()
    ray.shutdown()
