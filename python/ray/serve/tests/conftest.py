import os

import pytest

from ray import serve

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False):
    serve.master._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def serve_instance():
    serve.init(blocking=True, ray_init_kwargs={"num_cpus": 36})
    yield


@pytest.fixture(scope="session")
def ray_instance(serve_instance):
    pass
