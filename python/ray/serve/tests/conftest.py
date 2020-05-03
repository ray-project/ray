import os

import pytest

import ray
from ray import serve

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False):
    serve.master._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def serve_instance():
    serve.init(blocking=True, ray_init_kwargs={"num_cpus": 36})
    yield


@pytest.fixture(scope="session")
def ray_instance():
    ray_already_initialized = ray.is_initialized()
    if not ray_already_initialized:
        ray.init(num_cpus=36, object_store_memory=int(1e8))
    yield
    if not ray_already_initialized:
        ray.shutdown()
