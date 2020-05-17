import os

import pytest

from ray import serve
from ray.serve.utils import retry_actor_failures

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False):
    serve.master._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def _shared_serve_instance():
    serve.init(blocking=True, ray_init_kwargs={"num_cpus": 36})
    yield


@pytest.fixture
def serve_instance(_shared_serve_instance):
    serve.init()
    yield
    master = serve.api._get_master_actor()
    # Clear all state between tests to avoid naming collisions.
    for endpoint in retry_actor_failures(master.get_all_endpoints):
        serve.delete_endpoint(endpoint)
    for backend in retry_actor_failures(master.get_all_backends):
        serve.delete_backend(backend)
