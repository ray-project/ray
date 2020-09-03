import os

import pytest

import ray
from ray import serve

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False):
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def _shared_serve_instance():
    # Uncomment the line below to turn on debug log for tests.
    # os.environ["SERVE_LOG_DEBUG"] = "1"
    ray.init(num_cpus=36)
    serve.init()
    yield


@pytest.fixture
def serve_instance(_shared_serve_instance):
    serve.init()
    yield
    # Re-init if necessary.
    serve.init()
    controller = serve.api._get_controller()
    # Clear all state between tests to avoid naming collisions.
    for endpoint in ray.get(controller.get_all_endpoints.remote()):
        serve.delete_endpoint(endpoint)
    for backend in ray.get(controller.get_all_backends.remote()):
        serve.delete_backend(backend)
