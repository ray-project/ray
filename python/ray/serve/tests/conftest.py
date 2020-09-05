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
    ray.init(
        num_cpus=36,
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000})
    yield serve.start(detached=True)


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    controller = _shared_serve_instance._controller
    # Clear all state between tests to avoid naming collisions.
    for endpoint in ray.get(controller.get_all_endpoints.remote()):
        _shared_serve_instance.delete_endpoint(endpoint)
    for backend in ray.get(controller.get_all_backends.remote()):
        _shared_serve_instance.delete_backend(backend)
