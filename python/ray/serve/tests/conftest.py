import os

import pytest

import ray
from ray import serve
from ray.serve.pipeline.generate import DeploymentNameGenerator

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture
def ray_shutdown():
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture(scope="session")
def _shared_serve_instance():
    # Note(simon):
    # This line should be not turned on on master because it leads to very
    # spammy and not useful log in case of a failure in CI.
    # To run locally, please use this instead.
    # SERVE_LOG_DEBUG=1 pytest -v -s test_api.py
    # os.environ["SERVE_LOG_DEBUG"] = "1" <- Do not uncomment this.

    # Overriding task_retry_delay_ms to relaunch actors more quickly
    ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    yield serve.start(detached=True)


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state between tests to avoid naming collisions.
    _shared_serve_instance.delete_deployments(serve.list_deployments().keys())
    # Clear the ServeHandle cache between tests to avoid them piling up.
    _shared_serve_instance.handle_cache.clear()
    # Clear deployment generation shared state between tests
    DeploymentNameGenerator.reset()
