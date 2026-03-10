"""Deployment actor fault tolerance tests that require a fresh cluster.

These tests use ray_start_stop to get an isolated cluster, avoiding interaction
with tests that kill the controller or leave the proxy in a degraded state.
"""

import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import get_application_url, request_with_retries
from ray.serve.config import DeploymentActorConfig
from ray.util.state import list_actors


@ray.remote
class SharedCounter:
    """Trivial deployment-scoped actor for fault tolerance tests."""

    def __init__(self, start: int = 0):
        self.count = start

    def get(self):
        return self.count


@ray.remote
class SharedCache:
    def __init__(self):
        self.data = {}

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)


def _get_deployment_actor_names():
    """Return Ray actor names matching the deployment-actor naming prefix."""
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    return [
        a["name"]
        for a in actors
        if a["name"] and a["name"].startswith(SERVE_DEPLOYMENT_ACTOR_PREFIX)
    ]


def _get_deployment_actor_names_for_app(app_name: str, deployment_name: str):
    """Return deployment actor names for a specific app and deployment."""
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    return [
        a["name"]
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a.get("name", "").startswith(prefix)
    ]


def test_controller_and_one_actor_killed_app_starts(ray_shutdown):
    """Two deployment actors running; kill controller and one actor; app recovers.

    Controller restarts, recovers the surviving actor, recreates the killed one,
    and the app starts serving.
    """
    serve.start()
    client = serve.context._global_client

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 10},
            ),
            DeploymentActorConfig(
                name="cache",
                actor_class=SharedCache,
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            cache = serve.get_deployment_actor("cache")
            val = ray.get(counter.get.remote())
            ray.get(cache.put.remote("count", val))
            cached = ray.get(cache.get.remote("count"))
            return str(cached)

    serve.run(MyDeployment.bind(), name="app")
    for _ in range(5):
        resp = request_with_retries(timeout=30, app_name="app")
        assert resp.text == "10"

    actor_names = _get_deployment_actor_names_for_app("app", "MyDeployment")
    assert len(actor_names) == 2

    # Kill controller (no_restart=True so it stays dead)
    ray.kill(client._controller, no_restart=True)

    # Kill one deployment actor
    handle = ray.get_actor(actor_names[0], namespace=SERVE_NAMESPACE)
    ray.kill(handle, no_restart=True)

    # Start controller again (like test_refresh_controller_after_death in test_standalone_2)
    serve.start()
    # Refresh controller reference for teardown
    client._controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)

    # Wait for app to be available and serving
    wait_for_condition(
        lambda: get_application_url("HTTP", "app", use_localhost=True) is not None,
        timeout=30,
    )
    for _ in range(10):
        resp = request_with_retries(timeout=30, app_name="app")
        assert resp.status_code == 200
        assert resp.text == "10"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
