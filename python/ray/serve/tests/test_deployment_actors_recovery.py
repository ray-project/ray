"""Tests for deployment-scoped actor kill-and-recovery and controller restart.

These tests are isolated from test_deployment_actors.py because they kill
deployment actors or the Serve controller, which can destabilize the Ray
cluster under CI resource constraints. Running them with a function-scoped
fixture (fresh Ray cluster per test) prevents cascading failures from
accumulated controller state.
"""

import sys
import time

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.exceptions import RayActorError
from ray.serve._private.common import DeploymentStatus
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import get_application_url, request_with_retries
from ray.serve.config import DeploymentActorConfig
from ray.serve.context import _get_global_client
from ray.util.state import list_actors

# ---------------------------------------------------------------------------
# Fixture: function-scoped Ray cluster for isolation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def serve_instance_isolated():
    """Function-scoped Serve instance for kill-and-recovery tests.

    Each test gets a fresh Ray cluster so that killing deployment actors
    or the controller does not destabilize subsequent tests.
    """
    ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    serve.start(proxy_location="HeadOnly", http_options={"host": "0.0.0.0"})
    yield _get_global_client()
    serve.shutdown()
    ray.shutdown()


# ---------------------------------------------------------------------------
# Actor classes
# ---------------------------------------------------------------------------


@ray.remote
class SharedCounter:
    """A trivial deployment-scoped actor used across several tests."""

    def __init__(self, start: int = 0):
        self.count = start

    def increment(self):
        self.count += 1
        return self.count

    def get(self):
        return self.count

    def ray_actor_id(self) -> str:
        return ray.get_runtime_context().get_actor_id()


@ray.remote
class ConstructorGate:
    """Coordination actor: blocks ``__init__`` until opened."""

    def __init__(self):
        self._open = True

    def close(self):
        self._open = False

    def open(self):
        self._open = True

    def wait_until_open(self):
        import time

        while not self._open:
            time.sleep(0.01)


@ray.remote
class GatedSharedCounter:
    """Like ``SharedCounter`` but blocks in ``__init__`` until gate opens."""

    def __init__(self, gate, start: int = 0):
        ray.get(gate.wait_until_open.remote())
        self.count = start

    def get(self):
        return self.count

    def ray_actor_id(self) -> str:
        return ray.get_runtime_context().get_actor_id()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_deployment_actor_names() -> list:
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    return [
        a["name"]
        for a in actors
        if a["name"] and a["name"].startswith(SERVE_DEPLOYMENT_ACTOR_PREFIX)
    ]


def _get_deployment_actor_names_for_app(app_name: str, deployment_name: str) -> list:
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    return [
        a["name"]
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a.get("name", "").startswith(prefix)
    ]


# ---------------------------------------------------------------------------
# Tests — Deployment actor crash and recovery
# ---------------------------------------------------------------------------


def test_deployment_actor_restarts_on_crash(serve_instance_isolated):
    """When a deployment actor dies, the Serve controller recreates it.

    Replicas call ``serve.get_deployment_actor()`` per request here, so they
    resolve a current handle after recreation. HTTP bodies include
    ``ray_actor_id`` so we assert a new id after Serve recreates the
    deployment actor.
    """

    @serve.deployment(
        name="CrashTestDeployment",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 100},
            ),
        ],
    )
    class CrashTestDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            val = ray.get(counter.get.remote())
            aid = ray.get(counter.ray_actor_id.remote())
            return f"{val},{aid}"

    serve.run(CrashTestDeployment.bind())
    url = f"{get_application_url()}/"

    def _parse(text: str) -> tuple[str, str]:
        val, aid = text.split(",", 1)
        return val, aid

    wait_for_condition(lambda: _parse(httpx.get(url).text)[0] == "100")
    _, old_actor_id = _parse(httpx.get(url).text)

    actor_names = [
        n
        for n in _get_deployment_actor_names()
        if "CrashTestDeployment" in n and "counter" in n
    ]
    assert len(actor_names) == 1
    handle = ray.get_actor(actor_names[0], namespace=SERVE_NAMESPACE)

    ray.kill(handle, no_restart=True)

    def recovered_new_deployment_actor():
        text = httpx.get(url, timeout=5).text
        val, aid = _parse(text)
        return val == "100" and aid != old_actor_id

    wait_for_condition(recovered_new_deployment_actor, timeout=120)


def test_deployment_actor_health_check_failure_then_recovery_to_healthy(
    serve_instance_isolated,
):
    """E2E: deployment actor fails health poll -> UNHEALTHY -> recreate -> HEALTHY.

    After killing the deployment actor, we close a ConstructorGate so the
    replacement actor blocks in __init__. While stuck, the deployment stays
    UNHEALTHY. Opening the gate lets construction finish and recover.
    """

    dep_name = "DAHealthRecoveryDeployment"
    logical_actor = "counter"
    gate = ConstructorGate.remote()

    @serve.deployment(
        name=dep_name,
        deployment_actors=[
            DeploymentActorConfig(
                name=logical_actor,
                actor_class=GatedSharedCounter,
                init_kwargs={"gate": gate, "start": 42},
            ),
        ],
    )
    class DAHealthRecoveryDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor(logical_actor)
            val = ray.get(counter.get.remote())
            aid = ray.get(counter.ray_actor_id.remote())
            return f"{val},{aid}"

    serve.run(DAHealthRecoveryDeployment.bind(), name=SERVE_DEFAULT_APP_NAME)
    url = f"{get_application_url()}/"

    def _parse(text: str) -> tuple[str, str]:
        val, aid = text.split(",", 1)
        return val, aid

    def deployment_status():
        return (
            serve.status()
            .applications[SERVE_DEFAULT_APP_NAME]
            .deployments[dep_name]
            .status
        )

    def counter_actor_name() -> str:
        names = [
            n
            for n in _get_deployment_actor_names()
            if dep_name in n and logical_actor in n
        ]
        assert len(names) == 1
        return names[0]

    def counter_actor_id_for_name(name: str) -> str:
        for a in list_actors(filters=[("state", "=", "ALIVE")]):
            if a.get("name") == name:
                return a.actor_id
        raise AssertionError(f"No ALIVE actor record for name={name!r}")

    wait_for_condition(lambda: _parse(httpx.get(url).text)[0] == "42")
    assert deployment_status() == DeploymentStatus.HEALTHY

    da_name = counter_actor_name()
    old_actor_id = counter_actor_id_for_name(da_name)
    _, http_actor_id_before = _parse(httpx.get(url).text)
    assert http_actor_id_before == old_actor_id

    ray.get(gate.close.remote())
    ray.kill(
        ray.get_actor(da_name, namespace=SERVE_NAMESPACE),
        no_restart=True,
    )

    wait_for_condition(
        lambda: deployment_status() == DeploymentStatus.UNHEALTHY,
        timeout=120,
    )
    assert deployment_status() == DeploymentStatus.UNHEALTHY

    ray.get(gate.open.remote())

    wait_for_condition(
        lambda: deployment_status() == DeploymentStatus.HEALTHY,
        timeout=120,
    )

    def http_shows_new_deployment_actor():
        val, aid = _parse(httpx.get(url, timeout=5).text)
        return val == "42" and aid != old_actor_id

    wait_for_condition(http_shows_new_deployment_actor, timeout=120)

    new_name = counter_actor_name()
    assert new_name == da_name
    new_actor_id = counter_actor_id_for_name(new_name)
    assert new_actor_id != old_actor_id
    _, http_actor_id_after = _parse(httpx.get(url).text)
    assert http_actor_id_after == new_actor_id


def test_cached_get_deployment_actor_handle_stale_after_recreation(
    serve_instance_isolated,
):
    """Stale handle in __init__ vs cache + refresh after the deployment actor dies.

    (1) A handle cached in __init__ still points at the dead actor after Serve
    recreates it; HTTP stays 5xx.

    (2) On RayActorError, call get_deployment_actor again to resolve the new
    actor. Retry briefly until the controller finishes recreating.
    """

    counter_cfg = DeploymentActorConfig(
        name="counter",
        actor_class=SharedCounter,
        init_kwargs={"start": 7},
    )

    def kill_stale_test_counter():
        names = [
            n
            for n in _get_deployment_actor_names()
            if "StaleHandleDeployment" in n and "counter" in n
        ]
        assert len(names) == 1
        ray.kill(
            ray.get_actor(names[0], namespace=SERVE_NAMESPACE),
            no_restart=True,
        )

    @serve.deployment(
        name="StaleHandleDeployment",
        deployment_actors=[counter_cfg],
    )
    class StaleHandleDeployment:
        def __init__(self):
            self._counter = serve.get_deployment_actor("counter")

        def __call__(self):
            return str(ray.get(self._counter.get.remote()))

    serve.run(StaleHandleDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "7")

    kill_stale_test_counter()

    def replica_still_fails_after_recreation():
        r = httpx.get(url, timeout=10)
        return r.status_code >= 500

    wait_for_condition(replica_still_fails_after_recreation, timeout=120)

    @serve.deployment(
        name="StaleHandleDeployment",
        deployment_actors=[counter_cfg],
    )
    class CacheAndRefreshOnRayActorError:
        def __init__(self):
            self._counter = serve.get_deployment_actor("counter")

        def _resolve_counter_after_actor_died(self):
            deadline = time.monotonic() + 30.0
            last_exc = None
            while time.monotonic() < deadline:
                try:
                    self._counter = serve.get_deployment_actor("counter")
                    return
                except ValueError as e:
                    last_exc = e
                    time.sleep(0.05)
            if last_exc is not None:
                raise last_exc
            raise TimeoutError(
                "Timed out waiting for deployment actor name after recreation."
            )

        def __call__(self):
            try:
                return str(ray.get(self._counter.get.remote()))
            except RayActorError:
                self._resolve_counter_after_actor_died()
                return str(ray.get(self._counter.get.remote()))

    serve.run(CacheAndRefreshOnRayActorError.bind())
    wait_for_condition(lambda: httpx.get(url).text == "7")
    kill_stale_test_counter()
    wait_for_condition(lambda: httpx.get(url).text == "7", timeout=120)


# ---------------------------------------------------------------------------
# Tests — Controller restart with deployment actors
# ---------------------------------------------------------------------------


def test_deployment_actor_survives_controller_restart(serve_instance_isolated):
    """Deployment actors are detached and survive controller restart.

    After controller recovers from checkpoint, it discovers existing
    deployment actors and marks them ready. App continues with the same
    Ray actor process.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 42},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            val = ray.get(counter.get.remote())
            aid = ray.get(counter.ray_actor_id.remote())
            return f"{val},{aid}"

    serve.run(
        MyDeployment.bind(),
        name="app",
        route_prefix="/survives_controller_restart_da",
    )

    def _parse(text: str) -> tuple[str, str]:
        val, aid = text.split(",", 1)
        return val, aid

    actor_id_before = None
    for _ in range(5):
        resp = request_with_retries(timeout=30, app_name="app")
        val, aid = _parse(resp.text)
        assert val == "42"
        if actor_id_before is None:
            actor_id_before = aid
        else:
            assert aid == actor_id_before

    actor_names_before = _get_deployment_actor_names_for_app("app", "MyDeployment")
    assert len(actor_names_before) == 1

    ray.kill(serve_instance_isolated._controller, no_restart=False)

    wait_for_condition(
        lambda: get_application_url("HTTP", "app", use_localhost=True) is not None
    )
    for _ in range(10):
        resp = request_with_retries(timeout=30, app_name="app")
        val, aid = _parse(resp.text)
        assert val == "42"
        assert aid == actor_id_before

    actor_names_after = _get_deployment_actor_names_for_app("app", "MyDeployment")
    assert actor_names_after == actor_names_before


def test_controller_restart_preserves_mutated_actor_state(serve_instance_isolated):
    """Controller restart preserves mutated deployment actor state.

    Mutates the actor's state before the restart and verifies the mutations
    survive — proving the same actor instance continues running.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(
        MyDeployment.bind(),
        name="app",
        route_prefix="/preserves_mutated_state_da",
    )
    resp = request_with_retries(timeout=30, app_name="app")
    assert resp.text == "0"

    actor_names = _get_deployment_actor_names_for_app("app", "MyDeployment")
    assert len(actor_names) == 1
    handle = ray.get_actor(actor_names[0], namespace=SERVE_NAMESPACE)
    for _ in range(5):
        ray.get(handle.increment.remote())
    assert ray.get(handle.get.remote()) == 5

    ray.kill(serve_instance_isolated._controller, no_restart=False)

    wait_for_condition(
        lambda: get_application_url("HTTP", "app", use_localhost=True) is not None
    )

    for _ in range(5):
        resp = request_with_retries(timeout=30, app_name="app")
        assert resp.text == "5"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
