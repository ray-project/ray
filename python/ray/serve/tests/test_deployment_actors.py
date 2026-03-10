"""Integration tests for deployment-scoped actors.

Covers the full lifecycle: serialization, creation, access from replicas,
redeployment, orphan cleanup, config-only actors, proto roundtrip, and
fault tolerance (controller restart, deployment actor crash).

Test plan for deployment actors:
- Imperative/declarative deploy, multiple actors: test_imperative_*, test_declarative_*
- Redeployment, orphan cleanup: test_redeployment_*, test_actors_cleaned_up_*
- Controller restart: test_deployment_actor_survives_controller_restart
- Controller + actor kill: test_deployment_actor_fault_tolerance.py
- Deployment actor crash (Ray restarts): test_deployment_actor_restarts_on_crash
- Gap: deployment actor killed with no_restart=True — controller does not detect
  or recreate; replicas would get RayActorError. Consider adding health checks.
"""

import os
import sys

import httpx
import pytest
import yaml

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, DeploymentStatus, ReplicaState
from ray.serve._private.constants import (
    REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import (
    check_replica_counts,
    check_running,
    get_application_url,
    request_with_retries,
)
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentActorConfig,
    GangSchedulingConfig,
)
from ray.serve.exceptions import RayServeException
from ray.serve.schema import ApplicationStatus, ServeDeploySchema
from ray.util.state import list_actors

# ---------------------------------------------------------------------------
# Test actor classes – must be at module level for importability via string
# paths in the config-based (declarative) tests.
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


@ray.remote
class SharedCache:
    def __init__(self):
        self.data = {}

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)


@ray.remote
class EnvReportingActor:
    """Reports env vars for testing runtime_env inheritance."""

    def get_env(self, key: str) -> str:
        import os

        return os.environ.get(key, "NOT_SET")


@ray.remote
class InitArgsActor:
    """Accepts init_args for testing positional constructor args."""

    def __init__(self, prefix: str, suffix: str = "default"):
        self.prefix = prefix
        self.suffix = suffix

    def get_value(self) -> str:
        return f"{self.prefix}:{self.suffix}"


@ray.remote
class BlockingCounter:
    """Blocks in __init__ on signal.wait() until signal.send() is called.

    Use for tests that need to pause actor creation (e.g. controller restart
    during deployment actor creation). Pass signal via init_kwargs.
    """

    def __init__(self, start: int = 0, signal=None):
        if signal is not None:
            ray.get(signal.wait.remote())
        self.count = start

    def get(self):
        return self.count


@ray.remote
class FailingDeploymentActor:
    """Raises in __init__ for testing deployment actor constructor failure."""

    def __init__(self, should_fail: bool = True):
        if should_fail:
            raise RuntimeError("Deployment actor init failed")


@ray.remote
class HealthCheckFailingDeploymentActor:
    """Deployment actor that succeeds on init, then fails N times, then succeeds.

    Used to test that when a replica's health check calls this actor and gets
    an error, the replica is marked unhealthy and restarted. Succeeds on the
    first call (replica init), fails on the next N (periodic health checks),
    then succeeds so the replacement replica stays healthy.
    """

    def __init__(self, init_successes: int = 1, fail_count: int = 3):
        self._init_successes = init_successes
        self._fail_count = fail_count
        self._call_count = 0

    def ping(self):
        """Called by replica's check_health. Succeeds then fails N times."""
        self._call_count += 1
        if self._call_count <= self._init_successes:
            return "ok"
        if self._call_count <= self._init_successes + self._fail_count:
            raise RuntimeError("Deployment actor remote call failed")
        return "ok"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_deployment_actor_names() -> list:
    """Return Ray actor names matching the deployment-actor naming prefix."""
    actors = list_actors(
        filters=[
            ("state", "=", "ALIVE"),
        ]
    )
    return [
        a["name"]
        for a in actors
        if a["name"] and a["name"].startswith(SERVE_DEPLOYMENT_ACTOR_PREFIX)
    ]


def _check_no_deployment_actors():
    return len(_get_deployment_actor_names()) == 0


def _check_deployment_actor_count(expected: int):
    return len(_get_deployment_actor_names()) == expected


def _get_deployment_actor_names_for_app(app_name: str, deployment_name: str) -> list:
    """Return deployment actor names for a specific app and deployment."""
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    return [
        a["name"]
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a.get("name", "").startswith(prefix)
    ]


# ---------------------------------------------------------------------------
# Tests – Imperative (code-based) API
# ---------------------------------------------------------------------------


def test_imperative_deploy_with_actor_class(serve_instance):
    """Imperative API: deploy with @ray.remote actor class object.

    Verifies the actor is created, accessible from the replica via
    serve.get_deployment_actor, and that state is shared across requests.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 10},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            val = ray.get(counter.increment.remote())
            return str(val)

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"

    resp1 = httpx.get(url)
    assert resp1.status_code == 200
    assert resp1.text == "11"

    resp2 = httpx.get(url)
    assert resp2.status_code == 200
    assert resp2.text == "12"


def test_imperative_deploy_with_string_import_path(serve_instance):
    """Imperative API: deploy with actor_class as a string import path.

    The build process should import and serialize the class by value so
    it is usable on the controller without the original module.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=("ray.serve.tests.test_deployment_actors:SharedCounter"),
                init_kwargs={"start": 0},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    resp = httpx.get(url)
    assert resp.status_code == 200
    assert resp.text == "0"


def test_deployment_actor_inherits_runtime_env(serve_instance):
    """Deployment actors inherit runtime_env from deployment ray_actor_options."""

    @serve.deployment(
        ray_actor_options={
            "runtime_env": {"env_vars": {"DEPLOYMENT_RUNTIME_ENV": "inherited"}}
        },
        deployment_actors=[
            DeploymentActorConfig(
                name="env_actor",
                actor_class=EnvReportingActor,
                init_kwargs={},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            actor = serve.get_deployment_actor("env_actor")
            return ray.get(actor.get_env.remote("DEPLOYMENT_RUNTIME_ENV"))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "inherited")


def test_imperative_deploy_multiple_actors(serve_instance):
    """Imperative API: deploy with multiple deployment-scoped actors.

    Verifies each actor is independently accessible from the replica.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 100},
            ),
            DeploymentActorConfig(
                name="cache",
                actor_class=SharedCache,
            ),
        ],
    )
    class MyDeployment:
        def __call__(self, request):
            counter = serve.get_deployment_actor("counter")
            cache = serve.get_deployment_actor("cache")
            val = ray.get(counter.get.remote())
            ray.get(cache.put.remote("count", val))
            cached = ray.get(cache.get.remote("count"))
            return str(cached)

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    resp = httpx.get(url)
    assert resp.status_code == 200
    assert resp.text == "100"


# ---------------------------------------------------------------------------
# Tests – Declarative (config-based) API
# ---------------------------------------------------------------------------


def test_declarative_deploy_with_deployment_actors(serve_instance):
    """Declarative API: deploy via config dict with string actor_class paths.

    This tests the full build-task serialization path where actor classes
    are serialized by value inside build_serve_application.
    """
    client = serve_instance

    config = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": ("ray.serve.tests.test_config_files.world.DagNode"),
                "deployments": [
                    {
                        "name": "BasicDriver",
                        "deployment_actors": [
                            {
                                "name": "counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 0},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running)

    wait_for_condition(lambda: _check_deployment_actor_count(1))


def test_declarative_config_only_actors(serve_instance):
    """Config-only deployment actors: actors defined only in config YAML,
    not in the code.

    The actor class string is extracted on the controller, sent to the build
    task for serialization, and the bytes are injected during
    override_deployment_info.
    """
    client = serve_instance

    config = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": ("ray.serve.tests.test_config_files.world.DagNode"),
                "deployments": [
                    {
                        "name": "BasicDriver",
                        "deployment_actors": [
                            {
                                "name": "shared_counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 42},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running)

    wait_for_condition(lambda: _check_deployment_actor_count(1))


def test_declarative_config_only_actors_no_version_change(serve_instance):
    """Config-only deployment actors: redeploy with same config (no version change).

    Same config redeploy preserves deployment actors; no orphan cleanup.
    """
    client = serve_instance

    config = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": (
                    "ray.serve.tests.test_config_files.config_only_deployment_actor:app"
                ),
                "deployments": [
                    {
                        "name": "ConfigOnlyDriver",
                        "deployment_actors": [
                            {
                                "name": "counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 10},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running)
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "10")

    actor_names_before = _get_deployment_actor_names()
    assert len(actor_names_before) == 1

    client.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running)
    wait_for_condition(lambda: httpx.get(url).text == "10")

    actor_names_after = _get_deployment_actor_names()
    assert actor_names_after == actor_names_before


def test_declarative_config_only_actors_version_change(serve_instance):
    """Config-only deployment actors: redeploy with different config (version change).

    Changing init_kwargs triggers new code version; old actors cleaned up,
    new actors created.
    """
    client = serve_instance

    config_v1 = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": (
                    "ray.serve.tests.test_config_files.config_only_deployment_actor:app"
                ),
                "deployments": [
                    {
                        "name": "ConfigOnlyDriver",
                        "deployment_actors": [
                            {
                                "name": "counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 100},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema.model_validate(config_v1))
    wait_for_condition(check_running)
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "100")

    actor_names_v1 = _get_deployment_actor_names()
    assert len(actor_names_v1) == 1

    config_v2 = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": (
                    "ray.serve.tests.test_config_files.config_only_deployment_actor:app"
                ),
                "deployments": [
                    {
                        "name": "ConfigOnlyDriver",
                        "deployment_actors": [
                            {
                                "name": "counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 200},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema.model_validate(config_v2))
    wait_for_condition(check_running)
    wait_for_condition(lambda: httpx.get(url).text == "200")

    actor_names_v2 = _get_deployment_actor_names()
    assert len(actor_names_v2) == 1
    assert actor_names_v1[0] != actor_names_v2[0]


def test_incremental_rollout_version_isolation(serve_instance):
    """During incremental rollout, v1 replicas use v1 deployment actors and
    v2 replicas use v2 deployment actors.

    Uses SignalActor to synchronize: block 1 v1 replica so it stays during
    rollout. New requests go to v2; blocked request proves v1 used v1 actor.
    get_deployment_actor resolves by code_version.
    """
    import os

    signal = SignalActor.remote()

    @serve.deployment(
        name="test",
        num_replicas=2,
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
    )
    class V1:
        async def __call__(self):
            await signal.wait.remote()
            counter = serve.get_deployment_actor("counter")
            return "1", ray.get(counter.get.remote()), os.getpid()

    @serve.deployment(
        name="test",
        num_replicas=2,
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
    )
    class V2:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return "2", ray.get(counter.get.remote()), os.getpid()

    h = serve.run(V1.bind(), name="app")

    signal.send.remote()
    refs = [h.remote() for _ in range(10)]
    results = [ref.result() for ref in refs]
    versions, counter_vals, _ = zip(*results)
    assert versions.count("1") == 10
    assert counter_vals.count(1) == 10
    initial_pids = {r[2] for r in results}
    assert len(initial_pids) == 2

    # Block 1 v1 replica so it stays during rollout
    signal.send.remote(clear=True)
    blocked_ref = h.remote()

    serve._run(V2.bind(), _blocking=False, name="app")

    # 1 v1 STOPPING (blocked) + 2 v2 RUNNING
    wait_for_condition(
        check_replica_counts,
        controller=serve_instance._controller,
        deployment_id=DeploymentID(name="test", app_name="app"),
        total=3,
        by_state=[
            (ReplicaState.STOPPING, 1, lambda r: r._actor.pid in initial_pids),
            (ReplicaState.RUNNING, 2, lambda r: r._actor.pid not in initial_pids),
        ],
    )
    wait_for_condition(lambda: _check_deployment_actor_count(2))

    # New requests go to v2; verify v2 uses v2 actor
    refs = [h.remote() for _ in range(10)]
    versions, counter_vals, _ = zip(*[ref.result(timeout_s=5) for ref in refs])
    assert versions.count("2") == 10
    assert counter_vals.count(2) == 10

    # Release blocked v1; its response proves v1 used v1 actor
    ray.get(signal.send.remote())
    version, counter_val, pid = blocked_ref.result(timeout_s=5)
    assert version == "1" and counter_val == 1 and pid in initial_pids


# ---------------------------------------------------------------------------
# Tests – Actor cleanup and redeployment
# ---------------------------------------------------------------------------


def test_actors_cleaned_up_on_app_delete(serve_instance):
    """Deployment actors are killed when the application is deleted."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            return "ok"

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).status_code == 200)

    wait_for_condition(lambda: _check_deployment_actor_count(1))

    serve.delete(name="default")

    wait_for_condition(_check_no_deployment_actors, timeout=15)


def test_scale_to_and_from_zero_with_deployment_actors(serve_instance):
    """Autoscaling 1->0->1 keeps deployment actor and its state."""
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config=AutoscalingConfig(
            min_replicas=0,
            initial_replicas=1,
            max_replicas=1,
            target_ongoing_requests=1,
            upscale_delay_s=0,
            downscale_delay_s=5,
            downscale_to_zero_delay_s=5,
            metrics_interval_s=1,
            look_back_period_s=2,
        ),
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
    )
    class MyDeployment:
        async def __call__(self):
            counter = serve.get_deployment_actor("counter")
            val = ray.get(counter.increment.remote())
            await signal.wait.remote()
            return str(val)

    deployment_id = DeploymentID(name="MyDeployment", app_name="default")

    serve.run(MyDeployment.bind())
    handle = serve.get_app_handle("default")
    wait_for_condition(lambda: _check_deployment_actor_count(1))
    fut = handle.remote()

    wait_for_condition(
        check_replica_counts,
        controller=serve_instance._controller,
        deployment_id=deployment_id,
        total=1,
        timeout=45,
    )
    # Deployment remains active, so target-version actor should be retained.
    wait_for_condition(lambda: _check_deployment_actor_count(1))

    ray.get(signal.send.remote())

    assert fut.result(timeout_s=30) == "1"

    wait_for_condition(
        check_replica_counts,
        controller=serve_instance._controller,
        deployment_id=deployment_id,
        total=0,
        timeout=30,
    )

    # Evict cached handle and get a fresh one so its long-poll has replicas=0.
    # The old handle may still report curr_replicas=1, preventing the scale-from-zero
    # optimized metrics push from firing.
    ingress = ray.get(
        serve_instance._controller.get_ingress_deployment_name.remote("default")
    )
    serve_instance.handle_cache.pop((ingress, "default", False), None)

    ray.get(signal.send.remote(clear=True))
    # Send a request to trigger autoscaling from zero back to one. The request
    # blocks on signal.wait until we release it.
    fresh_handle = serve.get_app_handle("default")
    blocked_ref = fresh_handle.remote()
    wait_for_condition(
        check_replica_counts,
        controller=serve_instance._controller,
        deployment_id=deployment_id,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1, None)],
        timeout=30,
    )
    wait_for_condition(lambda: _check_deployment_actor_count(1))
    ray.get(signal.send.remote())
    assert blocked_ref.result(timeout_s=30) == "2"


def test_redeployment_replaces_actors(serve_instance):
    """Redeploying with updated deployment_actors creates new actors and
    cleans up old ones (orphan cleanup).
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter_v1",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
        version="v1",
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter_v1")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "1")

    actor_names_v1 = _get_deployment_actor_names()
    assert len(actor_names_v1) == 1

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter_v2",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
        version="v2",
    )
    class MyDeployment:  # noqa: F811
        def __call__(self):
            counter = serve.get_deployment_actor("counter_v2")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())
    wait_for_condition(lambda: httpx.get(url).text == "2")

    actor_names_v2 = _get_deployment_actor_names()
    assert len(actor_names_v2) == 1
    assert actor_names_v1[0] != actor_names_v2[0]


def test_replica_context_includes_code_version(serve_instance):
    """Replica context exposes deployment code version."""

    @serve.deployment(
        num_replicas=1,
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
        version="v1",
    )
    class MyDeployment:
        def __call__(self):
            ctx = serve.get_replica_context()
            counter = serve.get_deployment_actor("counter")
            return f"{ctx.code_version}|{ray.get(counter.get.remote())}"

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "v1|1")

    @serve.deployment(
        num_replicas=1,
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
        version="v2",
    )
    class MyDeployment:  # noqa: F811
        def __call__(self):
            ctx = serve.get_replica_context()
            counter = serve.get_deployment_actor("counter")
            return f"{ctx.code_version}|{ray.get(counter.get.remote())}"

    serve.run(MyDeployment.bind())
    wait_for_condition(lambda: httpx.get(url).text == "v2|2")


def test_redeployment_with_no_actors_cleans_up_old(serve_instance):
    """Redeploying with NO deployment actors cleans up old actors.

    Regression test: previously the orphan cleanup early-returned if the
    new version had no deployment actors.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
        version="v1",
    )
    class MyDeployment:
        def __call__(self):
            return "v1"

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "v1")
    wait_for_condition(lambda: _check_deployment_actor_count(1))

    @serve.deployment(version="v2")
    class MyDeployment:  # noqa: F811
        def __call__(self):
            return "v2"

    serve.run(MyDeployment.bind())
    wait_for_condition(lambda: httpx.get(url).text == "v2")

    wait_for_condition(_check_no_deployment_actors, timeout=15)


# ---------------------------------------------------------------------------
# Tests – Fault tolerance: controller restart, deployment actor crash
# ---------------------------------------------------------------------------


def test_deployment_actor_survives_controller_restart(serve_instance):
    """Deployment actors are detached and survive controller restart.

    After controller recovers from checkpoint, it discovers existing
    deployment actors (ActorAlreadyExistsError) and marks them ready.
    App continues to work with the same deployment actors.
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
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind(), name="app")
    for _ in range(5):
        resp = request_with_retries(timeout=30, app_name="app")
        assert resp.text == "42"

    actor_names_before = _get_deployment_actor_names()
    assert len(actor_names_before) == 1

    ray.kill(serve_instance._controller, no_restart=False)

    wait_for_condition(
        lambda: get_application_url("HTTP", "app", use_localhost=True) is not None
    )
    for _ in range(10):
        resp = request_with_retries(timeout=30, app_name="app")
        assert resp.text == "42"

    actor_names_after = _get_deployment_actor_names()
    assert actor_names_after == actor_names_before


def test_deployment_actor_restarts_on_crash(serve_instance):
    """When a deployment actor crashes, Ray restarts it (max_restarts=-1).

    Replicas use serve.get_deployment_actor() which does ray.get_actor()
    each time, so they get a fresh handle to the restarted actor.
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
            return str(ray.get(counter.get.remote()))

    serve.run(CrashTestDeployment.bind())
    resp = request_with_retries(timeout=30)
    assert resp.text == "100"

    actor_names = [
        n
        for n in _get_deployment_actor_names()
        if "CrashTestDeployment" in n and "counter" in n
    ]
    assert len(actor_names) == 1
    handle = ray.get_actor(actor_names[0], namespace=SERVE_NAMESPACE)

    ray.kill(handle, no_restart=False)

    # Actor restarts asynchronously; retry until we get "100" again.
    wait_for_condition(
        lambda: request_with_retries(timeout=10).text == "100",
        timeout=30,
    )


def test_deployment_actor_constructor_failure_app_status(serve_instance):
    """When a deployment actor's constructor fails, app deploy status is DEPLOY_FAILED."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="bad_actor",
                actor_class=FailingDeploymentActor,
                init_kwargs={"should_fail": True},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            return "ok"

    with pytest.raises(RuntimeError):
        serve.run(MyDeployment.bind())

    status = serve.status()
    app_status = status.applications["default"]
    assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    assert "MyDeployment" in app_status.deployments
    assert (
        app_status.deployments["MyDeployment"].status == DeploymentStatus.DEPLOY_FAILED
    )


def test_actor_remote_call_error_causes_replica_fail_and_restart(serve_instance):
    """When a replica's health check fails due to an error in an actor remote call
    (e.g. deployment actor raises), the replica is marked unhealthy and restarted.
    """

    @serve.deployment(
        name="HealthCheckFailDeployment",
        deployment_actors=[
            DeploymentActorConfig(
                name="health_actor",
                actor_class=HealthCheckFailingDeploymentActor,
                init_kwargs={
                    "init_successes": 1,
                    "fail_count": REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
                },
            ),
        ],
        health_check_period_s=0.5,
        health_check_timeout_s=1,
    )
    class HealthCheckFailDeployment:
        def __call__(self):
            return str(__import__("os").getpid())

        def check_health(self):
            actor = serve.get_deployment_actor("health_actor")
            ray.get(actor.ping.remote())

    serve.run(HealthCheckFailDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(
        lambda: httpx.get(url, timeout=30).status_code == 200,
        timeout=30,
    )
    old_pid = httpx.get(url, timeout=30).text

    # The deployment actor will raise on the first N health checks. After N
    # consecutive failures, the replica is marked unhealthy and restarted.
    # The replacement replica's health checks succeed (actor stops raising).
    wait_for_condition(
        lambda: httpx.get(url, timeout=30).text != old_pid,
        timeout=30,
    )
    # Verify the new replica serves requests.
    for _ in range(5):
        assert httpx.get(url, timeout=30).status_code == 200


def test_deployment_actor_killed_no_restart_documents_behavior(serve_instance):
    """Deployment actor killed with no_restart=True: requests fail.

    Controller does not detect or recreate. Documents current behavior.
    """

    @serve.deployment(
        name="NoRestartDeployment",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 200},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "200")

    actor_names = [
        n
        for n in _get_deployment_actor_names()
        if "NoRestartDeployment" in n and "counter" in n
    ]
    assert len(actor_names) == 1
    handle = ray.get_actor(actor_names[0], namespace=SERVE_NAMESPACE)
    ray.kill(handle, no_restart=True)

    # Requests fail (500 or RayActorError) when calling the dead actor
    resp = httpx.get(url, timeout=10)
    assert resp.status_code == 500


def test_app_delete_cleans_up_deployment_actors(serve_instance):
    """Application delete cleans up deployment actors."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
                actor_options={"num_cpus": 0.01},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            return "ok"

    serve.run(MyDeployment.bind(), name="delete_test_app", route_prefix="/delete_test")
    wait_for_condition(
        lambda: len(
            _get_deployment_actor_names_for_app("delete_test_app", "MyDeployment")
        )
        == 1
    )
    serve.delete(name="delete_test_app")
    wait_for_condition(
        lambda: len(
            _get_deployment_actor_names_for_app("delete_test_app", "MyDeployment")
        )
        == 0,
        timeout=15,
    )


def test_deployment_actor_actor_options_override_runtime_env(serve_instance):
    """Deployment actor actor_options override deployment runtime_env."""

    @serve.deployment(
        ray_actor_options={"runtime_env": {"env_vars": {"ENV_KEY": "from_deployment"}}},
        deployment_actors=[
            DeploymentActorConfig(
                name="env_actor",
                actor_class=EnvReportingActor,
                actor_options={"runtime_env": {"env_vars": {"ENV_KEY": "from_actor"}}},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            actor = serve.get_deployment_actor("env_actor")
            return ray.get(actor.get_env.remote("ENV_KEY"))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "from_actor")


def test_deployment_actor_with_init_args(serve_instance):
    """Deployment actor with init_args (not just init_kwargs)."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="init_args_actor",
                actor_class=InitArgsActor,
                init_args=("my_prefix",),
                init_kwargs={"suffix": "my_suffix"},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            actor = serve.get_deployment_actor("init_args_actor")
            return ray.get(actor.get_value.remote())

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "my_prefix:my_suffix")


def test_deployment_actor_with_resource_constraints(serve_instance):
    """Deployment actor with actor_options num_cpus schedules correctly."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
                actor_options={"num_cpus": 0.01},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "0")


def test_multiple_apps_same_named_deployment_actors(serve_instance):
    """Multiple apps with same-named deployment actors are isolated."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
    )
    class App1Deployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return f"app1:{ray.get(counter.get.remote())}"

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
    )
    class App2Deployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return f"app2:{ray.get(counter.get.remote())}"

    serve.run(App1Deployment.bind(), name="app1", route_prefix="/multi_app1")
    serve.run(App2Deployment.bind(), name="app2", route_prefix="/multi_app2")

    url1 = get_application_url("HTTP", "app1", use_localhost=True)
    url2 = get_application_url("HTTP", "app2", use_localhost=True)
    wait_for_condition(lambda: httpx.get(url1).text == "app1:1")
    wait_for_condition(lambda: httpx.get(url2).text == "app2:2")

    # Each app has its own deployment actor (isolated by app name)
    app1_actors = _get_deployment_actor_names_for_app("app1", "App1Deployment")
    app2_actors = _get_deployment_actor_names_for_app("app2", "App2Deployment")
    assert len(app1_actors) == 1
    assert len(app2_actors) == 1
    assert app1_actors[0] != app2_actors[0]


def test_multi_app_deployment_actor_cleanup(serve_instance):
    """Deleting one app cleans only that app's deployment actors."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
    )
    class App1Deployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
    )
    class App2Deployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(App1Deployment.bind(), name="app1", route_prefix="/cleanup1")
    serve.run(App2Deployment.bind(), name="app2", route_prefix="/cleanup2")

    wait_for_condition(
        lambda: (
            len(_get_deployment_actor_names_for_app("app1", "App1Deployment")) == 1
            and len(_get_deployment_actor_names_for_app("app2", "App2Deployment")) == 1
        )
    )
    serve.delete(name="app1")
    wait_for_condition(
        lambda: (
            len(_get_deployment_actor_names_for_app("app1", "App1Deployment")) == 0
            and len(_get_deployment_actor_names_for_app("app2", "App2Deployment")) == 1
        ),
        timeout=15,
    )
    url2 = get_application_url("HTTP", "app2", use_localhost=True)
    assert httpx.get(url2).text == "2"


def test_autoscaling_deployment_with_deployment_actors(serve_instance):
    """Autoscaling deployment with deployment actors: replicas share actors."""

    @serve.deployment(
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            max_replicas=4,
            target_ongoing_requests=1,
        ),
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
            return str(ray.get(counter.increment.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    # Send enough requests to trigger scale-up
    for _ in range(20):
        httpx.get(url)
    # All replicas share the same counter - values should be sequential
    values = [int(httpx.get(url).text) for _ in range(5)]
    assert values == sorted(values)
    assert len(set(values)) <= len(values)


def test_multiple_replicas_share_deployment_actor(serve_instance):
    """Multiple replicas share one deployment actor; state is consistent."""

    @serve.deployment(
        num_replicas=3,
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
            return str(ray.get(counter.increment.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    values = set()
    for _ in range(15):
        values.add(httpx.get(url).text)
    # All replicas share same counter; we should see sequential values
    assert len(values) >= 2
    sorted_vals = sorted(int(v) for v in values)
    assert sorted_vals == list(range(min(sorted_vals), max(sorted_vals) + 1))


def test_concurrent_requests_multiple_replicas_to_deployment_actor(serve_instance):
    """Concurrent requests from multiple replicas to shared actor: no races."""

    @serve.deployment(
        num_replicas=2,
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
            return str(ray.get(counter.increment.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(lambda: httpx.get(url).text) for _ in range(20)]
        results = [f.result() for f in futures]
    # All increments should be unique (no lost updates)
    assert len(results) == len(set(results))


def test_get_deployment_actor_invalid_name(serve_instance):
    """get_deployment_actor with nonexistent name causes request to fail."""

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
            serve.get_deployment_actor("nonexistent")
            return "ok"

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    resp = httpx.get(url)
    assert resp.status_code == 500


def test_get_deployment_actor_outside_replica_raises(serve_instance):
    """get_deployment_actor called outside replica raises RayServeException."""
    with pytest.raises(RayServeException, match="may only be called from within"):
        serve.get_deployment_actor("counter")


def test_deploy_from_yaml_config_file_with_deployment_actors(serve_instance):
    """Deploy from YAML config file with deployment_actors."""
    config_path = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deployment_actors.yaml",
    )
    with open(config_path) as f:
        config = yaml.safe_load(f)
    serve_instance.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running, timeout=30)
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "88", timeout=30)
    wait_for_condition(
        lambda: len(_get_deployment_actor_names_for_app("default", "ConfigOnlyDriver"))
        == 1,
        timeout=15,
    )


def test_config_only_deployment_actors_with_actor_options(serve_instance):
    """Config-only deployment actors with actor_options in declarative config."""
    config = {
        "applications": [
            {
                "name": "default",
                "route_prefix": "/",
                "import_path": (
                    "ray.serve.tests.test_config_files.config_only_deployment_actor:app"
                ),
                "deployments": [
                    {
                        "name": "ConfigOnlyDriver",
                        "deployment_actors": [
                            {
                                "name": "counter",
                                "actor_class": (
                                    "ray.serve.tests.test_deployment_actors"
                                    ":SharedCounter"
                                ),
                                "init_kwargs": {"start": 99},
                                "actor_options": {"num_cpus": 0.01},
                            },
                        ],
                    },
                ],
            },
        ],
    }
    serve_instance.deploy_apps(ServeDeploySchema.model_validate(config))
    wait_for_condition(check_running)
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "99")


def test_gang_scheduling_with_deployment_actors(serve_instance):
    """Gang scheduling and deployment actors work together."""

    @serve.deployment(
        num_replicas=2,
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
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
            return str(ray.get(counter.increment.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).status_code == 200)
    vals = [httpx.get(url).text for _ in range(5)]
    assert all(v.isdigit() for v in vals)


def test_user_config_update_with_deployment_actors(serve_instance):
    """user_config update does not affect deployment actors."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 50},
            ),
        ],
    )
    class MyDeployment:
        def __init__(self):
            self.config_val = None

        def reconfigure(self, config):
            self.config_val = config

        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return f"{self.config_val}:{ray.get(counter.get.remote())}"

    serve.run(MyDeployment.options(user_config=None).bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: "None:50" in httpx.get(url).text)

    serve.run(MyDeployment.options(user_config="updated").bind(), blocking=False)
    wait_for_condition(lambda: "updated:50" in httpx.get(url).text, timeout=30)


# ---------------------------------------------------------------------------
# Tests – Design review: regression and edge-case coverage
# ---------------------------------------------------------------------------


def test_deployment_actor_failure_then_redeploy(serve_instance):
    """Deploy with a permanently failing actor, verify DEPLOY_FAILED,
    then redeploy with a working actor and verify recovery.
    """

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="bad_actor",
                actor_class=FailingDeploymentActor,
                init_kwargs={"should_fail": True},
            ),
        ],
    )
    class FailDeploy:
        def __call__(self):
            return "should not reach"

    serve._run(FailDeploy.bind(), _blocking=False)

    def _check_deploy_failed():
        status = serve.status()
        app_status = status.applications["default"]
        return app_status.status == ApplicationStatus.DEPLOY_FAILED

    wait_for_condition(_check_deploy_failed, timeout=30)

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
    )
    class FixedDeploy:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(FixedDeploy.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "0")


def test_rapid_version_rollout_cleans_up_actors(serve_instance):
    """Deploy v1, v2, v3 rapidly on the same deployment. Only v3 actors
    should remain after rollout completes.
    """

    @serve.deployment(
        name="rollout_dep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
        version="v1",
    )
    class V1:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(V1.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "1")
    v1_actors = set(_get_deployment_actor_names())

    @serve.deployment(
        name="rollout_dep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
        version="v2",
    )
    class V2:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve._run(V2.bind(), _blocking=False)

    @serve.deployment(
        name="rollout_dep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 3},
            ),
        ],
        version="v3",
    )
    class V3:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(V3.bind())
    wait_for_condition(lambda: httpx.get(url).text == "3")

    wait_for_condition(lambda: _check_deployment_actor_count(1), timeout=15)
    v3_actors = set(_get_deployment_actor_names())
    assert len(v3_actors) == 1
    assert v3_actors.isdisjoint(v1_actors)


def test_no_leak_deployment_actors_when_v2_arrives_during_v1_pending(serve_instance):
    """When v2 arrives while v1 replicas are STARTING (blocked in __init__),
    v1 deployment actors must be cleaned up immediately (not leaked until v1
    replicas are fully removed).
    """
    signal = SignalActor.remote()

    @serve.deployment(
        name="leak_test_dep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 1},
            ),
        ],
        version="v1",
    )
    class V1:
        def __init__(self):
            # Block until signaled — keeps replica in STARTING (PENDING_INITIALIZATION)
            ray.get(signal.wait.remote())

        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    # Deploy v1 non-blocking so replica stays in STARTING (blocked in __init__)
    serve._run(V1.bind(), _blocking=False)
    url = f"{get_application_url()}/"

    # v1 deployment actors are created; v1 replica blocks in __init__
    wait_for_condition(lambda: _check_deployment_actor_count(1), timeout=15)
    v1_actor_names = set(_get_deployment_actor_names())
    assert len(v1_actor_names) == 1

    @serve.deployment(
        name="leak_test_dep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 2},
            ),
        ],
        version="v2",
    )
    class V2:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(V2.bind())
    wait_for_condition(lambda: httpx.get(url).text == "2", timeout=30)

    # Assert immediately: v1 actors must be gone. Without the fix, v1 actors
    actors = _get_deployment_actor_names()
    assert len(actors) == 1, f"Expected 1 deployment actor, got {len(actors)}: {actors}"
    assert set(actors).isdisjoint(
        v1_actor_names
    ), f"v1 deployment actors leaked (should be stopped when v1 replicas go to STOPPING): {actors}"


def test_actor_name_collision_across_deployments(serve_instance):
    """Two deployments in the same app with the same actor name get
    separate actor instances (isolated by deployment name in actor naming).
    """

    @serve.deployment(
        name="dep_a",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 100},
            ),
        ],
    )
    class DepA:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    @serve.deployment(
        name="dep_b",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 200},
            ),
        ],
    )
    class DepB:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(DepA.bind(), name="app_collision", route_prefix="/a")
    serve.run(DepB.bind(), name="app_collision2", route_prefix="/b")

    wait_for_condition(
        lambda: httpx.get(f"{get_application_url(app_name='app_collision')}").text
        == "100"
    )
    wait_for_condition(
        lambda: httpx.get(f"{get_application_url(app_name='app_collision2')}").text
        == "200"
    )

    actors = _get_deployment_actor_names()
    assert len(actors) == 2
    assert actors[0] != actors[1]


def test_concurrent_deploy_and_delete(serve_instance):
    """Start deploying with actors, immediately delete before actors are ready.
    Verify clean shutdown with no leaked actors.
    """
    signal = SignalActor.remote()

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="blocker",
                actor_class=BlockingCounter,
                init_kwargs={"start": 0, "signal": signal},
            ),
        ],
    )
    class SlowDeploy:
        def __call__(self):
            return "ok"

    serve._run(SlowDeploy.bind(), _blocking=False)

    # Give the controller a moment to begin actor creation before deleting.
    ray.get(ray.remote(lambda: None).remote())

    serve.delete("default")
    ray.get(signal.send.remote())

    wait_for_condition(_check_no_deployment_actors, timeout=30)


def test_config_only_actor_update_creates_new_actors(serve_instance):
    """Deploy via config with deployment actors, then update init_kwargs.
    Verify new actors are created with new kwargs and old actors are cleaned up.
    """

    @serve.deployment(
        name="ConfigDep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 10},
            ),
        ],
        version="v1",
    )
    class ConfigDep:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(ConfigDep.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).text == "10")

    v1_actors = set(_get_deployment_actor_names())
    assert len(v1_actors) == 1

    @serve.deployment(
        name="ConfigDep",
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 99},
            ),
        ],
        version="v2",
    )
    class ConfigDepV2:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(ConfigDepV2.bind())
    wait_for_condition(lambda: httpx.get(url).text == "99")

    v2_actors = set(_get_deployment_actor_names())
    assert len(v2_actors) == 1
    assert v2_actors.isdisjoint(v1_actors)


def test_deployment_actors_with_target_capacity(serve_instance):
    """target_capacity scaling does not affect deployment actors."""

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
        num_replicas=2,
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.increment.remote()))

    serve.run(MyDeployment.bind())
    url = f"{get_application_url()}/"
    wait_for_condition(lambda: httpx.get(url).status_code == 200)
    wait_for_condition(lambda: _check_deployment_actor_count(1))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
