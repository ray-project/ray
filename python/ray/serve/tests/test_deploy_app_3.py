import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
)
from ray.serve._private.test_utils import Accumulator, check_running
from ray.serve.schema import ApplicationStatus, ServeDeploySchema
from ray.serve.tests.test_config_files.fail_on_flag import INGRESS_DRAIN_ENV
from ray.serve.tests.test_deploy_app_2 import (
    FAIL_ON_FLAG_IMPORT_PATH,
    _assert_rollout_stays_stopped,
    _background_traffic,
    _check_terminal_rolling_update,
    _config_options,
    _deployment_details,
    _rolling_update_config,
    _running_replica_pids,
    _wait_for_app_build,
)
from ray.util.state import list_actors

SURGE_DEPLOYMENT = {"name": "FailOnFlag", "num_replicas": 3, "max_surge_percent": 34}


def _replica_states(
    deployment: str = "FailOnFlag", app_name: str = SERVE_DEFAULT_APP_NAME
) -> Dict[str, int]:
    _wait_for_app_build(deployment, app_name)
    return serve.status().applications[app_name].deployments[deployment].replica_states


def _app_running(app_name: str = SERVE_DEFAULT_APP_NAME) -> bool:
    return serve.status().applications[app_name].status == ApplicationStatus.RUNNING


def _env_override(deployment: dict, **env: str) -> dict:
    """Return a deployment config with new environment variables."""
    ray_actor_options = {
        **deployment.get("ray_actor_options", {}),
        "runtime_env": {"env_vars": env},
    }
    return {**deployment, "ray_actor_options": ray_actor_options}


def _drained(deployment: dict) -> dict:
    """Return the deployment with INGRESS_DRAIN_ENV added to its environment."""
    ray_actor_options = deployment.get("ray_actor_options", {})
    runtime_env = ray_actor_options.get("runtime_env", {})
    env_vars = {**INGRESS_DRAIN_ENV, **runtime_env.get("env_vars", {})}
    runtime_env = {**runtime_env, "env_vars": env_vars}
    return {
        **deployment,
        "ray_actor_options": {**ray_actor_options, "runtime_env": runtime_env},
    }


def _drained_config(deployment: dict) -> ServeDeploySchema:
    """Config for one deployment whose stopping replicas outlast an HAProxy reload."""
    return _rolling_update_config(_drained(deployment))


def _check_surged(client, states: Dict[str, int], old_pids: List[int]):
    """Replacements use the surge allowance while every old replica keeps running."""
    assert _replica_states() == states, _replica_states()
    assert _running_replica_pids(client) == old_pids
    return True


@pytest.mark.parametrize("restart_controller", [False, True])
def test_surge_rolling_update_keeps_capacity_and_replaces(
    serve_instance_with_signal, restart_controller
):
    """Keep capacity and serve traffic through failure, rollback, and replacement."""
    client, signal = serve_instance_with_signal
    client.deploy_apps(_drained_config(SURGE_DEPLOYMENT))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    assert len(initial_pids) == 3
    surged = {"RUNNING": 3, "STARTING": 2}

    with _background_traffic():
        client.deploy_apps(
            _drained_config(
                _env_override(
                    SURGE_DEPLOYMENT, BLOCK_INIT_ON_SIGNAL="1", FAIL_ON_INIT="1"
                )
            )
        )
        wait_for_condition(
            _check_surged, client=client, states=surged, old_pids=initial_pids
        )
        ray.get(signal.send.remote())

        def check_failed_keeping_capacity():
            assert _running_replica_pids(client) == initial_pids
            states = _replica_states()
            assert states["RUNNING"] == 3 and states.get("STARTING", 0) <= 2, states
            app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            return app.status == ApplicationStatus.DEPLOY_FAILED and states == {
                "RUNNING": 3
            }

        wait_for_condition(
            check_failed_keeping_capacity, raise_exceptions=True, timeout=60
        )
        _check_terminal_rolling_update(client, running=3)
        assert _deployment_details(client).status_trigger == "REPLICA_STARTUP_FAILED"
        _assert_rollout_stays_stopped(client, initial_pids)

        # The old replicas already match the rolled-back version: no restarts.
        client.deploy_apps(_drained_config(SURGE_DEPLOYMENT))

        def check_rolled_back():
            assert _running_replica_pids(client) == initial_pids
            return _app_running() and _replica_states() == {"RUNNING": 3}

        wait_for_condition(check_rolled_back, raise_exceptions=True, timeout=60)

        # Hold the working replacements' constructors so the surge is observable.
        ray.get(signal.send.remote(clear=True))
        blocked = _env_override(SURGE_DEPLOYMENT, BLOCK_INIT_ON_SIGNAL="1", MARKER="v2")
        client.deploy_apps(_drained_config(blocked))
        wait_for_condition(
            _check_surged, client=client, states=surged, old_pids=initial_pids
        )
        # Nothing stops while the replacements are still starting.
        deadline = time.monotonic() + 3
        wait_for_condition(
            lambda: _check_surged(client, surged, initial_pids)
            and time.monotonic() >= deadline,
            raise_exceptions=True,
            timeout=15,
        )

        if restart_controller:
            old_pid = ray.get(client._controller.get_pid.remote())
            ray.kill(client._controller, no_restart=False)
            wait_for_condition(
                lambda: ray.get(client._controller.get_pid.remote()) != old_pid
            )

            def check_recovered():
                states = _replica_states()
                assert states["RUNNING"] == 3 and sum(states.values()) == 5, states
                assert set(states) <= {"RUNNING", "RECOVERING", "STARTING"}, states
                assert _running_replica_pids(client) == initial_pids
                return "STARTING" not in states or states == surged

            wait_for_condition(check_recovered, timeout=60)

        ray.get(signal.send.remote())

        def check_replaced():
            states = _replica_states()
            assert states.get("RUNNING", 0) >= 3, states
            assert states.get("RUNNING", 0) + states.get("STARTING", 0) <= 5, states
            running = _running_replica_pids(client)
            return (
                _app_running()
                and len(running) == 3
                and not set(running) & set(initial_pids)
            )

        wait_for_condition(check_replaced, raise_exceptions=True, timeout=60)
        replaced_pids = _running_replica_pids(client)

        # A lightweight change reconfigures the replicas without any surge.
        client.deploy_apps(_drained_config({**blocked, "max_ongoing_requests": 3}))
        wait_for_condition(lambda: _config_options(client) == (3, 3))
        wait_for_condition(check_running, timeout=60)
        assert _running_replica_pids(client) == replaced_pids
        assert _replica_states() == {"RUNNING": 3}


def test_surge_rolling_updates_roll_apps_independently(serve_instance_with_signal):
    """One app fails and rolls back while another rolls forward, both with surge."""
    client, signal = serve_instance_with_signal
    deployment = {"name": "FailOnFlag", "num_replicas": 2, "max_surge_percent": 50}

    def config(a: dict, b: dict) -> ServeDeploySchema:
        return ServeDeploySchema(
            applications=[
                {
                    "name": name,
                    "route_prefix": f"/{name}",
                    "import_path": FAIL_ON_FLAG_IMPORT_PATH,
                    "deployments": [_drained(spec)],
                }
                for name, spec in (("a", a), ("b", b))
            ]
        )

    client.deploy_apps(config(deployment, deployment))
    wait_for_condition(lambda: check_running("a") and check_running("b"), timeout=60)
    pids = {app: _running_replica_pids(client, app_name=app) for app in ("a", "b")}
    forward_b = _env_override(deployment, BLOCK_INIT_ON_SIGNAL="1", MARKER="v2")

    with _background_traffic("http://localhost:8000/a"), _background_traffic(
        "http://localhost:8000/b"
    ):
        client.deploy_apps(
            config(
                _env_override(deployment, BLOCK_INIT_ON_SIGNAL="1", FAIL_ON_INIT="1"),
                forward_b,
            )
        )

        def check_both_surged():
            for app in ("a", "b"):
                states = _replica_states(app_name=app)
                assert states == {"RUNNING": 2, "STARTING": 1}, (app, states)
                assert _running_replica_pids(client, app_name=app) == pids[app]
            return True

        wait_for_condition(check_both_surged, timeout=60)
        ray.get(signal.send.remote())

        def check_a_failed_and_b_replaced():
            assert _running_replica_pids(client, app_name="a") == pids["a"]
            apps = serve.status().applications
            running_b = _running_replica_pids(client, app_name="b")
            return (
                apps["a"].status == ApplicationStatus.DEPLOY_FAILED
                and _replica_states(app_name="a") == {"RUNNING": 2}
                and apps["b"].status == ApplicationStatus.RUNNING
                and len(running_b) == 2
                and not set(running_b) & set(pids["b"])
            )

        wait_for_condition(
            check_a_failed_and_b_replaced, raise_exceptions=True, timeout=90
        )
        replaced_b = _running_replica_pids(client, app_name="b")

        # Rolling back a leaves b on its new replicas.
        client.deploy_apps(config(deployment, forward_b))
        wait_for_condition(check_running, app_name="a", timeout=60)
        assert _running_replica_pids(client, app_name="a") == pids["a"]
        assert check_running("b")
        assert _running_replica_pids(client, app_name="b") == replaced_b


def test_surge_rolling_update_with_autoscaling(serve_instance_with_signal):
    """Surge follows the autoscaled target, and failure and rollback work from zero."""
    client, signal = serve_instance_with_signal
    healthy = {
        "name": "FailOnFlag",
        "max_surge_percent": 50,
        "autoscaling_config": {
            "min_replicas": 0,
            "max_replicas": 4,
            "target_ongoing_requests": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 5,
            "downscale_to_zero_delay_s": 5,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 1,
        },
        "graceful_shutdown_timeout_s": 60,
        "ray_actor_options": {"runtime_env": {"env_vars": {"BLOCK_ON_SIGNAL": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    assert _running_replica_pids(client) == []
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    responses = [handle.remote() for _ in range(8)]
    try:
        # Blocked requests scale the deployment from zero up to its maximum.
        wait_for_condition(lambda: len(_running_replica_pids(client)) == 4, timeout=60)
        scaled_pids = _running_replica_pids(client)

        forward = _env_override(healthy, BLOCK_ON_SIGNAL="1", MARKER="v2")
        client.deploy_apps(_rolling_update_config(forward))

        def check_surge_at_autoscaled_target():
            details = _deployment_details(client)
            assert details.target_num_replicas == 4
            states = [r.state for r in details.replicas]
            assert sum(state != "STOPPING" for state in states) <= 6, states
            running = [r.pid for r in details.replicas if r.state == "RUNNING"]
            assert len(running) >= 4, states
            # Old replicas drain their requests instead of being killed.
            assert all(
                r.state in ("RUNNING", "STOPPING")
                for r in details.replicas
                if r.pid in scaled_pids
            ), states
            return bool(set(running) - set(scaled_pids))

        wait_for_condition(
            check_surge_at_autoscaled_target, raise_exceptions=True, timeout=60
        )
        ray.get(signal.send.remote())
        assert [r.result(timeout_s=60) for r in responses] == ["ok"] * 8

        # With the requests done, the rollout finishes and the deployment scales
        # to zero.
        def check_replaced():
            running = _running_replica_pids(client)
            assert len(running) <= 6, running
            return not set(running) & set(scaled_pids)

        wait_for_condition(check_replaced, raise_exceptions=True, timeout=90)
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 0
            and _replica_states() == {}
            and _app_running(),
            timeout=90,
        )

        # A request wakes the deployment from zero, so a failing version fails there.
        client.deploy_apps(
            _rolling_update_config(
                _env_override(healthy, BLOCK_ON_SIGNAL="1", FAIL_ON_INIT="1")
            )
        )
        pending = handle.remote()

        def check_failed_from_zero():
            # The empty deployment reports HEALTHY until the request scales it up,
            # so the startup failures surface as DEPLOY_FAILED or UNHEALTHY
            # depending on whether the request arrived before it settled.
            app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            assert app.status in (
                ApplicationStatus.DEPLOY_FAILED,
                ApplicationStatus.UNHEALTHY,
            ), app.status
            assert _deployment_details(client).status_trigger == (
                "REPLICA_STARTUP_FAILED"
            )
            return _replica_states() == {}

        wait_for_condition(check_failed_from_zero, timeout=60)

        # Rolling back to the working version scales up from zero again and
        # serves the request that was waiting.
        client.deploy_apps(_rolling_update_config(forward))
        assert pending.result(timeout_s=60) == "ok"
        wait_for_condition(check_running, timeout=60)
        assert _running_replica_pids(client)
    finally:
        ray.get(signal.send.remote())
        for response in responses:
            response.cancel()


def test_gang_surge_rolling_update_and_rollback(serve_instance_with_signal):
    """Surge drains requests and replaces gangs; a failed update preserves rollback."""
    client, signal = serve_instance_with_signal
    failures = Accumulator.options(name="failed-gangs").remote()
    request_env = {
        "BLOCK_ON_SIGNAL": "1",
        # Keep draining replicas reachable while HAProxy reloads its backends.
        "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "5",
    }
    healthy = {
        "name": "FailOnFlag",
        "num_replicas": 4,
        "max_ongoing_requests": 1,
        "max_surge_percent": 25,
        "gang_scheduling_config": {"gang_size": 2},
        "graceful_shutdown_timeout_s": 60,
        "ray_actor_options": {
            "num_cpus": 0.1,
            "runtime_env": {"env_vars": request_env},
        },
    }
    deployment_id = DeploymentID(name="FailOnFlag", app_name=SERVE_DEFAULT_APP_NAME)

    def running_gang_sizes() -> Dict[str, int]:
        replicas = ray.get(
            client._controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        return dict(
            Counter(
                r.gang_context.gang_id for r in replicas.get([ReplicaState.RUNNING])
            )
        )

    try:
        # Undrained: gangs are force-stopped, so a drain would only keep them
        # accepting requests until the kill.
        client.deploy_apps(_rolling_update_config(healthy))
        wait_for_condition(check_running, timeout=60)
        initial_pids = _running_replica_pids(client)
        initial_gangs = running_gang_sizes()
        assert len(initial_gangs) == 2 and set(initial_gangs.values()) == {2}

        client.deploy_apps(
            _rolling_update_config(
                _env_override(
                    healthy,
                    **request_env,
                    BLOCK_INIT_ON_SIGNAL="1",
                    FAIL_ON_INIT="1",
                    RECORD_FAILED_GANGS="failed-gangs",
                )
            )
        )
        # The one replica allowance rounds up to a whole replacement gang.
        wait_for_condition(
            _check_surged,
            client=client,
            states={"RUNNING": 4, "STARTING": 2},
            old_pids=initial_pids,
        )
        ray.get(signal.send.remote())

        with _background_traffic():

            def check_failed_keeping_gangs():
                assert _running_replica_pids(client) == initial_pids
                assert running_gang_sizes() == initial_gangs
                app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
                return (
                    app.status == ApplicationStatus.DEPLOY_FAILED
                    and _replica_states() == {"RUNNING": 4}
                )

            wait_for_condition(
                check_failed_keeping_gangs, raise_exceptions=True, timeout=90
            )
            _check_terminal_rolling_update(client, running=4)
            # Every retry used a fresh replacement gang; the budget is per gang.
            assert len(set(ray.get(failures.get.remote()))) == 3

            # Rolling back reuses the old gangs as they are.
            client.deploy_apps(_rolling_update_config(healthy))
            wait_for_condition(check_running, timeout=60)
            assert _running_replica_pids(client) == initial_pids
            assert running_gang_sizes() == initial_gangs

        ray.get(signal.send.remote(clear=True))
        with ThreadPoolExecutor(max_workers=4) as executor:
            responses = []
            try:
                # One blocked request per replica makes premature kills observable.
                # Confirm each admission before sending the next request so HAProxy's
                # leastconn policy routes it to an idle replica.
                for num_waiters in range(1, 5):
                    responses.append(
                        executor.submit(httpx.get, "http://localhost:8000/", timeout=90)
                    )
                    wait_for_condition(
                        lambda: ray.get(signal.cur_num_waiters.remote()) == num_waiters,
                        timeout=60,
                    )

                # A working version replaces one whole gang at a time.
                forward = _env_override(healthy, **request_env, MARKER="v2")
                client.deploy_apps(_rolling_update_config(forward))

                def check_old_gangs_draining():
                    assert not any(response.done() for response in responses)
                    replicas = _deployment_details(client).replicas
                    old = [r for r in replicas if r.pid in initial_pids]
                    return (
                        len(old) == 4
                        and sum(r.state == "STOPPING" for r in old) == 2
                        and _replica_states() == {"RUNNING": 4, "STOPPING": 2}
                    )

                wait_for_condition(
                    check_old_gangs_draining, raise_exceptions=True, timeout=60
                )
                # Observe further reconciliations before releasing requests, so
                # the test cannot pass by releasing them before an early kill.
                loops = ray.get(client._controller.get_health_metrics.remote())[
                    "num_control_loops"
                ]
                wait_for_condition(
                    lambda: ray.get(client._controller.get_health_metrics.remote())[
                        "num_control_loops"
                    ]
                    >= loops + 2,
                    timeout=60,
                )
                assert check_old_gangs_draining()
            finally:
                ray.get(signal.send.remote())

            for response in responses:
                result = response.result(timeout=60)
                assert result.status_code == 200 and result.text == "ok"

        with _background_traffic():

            def check_gangs_replaced():
                gangs = running_gang_sizes()
                assert sum(gangs.values()) >= 4, gangs
                assert all(
                    size == 2 for gang, size in gangs.items() if gang in initial_gangs
                ), gangs
                running = _running_replica_pids(client)
                return (
                    _app_running()
                    and len(running) == 4
                    and not set(running) & set(initial_pids)
                )

            wait_for_condition(check_gangs_replaced, raise_exceptions=True, timeout=90)
            assert set(running_gang_sizes().values()) == {2}
    finally:
        ray.get(signal.send.remote())
        client.delete_apps([SERVE_DEFAULT_APP_NAME])
        ray.kill(failures)


def test_surge_rolling_update_with_num_replicas_change(serve_instance_with_signal):
    """Surge bounds a code change that also scales the deployment up or down.

    The running count never drops below the smaller target, and old replicas are
    all replaced.
    """
    client, signal = serve_instance_with_signal
    v1 = {**SURGE_DEPLOYMENT, "max_surge_percent": 50}
    client.deploy_apps(_drained_config(v1))
    wait_for_condition(check_running, timeout=60)
    v1_pids = _running_replica_pids(client)

    def check_replaced(target: int, old_pids: List[int], min_running: int):
        states = _replica_states()
        assert states.get("RUNNING", 0) >= min_running, states
        running = _running_replica_pids(client)
        return (
            _app_running()
            and len(running) == target
            and not set(running) & set(old_pids)
        )

    with _background_traffic():
        # Scale 3 -> 5 with new code: five replacements fit within 5 + ceil(2.5).
        v2 = _env_override(v1, BLOCK_INIT_ON_SIGNAL="1", MARKER="v2")
        client.deploy_apps(_drained_config({**v2, "num_replicas": 5}))
        wait_for_condition(
            _check_surged,
            client=client,
            states={"RUNNING": 3, "STARTING": 5},
            old_pids=v1_pids,
        )
        ray.get(signal.send.remote())
        wait_for_condition(
            check_replaced,
            target=5,
            old_pids=v1_pids,
            min_running=3,
            raise_exceptions=True,
            timeout=90,
        )
        v2_pids = _running_replica_pids(client)

        # Scale 5 -> 2 with new code: extra old replicas stop first, then one
        # replacement at a time fits within 2 + ceil(1).
        ray.get(signal.send.remote(clear=True))
        v3 = _env_override(v1, BLOCK_INIT_ON_SIGNAL="1", MARKER="v3")
        client.deploy_apps(_drained_config({**v3, "num_replicas": 2}))

        def check_scaled_down_then_surged():
            states = _replica_states()
            assert states.get("RUNNING", 0) >= 2, states
            assert set(_running_replica_pids(client)) <= set(v2_pids)
            return states == {"RUNNING": 2, "STARTING": 1}

        wait_for_condition(
            check_scaled_down_then_surged, raise_exceptions=True, timeout=60
        )
        ray.get(signal.send.remote())
        wait_for_condition(
            check_replaced,
            target=2,
            old_pids=v2_pids,
            min_running=2,
            raise_exceptions=True,
            timeout=90,
        )


def test_surge_rolling_update_stops_after_health_check_failures(serve_instance):
    """Replacements that start and then fail health checks end the update; rollback
    reuses the surviving old replicas."""
    client = serve_instance
    healthy = {**SURGE_DEPLOYMENT, "health_check_period_s": 0.1}
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)

    client.deploy_apps(
        _rolling_update_config(_env_override(healthy, FAIL_HEALTH_CHECK="1"))
    )
    wait_for_condition(_check_terminal_rolling_update, client=client, timeout=120)

    # Flapping replacements pass their first health check, so each one running at
    # once lets an old replica stop before the failure budget is spent. Once the
    # update is terminal, no replacement is running or started again.
    def check_settled():
        # One snapshot: separate reads can straddle a replacement's last health
        # check, reporting the deployment settled while that replica still stops.
        replicas = _deployment_details(client).replicas
        states = Counter(r.state.value for r in replicas)
        assert set(states) <= {"RUNNING"}, states
        assert {r.pid for r in replicas} <= set(initial_pids)
        return True

    wait_for_condition(check_settled, timeout=60)
    survivors = _running_replica_pids(client)
    deadline = time.monotonic() + 5
    wait_for_condition(
        lambda: _check_terminal_rolling_update(client, running=len(survivors))
        and time.monotonic() >= deadline,
        raise_exceptions=True,
        timeout=15,
    )

    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    wait_for_condition(lambda: len(_running_replica_pids(client)) == 3, timeout=60)
    assert set(survivors) <= set(_running_replica_pids(client))


def test_surge_replacements_wait_for_capacity(serve_instance):
    """Unplaceable replacements leave the old replicas serving until rollback."""
    client = serve_instance
    # Three replicas hold 33 of the 36 CPUs, so no replacement can be placed.
    healthy = {**SURGE_DEPLOYMENT, "ray_actor_options": {"num_cpus": 11}}
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)

    with _background_traffic():
        client.deploy_apps(_rolling_update_config(_env_override(healthy, MARKER="v2")))
        surged = {"RUNNING": 3, "STARTING": 2}
        wait_for_condition(
            _check_surged, client=client, states=surged, old_pids=initial_pids
        )
        deadline = time.monotonic() + 10

        def check_replacements_wait():
            _check_surged(client, surged, initial_pids)
            app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            assert app.status == ApplicationStatus.DEPLOYING, app.status
            assert app.deployments["FailOnFlag"].status == "UPDATING"
            return time.monotonic() >= deadline

        wait_for_condition(check_replacements_wait, raise_exceptions=True, timeout=30)

        # Rolling back drops the pending replacements and keeps the old replicas.
        client.deploy_apps(_rolling_update_config(healthy))

        def check_rolled_back():
            assert _running_replica_pids(client) == initial_pids
            return _app_running() and _replica_states() == {"RUNNING": 3}

        wait_for_condition(check_rolled_back, raise_exceptions=True, timeout=60)


def test_surge_rolling_update_with_one_replica(serve_instance_with_signal):
    """Keep one replica serving through failure, rollback, and replacement.

    Verify constructor ranks and reassignment after a successful rollout.
    """
    client, signal = serve_instance_with_signal
    one = {"name": "FailOnFlag", "num_replicas": 1, "max_surge_percent": 100}
    client.deploy_apps(_drained_config(one))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    assert len(initial_pids) == 1
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    assert handle.ranks.remote().result() == {"init": (0, 1), "current": (0, 1)}
    surged = {"RUNNING": 1, "STARTING": 1}

    with _background_traffic():
        client.deploy_apps(
            _drained_config(
                _env_override(one, BLOCK_INIT_ON_SIGNAL="1", FAIL_ON_INIT="1")
            )
        )
        wait_for_condition(
            _check_surged, client=client, states=surged, old_pids=initial_pids
        )
        ray.get(signal.send.remote())

        def check_failed_keeping_the_replica():
            assert _running_replica_pids(client) == initial_pids
            app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            return app.status == ApplicationStatus.DEPLOY_FAILED and (
                _replica_states() == {"RUNNING": 1}
            )

        wait_for_condition(
            check_failed_keeping_the_replica, raise_exceptions=True, timeout=60
        )
        _check_terminal_rolling_update(client, running=1)
        _assert_rollout_stays_stopped(client, initial_pids)

        client.deploy_apps(_drained_config(one))

        def check_rolled_back():
            assert _running_replica_pids(client) == initial_pids
            return _app_running() and _replica_states() == {"RUNNING": 1}

        wait_for_condition(check_rolled_back, raise_exceptions=True, timeout=60)

        ray.get(signal.send.remote(clear=True))
        client.deploy_apps(
            _drained_config(_env_override(one, BLOCK_INIT_ON_SIGNAL="1", MARKER="v2"))
        )
        wait_for_condition(
            _check_surged, client=client, states=surged, old_pids=initial_pids
        )
        ray.get(signal.send.remote())

        def check_replaced():
            running = _running_replica_pids(client)
            assert len(running) == 1, _replica_states()
            return _app_running() and running != initial_pids

        wait_for_condition(check_replaced, raise_exceptions=True, timeout=60)

    # The replacement starts with (rank, world_size) = (1, 1). After the rollout
    # completes, its rank becomes 0 without restarting the replica.
    replaced_pids = _running_replica_pids(client)

    def check_rank_reassigned():
        ranks = handle.ranks.remote().result()
        assert ranks["init"] == (1, 1), ranks
        return ranks["current"] == (0, 1)

    wait_for_condition(check_rank_reassigned, raise_exceptions=True, timeout=60)
    assert _running_replica_pids(client) == replaced_pids


def test_lightweight_change_during_a_capacity_blocked_surge(serve_instance):
    """Defer lightweight reconfiguration until the blocked rollout completes.

    Old replicas keep serving, and reconfiguration preserves the new replicas.
    """
    client = serve_instance
    deployment_id = DeploymentID("FailOnFlag", SERVE_DEFAULT_APP_NAME)
    applied = Accumulator.options(name="applied-configs").remote()
    v1 = {**SURGE_DEPLOYMENT, "ray_actor_options": {"num_cpus": 4}}
    v2 = _env_override(
        {**SURGE_DEPLOYMENT, "ray_actor_options": {"num_cpus": 8}},
        MARKER="v2",
        RECORD_APPLIED_CONFIGS="applied-configs",
    )
    # Of 36 CPUs, 12 for v1 and 16 reserved leave room for one 8-CPU
    # replacement. Stopping a 4-CPU old replica doesn't make room for another.
    reserved = ray.util.placement_group([{"CPU": 1}] * 16)
    ray.get(reserved.ready())

    def running_max_ongoing_requests() -> Dict[int, int]:
        """The max_ongoing_requests each RUNNING replica is configured with."""
        replicas = ray.get(
            client._controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        return {
            r.actor_details.pid: r.version.deployment_config.max_ongoing_requests
            for r in replicas.get([ReplicaState.RUNNING])
        }

    def applied_user_config(pid: int):
        """The latest config reconfigure() applied on the given replica."""
        return dict(ray.get(applied.get.remote())).get(pid)

    try:
        client.deploy_apps(_drained_config(v1))
        wait_for_condition(check_running, timeout=60)
        initial_pids = _running_replica_pids(client)

        with _background_traffic():
            client.deploy_apps(_drained_config(v2))

            # One replacement runs and one old replica stops; the allowance then
            # admits a third replacement, but neither pending one can be placed.
            def check_stalled():
                # One snapshot: separate reads can straddle the first replacement
                # starting, just before which the counts already match the stall.
                replicas = _deployment_details(client).replicas
                states = Counter(r.state.value for r in replicas)
                assert states.get("RUNNING", 0) >= 3, states
                running = [r.pid for r in replicas if r.state == "RUNNING"]
                assert len(set(running) & set(initial_pids)) >= 2, running
                return states == {"RUNNING": 3, "STARTING": 2} and (
                    len(set(running) - set(initial_pids)) == 1
                )

            wait_for_condition(check_stalled, raise_exceptions=True, timeout=60)
            replaced_pid = (
                set(_running_replica_pids(client)) - set(initial_pids)
            ).pop()
            assert running_max_ongoing_requests()[replaced_pid] == 7
            assert applied_user_config(replaced_pid) is None

            # The change reconfigures nothing until the old replicas are gone.
            user_config = {"flag": "lightweight"}
            client.deploy_apps(
                _drained_config(
                    {**v2, "max_ongoing_requests": 3, "user_config": user_config}
                )
            )
            wait_for_condition(lambda: _config_options(client) == (3, 3))
            deadline = time.monotonic() + 5

            def check_still_stalled():
                assert check_stalled()
                assert _deployment_details(client).status == "UPDATING"
                assert running_max_ongoing_requests()[replaced_pid] == 7
                assert applied_user_config(replaced_pid) is None
                return time.monotonic() >= deadline

            wait_for_condition(check_still_stalled, raise_exceptions=True, timeout=20)

            ray.util.remove_placement_group(reserved)
            reserved = None
            wait_for_condition(check_running, timeout=90)

        # Verify reconfiguration preserves the first replacement's PID.
        configured = running_max_ongoing_requests()
        assert len(configured) == 3 and set(configured.values()) == {3}, configured
        assert replaced_pid in configured
        assert not set(configured) & set(initial_pids)
        assert applied_user_config(replaced_pid) == user_config
    finally:
        ray.kill(applied)
        if reserved is not None:
            ray.util.remove_placement_group(reserved)


def test_surge_rolling_update_follows_autoscaling_mid_rollout(
    serve_instance_with_signal,
):
    """Follow autoscaling from 1 to 4 to 1 during rollout, then scale to zero."""
    client, signal = serve_instance_with_signal
    v1 = {
        "name": "FailOnFlag",
        "max_surge_percent": 50,
        "autoscaling_config": {
            "min_replicas": 0,
            "max_replicas": 4,
            "target_ongoing_requests": 1,
            # Under direct ingress a request moving from the handle to a replica can
            # briefly count twice; the delay keeps that from raising the target.
            "upscale_delay_s": 2,
            "downscale_delay_s": 2,
            "downscale_to_zero_delay_s": 2,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 1,
        },
        "graceful_shutdown_timeout_s": 60,
        "ray_actor_options": {"runtime_env": {"env_vars": {"BLOCK_ON_SIGNAL": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(v1))
    wait_for_condition(check_running, timeout=60)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    first = handle.remote()
    more = []
    try:
        wait_for_condition(lambda: len(_running_replica_pids(client)) == 1, timeout=60)
        old_pids = _running_replica_pids(client)

        # Replacements block in their constructors, so the rollout stays in flight.
        v2 = _env_override(
            v1, BLOCK_ON_SIGNAL="1", BLOCK_INIT_ON_SIGNAL="1", MARKER="v2"
        )
        client.deploy_apps(_rolling_update_config(v2))
        wait_for_condition(
            _check_surged,
            client=client,
            states={"RUNNING": 1, "STARTING": 1},
            old_pids=old_pids,
        )

        # More blocked requests raise the target to four; the surge follows it.
        more = [handle.remote() for _ in range(3)]

        def check_scaled_up():
            assert _running_replica_pids(client) == old_pids
            states = _replica_states()
            assert states.get("RUNNING") == 1 and "STOPPING" not in states, states
            assert states.get("STARTING", 0) <= 4, states
            return _deployment_details(client).target_num_replicas == 4 and (
                states == {"RUNNING": 1, "STARTING": 4}
            )

        wait_for_condition(check_scaled_up, raise_exceptions=True, timeout=60)

        # Cancelling them lowers the target to one: the old replica keeps serving
        # its request and no further replacement starts.
        for response in more:
            response.cancel()
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 1, timeout=60
        )
        deadline = time.monotonic() + 3

        def check_scaled_down():
            assert _running_replica_pids(client) == old_pids
            states = _replica_states()
            assert states.get("RUNNING") == 1 and "STOPPING" not in states, states
            assert states.get("STARTING", 0) <= 4, states
            return time.monotonic() >= deadline

        wait_for_condition(check_scaled_down, raise_exceptions=True, timeout=15)

        # Releasing the signal finishes the replacements and the pending request.
        ray.get(signal.send.remote())
        assert first.result(timeout_s=60) == "ok"

        def check_replaced():
            running = _running_replica_pids(client)
            return bool(running) and not set(running) & set(old_pids)

        wait_for_condition(check_replaced, timeout=60)
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 0
            and _replica_states() == {}
            and _app_running(),
            timeout=90,
        )
    finally:
        ray.get(signal.send.remote())
        for response in [first, *more]:
            response.cancel()


def test_autoscaling_after_a_failed_surge_rollout(serve_instance_with_signal):
    """Allow downscaling after terminal failure without retrying the broken version."""
    client, signal = serve_instance_with_signal
    v1 = {
        "name": "FailOnFlag",
        "max_surge_percent": 50,
        "autoscaling_config": {
            "min_replicas": 0,
            "max_replicas": 6,
            "target_ongoing_requests": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 2,
            "downscale_to_zero_delay_s": 2,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 1,
        },
        "graceful_shutdown_timeout_s": 60,
        "ray_actor_options": {"runtime_env": {"env_vars": {"BLOCK_ON_SIGNAL": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(v1))
    wait_for_condition(check_running, timeout=60)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    requests = [handle.remote() for _ in range(4)]
    try:
        # Metrics can briefly overshoot while requests move from the handle to
        # the replicas, so wait for the target to settle at four.
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 4
            and _replica_states() == {"RUNNING": 4},
            timeout=60,
        )
        old_pids = _running_replica_pids(client)

        client.deploy_apps(
            _rolling_update_config(
                _env_override(v1, BLOCK_ON_SIGNAL="1", FAIL_ON_INIT="1")
            )
        )

        def check_failed_keeping_capacity():
            assert _running_replica_pids(client) == old_pids
            app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            return app.status == ApplicationStatus.DEPLOY_FAILED and (
                _replica_states() == {"RUNNING": 4}
            )

        wait_for_condition(
            check_failed_keeping_capacity, raise_exceptions=True, timeout=90
        )
        _check_terminal_rolling_update(client, running=4)

        # Two requests fewer: the autoscaler shrinks the old replicas to two. A
        # replica still holding a request drains until the signal.
        for response in requests[2:]:
            response.cancel()

        def check_scaled_down():
            assert set(_running_replica_pids(client)) <= set(old_pids)
            states = _replica_states()
            assert set(states) <= {"RUNNING", "STOPPING"}, states
            return (
                _deployment_details(client).target_num_replicas == 2
                and states.get("RUNNING") == 2
            )

        wait_for_condition(check_scaled_down, raise_exceptions=True, timeout=60)
        _check_terminal_rolling_update(client)
        survivors = _running_replica_pids(client)

        # Replace the remaining requests with six fresh ones on running replicas.
        # Requests on draining replicas do not count toward the autoscaling target;
        # cancelling them also lets those replicas stop.
        earlier, requests = requests[:2], [handle.remote() for _ in range(6)]
        for response in earlier:
            response.cancel()
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 6
            and _replica_states() == {"RUNNING": 2},
            timeout=60,
        )
        deadline = time.monotonic() + 5

        def check_no_replacements():
            assert _running_replica_pids(client) == survivors
            assert _replica_states() == {"RUNNING": 2}, _replica_states()
            _check_terminal_rolling_update(client, running=2)
            return time.monotonic() >= deadline

        wait_for_condition(check_no_replacements, raise_exceptions=True, timeout=20)

        # Finishing the requests scales the deployment to zero.
        ray.get(signal.send.remote())
        assert [r.result(timeout_s=60) for r in requests] == ["ok"] * 6
        wait_for_condition(
            lambda: _deployment_details(client).target_num_replicas == 0
            and _replica_states() == {},
            timeout=90,
        )
        _check_terminal_rolling_update(client, running=0)
    finally:
        ray.get(signal.send.remote())
        for response in requests:
            response.cancel()


def test_surge_rolling_update_with_deployment_actors(serve_instance_with_signal):
    """Use separate actors per version and preserve them after a failed update."""
    client, signal = serve_instance_with_signal
    actor_prefix = (
        f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{SERVE_DEFAULT_APP_NAME}::FailOnFlag::"
    )

    def live_deployment_actor_ids() -> set:
        return {
            a["actor_id"]
            for a in list_actors(filters=[("state", "=", "ALIVE")])
            if (a["name"] or "").startswith(actor_prefix)
        }

    def counter_actors(start: int) -> List[dict]:
        return [
            {
                "name": "counter",
                "actor_class": "ray.serve.tests.test_deployment_actors:SharedCounter",
                "init_kwargs": {"start": start},
            }
        ]

    def counters_by_pid() -> Dict[int, int]:
        """Read each responding replica's deployment-actor counter."""
        handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
        responses = [handle.counter.remote() for _ in range(12)]
        return dict(r.result(timeout_s=30) for r in responses)

    v1 = {**SURGE_DEPLOYMENT, "deployment_actors": counter_actors(1)}
    client.deploy_apps(_drained_config(v1))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    assert len(live_deployment_actor_ids()) == 1
    counters = counters_by_pid()
    assert set(counters) <= set(initial_pids) and set(counters.values()) == {1}

    with _background_traffic():
        v2 = _env_override(
            {**v1, "deployment_actors": counter_actors(2)},
            BLOCK_INIT_ON_SIGNAL="1",
            MARKER="v2",
        )
        client.deploy_apps(_drained_config(v2))
        wait_for_condition(
            _check_surged,
            client=client,
            states={"RUNNING": 3, "STARTING": 2},
            old_pids=initial_pids,
            timeout=30,
        )
        # Keep both versions' actors alive during the rollout.
        assert len(live_deployment_actor_ids()) == 2
        assert set(counters_by_pid().values()) == {1}

        ray.get(signal.send.remote())

        def check_replaced():
            running = _running_replica_pids(client)
            assert len(running) >= 3, _replica_states()
            return (
                _app_running()
                and len(running) == 3
                and not set(running) & set(initial_pids)
            )

        wait_for_condition(check_replaced, raise_exceptions=True, timeout=60)
        replaced_pids = _running_replica_pids(client)
        wait_for_condition(lambda: len(live_deployment_actor_ids()) == 1, timeout=30)
        replaced_actor_ids = live_deployment_actor_ids()
        counters = counters_by_pid()
        assert set(counters) <= set(replaced_pids) and set(counters.values()) == {2}

        # Failed actor startup must preserve the serving replicas.
        failing = {
            **v2,
            "deployment_actors": [
                {
                    "name": "counter",
                    "actor_class": (
                        "ray.serve.tests.test_deployment_actors"
                        ":FailingDeploymentActor"
                    ),
                    "init_kwargs": {"should_fail": True},
                }
            ],
        }
        client.deploy_apps(_drained_config(failing))

        def check_actor_failure_keeps_replicas():
            assert _running_replica_pids(client) == replaced_pids
            assert _replica_states() == {"RUNNING": 3}, _replica_states()
            details = _deployment_details(client)
            return (
                details.status == "DEPLOY_FAILED"
                and details.status_trigger == "DEPLOYMENT_ACTOR_FAILED"
            )

        wait_for_condition(
            check_actor_failure_keeps_replicas, raise_exceptions=True, timeout=60
        )
        _assert_rollout_stays_stopped(client, replaced_pids)
        assert live_deployment_actor_ids() == replaced_actor_ids
        assert set(counters_by_pid().values()) == {2}

        # Rolling back reuses the replicas and their actor.
        client.deploy_apps(_drained_config(v2))
        wait_for_condition(check_running, timeout=60)
        assert _running_replica_pids(client) == replaced_pids
        assert live_deployment_actor_ids() == replaced_actor_ids
        assert set(counters_by_pid().values()) == {2}


if __name__ == "__main__":
    # Forward bazel `args` (the -k filter in BUILD.bazel) through to pytest.
    sys.exit(pytest.main(["-v", "-s", __file__] + sys.argv[1:]))
