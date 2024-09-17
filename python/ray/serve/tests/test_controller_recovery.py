import asyncio
import logging
import os
import re
import sys
import time

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.exceptions import RayTaskError
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import (
    RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS,
    SERVE_CONTROLLER_NAME,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
)
from ray.serve._private.test_utils import check_replica_counts
from ray.serve.schema import LoggingConfig
from ray.serve.tests.test_failure import request_with_retries
from ray.util.state import list_actors


@pytest.mark.parametrize(
    "deployment_options",
    [
        {"num_replicas": 2},
        {"autoscaling_config": {"min_replicas": 2, "max_replicas": 2}},
    ],
)
def test_recover_start_from_replica_actor_names(serve_instance, deployment_options):
    """Test controller is able to recover starting -> running replicas from
    actor names.
    """

    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    @serve.deployment(
        name="recover_start_from_replica_actor_names", **deployment_options
    )
    class TransientConstructorFailureDeployment:
        def __init__(self):
            pass

        def __call__(self, *args):
            return "hii"

    serve.run(TransientConstructorFailureDeployment.bind(), name="app")
    for _ in range(10):
        response = request_with_retries(
            "/recover_start_from_replica_actor_names/", timeout=30
        )
        assert response.text == "hii"
    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy() call with transient error
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    id = DeploymentID(name="recover_start_from_replica_actor_names", app_name="app")
    assert len(deployment_dict[id]) == 2

    replica_version_hash = None
    for replica in deployment_dict[id]:
        ref = replica.actor_handle._get_metadata.remote()
        _, version, _, _ = ray.get(ref)
        if replica_version_hash is None:
            replica_version_hash = hash(version)
        assert replica_version_hash == hash(version), (
            "Replica version hash should be the same for "
            "same code version and user config."
        )

    # Sample: [
    # 'TransientConstructorFailureDeployment#xlituP',
    # 'SERVE_CONTROLLER_ACTOR',
    # 'TransientConstructorFailureDeployment#NosHNA',
    # 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-node:192.168.86.165-0']
    actor_infos = list_actors(filters=[("state", "=", "ALIVE")])
    replica_names = [
        actor_info["name"]
        for actor_info in actor_infos
        if (
            SERVE_CONTROLLER_NAME not in actor_info["name"]
            and SERVE_PROXY_NAME not in actor_info["name"]
        )
    ]
    assert (
        len(replica_names) == 2
    ), "Should have two running replicas fetched from ray API."

    # Kill controller and wait for endpoint to be available again
    ray.kill(serve.context._global_client._controller, no_restart=False)
    for _ in range(10):
        response = request_with_retries(
            "/recover_start_from_replica_actor_names/", timeout=30
        )
        assert response.text == "hii"

    # Ensure recovered replica names are the same
    recovered_actor_infos = list_actors(filters=[("state", "=", "ALIVE")])
    recovered_replica_names = [
        actor_info["name"]
        for actor_info in recovered_actor_infos
        if (
            SERVE_CONTROLLER_NAME not in actor_info["name"]
            and SERVE_PROXY_NAME not in actor_info["name"]
        )
    ]
    assert (
        recovered_replica_names == replica_names
    ), "Running replica actor names after recovery must match"

    # Ensure recovered replica version has are the same
    for replica_name in recovered_replica_names:
        actor_handle = ray.get_actor(replica_name, namespace=SERVE_NAMESPACE)
        ref = actor_handle._get_metadata.remote()
        _, version, _, _ = ray.get(ref)
        assert replica_version_hash == hash(
            version
        ), "Replica version hash should be the same after recover from actor names"


def test_recover_rolling_update_from_replica_actor_names(serve_instance):
    """Test controller can recover replicas during rolling update.

    Replicas starting -> updating -> running
    replicas from actor names, with right replica versions during rolling
    update.
    """

    signal = SignalActor.remote()

    @serve.deployment(name="test", num_replicas=2)
    class V1:
        async def __call__(self):
            await signal.wait.remote()
            return "1", os.getpid()

    @serve.deployment(name="test", num_replicas=2)
    class V2:
        async def __call__(self):
            return "2", os.getpid()

    h = serve.run(V1.bind(), name="app")

    # Send requests to get pids of initial 2 replicas
    signal.send.remote()
    refs = [h.remote() for _ in range(10)]
    versions, pids = zip(*[ref.result() for ref in refs])
    assert versions.count("1") == 10
    initial_pids = set(pids)
    assert len(initial_pids) == 2

    # blocked_ref will block a single replica until the signal is sent.
    signal.send.remote(clear=True)
    blocked_ref = h.remote()

    # Kill the controller
    ray.kill(serve.context._global_client._controller, no_restart=False)

    # Redeploy new version.
    serve._run(V2.bind(), _blocking=False, name="app")

    # One replica of the old version should be stuck in stopping because
    # of the blocked request.
    if RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
        # Two replicas of the new version should be brought up without
        # waiting for the old replica to stop.
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

        # All new requests should be sent to the new running replicas
        refs = [h.remote() for _ in range(10)]
        versions, pids = zip(*[ref.result(timeout_s=5) for ref in refs])
        assert versions.count("2") == 10
        pids2 = set(pids)
        assert len(pids2 & initial_pids) == 0
    else:
        with pytest.raises(TimeoutError):
            serve_instance._wait_for_application_running("app", timeout_s=0.1)

        refs = [h.remote() for _ in range(10)]

    # Kill the controller
    ray.kill(serve.context._global_client._controller, no_restart=False)

    # Release the signal so that the old replica can shutdown
    ray.get(signal.send.remote())
    val, pid = blocked_ref.result()
    assert val == "1"
    assert pid in initial_pids

    if not RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
        versions, pids = zip(*[ref.result(timeout_s=5) for ref in refs])
        assert versions.count("1") == 10
        assert len(set(pids) & initial_pids)

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    serve_instance._wait_for_application_running("app")
    check_replica_counts(
        controller=serve_instance._controller,
        deployment_id=DeploymentID(name="test", app_name="app"),
        total=2,
        by_state=(
            [(ReplicaState.RUNNING, 2, lambda r: r._actor.pid not in initial_pids)]
        ),
    )


def test_controller_recover_initializing_actor(serve_instance):
    """Recover the actor which is under PENDING_INITIALIZATION"""

    signal = SignalActor.remote()
    signal2 = SignalActor.remote()
    client = serve_instance

    @ray.remote
    def pending_init_indicator():
        ray.get(signal2.wait.remote())
        return True

    @serve.deployment
    class V1:
        async def __init__(self):
            ray.get(signal2.send.remote())
            await signal.wait.remote()

        def __call__(self, request):
            return f"1|{os.getpid()}"

    serve._run(V1.bind(), _blocking=False, name="app")
    ray.get(pending_init_indicator.remote())

    def get_actor_info(name: str):
        all_current_actors = list_actors(filters=[("state", "=", "ALIVE")])
        for actor in all_current_actors:
            if SERVE_PROXY_NAME in actor["name"]:
                continue
            if name in actor["name"]:
                print(actor)
                return actor["name"], actor["pid"]

    actor_tag, _ = get_actor_info(f"app#{V1.name}")
    _, controller1_pid = get_actor_info(SERVE_CONTROLLER_NAME)
    ray.kill(serve.context._global_client._controller, no_restart=False)
    # wait for controller is alive again
    wait_for_condition(get_actor_info, name=SERVE_CONTROLLER_NAME)
    assert controller1_pid != get_actor_info(SERVE_CONTROLLER_NAME)[1]

    # Let the actor proceed initialization
    ray.get(signal.send.remote())
    client._wait_for_application_running("app")
    # Make sure the actor before controller dead is staying alive.
    assert actor_tag == get_actor_info(f"app#{V1.name}")[0]


def test_replica_deletion_after_controller_recover(serve_instance):
    """Test that replicas are deleted when controller is recovered"""

    controller = serve.context._global_client._controller

    @serve.deployment(graceful_shutdown_timeout_s=3)
    class V1:
        async def __call__(self):
            while True:
                await asyncio.sleep(0.1)

    handle = serve.run(V1.bind(), name="app")
    _ = handle.remote()
    serve.delete("app", _blocking=False)

    def check_replica(replica_state=None):
        id = DeploymentID(name="V1", app_name="app")
        try:
            replicas = ray.get(controller._dump_replica_states_for_testing.remote(id))
        except RayTaskError as ex:
            # Deployment is not existed any more.
            if isinstance(ex, KeyError):
                return []
            # Unexpected exception raised.
            raise ex
        if replica_state is None:
            replica_state = list(ReplicaState)
        else:
            replica_state = [replica_state]
        return replicas.get(replica_state)

    # Make sure the replica is in STOPPING state.
    wait_for_condition(lambda: len(check_replica(ReplicaState.STOPPING)) > 0)
    ray.kill(serve.context._global_client._controller, no_restart=False)
    # Make sure the replica is in STOPPING state.
    wait_for_condition(lambda: len(check_replica(ReplicaState.STOPPING)) > 0)

    # The graceful shutdown timeout of 3 seconds should be used
    wait_for_condition(lambda: len(check_replica()) == 0, timeout=20)
    # Application should be removed soon after
    wait_for_condition(lambda: "app" not in serve.status().applications, timeout=20)


def test_recover_deleting_application(serve_instance):
    """Test that replicas that are stuck on __del__ when the controller crashes,
    is properly recovered when the controller is recovered.

    This is similar to the test test_replica_deletion_after_controller_recover,
    except what's blocking the deployment is __del__ instead of ongoing requests
    """

    signal = SignalActor.remote()

    @serve.deployment
    class A:
        async def __del__(self):
            await signal.wait.remote()

    id = DeploymentID(name="A")
    serve.run(A.bind())

    @ray.remote
    def delete_task():
        serve.delete(SERVE_DEFAULT_APP_NAME)

    # Delete application and make sure it is stuck on deleting
    delete_ref = delete_task.remote()
    print("Started task to delete application `default`")

    def application_deleting():
        # Confirm application is in deleting state
        app_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert app_status.status == "DELETING"

        # Confirm deployment is in updating state
        status = serve_instance.get_all_deployment_statuses()[0]
        assert status.name == "A" and status.status == "UPDATING"

        # Confirm replica is stopping
        replicas = ray.get(
            serve_instance._controller._dump_replica_states_for_testing.remote(id)
        )
        assert replicas.count(states=[ReplicaState.STOPPING]) == 1

        # Confirm delete task is still blocked
        finished, pending = ray.wait([delete_ref], timeout=0)
        assert pending and not finished
        return True

    def check_deleted():
        deployment_statuses = serve_instance.get_all_deployment_statuses()
        if len(deployment_statuses) != 0:
            return False

        finished, pending = ray.wait([delete_ref], timeout=0)
        return finished and not pending

    wait_for_condition(application_deleting)
    for _ in range(10):
        time.sleep(0.1)
        assert application_deleting()

    print("Confirmed that application `default` is stuck on deleting.")

    # Kill controller while the application is stuck on deleting
    ray.kill(serve.context._global_client._controller, no_restart=False)
    print("Finished killing the controller (with restart).")

    def check_controller_alive():
        all_current_actors = list_actors(filters=[("state", "=", "ALIVE")])
        for actor in all_current_actors:
            if actor["class_name"] == "ServeController":
                return True
        return False

    wait_for_condition(check_controller_alive)
    print("Controller is back alive.")

    wait_for_condition(application_deleting)
    # Before we send the signal, the application should still be deleting
    for _ in range(10):
        time.sleep(0.1)
        assert application_deleting()

    print("Confirmed that application is still stuck on deleting.")

    # Since we've confirmed the replica is in a stopping state, we can grab
    # the reference to the in-progress graceful shutdown task
    replicas = ray.get(
        serve_instance._controller._dump_replica_states_for_testing.remote(id)
    )
    graceful_shutdown_ref = replicas.get()[0]._actor._graceful_shutdown_ref

    signal.send.remote()
    print("Sent signal to unblock deletion of application")
    wait_for_condition(check_deleted)
    print("Confirmed that application finished deleting and delete task has returned.")

    # Make sure graceful shutdown ran successfully
    ray.get(graceful_shutdown_ref)
    print("Confirmed that graceful shutdown ran successfully.")


def test_controller_crashes_with_logging_config(serve_instance):
    """Controller persists logging config into kv store, and when controller recover
    from crash, it will read logging config from kv store and apply to the
    controller and proxy.
    """

    @serve.deployment
    class Model:
        def __init__(self):
            self.logger = logging.getLogger("ray.serve")

        def __call__(self):
            self.logger.debug("this_is_debug_info")
            return

    serve.run(Model.bind())

    client = serve_instance

    # Update the logging config
    client.update_global_logging_config(
        LoggingConfig(encoding="JSON", log_level="DEBUG")
    )

    def check_log_file(log_file: str, expected_regex: list):
        with open(log_file, "r") as f:
            s = f.read()
            for regex in expected_regex:
                assert re.findall(regex, s) != []
        return True

    # Check the controller update
    def check_log_state():
        logging_config, _ = ray.get(client._controller._get_logging_config.remote())
        assert logging_config.encoding == "JSON"
        assert logging_config.log_level == "DEBUG"
        return True

    wait_for_condition(check_log_state, timeout=60)
    _, log_file_path = ray.get(client._controller._get_logging_config.remote())
    # DEBUG level check & JSON check
    check_log_file(
        log_file_path,
        [".*Configure the serve controller logger.*", '.*"component_name":.*'],
    )

    ray.kill(client._controller, no_restart=False)

    def check_controller_alive():
        all_current_actors = list_actors(filters=[("state", "=", "ALIVE")])
        for actor in all_current_actors:
            if actor["class_name"] == "ServeController":
                return True
        return False

    wait_for_condition(check_controller_alive)

    # Check the controller log config
    wait_for_condition(check_log_state)
    _, new_log_file_path = ray.get(client._controller._get_logging_config.remote())
    assert new_log_file_path != log_file_path
    # Check again, make sure the logging config is recovered.
    check_log_file(new_log_file_path, ['.*"component_name":.*'])

    # Check proxy logging
    def check_proxy_handle_in_controller():
        proxy_handles = ray.get(client._controller.get_proxies.remote())
        assert len(proxy_handles) == 1
        return True

    wait_for_condition(check_proxy_handle_in_controller)
    proxy_handles = ray.get(client._controller.get_proxies.remote())
    proxy_handle = list(proxy_handles.values())[0]
    file_path = ray.get(proxy_handle._get_logging_config.remote())
    # Send request, we should see json logging and debug log message in proxy log.
    resp = requests.get("http://127.0.0.1:8000")
    assert resp.status_code == 200
    wait_for_condition(
        check_log_file, log_file=file_path, expected_regex=['.*"message":.*GET 200.*']
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
