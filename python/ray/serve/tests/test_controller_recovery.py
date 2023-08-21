import asyncio
import os
import sys
import time
import pytest
from collections import defaultdict

import ray
from ray.exceptions import RayTaskError
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.util.state import list_actors


from ray import serve
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    SERVE_CONTROLLER_NAME,
    SERVE_PROXY_NAME,
    SERVE_NAMESPACE,
)
from ray.serve.tests.test_failure import request_with_retries
from ray.serve._private.utils import get_random_letters


def test_recover_start_from_replica_actor_names(serve_instance):
    """Test controller is able to recover starting -> running replicas from
    actor names.
    """

    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    @serve.deployment(name="recover_start_from_replica_actor_names", num_replicas=2)
    class TransientConstructorFailureDeployment:
        def __init__(self):
            return True

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
    id = DeploymentID("recover_start_from_replica_actor_names", "app")
    assert len(deployment_dict[id]) == 2

    replica_version_hash = None
    for replica in deployment_dict[id]:
        ref = replica.actor_handle._get_metadata.remote()
        _, version = ray.get(ref)
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
        _, version = ray.get(ref)
        assert replica_version_hash == hash(
            version
        ), "Replica version hash should be the same after recover from actor names"


def test_recover_rolling_update_from_replica_actor_names(serve_instance):
    """Test controller is able to recover starting -> updating -> running
    replicas from actor names, with right replica versions during rolling
    update.
    """
    client = serve_instance

    name = "test"

    @ray.remote(num_cpus=0)
    def call(block=False):
        handle = serve.get_deployment_handle(name, "app")
        ret = handle.handler.remote(block).result()

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal#{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    @serve.deployment(name=name, version="1", num_replicas=2)
    class V1:
        async def handler(self, block: bool):
            if block:
                signal = ray.get_actor(signal_name)
                await signal.wait.remote()

            return f"1|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler(request.query_params["block"] == "True")

    class V2:
        async def handler(self, *args):
            return f"2|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler()

    def make_nonblocking_calls(expected, expect_blocking=False, num_returns=1):
        # Returns dict[val, set(pid)].
        blocking = []
        responses = defaultdict(set)
        start = time.time()
        timeout_value = 60 if sys.platform == "win32" else 30
        while time.time() - start < timeout_value:
            refs = [call.remote(block=False) for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=5, num_returns=num_returns)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)
            for ref in not_ready:
                blocking.extend(not_ready)

            if all(len(responses[val]) >= num for val, num in expected.items()) and (
                expect_blocking is False or len(blocking) > 0
            ):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    serve.run(V1.bind(), name="app")
    responses1, _ = make_nonblocking_calls({"1": 2}, num_returns=2)
    pids1 = responses1["1"]

    # ref2 will block a single replica until the signal is sent. Check that
    # some requests are now blocking.
    ref2 = call.remote(block=True)
    responses2, blocking2 = make_nonblocking_calls({"1": 1}, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    ray.kill(serve.context._global_client._controller, no_restart=False)

    # Redeploy new version. Since there is one replica blocking, only one new
    # replica should be started up.
    V2 = V1.options(func_or_class=V2, version="2")
    serve.run(V2.bind(), _blocking=False, name="app")
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("app", timeout_s=0.1)
    responses3, blocking3 = make_nonblocking_calls({"1": 1}, expect_blocking=True)

    ray.kill(serve.context._global_client._controller, no_restart=False)

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    client._wait_for_application_running("app")
    make_nonblocking_calls({"2": 2}, num_returns=2)


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

    serve.run(V1.bind(), _blocking=False, name="app")
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
        id = DeploymentID("V1", "app")
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

    id = DeploymentID("A", SERVE_DEFAULT_APP_NAME)
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
