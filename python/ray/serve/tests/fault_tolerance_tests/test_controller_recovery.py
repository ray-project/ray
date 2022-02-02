from collections import defaultdict

import sys
import os
import pytest
import time

import ray
from ray import serve
from ray.serve.constants import SERVE_CONTROLLER_NAME, SERVE_PROXY_NAME
from ray.serve.tests.test_failure import request_with_retries
from ray._private.test_utils import SignalActor
from ray.serve.utils import get_random_letters


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

    TransientConstructorFailureDeployment.deploy()
    for _ in range(10):
        response = request_with_retries(
            "/recover_start_from_replica_actor_names/", timeout=30
        )
        assert response.text == "hii"
    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy() call with transient error
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    assert len(deployment_dict["recover_start_from_replica_actor_names"]) == 2

    replica_version_hash = None
    for replica in deployment_dict["recover_start_from_replica_actor_names"]:
        ref = replica.actor_handle.get_metadata.remote()
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
    all_actor_names = ray.util.list_named_actors()
    all_replica_names = [
        actor_name
        for actor_name in all_actor_names
        if (
            SERVE_CONTROLLER_NAME not in actor_name
            and SERVE_PROXY_NAME not in actor_name
        )
    ]
    assert (
        len(all_replica_names) == 2
    ), "Should have two running replicas fetched from ray API."

    # Kill controller and wait for endpoint to be available again
    ray.kill(serve.api._global_client._controller, no_restart=False)
    for _ in range(10):
        response = request_with_retries(
            "/recover_start_from_replica_actor_names/", timeout=30
        )
        assert response.text == "hii"

    # Ensure recovered replica names are the same
    recovered_all_actor_names = ray.util.list_named_actors()
    recovered_all_replica_names = [
        actor_name
        for actor_name in recovered_all_actor_names
        if (
            SERVE_CONTROLLER_NAME not in actor_name
            and SERVE_PROXY_NAME not in actor_name
        )
    ]
    assert (
        recovered_all_replica_names == all_replica_names
    ), "Running replica actor names after recovery must match"

    # Ensure recovered replica version has are the same
    for replica_name in recovered_all_replica_names:
        actor_handle = ray.get_actor(replica_name)
        ref = actor_handle.get_metadata.remote()
        _, version = ray.get(ref)
        assert replica_version_hash == hash(version), (
            "Replica version hash should be the same after " "recover from actor names"
        )


def test_recover_rolling_update_from_replica_actor_names(serve_instance):
    """Test controller is able to recover starting -> updating -> running
    replicas from actor names, with right replica versions during rolling
    update.
    """
    client = serve_instance

    name = "test"

    @ray.remote(num_cpus=0)
    def call(block=False):
        handle = serve.get_deployment(name).get_handle()
        ret = ray.get(handle.handler.remote(block))

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

    V1.deploy()
    responses1, _ = make_nonblocking_calls({"1": 2}, num_returns=2)
    pids1 = responses1["1"]

    # ref2 will block a single replica until the signal is sent. Check that
    # some requests are now blocking.
    ref2 = call.remote(block=True)
    responses2, blocking2 = make_nonblocking_calls({"1": 1}, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    ray.kill(serve.api._global_client._controller, no_restart=False)

    # Redeploy new version. Since there is one replica blocking, only one new
    # replica should be started up.
    V2 = V1.options(func_or_class=V2, version="2")
    goal_ref = V2.deploy(_blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    responses3, blocking3 = make_nonblocking_calls({"1": 1}, expect_blocking=True)

    ray.kill(serve.api._global_client._controller, no_restart=False)

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    assert client._wait_for_goal(goal_ref)
    make_nonblocking_calls({"2": 2}, num_returns=2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
