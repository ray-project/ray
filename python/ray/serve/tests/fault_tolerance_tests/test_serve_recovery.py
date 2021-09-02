

import sys

import pytest

import ray
from ray import serve
from ray.serve.constants import (SERVE_CONTROLLER_NAME, SERVE_PROXY_NAME)
from ray.serve.tests.test_failure import request_with_retries

@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_recover_from_replica_actor_names(serve_instance):
    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    @serve.deployment(name="recover_from_replica_actor_names", num_replicas=2)
    class TransientConstructorFailureDeployment:
        def __init__(self):
            return True

        def __call__(self, *args):
            return "hii"

    TransientConstructorFailureDeployment.deploy()
    # Assert 2 replicas are running in deployment backend after partially
    # successful deploy() call with transient error
    backend_dict = ray.get(
        serve_instance._controller._all_replica_handles.remote())
    assert len(backend_dict["recover_from_replica_actor_names"]) == 2

    # Sample: [
    # 'TransientConstructorFailureDeployment#xlituP',
    # 'SERVE_CONTROLLER_ACTOR',
    # 'TransientConstructorFailureDeployment#NosHNA',
    # 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-node:192.168.86.165-0']
    all_actor_names = ray.util.list_named_actors()
    all_replica_names = [
        actor_name for actor_name in all_actor_names
        if (SERVE_CONTROLLER_NAME not in actor_name
            and SERVE_PROXY_NAME not in actor_name)
    ]
    assert len(all_replica_names) == 2, (
        "Should have two running replicas fetched from ray API.")

    # Kill controller and wait for endpoint to be available again
    ray.kill(serve.api._global_client._controller, no_restart=False)
    for _ in range(10):
        response = request_with_retries(
            "/recover_from_replica_actor_names/", timeout=30)
        assert response.text == "hii"

    # Ensure recovered replica names are the same
    recovered_all_actor_names = ray.util.list_named_actors()
    recovered_all_replica_names = [
        actor_name for actor_name in recovered_all_actor_names
        if (SERVE_CONTROLLER_NAME not in actor_name
            and SERVE_PROXY_NAME not in actor_name)
    ]
    assert recovered_all_replica_names == all_replica_names, (
        "Running replica actor names after recovery must match")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
