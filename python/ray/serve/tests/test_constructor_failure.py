import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.test_utils import FailedReplicaStore


def test_deploy_with_consistent_constructor_failure(serve_instance):
    # Test failed to deploy with total of 1 replica
    @serve.deployment(num_replicas=1)
    class ConstructorFailureDeploymentOneReplica:
        def __init__(self):
            raise RuntimeError("Intentionally throwing on only one replica")

        async def serve(self, request):
            return "hi"

    with pytest.raises(RuntimeError):
        serve.run(ConstructorFailureDeploymentOneReplica.bind())

    # Assert no replicas are running in deployment deployment after failed
    # deploy call
    deployment_id = DeploymentID(name="ConstructorFailureDeploymentOneReplica")
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    assert deployment_dict[deployment_id] == []

    # # Test failed to deploy with total of 2 replicas
    @serve.deployment(num_replicas=2)
    class ConstructorFailureDeploymentTwoReplicas:
        def __init__(self):
            raise RuntimeError("Intentionally throwing on both replicas")

        async def serve(self, request):
            return "hi"

    with pytest.raises(RuntimeError):
        serve.run(ConstructorFailureDeploymentTwoReplicas.bind())

    # Assert no replicas are running in deployment deployment after failed
    # deploy call
    deployment_id = DeploymentID(name="ConstructorFailureDeploymentTwoReplicas")
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    assert deployment_dict[deployment_id] == []


def test_deploy_with_partial_constructor_failure(serve_instance):
    # Test deploy with 2 replicas but one of them failed all
    # attempts.
    failed_store = FailedReplicaStore.remote()

    @serve.deployment(num_replicas=2)
    class PartialConstructorFailureDeployment:
        def __init__(self, store):
            if ray.get(store.should_fail.remote()):
                raise RuntimeError("Consistently throwing on same replica.")

        async def serve(self, request):
            return "hi"

    serve._run(PartialConstructorFailureDeployment.bind(failed_store), _blocking=False)

    deployment_id = DeploymentID(name="PartialConstructorFailureDeployment")

    def _one_replica_running() -> bool:
        deployment_dict = ray.get(
            serve_instance._controller._all_running_replicas.remote()
        )
        return len(deployment_dict.get(deployment_id, [])) == 1

    wait_for_condition(_one_replica_running, timeout=30)

    # Wait well past the failed-to-start threshold
    # (max(num_replicas * 3, 6) = 6 for 2 replicas)
    # to prove the deployment stays stuck and never transitions.
    def _enough_retries_and_still_stable() -> bool:
        fail_count = ray.get(failed_store.get_fail_count.remote())
        return fail_count >= 8 and _one_replica_running()

    wait_for_condition(_enough_retries_and_still_stable, timeout=90)


def test_deploy_with_transient_constructor_failure(serve_instance):
    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    failed_store = FailedReplicaStore.remote(fail_first=True)

    @serve.deployment(num_replicas=2)
    class TransientConstructorFailureDeployment:
        def __init__(self, store):
            if ray.get(store.should_fail.remote()):
                raise RuntimeError("Intentionally throw on first try.")

        async def serve(self, request):
            return "hi"

    serve.run(TransientConstructorFailureDeployment.bind(failed_store))
    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy call with transient error
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    deployment_id = DeploymentID(name="TransientConstructorFailureDeployment")
    assert len(deployment_dict[deployment_id]) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
