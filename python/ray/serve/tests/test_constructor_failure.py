import sys

import pytest

import ray
from ray import serve
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
            replica_id = serve.get_replica_context().replica_id.unique_id
            is_first = ray.get(store.set_if_first.remote(replica_id))
            if is_first:
                raise RuntimeError("Consistently throwing on same replica.")
            failed_id = ray.get(store.get.remote())
            if replica_id == failed_id:
                raise RuntimeError("Consistently throwing on same replica.")

        async def serve(self, request):
            return "hi"

    serve.run(PartialConstructorFailureDeployment.bind(failed_store))

    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy call
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    deployment_id = DeploymentID(name="PartialConstructorFailureDeployment")
    assert len(deployment_dict[deployment_id]) == 2


def test_deploy_with_transient_constructor_failure(serve_instance):
    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    failed_store = FailedReplicaStore.remote()

    @serve.deployment(num_replicas=2)
    class TransientConstructorFailureDeployment:
        def __init__(self, store):
            is_first = ray.get(store.set_if_first.remote("ONE"))
            if is_first:
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
