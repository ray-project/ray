import os
import sys
import tempfile

import pytest

import ray
from ray import serve


def test_deploy_with_consistent_constructor_failure(serve_instance):
    # # Test failed to deploy with total of 1 replica
    @serve.deployment(num_replicas=1)
    class ConstructorFailureDeploymentOneReplica:
        def __init__(self):
            raise RuntimeError("Intentionally throwing on only one replica")

        async def serve(self, request):
            return "hi"

    with pytest.raises(RuntimeError):
        ConstructorFailureDeploymentOneReplica.deploy()

    # Assert no replicas are running in deployment deployment after failed
    # deploy() call
    deployment_dict = ray.get(
        serve_instance._controller._all_running_replicas.remote())
    assert deployment_dict["ConstructorFailureDeploymentOneReplica"] == []

    # # Test failed to deploy with total of 2 replicas
    @serve.deployment(num_replicas=2)
    class ConstructorFailureDeploymentTwoReplicas:
        def __init__(self):
            raise RuntimeError("Intentionally throwing on both replicas")

        async def serve(self, request):
            return "hi"

    with pytest.raises(RuntimeError):
        ConstructorFailureDeploymentTwoReplicas.deploy()

    # Assert no replicas are running in deployment deployment after failed
    # deploy() call
    deployment_dict = ray.get(
        serve_instance._controller._all_running_replicas.remote())
    assert deployment_dict["ConstructorFailureDeploymentTwoReplicas"] == []


def test_deploy_with_partial_constructor_failure(serve_instance):
    # Test deploy with 2 replicas but one of them failed all
    # attempts
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_deploy.txt")

        @serve.deployment(num_replicas=2)
        class PartialConstructorFailureDeployment:
            def __init__(self):
                if not os.path.exists(file_path):
                    with open(file_path, "w") as f:
                        # Write first replica tag to local file so that it will
                        # consistently fail even retried on other actor
                        f.write(serve.get_replica_context().replica_tag)
                    raise RuntimeError(
                        "Consistently throwing on same replica.")
                else:
                    with open(file_path) as f:
                        content = f.read()
                        if content == serve.get_replica_context().replica_tag:
                            raise RuntimeError(
                                "Consistently throwing on same replica.")
                        else:
                            return True

            async def serve(self, request):
                return "hi"

        PartialConstructorFailureDeployment.deploy()

    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy() call
    deployment_dict = ray.get(
        serve_instance._controller._all_running_replicas.remote())
    assert len(deployment_dict["PartialConstructorFailureDeployment"]) == 2


def test_deploy_with_transient_constructor_failure(serve_instance):
    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_deploy.txt")

        @serve.deployment(num_replicas=2)
        class TransientConstructorFailureDeployment:
            def __init__(self):
                if os.path.exists(file_path):
                    return True
                else:
                    with open(file_path, "w") as f:
                        f.write("ONE")
                    raise RuntimeError("Intentionally throw on first try.")

            async def serve(self, request):
                return "hi"

        TransientConstructorFailureDeployment.deploy()
    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy() call with transient error
    deployment_dict = ray.get(
        serve_instance._controller._all_running_replicas.remote())
    assert len(deployment_dict["TransientConstructorFailureDeployment"]) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
