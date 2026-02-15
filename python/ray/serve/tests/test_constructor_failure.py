import os
import sys
import tempfile

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID
from ray.serve.config import GangSchedulingConfig


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
                        f.write(serve.get_replica_context().replica_id.unique_id)
                    raise RuntimeError("Consistently throwing on same replica.")
                else:
                    with open(file_path) as f:
                        content = f.read()
                    if content == serve.get_replica_context().replica_id.unique_id:
                        raise RuntimeError("Consistently throwing on same replica.")

            async def serve(self, request):
                return "hi"

        serve.run(PartialConstructorFailureDeployment.bind())

    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy call
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    deployment_id = DeploymentID(name="PartialConstructorFailureDeployment")
    assert len(deployment_dict[deployment_id]) == 2


def test_deploy_with_transient_constructor_failure(serve_instance):
    # Test failed to deploy with total of 2 replicas,
    # but first constructor call fails.
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_deploy.txt")

        @serve.deployment(num_replicas=2)
        class TransientConstructorFailureDeployment:
            def __init__(self):
                if os.path.exists(file_path):
                    return
                with open(file_path, "w") as f:
                    f.write("ONE")
                raise RuntimeError("Intentionally throw on first try.")

            async def serve(self, request):
                return "hi"

        serve.run(TransientConstructorFailureDeployment.bind())
    # Assert 2 replicas are running in deployment deployment after partially
    # successful deploy call with transient error
    deployment_dict = ray.get(serve_instance._controller._all_running_replicas.remote())
    deployment_id = DeploymentID(name="TransientConstructorFailureDeployment")
    assert len(deployment_dict[deployment_id]) == 2


def test_gang_deploy_with_consistent_constructor_failure(ray_shutdown):
    """Validates gang deployment where all replicas consistently fail their constructor."""
    ray.init(num_cpus=4)
    serve.start()

    @serve.deployment(
        num_replicas=4,
        ray_actor_options={"num_cpus": 0.1},
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
    )
    class GangConstructorFailure:
        def __init__(self):
            raise RuntimeError("Intentionally failing gang replica constructor")

        async def __call__(self, request):
            return "hi"

    with pytest.raises(RuntimeError):
        serve.run(GangConstructorFailure.bind())

    client = serve.context._get_global_client()
    deployment_dict = ray.get(client._controller._all_running_replicas.remote())
    deployment_id = DeploymentID(name="GangConstructorFailure")
    assert len(deployment_dict[deployment_id]) == 0


def test_gang_deploy_with_partial_constructor_failure(ray_shutdown):
    """Validates gang deployment where one replica consistently fails."""
    ray.init(num_cpus=8)
    serve.start()

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_deploy.txt")

        @serve.deployment(
            num_replicas=4,
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangPartialConstructorFailure:
            def __init__(self):
                if not os.path.exists(file_path):
                    with open(file_path, "w") as f:
                        f.write(serve.get_replica_context().replica_id.unique_id)
                    raise RuntimeError("Consistently throwing on same replica.")
                else:
                    with open(file_path) as f:
                        content = f.read()
                    if content == serve.get_replica_context().replica_id.unique_id:
                        raise RuntimeError("Consistently throwing on same replica.")

            async def __call__(self, request):
                return "hi"

        serve.run(GangPartialConstructorFailure.bind())

    client = serve.context._get_global_client()
    deployment_id = DeploymentID(name="GangPartialConstructorFailure")
    deployment_dict = ray.get(client._controller._all_running_replicas.remote())
    assert len(deployment_dict[deployment_id]) == 4


def test_gang_deploy_with_transient_constructor_failure(ray_shutdown):
    """Validates gang deployment where the first constructor call fails then succeeds."""
    ray.init(num_cpus=8)
    serve.start()

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test_deploy.txt")

        @serve.deployment(
            num_replicas=4,
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangTransientConstructorFailure:
            def __init__(self):
                if os.path.exists(file_path):
                    return
                with open(file_path, "w") as f:
                    f.write("ONE")
                raise RuntimeError("Intentionally throw on first try.")

            async def __call__(self, request):
                return "hi"

        serve.run(GangTransientConstructorFailure.bind())

    client = serve.context._get_global_client()
    deployment_id = DeploymentID(name="GangTransientConstructorFailure")
    deployment_dict = ray.get(client._controller._all_running_replicas.remote())
    assert len(deployment_dict[deployment_id]) == 4


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
