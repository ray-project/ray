import pytest

import ray
from ray.job_config import JobConfig
from ray import serve
from ray.serve.config import ReplicaConfig, DeploymentConfig
from ray.serve.generated.serve_pb2 import JAVA, RequestMetadata
from ray.tests.conftest import shutdown_only, maybe_external_redis  # noqa: F401


@pytest.mark.skip(reason="TIMEOUT, see https://github.com/ray-project/ray/issues/26513")
def test_controller_starts_java_replica(shutdown_only):  # noqa: F811
    ray.init(
        num_cpus=8,
        namespace="default_test_namespace",
        # A dummy code search path to enable cross language.
        job_config=JobConfig(code_search_path=["."]),
    )
    client = serve.start(detached=True)

    controller = client._controller

    config = DeploymentConfig()
    config.deployment_language = JAVA
    config.is_cross_language = True

    replica_config = ReplicaConfig.create(
        "io.ray.serve.util.ExampleEchoDeployment",
        init_args=["my_prefix "],
    )

    # Deploy it
    deployment_name = "my_java"
    updating = ray.get(
        controller.deploy.remote(
            name=deployment_name,
            deployment_config_proto_bytes=config.to_proto_bytes(),
            replica_config_proto_bytes=replica_config.to_proto_bytes(),
            route_prefix=None,
            deployer_job_id=ray.get_runtime_context().job_id,
        )
    )
    assert updating
    client._wait_for_deployment_healthy(deployment_name)

    # Let's try to call it!
    all_handles = ray.get(controller._all_running_replicas.remote())
    backend_handle = all_handles["my_java"][0].actor_handle
    out = backend_handle.handleRequest.remote(
        RequestMetadata(
            request_id="id-1",
            endpoint="endpoint",
            call_method="call",
        ).SerializeToString(),
        ["hello"],
    )
    assert ray.get(out) == "my_prefix hello"

    handle = serve.get_deployment("my_java").get_handle()
    handle_out = handle.remote("hello handle")
    assert ray.get(handle_out) == "my_prefix hello handle"

    ray.get(controller.delete_deployment.remote(deployment_name))
    client._wait_for_deployment_deleted(deployment_name)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
