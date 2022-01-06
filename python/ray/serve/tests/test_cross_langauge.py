import ray
from ray.serve.api import _get_global_client
from ray.serve.config import ReplicaConfig, DeploymentConfig
from ray.serve.utils import msgpack_serialize
from ray.serve.generated.serve_pb2 import (JAVA, RequestMetadata,
                                           RequestWrapper)


def test_controller_starts_java_replica(serve_instance):
    controller = _get_global_client()._controller

    config = DeploymentConfig()
    config.deployment_language = JAVA
    config.is_cross_language = True

    replica_config = ReplicaConfig(
        "io.ray.serve.util.ExampleEchoDeployment",
        init_args=["my_prefix "],
    )

    # Deploy it
    # TODO(simon): we should be API wrapper for this.
    goal_id, _ = ray.get(
        controller.deploy.remote(
            name="my_java",
            deployment_config_proto_bytes=config.to_proto_bytes(),
            replica_config=replica_config,
            version=None,
            prev_version=None,
            route_prefix=None,
            deployer_job_id=ray.get_runtime_context().job_id))
    ray.get(controller.wait_for_goal.remote(goal_id))

    # Let's try to call it!

    all_handles = ray.get(controller._all_running_replicas.remote())
    backend_handle = all_handles["my_java"][0].actor_handle
    out = backend_handle.handleRequest.remote(
        RequestMetadata(
            request_id="id-1",
            endpoint="endpoint",
            call_method="call",
        ).SerializeToString(),
        RequestWrapper(body=msgpack_serialize("hello")).SerializeToString(),
    )
    assert ray.get(out) == "my_prefix hello"

    ray.get(
        controller.wait_for_goal.remote(
            ray.get(controller.delete_deployment.remote("my_java"))))
