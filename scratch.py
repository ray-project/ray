import ray
from ray.serve.generated.serve_pb2 import BackendConfig, JAVA, RequestMetadata, RequestWrapper
from ray.serve.config import ReplicaConfig
from ray import serve
from ray.serve.api import _get_global_client

# Start Ray
ray.init(job_config=ray.job_config.JobConfig(code_search_path=["dummy"]))
serve.start(detached=True, http_options={"location": None})
controller = _get_global_client()._controller

# call controller deploy
backend_config = BackendConfig()
backend_config.is_cross_language = False
backend_config.backend_language = JAVA

replica_config = ReplicaConfig(
    "io.ray.serve.util.ExampleEchoDeployment",
    *["my-prefix "]  #init_args
)

goal_id, _ = ray.get(
    controller.deploy.remote(
        name="my_java_backend",
        backend_config_proto_bytes=backend_config.SerializeToString(),
        replica_config=replica_config,
        version=None,
        prev_version=None,
        route_prefix=None,
    ))
ray.get(controller.wait_for_goal.remote(goal_id))

# Let's try to call it!


def msgpack_serialize(init_args):
    ctx = ray.worker.global_worker.get_serialization_context()
    buffer = ctx.serialize(init_args)
    init_args_serialized = buffer.to_bytes()
    return init_args_serialized


all_handles = ray.get(controller._all_replica_handles.remote())
backend_handle = list(all_handles["my_java_backend"].values())[0]
out = backend_handle.handleRequest.remote(
    RequestMetadata(
        request_id="id-1",
        endpoint="endpoint",
        call_method="call",
    ).SerializeToString(),
    RequestWrapper(body=msgpack_serialize(["hello"])).SerializeToString(),
)
print(ray.get(out))
