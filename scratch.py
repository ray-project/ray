import ray
from ray.serve.generated.serve_pb2 import BackendConfig
import msgpack
from ray import serve
from ray.serve.api import _get_global_client

# ray.init()
ray.init(job_config=ray.job_config.JobConfig(code_search_path=["dummy"]))

serve.start(detached=True)
controller = _get_global_client()._controller
controller_name = _get_global_client()._controller_name

backend_config = BackendConfig()
backend_config.is_cross_language = False
# init_args = ["backend_tag", "replicaTag", "controllerName", None]
init_args = ["prefix"]

ctx = ray.worker.global_worker.get_serialization_context()
buffer = ctx.serialize(init_args)
init_args_serialized = buffer.to_bytes()

cls = ray.java_actor_class("io.ray.serve.RayServeWrappedReplica")
handle = cls.options(num_returns=0).remote(
    'backend_tag',
    'replica_tag',
    "io.ray.serve.util.ExampleEchoDeployment",
    init_args_serialized,
    backend_config.SerializeToString(),
    # controller,
    controller_name,
)
import time
time.sleep(2)
ray.get(handle.ready.remote())