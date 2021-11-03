import logging

from ray.core.generated.common_pb2 import RuntimeEnv, ContainerRuntimeEnv

default_logger = logging.getLogger(__name__)


def build_proto_container_runtime_env(runtime_env_dict: dict, runtime_env: RuntimeEnv):
    """ Construct container runtime env protobuf from runtime env dict.
    """
    if runtime_env_dict.get("container"):
        container = runtime_env_dict["container"]
        runtime_env.container_runtime_env.image = container.get("image", "")
        runtime_env.container_runtime_env.worker_path = container.get("worker_path", "")
        runtime_env.container_runtime_env.run_options.extend(container.get("run_options", []))


def parse_proto_container_runtime_env(runtime_env: RuntimeEnv, runtime_env_dict: dict):
    """ Parse container runtime env protobuf to runtime env dict.
    """
    if runtime_env.HasField("py_container_runtime_env"):
        runtime_env_dict["container"]["image"] = runtime_env.container_runtime_env.image
        runtime_env_dict["container"]["worker_path"] = runtime_env.container_runtime_env.worker_path
        runtime_env_dict["container"]["run_options"] = list(runtime_env.container_runtime_env.run_options)
