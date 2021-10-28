import logging

from ray.core.generated.common_pb2 import ContainerRuntimeEnv

default_logger = logging.getLogger(__name__)


def get_proto_container_runtime_env(runtime_env) -> ContainerRuntimeEnv:
    """ Construct a pip runtime env protobuf from a runtime env dict.
    """

    if runtime_env.get("container"):
        container = runtime_env["container"]
        container_runtime_env = ContainerRuntimeEnv()
        container_runtime_env.image = container.get("image", "")
        container_runtime_env.worker_path = container.get("worker_path", "")
        container_runtime_env.run_options.extend(container.get("run_options", []))
        return container_runtime_env

    return None
