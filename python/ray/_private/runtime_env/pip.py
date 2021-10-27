import logging

from ray.core.generated.common_pb2 import PipRuntimeEnv

default_logger = logging.getLogger(__name__)


def get_proto_pip_runtime_env(runtime_env) -> PipRuntimeEnv:
    """ Construct a pip runtime env protobuf from a runtime env dict.
    """

    if runtime_env.get("pip"):
        config = PipRuntimeEnv.Config(runtime_env["pip"])
        pip_runtime_env = PipRuntimeEnv()
        pip_runtime_env.config = config
        return pip_runtime_env

    return None
