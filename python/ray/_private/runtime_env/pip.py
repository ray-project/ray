import logging

from ray.core.generated.common_pb2 import RuntimeEnv, PipRuntimeEnv

default_logger = logging.getLogger(__name__)


def build_proto_pip_runtime_env(runtime_env_dict: dict, runtime_env: RuntimeEnv):
    """ Construct pip runtime env protobuf from runtime env dict.
    """
    if runtime_env_dict.get("pip"):
        runtime_env.pip_runtime_env.config.packages(runtime_env["pip"])


def parse_proto_pip_runtime_env(runtime_env: RuntimeEnv, runtime_env_dict: dict):
    """ Parse pip runtime env protobuf to runtime env dict.
    """
    if runtime_env.HasField("pip_runtime_env"):
        runtime_env_dict["pip"] = list(runtime_env.pip_runtime_env.config.packages)
