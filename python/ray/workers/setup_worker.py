import argparse
import logging
import os

from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")

parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context")

parser.add_argument(
    "--language", type=str, help="the language type of the worker")

if __name__ == "__main__":
    args, remaining_args = parser.parse_known_args()
    # NOTE(chenk008): we still need it to start worker in container.
    remaining_args.append("--worker-shim-pid={}".format(os.getpid()))
    # NOTE(edoakes): args.serialized_runtime_env_context is only None when
    # we're starting the main Ray client proxy server. That case should
    # probably not even go through this codepath.
    runtime_env_context = RuntimeEnvContext.deserialize(
        args.serialized_runtime_env_context or "{}")
    runtime_env_context.exec_worker(remaining_args,
                                    Language.Value(args.language))
