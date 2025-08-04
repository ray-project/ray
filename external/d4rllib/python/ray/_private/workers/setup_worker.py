import argparse
import logging

from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL
from ray._private.ray_logging import setup_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=("Set up the environment for a Ray worker and launch the worker.")
)

parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context",
)

parser.add_argument("--language", type=str, help="the language type of the worker")


if __name__ == "__main__":
    setup_logger(LOGGER_LEVEL, LOGGER_FORMAT)
    args, remaining_args = parser.parse_known_args()
    # NOTE(edoakes): args.serialized_runtime_env_context is only None when
    # we're starting the main Ray client proxy server. That case should
    # probably not even go through this codepath.
    runtime_env_context = RuntimeEnvContext.deserialize(
        args.serialized_runtime_env_context or "{}"
    )
    runtime_env_context.exec_worker(remaining_args, Language.Value(args.language))
