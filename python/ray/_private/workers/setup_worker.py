import argparse
import logging
import os

from ray._private.ray_constants import (
    LOGGER_FORMAT,
    LOGGER_LEVEL,
    SERIALIZED_RUNTIME_ENV_CONTEXT_ENV_VAR,
)
from ray._private.ray_logging import setup_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=("Set up the environment for a Ray worker and launch the worker.")
)

parser.add_argument("--language", type=str, help="the language type of the worker")


if __name__ == "__main__":
    setup_logger(LOGGER_LEVEL, LOGGER_FORMAT)
    args, remaining_args = parser.parse_known_args()
    # NOTE(edoakes): the context is only unset when we're starting the main
    # Ray client proxy server. That case should probably not even go through
    # this codepath.
    # Popped so the worker doesn't inherit it.
    runtime_env_context = RuntimeEnvContext.deserialize(
        os.environ.pop(SERIALIZED_RUNTIME_ENV_CONTEXT_ENV_VAR, None) or "{}"
    )
    runtime_env_context.exec_worker(remaining_args, Language.Value(args.language))
