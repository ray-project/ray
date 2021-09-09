import sys
import argparse
import logging

from ray._private.runtime_env import RuntimeEnvContext

logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser()

parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context")


def setup_worker(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    # NOTE(edoakes): args.serialized_runtime_env_context is only None in the
    # case that we're starting the main Ray client proxy server. That case
    # should probably not even go through this codepath.
    runtime_env_context = RuntimeEnvContext.deserialize(
        args.serialized_runtime_env_context or "{}")

    runtime_env_context.exec_worker(remaining_args)


if __name__ == "__main__":
    setup_worker(sys.argv[1:])
