import sys
import argparse
import json
import logging

from ray._private.runtime_env import RuntimeEnvContext
from ray._private.runtime_env.conda import CondaManager

logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser()

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")
parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context")
parser.add_argument(
    "--from-ray-client", type=bool, required=False, default=False)


def setup_worker(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
    runtime_env_context: RuntimeEnvContext = None
    if args.serialized_runtime_env_context:
        runtime_env_context = RuntimeEnvContext.deserialize(
            args.serialized_runtime_env_context)
    else:
        runtime_env_context = RuntimeEnvContext(
            env_vars=runtime_env.get("env_vars"))

    # Ray client server setups runtime env by itself instead of agent.
    if args.from_ray_client:
        if runtime_env.get("conda") or runtime_env.get("pip"):
            CondaManager(runtime_env_context.resources_dir).setup(
                runtime_env, runtime_env_context, logger=logger)

    runtime_env_context.exec_worker(remaining_args)


if __name__ == "__main__":
    setup_worker(sys.argv[1:])
