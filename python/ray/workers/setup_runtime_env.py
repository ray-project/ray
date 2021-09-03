import os
import sys
import argparse
import json
import logging

from ray._private.runtime_env import RuntimeEnvContext
from ray._private.runtime_env.conda import setup_conda_or_pip
from ray._private.runtime_env.conda_utils import get_conda_activate_commands

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

# The worker is not set up yet, so we can't get session_dir from the worker.
parser.add_argument(
    "--session-dir", type=str, help="the directory for the current session")


def setup_worker(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    commands = []
    py_executable: str = sys.executable
    runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
    runtime_env_context: RuntimeEnvContext = None
    if args.serialized_runtime_env_context:
        runtime_env_context = RuntimeEnvContext.deserialize(
            args.serialized_runtime_env_context)

    # Ray client server setups runtime env by itself instead of agent.
    if runtime_env.get("conda") or runtime_env.get("pip"):
        if not args.serialized_runtime_env_context:
            runtime_env_context = RuntimeEnvContext(args.session_dir)
            setup_conda_or_pip(runtime_env, runtime_env_context, logger=logger)

    if runtime_env_context and runtime_env_context.working_dir is not None:
        commands += [f"cd {runtime_env_context.working_dir}"]

        # Insert the working_dir as the first entry in PYTHONPATH. This is
        # compatible with users providing their own PYTHONPATH in env_vars.
        env_vars = runtime_env.get("env_vars", None) or {}
        python_path = runtime_env_context.working_dir
        if "PYTHONPATH" in env_vars:
            python_path += os.pathsep + runtime_env["PYTHONPATH"]
        env_vars["PYTHONPATH"] = python_path
        runtime_env["env_vars"] = env_vars

    # Add a conda activate command prefix if using a conda env.
    if runtime_env_context and runtime_env_context.conda_env_name is not None:
        py_executable = "python"
        conda_activate_commands = get_conda_activate_commands(
            runtime_env_context.conda_env_name)
        if (conda_activate_commands):
            commands += conda_activate_commands
    elif runtime_env.get("conda"):
        logger.warning(
            "Conda env name is not found in context, "
            "but conda exists in runtime env. The runtime env %s, "
            "the context %s.", args.serialized_runtime_env,
            args.serialized_runtime_env_context)

    commands += [" ".join([f"exec {py_executable}"] + remaining_args)]
    command_str = " && ".join(commands)

    # update env vars
    if runtime_env.get("env_vars"):
        env_vars = runtime_env["env_vars"]
        os.environ.update(env_vars)
    os.execvp("bash", ["bash", "-c", command_str])


if __name__ == "__main__":
    setup_worker(sys.argv[1:])
