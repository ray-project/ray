import json
import os
import sys
from typing import Any, Dict, Optional


def runtime_env_override_hook(runtime_env) -> Dict[str, Any]:
    """Hook to override the runtime environment from RAY_OVERRIDE_RUNTIME_ENV_JSON."""

    if "RAY_OVERRIDE_RUNTIME_ENV_JSON" in os.environ:
        runtime_env = json.loads(os.environ["RAY_OVERRIDE_RUNTIME_ENV_JSON"])

    return runtime_env


def uv_run_runtime_env_hook(runtime_env: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Hook that detects if the driver is run in 'uv run' and sets the runtime environment accordingly."""

    runtime_env = runtime_env or {}

    import argparse
    import psutil

    parent = psutil.Process().parent()
    cmdline = parent.cmdline()
    if os.path.basename(cmdline[0]) != "uv" or cmdline[1] != "run":
        # This means the driver was not run with 'uv run' -- in this case
        # we leave the runtime environment unchanged
        return runtime_env

    # Parse known arguments of uv run that impact the runtime environment
    uv_run_parser = argparse.ArgumentParser()
    uv_run_parser.add_argument("--directory", nargs="?")
    known_args, unknown_args = uv_run_parser.parse_known_args(cmdline)

    # Extract the arguments of 'uv run' that are not arguments of the script
    uv_run_args = cmdline[: len(cmdline) - len(sys.argv)]
    runtime_env["py_executable"] = " ".join(uv_run_args)

    # If the user specified a working_dir, we will always honor it, otherwise
    # use the same working_dir that uv run would use
    if "working_dir" not in runtime_env:
        runtime_env["working_dir"] = known_args.directory or os.getcwd()

    return runtime_env


# List of files to exclude from the Ray directory when using runtime_env for
# Ray development. These are not necessary in the Ray workers.
RAY_WORKER_DEV_EXCLUDES = ["raylet", "gcs_server", "cpp/", "tests/", "core/src"]
