import argparse
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional

import psutil


def hook(runtime_env: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Hook that detects if the driver is run in 'uv run' and sets the runtime environment accordingly."""

    runtime_env = runtime_env or {}

    parent = psutil.Process().parent()
    cmdline = parent.cmdline()
    if os.path.basename(cmdline[0]) != "uv" or cmdline[1] != "run":
        # This means the driver was not run with 'uv run' -- in this case
        # we leave the runtime environment unchanged
        return runtime_env

    # Parse known arguments of uv run that impact the runtime environment
    uv_run_parser = argparse.ArgumentParser()
    uv_run_parser.add_argument("--directory", nargs="?")
    uv_run_parser.add_argument("--with-requirements", nargs="?")
    uv_run_parser.add_argument("--project", nargs="?")
    uv_run_parser.add_argument("--no-project", action="store_true")
    known_args, unknown_args = uv_run_parser.parse_known_args(cmdline)

    # Extract the arguments of 'uv run' that are not arguments of the script
    uv_run_args = cmdline[: len(cmdline) - len(sys.argv)]
    runtime_env["py_executable"] = " ".join(uv_run_args)

    # If the user specified a working_dir, we always honor it, otherwise
    # use the same working_dir that uv run would use
    if "working_dir" not in runtime_env:
        runtime_env["working_dir"] = known_args.directory or os.getcwd()

    working_dir = Path(runtime_env["working_dir"]).resolve()

    # Check if the requirements.txt file is in the working_dir
    if known_args.with_requirements and not Path(
        known_args.with_requirements
    ).resolve().is_relative_to(working_dir):
        raise RuntimeError(
            f"You specified --with-requirements={known_args.with_requirements} but "
            f"the requirements file is not in the working_dir {runtime_env['working_dir']}, "
            "so the workers will not have access to the file. Please make sure "
            "the requirements file is in the working directory. "
            "You can do so by specifying --directory in 'uv run', by changing the current "
            "working directory before running 'uv run', or by using the 'working_dir' "
            "parameter of the runtime_environment."
        )

    # Check if the pyproject.toml file is in the working_dir
    pyproject = None
    if known_args.no_project:
        pyproject = None
    elif known_args.project:
        pyproject = Path(known_args.project)
    else:
        # Walk up the directory tree until pyproject.toml is found
        current_path = Path.cwd().resolve()
        while current_path != current_path.parent:
            if (current_path / "pyproject.toml").exists():
                pyproject = Path(current_path / "pyproject.toml")
                break
            current_path = current_path.parent

    if pyproject and not pyproject.resolve().is_relative_to(working_dir):
        raise RuntimeError(
            f"Your {pyproject.resolve()} is not in the working_dir {runtime_env['working_dir']}, "
            "so the workers will not have access to the file. Please make sure "
            "the pyproject.toml file is in the working directory. "
            "You can do so by specifying --directory in 'uv run', by changing the current "
            "working directory before running 'uv run', or by using the 'working_dir' "
            "parameter of the runtime_environment."
        )

    return runtime_env


if __name__ == "__main__":
    import json

    # This is used for unit testing if the runtime_env_hook picks up the
    # right settings.
    runtime_env = json.loads(sys.argv[1])
    print(json.dumps(hook(runtime_env)))
