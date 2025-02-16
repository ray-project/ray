import argparse
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional

import psutil


def hook(runtime_env: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Hook that detects if the driver is run in 'uv run' and sets the runtime environment accordingly."""

    runtime_env = runtime_env or {}

    # uv spawns the python process as a child process, so to determine if
    # we are running under 'uv run', we check the parent process commandline.
    parent = psutil.Process().parent()
    cmdline = parent.cmdline()
    if os.path.basename(cmdline[0]) != "uv" or cmdline[1] != "run":
        # This means the driver was not run with 'uv run' -- in this case
        # we leave the runtime environment unchanged
        return runtime_env

    # Extract the arguments of 'uv run' that are not arguments of the script.
    # First we get the arguments of this script (without the executable):
    script_args = psutil.Process().cmdline()[1:]
    # Then, we remove those arguments from the parent process commandline:
    uv_run_args = cmdline[: len(cmdline) - len(script_args)]

    # Remove the "--directory" argument since it has already been taken into
    # account when setting the current working directory of the current process
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", nargs="?")
    _, remaining_uv_run_args = parser.parse_known_args(uv_run_args)

    runtime_env["py_executable"] = " ".join(remaining_uv_run_args)

    # If the user specified a working_dir, we always honor it, otherwise
    # use the same working_dir that uv run would use
    if "working_dir" not in runtime_env:
        runtime_env["working_dir"] = os.getcwd()

    # In the last part of the function we do some error checking that should catch
    # the most common cases of how things are different in Ray, i.e. not the whole
    # file system will be available on the workers, only the working_dir.

    # First parse the arguments we need to check
    uv_run_parser = argparse.ArgumentParser()
    uv_run_parser.add_argument("--with-requirements", nargs="?")
    uv_run_parser.add_argument("--project", nargs="?")
    uv_run_parser.add_argument("--no-project", action="store_true")
    known_args, _ = uv_run_parser.parse_known_args(uv_run_args)

    working_dir = Path(runtime_env["working_dir"]).resolve()

    # Check if the requirements.txt file is in the working_dir
    if known_args.with_requirements and not Path(
        known_args.with_requirements
    ).resolve().is_relative_to(working_dir):
        raise RuntimeError(
            f"You specified --with-requirements={known_args.with_requirements} but "
            f"the requirements file is not in the working_dir {runtime_env['working_dir']}, "
            "so the workers will not have access to the file. Make sure "
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
            "so the workers will not have access to the file. Make sure "
            "the pyproject.toml file is in the working directory. "
            "You can do so by specifying --directory in 'uv run', by changing the current "
            "working directory before running 'uv run', or by using the 'working_dir' "
            "parameter of the runtime_environment."
        )

    return runtime_env


# This __main__ is used for unit testing if the runtime_env_hook picks up the
# right settings.
if __name__ == "__main__":
    import json

    test_parser = argparse.ArgumentParser()
    test_parser.add_argument("runtime_env")
    args = test_parser.parse_args()
    # We purposefully modify sys.argv here to make sure the hook is robust
    # against such modification.
    sys.argv.pop(1)
    runtime_env = json.loads(args.runtime_env)
    print(json.dumps(hook(runtime_env)))
