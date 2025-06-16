import argparse
import copy
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil


def _create_uv_run_parser():
    """Create and return the argument parser for 'uv run' command."""

    parser = argparse.ArgumentParser(prog="uv run", add_help=False)

    # Positional argument - using remainder to capture everything after the command
    parser.add_argument("command", nargs=argparse.REMAINDER)

    # Main options group
    main_group = parser.add_argument_group("Main options")
    main_group.add_argument("--extra", action="append", dest="extras")
    main_group.add_argument("--all-extras", action="store_true")
    main_group.add_argument("--no-extra", action="append", dest="no_extras")
    main_group.add_argument("--no-dev", action="store_true")
    main_group.add_argument("--group", action="append", dest="groups")
    main_group.add_argument("--no-group", action="append", dest="no_groups")
    main_group.add_argument("--no-default-groups", action="store_true")
    main_group.add_argument("--only-group", action="append", dest="only_groups")
    main_group.add_argument("--all-groups", action="store_true")
    main_group.add_argument("-m", "--module")
    main_group.add_argument("--only-dev", action="store_true")
    main_group.add_argument("--no-editable", action="store_true")
    main_group.add_argument("--exact", action="store_true")
    main_group.add_argument("--env-file", action="append", dest="env_files")
    main_group.add_argument("--no-env-file", action="store_true")

    # With options
    with_group = parser.add_argument_group("With options")
    with_group.add_argument("--with", action="append", dest="with_packages")
    with_group.add_argument("--with-editable", action="append", dest="with_editable")
    with_group.add_argument(
        "--with-requirements", action="append", dest="with_requirements"
    )

    # Environment options
    env_group = parser.add_argument_group("Environment options")
    env_group.add_argument("--isolated", action="store_true")
    env_group.add_argument("--active", action="store_true")
    env_group.add_argument("--no-sync", action="store_true")
    env_group.add_argument("--locked", action="store_true")
    env_group.add_argument("--frozen", action="store_true")

    # Script options
    script_group = parser.add_argument_group("Script options")
    script_group.add_argument("-s", "--script", action="store_true")
    script_group.add_argument("--gui-script", action="store_true")

    # Workspace options
    workspace_group = parser.add_argument_group("Workspace options")
    workspace_group.add_argument("--all-packages", action="store_true")
    workspace_group.add_argument("--package")
    workspace_group.add_argument("--no-project", action="store_true")

    # Index options
    index_group = parser.add_argument_group("Index options")
    index_group.add_argument("--index", action="append", dest="indexes")
    index_group.add_argument("--default-index")
    index_group.add_argument("-i", "--index-url")
    index_group.add_argument(
        "--extra-index-url", action="append", dest="extra_index_urls"
    )
    index_group.add_argument("-f", "--find-links", action="append", dest="find_links")
    index_group.add_argument("--no-index", action="store_true")
    index_group.add_argument(
        "--index-strategy",
        choices=["first-index", "unsafe-first-match", "unsafe-best-match"],
    )
    index_group.add_argument("--keyring-provider", choices=["disabled", "subprocess"])

    # Resolver options
    resolver_group = parser.add_argument_group("Resolver options")
    resolver_group.add_argument("-U", "--upgrade", action="store_true")
    resolver_group.add_argument(
        "-P", "--upgrade-package", action="append", dest="upgrade_packages"
    )
    resolver_group.add_argument(
        "--resolution", choices=["highest", "lowest", "lowest-direct"]
    )
    resolver_group.add_argument(
        "--prerelease",
        choices=[
            "disallow",
            "allow",
            "if-necessary",
            "explicit",
            "if-necessary-or-explicit",
        ],
    )
    resolver_group.add_argument(
        "--fork-strategy", choices=["fewest", "requires-python"]
    )
    resolver_group.add_argument("--exclude-newer")
    resolver_group.add_argument("--no-sources", action="store_true")

    # Installer options
    installer_group = parser.add_argument_group("Installer options")
    installer_group.add_argument("--reinstall", action="store_true")
    installer_group.add_argument(
        "--reinstall-package", action="append", dest="reinstall_packages"
    )
    installer_group.add_argument(
        "--link-mode", choices=["clone", "copy", "hardlink", "symlink"]
    )
    installer_group.add_argument("--compile-bytecode", action="store_true")

    # Build options
    build_group = parser.add_argument_group("Build options")
    build_group.add_argument(
        "-C", "--config-setting", action="append", dest="config_settings"
    )
    build_group.add_argument("--no-build-isolation", action="store_true")
    build_group.add_argument(
        "--no-build-isolation-package",
        action="append",
        dest="no_build_isolation_packages",
    )
    build_group.add_argument("--no-build", action="store_true")
    build_group.add_argument(
        "--no-build-package", action="append", dest="no_build_packages"
    )
    build_group.add_argument("--no-binary", action="store_true")
    build_group.add_argument(
        "--no-binary-package", action="append", dest="no_binary_packages"
    )

    # Cache options
    cache_group = parser.add_argument_group("Cache options")
    cache_group.add_argument("-n", "--no-cache", action="store_true")
    cache_group.add_argument("--cache-dir")
    cache_group.add_argument("--refresh", action="store_true")
    cache_group.add_argument(
        "--refresh-package", action="append", dest="refresh_packages"
    )

    # Python options
    python_group = parser.add_argument_group("Python options")
    python_group.add_argument("-p", "--python")
    python_group.add_argument("--managed-python", action="store_true")
    python_group.add_argument("--no-managed-python", action="store_true")
    python_group.add_argument("--no-python-downloads", action="store_true")
    # note: the following is a legacy option and will be removed at some point
    # https://github.com/astral-sh/uv/pull/12246
    python_group.add_argument(
        "--python-preference",
        choices=["only-managed", "managed", "system", "only-system"],
    )

    # Global options
    global_group = parser.add_argument_group("Global options")
    global_group.add_argument("-q", "--quiet", action="count", default=0)
    global_group.add_argument("-v", "--verbose", action="count", default=0)
    global_group.add_argument("--color", choices=["auto", "always", "never"])
    global_group.add_argument("--native-tls", action="store_true")
    global_group.add_argument("--offline", action="store_true")
    global_group.add_argument(
        "--allow-insecure-host", action="append", dest="insecure_hosts"
    )
    global_group.add_argument("--no-progress", action="store_true")
    global_group.add_argument("--directory")
    global_group.add_argument("--project")
    global_group.add_argument("--config-file")
    global_group.add_argument("--no-config", action="store_true")

    return parser


def _check_working_dir_files(
    uv_run_args: argparse.Namespace, runtime_env: Dict[str, Any]
) -> None:
    """
    Check that the files required by uv are local to the working_dir. This catches
    the most common cases of how things are different in Ray, i.e. not the whole file
    system will be available on the workers, only the working_dir.

    The function won't return anything, it just raises a RuntimeError if there is an error.
    """
    working_dir = Path(runtime_env["working_dir"]).resolve()

    # Check if the requirements.txt file is in the working_dir
    if uv_run_args.with_requirements:
        for requirements_file in uv_run_args.with_requirements:
            if not Path(requirements_file).resolve().is_relative_to(working_dir):
                raise RuntimeError(
                    f"You specified --with-requirements={uv_run_args.with_requirements} but "
                    f"the requirements file is not in the working_dir {runtime_env['working_dir']}, "
                    "so the workers will not have access to the file. Make sure "
                    "the requirements file is in the working directory. "
                    "You can do so by specifying --directory in 'uv run', by changing the current "
                    "working directory before running 'uv run', or by using the 'working_dir' "
                    "parameter of the runtime_environment."
                )

    # Check if the pyproject.toml file is in the working_dir
    pyproject = None
    if uv_run_args.no_project:
        pyproject = None
    elif uv_run_args.project:
        pyproject = Path(uv_run_args.project)
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


def _get_uv_run_cmdline() -> Optional[List[str]]:
    """
    Return the command line of the first ancestor process that was run with
    "uv run" and None if there is no such ancestor.

    uv spawns the python process as a child process, so we first check the
    parent process command line. We also check our parent's parents since
    the Ray driver might be run as a subprocess of the 'uv run' process.
    """
    parents = psutil.Process().parents()
    for parent in parents:
        try:
            cmdline = parent.cmdline()
            if (
                len(cmdline) > 1
                and os.path.basename(cmdline[0]) == "uv"
                and cmdline[1] == "run"
            ):
                return cmdline
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            continue
    return None


def hook(runtime_env: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Hook that detects if the driver is run in 'uv run' and sets the runtime environment accordingly."""

    runtime_env = copy.deepcopy(runtime_env) or {}

    cmdline = _get_uv_run_cmdline()
    if not cmdline:
        # This means the driver was not run in a 'uv run' environment -- in this case
        # we leave the runtime environment unchanged
        return runtime_env

    # First check that the "uv" and "pip" runtime environments are not used.
    if "uv" in runtime_env or "pip" in runtime_env:
        raise RuntimeError(
            "You are using the 'pip' or 'uv' runtime environments together with "
            "'uv run'. These are not compatible since 'uv run' will run the workers "
            "in an isolated environment -- please add the 'pip' or 'uv' dependencies to your "
            "'uv run' environment e.g. by including them in your pyproject.toml."
        )

    # Extract the arguments uv_run_args of 'uv run' that are not part of the command.
    parser = _create_uv_run_parser()
    cmdline_args = parser.parse_args(cmdline[2:])
    if cmdline[-len(cmdline_args.command) :] != cmdline_args.command:
        raise AssertionError(
            f"uv run command {cmdline_args.command} is not a suffix of command line {cmdline}"
        )
    uv_run_args = cmdline[: -len(cmdline_args.command)]

    # Remove the "--directory" argument since it has already been taken into
    # account when setting the current working directory of the current process.
    # Also remove the "--module" argument, since the default_worker.py is
    # invoked as a script and not as a module.
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory")
    parser.add_argument("-m", "--module")
    _, remaining_uv_run_args = parser.parse_known_args(uv_run_args)

    runtime_env["py_executable"] = " ".join(remaining_uv_run_args)

    # If the user specified a working_dir, we always honor it, otherwise
    # use the same working_dir that uv run would use
    if "working_dir" not in runtime_env:
        runtime_env["working_dir"] = os.getcwd()
        _check_working_dir_files(cmdline_args, runtime_env)

    return runtime_env


# This __main__ is used for unit testing if the runtime_env_hook picks up the
# right settings.
if __name__ == "__main__":
    import json

    test_parser = argparse.ArgumentParser()
    test_parser.add_argument("runtime_env")
    args = test_parser.parse_args()

    # If the env variable is set, add one more level of subprocess indirection
    if os.environ.get("RAY_TEST_UV_ADD_SUBPROCESS_INDIRECTION") == "1":
        import subprocess

        env = os.environ.copy()
        env.pop("RAY_TEST_UV_ADD_SUBPROCESS_INDIRECTION")
        subprocess.check_call([sys.executable] + sys.argv, env=env)
        sys.exit(0)

    # If the following env variable is set, we use multiprocessing
    # spawn to start the subprocess, since it uses a different way to
    # modify the command line than subprocess.check_call
    if os.environ.get("RAY_TEST_UV_MULTIPROCESSING_SPAWN") == "1":
        import multiprocessing

        multiprocessing.set_start_method("spawn")
        pool = multiprocessing.Pool(processes=1)
        runtime_env = json.loads(args.runtime_env)
        print(json.dumps(pool.apply(hook, (runtime_env,))))
        sys.exit(0)

    # We purposefully modify sys.argv here to make sure the hook is robust
    # against such modification.
    sys.argv.pop(1)
    runtime_env = json.loads(args.runtime_env)
    print(json.dumps(hook(runtime_env)))
