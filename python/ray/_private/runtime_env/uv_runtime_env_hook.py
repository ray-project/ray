import argparse
import copy
import optparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psutil


def _create_uv_run_parser():
    """Create and return the argument parser for 'uv run' command."""

    parser = optparse.OptionParser(prog="uv run", add_help_option=False)

    # Disable interspersed args to stop parsing when we hit the first
    # argument that is not recognized by the parser.
    parser.disable_interspersed_args()

    # Main options group
    main_group = optparse.OptionGroup(parser, "Main options")
    main_group.add_option("--extra", action="append", dest="extras")
    main_group.add_option("--all-extras", action="store_true")
    main_group.add_option("--no-extra", action="append", dest="no_extras")
    main_group.add_option("--no-dev", action="store_true")
    main_group.add_option("--group", action="append", dest="groups")
    main_group.add_option("--no-group", action="append", dest="no_groups")
    main_group.add_option("--no-default-groups", action="store_true")
    main_group.add_option("--only-group", action="append", dest="only_groups")
    main_group.add_option("--all-groups", action="store_true")
    main_group.add_option("-m", "--module")
    main_group.add_option("--only-dev", action="store_true")
    main_group.add_option("--no-editable", action="store_true")
    main_group.add_option("--exact", action="store_true")
    main_group.add_option("--env-file", action="append", dest="env_files")
    main_group.add_option("--no-env-file", action="store_true")
    parser.add_option_group(main_group)

    # With options
    with_group = optparse.OptionGroup(parser, "With options")
    with_group.add_option("--with", action="append", dest="with_packages")
    with_group.add_option("--with-editable", action="append", dest="with_editable")
    with_group.add_option(
        "--with-requirements", action="append", dest="with_requirements"
    )
    parser.add_option_group(with_group)

    # Environment options
    env_group = optparse.OptionGroup(parser, "Environment options")
    env_group.add_option("--isolated", action="store_true")
    env_group.add_option("--active", action="store_true")
    env_group.add_option("--no-sync", action="store_true")
    env_group.add_option("--locked", action="store_true")
    env_group.add_option("--frozen", action="store_true")
    parser.add_option_group(env_group)

    # Script options
    script_group = optparse.OptionGroup(parser, "Script options")
    script_group.add_option("-s", "--script", action="store_true")
    script_group.add_option("--gui-script", action="store_true")
    parser.add_option_group(script_group)

    # Workspace options
    workspace_group = optparse.OptionGroup(parser, "Workspace options")
    workspace_group.add_option("--all-packages", action="store_true")
    workspace_group.add_option("--package")
    workspace_group.add_option("--no-project", action="store_true")
    parser.add_option_group(workspace_group)

    # Index options
    index_group = optparse.OptionGroup(parser, "Index options")
    index_group.add_option("--index", action="append", dest="indexes")
    index_group.add_option("--default-index")
    index_group.add_option("-i", "--index-url")
    index_group.add_option(
        "--extra-index-url", action="append", dest="extra_index_urls"
    )
    index_group.add_option("-f", "--find-links", action="append", dest="find_links")
    index_group.add_option("--no-index", action="store_true")
    index_group.add_option(
        "--index-strategy",
        type="choice",
        choices=["first-index", "unsafe-first-match", "unsafe-best-match"],
    )
    index_group.add_option(
        "--keyring-provider", type="choice", choices=["disabled", "subprocess"]
    )
    parser.add_option_group(index_group)

    # Resolver options
    resolver_group = optparse.OptionGroup(parser, "Resolver options")
    resolver_group.add_option("-U", "--upgrade", action="store_true")
    resolver_group.add_option(
        "-P", "--upgrade-package", action="append", dest="upgrade_packages"
    )
    resolver_group.add_option(
        "--resolution", type="choice", choices=["highest", "lowest", "lowest-direct"]
    )
    resolver_group.add_option(
        "--prerelease",
        type="choice",
        choices=[
            "disallow",
            "allow",
            "if-necessary",
            "explicit",
            "if-necessary-or-explicit",
        ],
    )
    resolver_group.add_option(
        "--fork-strategy", type="choice", choices=["fewest", "requires-python"]
    )
    resolver_group.add_option("--exclude-newer")
    resolver_group.add_option("--no-sources", action="store_true")
    parser.add_option_group(resolver_group)

    # Installer options
    installer_group = optparse.OptionGroup(parser, "Installer options")
    installer_group.add_option("--reinstall", action="store_true")
    installer_group.add_option(
        "--reinstall-package", action="append", dest="reinstall_packages"
    )
    installer_group.add_option(
        "--link-mode", type="choice", choices=["clone", "copy", "hardlink", "symlink"]
    )
    installer_group.add_option("--compile-bytecode", action="store_true")
    parser.add_option_group(installer_group)

    # Build options
    build_group = optparse.OptionGroup(parser, "Build options")
    build_group.add_option(
        "-C", "--config-setting", action="append", dest="config_settings"
    )
    build_group.add_option("--no-build-isolation", action="store_true")
    build_group.add_option(
        "--no-build-isolation-package",
        action="append",
        dest="no_build_isolation_packages",
    )
    build_group.add_option("--no-build", action="store_true")
    build_group.add_option(
        "--no-build-package", action="append", dest="no_build_packages"
    )
    build_group.add_option("--no-binary", action="store_true")
    build_group.add_option(
        "--no-binary-package", action="append", dest="no_binary_packages"
    )
    parser.add_option_group(build_group)

    # Cache options
    cache_group = optparse.OptionGroup(parser, "Cache options")
    cache_group.add_option("-n", "--no-cache", action="store_true")
    cache_group.add_option("--cache-dir")
    cache_group.add_option("--refresh", action="store_true")
    cache_group.add_option(
        "--refresh-package", action="append", dest="refresh_packages"
    )
    parser.add_option_group(cache_group)

    # Python options
    python_group = optparse.OptionGroup(parser, "Python options")
    python_group.add_option("-p", "--python")
    python_group.add_option("--managed-python", action="store_true")
    python_group.add_option("--no-managed-python", action="store_true")
    python_group.add_option("--no-python-downloads", action="store_true")
    # note: the following is a legacy option and will be removed at some point
    # https://github.com/astral-sh/uv/pull/12246
    python_group.add_option(
        "--python-preference",
        type="choice",
        choices=["only-managed", "managed", "system", "only-system"],
    )
    parser.add_option_group(python_group)

    # Global options
    global_group = optparse.OptionGroup(parser, "Global options")
    global_group.add_option("-q", "--quiet", action="count", default=0)
    global_group.add_option("-v", "--verbose", action="count", default=0)
    global_group.add_option(
        "--color", type="choice", choices=["auto", "always", "never"]
    )
    global_group.add_option("--native-tls", action="store_true")
    global_group.add_option("--offline", action="store_true")
    global_group.add_option(
        "--allow-insecure-host", action="append", dest="insecure_hosts"
    )
    global_group.add_option("--no-progress", action="store_true")
    global_group.add_option("--directory")
    global_group.add_option("--project")
    global_group.add_option("--config-file")
    global_group.add_option("--no-config", action="store_true")
    parser.add_option_group(global_group)

    return parser


def _parse_args(
    parser: optparse.OptionParser, args: List[str]
) -> Tuple[optparse.Values, List[str]]:
    """
    Parse the command-line options found in 'args'.

    Replacement for parser.parse_args that handles unknown arguments
    by keeping them in the command list instead of erroring and
    discarding them.
    """
    parser.rargs = args
    parser.largs = []
    options = parser.get_default_values()
    try:
        parser._process_args(parser.largs, parser.rargs, options)
    except optparse.BadOptionError as err:
        # If we hit an argument that is not recognized, we put it
        # back into the unconsumed arguments
        parser.rargs = [err.opt_str] + parser.rargs
    return options, parser.rargs


def _check_working_dir_files(
    uv_run_args: optparse.Values, runtime_env: Dict[str, Any]
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
    (options, command) = _parse_args(parser, cmdline[2:])

    if cmdline[-len(command) :] != command:
        raise AssertionError(
            f"uv run command {command} is not a suffix of command line {cmdline}"
        )
    uv_run_args = cmdline[: -len(command)]

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
        _check_working_dir_files(options, runtime_env)

    return runtime_env


# This __main__ is used for unit testing if the runtime_env_hook picks up the
# right settings.
if __name__ == "__main__":
    import json

    test_parser = argparse.ArgumentParser()
    test_parser.add_argument("--extra-args", action="store_true")
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
