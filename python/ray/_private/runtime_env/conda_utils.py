import logging
import os
import shutil
import subprocess
import hashlib
import json
from typing import Optional, List, Union, Tuple

"""Utilities for conda.  Adapted from https://github.com/mlflow/mlflow."""

# Name of environment variable indicating a path to a conda installation. Ray
# will default to running "conda" if unset.
RAY_CONDA_HOME = "RAY_CONDA_HOME"

_WIN32 = os.name == "nt"


def get_conda_activate_commands(conda_env_name: str) -> List[str]:
    """
    Get a list of commands to run to silently activate the given conda env.
    """
    #  Checking for newer conda versions
    if not _WIN32 and ("CONDA_EXE" in os.environ or RAY_CONDA_HOME in os.environ):
        conda_path = get_conda_bin_executable("conda")
        activate_conda_env = [
            ".",
            f"{os.path.dirname(conda_path)}/../etc/profile.d/conda.sh",
            "&&",
        ]
        activate_conda_env += ["conda", "activate", conda_env_name]

    else:
        activate_path = get_conda_bin_executable("activate")
        if not _WIN32:
            # Use bash command syntax
            activate_conda_env = ["source", activate_path, conda_env_name]
        else:
            activate_conda_env = ["conda", "activate", conda_env_name]
    return activate_conda_env + ["1>&2", "&&"]


def get_conda_bin_executable(executable_name: str) -> str:
    """
    Return path to the specified executable, assumed to be discoverable within
    a conda installation.

    The conda home directory (expected to contain a 'bin' subdirectory on
    linux) is configurable via the ``RAY_CONDA_HOME`` environment variable. If
    ``RAY_CONDA_HOME`` is unspecified, try the ``CONDA_EXE`` environment
    variable set by activating conda. If neither is specified, this method
    returns `executable_name`.
    """
    conda_home = os.environ.get(RAY_CONDA_HOME)
    if conda_home:
        if _WIN32:
            candidate = os.path.join(conda_home, "%s.exe" % executable_name)
            if os.path.exists(candidate):
                return candidate
            candidate = os.path.join(conda_home, "%s.bat" % executable_name)
            if os.path.exists(candidate):
                return candidate
        else:
            return os.path.join(conda_home, "bin/%s" % executable_name)
    else:
        conda_home = "."
    # Use CONDA_EXE as per https://github.com/conda/conda/issues/7126
    if "CONDA_EXE" in os.environ:
        conda_bin_dir = os.path.dirname(os.environ["CONDA_EXE"])
        if _WIN32:
            candidate = os.path.join(conda_home, "%s.exe" % executable_name)
            if os.path.exists(candidate):
                return candidate
            candidate = os.path.join(conda_home, "%s.bat" % executable_name)
            if os.path.exists(candidate):
                return candidate
        else:
            return os.path.join(conda_bin_dir, executable_name)
    if _WIN32:
        return executable_name + ".bat"
    return executable_name


def _get_conda_env_name(conda_env_path: str) -> str:
    conda_env_contents = open(conda_env_path).read()
    return "ray-%s" % hashlib.sha1(conda_env_contents.encode("utf-8")).hexdigest()


def create_conda_env_if_needed(
    conda_yaml_file: str, prefix: str, logger: Optional[logging.Logger] = None
) -> None:
    """
    Given a conda YAML, creates a conda environment containing the required
    dependencies if such a conda environment doesn't already exist.
    Args:
        conda_yaml_file: The path to a conda `environment.yml` file.
        prefix: Directory to install the environment into via
            the `--prefix` option to conda create.  This also becomes the name
            of the conda env; i.e. it can be passed into `conda activate` and
            `conda remove`
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    conda_path = get_conda_bin_executable("conda")
    try:
        exec_cmd([conda_path, "--help"], throw_on_error=False)
    except (EnvironmentError, FileNotFoundError):
        raise ValueError(
            f"Could not find Conda executable at '{conda_path}'. "
            "Ensure Conda is installed as per the instructions at "
            "https://conda.io/projects/conda/en/latest/"
            "user-guide/install/index.html. "
            "You can also configure Ray to look for a specific "
            f"Conda executable by setting the {RAY_CONDA_HOME} "
            "environment variable to the path of the Conda executable."
        )

    _, stdout, _ = exec_cmd([conda_path, "env", "list", "--json"])
    envs = json.loads(stdout)["envs"]

    if prefix in envs:
        logger.info(f"Conda environment {prefix} already exists.")
        return

    create_cmd = [
        conda_path,
        "env",
        "create",
        "--file",
        conda_yaml_file,
        "--prefix",
        prefix,
    ]

    logger.info(f"Creating conda environment {prefix}")
    exit_code, output = exec_cmd_stream_to_logger(create_cmd, logger)
    if exit_code != 0:
        if os.path.exists(prefix):
            shutil.rmtree(prefix)
        raise RuntimeError(
            f"Failed to install conda environment {prefix}:\nOutput:\n{output}"
        )


def delete_conda_env(prefix: str, logger: Optional[logging.Logger] = None) -> bool:
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"Deleting conda environment {prefix}")

    conda_path = get_conda_bin_executable("conda")
    delete_cmd = [conda_path, "remove", "-p", prefix, "--all", "-y"]
    exit_code, output = exec_cmd_stream_to_logger(delete_cmd, logger)

    if exit_code != 0:
        logger.debug(f"Failed to delete conda environment {prefix}:\n{output}")
        return False

    return True


def get_conda_env_list() -> list:
    """
    Get conda env list in full paths.
    """
    conda_path = get_conda_bin_executable("conda")
    try:
        exec_cmd([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise ValueError(f"Could not find Conda executable at {conda_path}.")
    _, stdout, _ = exec_cmd([conda_path, "env", "list", "--json"])
    envs = json.loads(stdout)["envs"]
    return envs


def get_conda_info_json() -> dict:
    """
    Get `conda info --json` output.

    Returns dict of conda info. See [1] for more details. We mostly care about these
    keys:

    - `conda_prefix`: str The path to the conda installation.
    - `envs`: List[str] absolute paths to conda environments.

    [1] https://github.com/conda/conda/blob/main/conda/cli/main_info.py
    """
    conda_path = get_conda_bin_executable("conda")
    try:
        exec_cmd([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise ValueError(f"Could not find Conda executable at {conda_path}.")
    _, stdout, _ = exec_cmd([conda_path, "info", "--json"])
    return json.loads(stdout)


def get_conda_envs(conda_info: dict) -> List[Tuple[str, str]]:
    """
    Gets the conda environments, as a list of (name, path) tuples.
    """
    prefix = conda_info["conda_prefix"]
    ret = []
    for env in conda_info["envs"]:
        if env == prefix:
            ret.append(("base", env))
        else:
            ret.append((os.path.basename(env), env))
    return ret


class ShellCommandException(Exception):
    pass


def exec_cmd(
    cmd: List[str], throw_on_error: bool = True, logger: Optional[logging.Logger] = None
) -> Union[int, Tuple[int, str, str]]:
    """
    Runs a command as a child process.

    A convenience wrapper for running a command from a Python script.

    Note on the return value: A tuple of the exit code,
    standard output and standard error is returned.

    Args:
        cmd: the command to run, as a list of strings
        throw_on_error: if true, raises an Exception if the exit code of the
            program is nonzero
    """
    child = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    (stdout, stderr) = child.communicate()
    exit_code = child.wait()
    if throw_on_error and exit_code != 0:
        raise ShellCommandException(
            "Non-zero exit code: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s"
            % (exit_code, stdout, stderr)
        )
    return exit_code, stdout, stderr


def exec_cmd_stream_to_logger(
    cmd: List[str], logger: logging.Logger, n_lines: int = 50, **kwargs
) -> Tuple[int, str]:
    """Runs a command as a child process, streaming output to the logger.

    The last n_lines lines of output are also returned (stdout and stderr).
    """
    if "env" in kwargs and _WIN32 and "PATH" not in [x.upper() for x in kwargs.keys]:
        raise ValueError("On windows, Popen requires 'PATH' in 'env'")
    child = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **kwargs,
    )
    last_n_lines = []
    with child.stdout:
        for line in iter(child.stdout.readline, b""):
            exit_code = child.poll()
            if exit_code is not None:
                break
            line = line.strip()
            if not line:
                continue
            last_n_lines.append(line.strip())
            last_n_lines = last_n_lines[-n_lines:]
            logger.info(line.strip())

    exit_code = child.wait()
    return exit_code, "\n".join(last_n_lines)
