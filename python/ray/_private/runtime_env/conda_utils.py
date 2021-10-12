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


def get_conda_activate_commands(conda_env_name: str) -> List[str]:
    """
    Get a list of commands to run to silently activate the given conda env.
    """
    #  Checking for newer conda versions
    if os.name != "nt" and ("CONDA_EXE" in os.environ
                            or RAY_CONDA_HOME in os.environ):
        conda_path = get_conda_bin_executable("conda")
        activate_conda_env = [
            ". {}/../etc/profile.d/conda.sh".format(
                os.path.dirname(conda_path))
        ]
        activate_conda_env += ["conda activate {} 1>&2".format(conda_env_name)]

    else:
        activate_path = get_conda_bin_executable("activate")
        # in case os name is not 'nt', we are not running on windows. Introduce
        # bash command otherwise.
        if os.name != "nt":
            return ["source %s %s 1>&2" % (activate_path, conda_env_name)]
        else:
            return ["conda activate %s" % (conda_env_name)]
    return activate_conda_env


def get_conda_bin_executable(executable_name: str) -> str:
    """
    Return path to the specified executable, assumed to be discoverable within
    the 'bin' subdirectory of a conda installation.

    The conda home directory (expected to contain a 'bin' subdirectory) is
    configurable via the ``RAY_CONDA_HOME`` environment variable. If
    ``RAY_CONDA_HOME`` is unspecified, this method simply returns the passed-in
    executable name.
    """
    conda_home = os.environ.get(RAY_CONDA_HOME)
    if conda_home:
        return os.path.join(conda_home, "bin/%s" % executable_name)
    # Use CONDA_EXE as per https://github.com/conda/conda/issues/7126
    if "CONDA_EXE" in os.environ:
        conda_bin_dir = os.path.dirname(os.environ["CONDA_EXE"])
        return os.path.join(conda_bin_dir, executable_name)
    return executable_name


def _get_conda_env_name(conda_env_path: str) -> str:
    conda_env_contents = open(conda_env_path).read() if conda_env_path else ""
    return "ray-%s" % hashlib.sha1(
        conda_env_contents.encode("utf-8")).hexdigest()


def get_or_create_conda_env(conda_env_path: str,
                            base_dir: Optional[str] = None,
                            logger: Optional[logging.Logger] = None) -> str:
    """
    Given a conda YAML, creates a conda environment containing the required
    dependencies if such a conda environment doesn't already exist. Returns the
    name of the conda environment, which is based on a hash of the YAML.

    Args:
        conda_env_path: Path to a conda environment YAML file.
        base_dir (str, optional): Directory to install the environment into via
            the --prefix option to conda create.  If not specified, will
            install into the default conda directory (e.g. ~/anaconda3/envs)
    Returns:
        The name of the env, or the path to the env if base_dir is specified.
            In either case, the return value should be valid to pass in to
            `conda activate`.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    conda_path = get_conda_bin_executable("conda")
    try:
        exec_cmd([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise ValueError(
            f"Could not find Conda executable at {conda_path}. "
            "Ensure Conda is installed as per the instructions at "
            "https://conda.io/projects/conda/en/latest/"
            "user-guide/install/index.html. "
            "You can also configure Ray to look for a specific "
            f"Conda executable by setting the {RAY_CONDA_HOME} "
            "environment variable to the path of the Conda executable.")
    _, stdout, _ = exec_cmd([conda_path, "env", "list", "--json"])
    envs = json.loads(stdout)["envs"]

    create_cmd = None
    env_name = _get_conda_env_name(conda_env_path)
    if base_dir:
        env_name = f"{base_dir}/{env_name}"
        if env_name not in envs:
            create_cmd = [
                conda_path, "env", "create", "--file", conda_env_path,
                "--prefix", env_name
            ]
    else:
        env_names = [os.path.basename(env) for env in envs]
        if env_name not in env_names:
            create_cmd = [
                conda_path, "env", "create", "-n", env_name, "--file",
                conda_env_path
            ]

    if create_cmd is not None:
        logger.info(f"Creating conda environment {env_name}")
        exit_code, output = exec_cmd_stream_to_logger(create_cmd, logger)
        if exit_code != 0:
            shutil.rmtree(env_name)
            raise RuntimeError(
                f"Failed to install conda environment:\n{output}")

    return env_name


class ShellCommandException(Exception):
    pass


def exec_cmd(cmd: List[str],
             throw_on_error: bool = True,
             logger: Optional[logging.Logger] = None
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
        universal_newlines=True)
    (stdout, stderr) = child.communicate()
    exit_code = child.wait()
    if throw_on_error and exit_code != 0:
        raise ShellCommandException(
            "Non-zero exit code: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
            (exit_code, stdout, stderr))
    return exit_code, stdout, stderr


def exec_cmd_stream_to_logger(cmd: List[str],
                              logger: logging.Logger,
                              n_lines: int = 10) -> Tuple[int, str]:
    """Runs a command as a child process, streaming output to the logger.

    The last n_lines lines of output are also returned (stdout and stderr).
    """
    child = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    exit_code = None
    last_n_lines = []
    with child.stdout:
        for line in iter(child.stdout.readline, b""):
            exit_code = child.poll()
            if exit_code is not None:
                break
            last_n_lines.append(line.strip())
            last_n_lines = last_n_lines[-n_lines:]
            logger.info(line.strip())

    exit_code = child.wait()
    return exit_code, "\n".join(last_n_lines)
