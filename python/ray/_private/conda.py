import logging
import os
"""Utilities for conda.  Adapted from https://github.com/mlflow/mlflow."""

logger = logging.getLogger(__name__)

# Environment variable indicating a path to a conda installation. Ray will
# default to running "conda" if unset
RAY_CONDA_HOME = "RAY_CONDA_HOME"


def get_conda_activate_commands(conda_env_name):
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


def get_conda_bin_executable(executable_name):
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
