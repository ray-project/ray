# Script used for checking changes for incremental testing cases
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import subprocess
import sys
from functools import partial
from pprint import pformat


def list_changed_files(commit_range):
    """Returns a list of names of files changed in the given commit range.

    The function works by opening a subprocess and running git. If an error
    occurs while running git, the script will abort.

    Args:
        commit_range (string): The commit range to diff, consisting of the two
            commit IDs separated by \"..\"

    Returns:
        list: List of changed files within the commit range
    """

    command = ["git", "diff", "--name-only", commit_range]
    out = subprocess.check_output(command)
    return [s.strip() for s in out.decode().splitlines() if s is not None]


if __name__ == "__main__":

    RAY_CI_TUNE_AFFECTED = 0
    RAY_CI_RLLIB_AFFECTED = 0
    RAY_CI_RLLIB_FULL_AFFECTED = 0
    RAY_CI_SERVE_AFFECTED = 0
    RAY_CI_JAVA_AFFECTED = 0
    RAY_CI_PYTHON_AFFECTED = 0
    RAY_CI_LINUX_WHEELS_AFFECTED = 0
    RAY_CI_MACOS_WHEELS_AFFECTED = 0
    RAY_CI_STREAMING_CPP_AFFECTED = 0
    RAY_CI_STREAMING_PYTHON_AFFECTED = 0
    RAY_CI_STREAMING_JAVA_AFFECTED = 0

    if os.environ["TRAVIS_EVENT_TYPE"] == "pull_request":

        files = list_changed_files(os.environ["TRAVIS_COMMIT_RANGE"])

        print(pformat(files), file=sys.stderr)

        skip_prefix_list = [
            "doc/", "examples/", "dev/", "docker/", "kubernetes/", "site/"
        ]

        for changed_file in files:
            if changed_file.startswith("python/ray/tune"):
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_RLLIB_FULL_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif re.match("^(python/ray/)?rllib/", changed_file):
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_RLLIB_FULL_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/serve"):
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/"):
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
            elif changed_file.startswith("java/"):
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
            elif any(
                    changed_file.startswith(prefix)
                    for prefix in skip_prefix_list):
                # nothing is run but linting in these cases
                pass
            elif changed_file.startswith("src/"):
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_CPP_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
            elif changed_file.startswith("streaming/src"):
                RAY_CI_STREAMING_CPP_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
            elif changed_file.startswith("streaming/python"):
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
            elif changed_file.startswith("streaming/java"):
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
            else:
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_CPP_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
    else:
        RAY_CI_TUNE_AFFECTED = 1
        RAY_CI_RLLIB_AFFECTED = 1
        RAY_CI_RLLIB_FULL_AFFECTED = 1
        RAY_CI_SERVE_AFFECTED = 1
        RAY_CI_JAVA_AFFECTED = 1
        RAY_CI_PYTHON_AFFECTED = 1
        RAY_CI_LINUX_WHEELS_AFFECTED = 1
        RAY_CI_MACOS_WHEELS_AFFECTED = 1
        RAY_CI_STREAMING_CPP_AFFECTED = 1
        RAY_CI_STREAMING_PYTHON_AFFECTED = 1
        RAY_CI_STREAMING_JAVA_AFFECTED = 1

    # Log the modified environment variables visible in console.
    for output_stream in [sys.stdout, sys.stderr]:
        _print = partial(print, file=output_stream)
        _print("export RAY_CI_TUNE_AFFECTED={}".format(RAY_CI_TUNE_AFFECTED))
        _print("export RAY_CI_RLLIB_AFFECTED={}".format(RAY_CI_RLLIB_AFFECTED))
        _print("export RAY_CI_RLLIB_FULL_AFFECTED={}".format(
            RAY_CI_RLLIB_FULL_AFFECTED))
        _print("export RAY_CI_SERVE_AFFECTED={}".format(RAY_CI_SERVE_AFFECTED))
        _print("export RAY_CI_JAVA_AFFECTED={}".format(RAY_CI_JAVA_AFFECTED))
        _print(
            "export RAY_CI_PYTHON_AFFECTED={}".format(RAY_CI_PYTHON_AFFECTED))
        _print("export RAY_CI_LINUX_WHEELS_AFFECTED={}"
               .format(RAY_CI_LINUX_WHEELS_AFFECTED))
        _print("export RAY_CI_MACOS_WHEELS_AFFECTED={}"
               .format(RAY_CI_MACOS_WHEELS_AFFECTED))
        _print("export RAY_CI_STREAMING_CPP_AFFECTED={}"
               .format(RAY_CI_STREAMING_CPP_AFFECTED))
        _print("export RAY_CI_STREAMING_PYTHON_AFFECTED={}"
               .format(RAY_CI_STREAMING_PYTHON_AFFECTED))
        _print("export RAY_CI_STREAMING_JAVA_AFFECTED={}"
               .format(RAY_CI_STREAMING_JAVA_AFFECTED))
