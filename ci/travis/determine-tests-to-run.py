# Script used for checking changes for incremental testing cases
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import subprocess

def list_changed_files(commit_range):
    """
    Returns the list of files changed within commit_range.
    """
    command = ["git", "diff", "--name-only", commit_range]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Command {cmdline} failed with code {returncode}, "
                           "stderr was:\n{stderr}\n"
                           .format(cmdline=command, returncode=proc.returncode,
                                   stderr=err.decode()))

    return list(filter(None, (s.strip() for s in out.decode().splitlines())))

if __name__ == "__main__":

    files = list_changed_files(os.environ['TRAVIS_COMMIT_RANGE'].replace('...', '..'))

    RAY_CI_TUNE_AFFECTED = "0"
    RAY_CI_RLLIB_AFFECTED = "0"
    RAY_CI_JAVA_AFFECTED = "0"
    RAY_CI_PYTHON_AFFECTED = "0"

    RAY_CI_LINUX_WHEELS_AFFECTED = "0"
    RAY_CI_MACOS_WHEELS_AFFECTED = "0"

    for changed_file in files:
        if changed_file.startswith("python/ray/tune/"):
            RAY_CI_TUNE_AFFECTED = "1"
            RAY_CI_RLLIB_AFFECTED = "1"
            RAY_CI_LINUX_WHEELS_AFFECTED = "1"
            RAY_CI_MACOS_WHEELS_AFFECTED = "1"
        elif changed_file.startswith("python/ray/rllib/"):
            RAY_CI_RLLIB_AFFECTED = "1"
            RAY_CI_LINUX_WHEELS_AFFECTED = "1"
            RAY_CI_MACOS_WHEELS_AFFECTED = "1"
        elif changed_file.startswith("python/"):
            RAY_CI_TUNE_AFFECTED = "1"
            RAY_CI_RLLIB_AFFECTED = "1"
            RAY_CI_PYTHON_AFFECTED = "1"
            RAY_CI_LINUX_WHEELS_AFFECTED = "1"
            RAY_CI_MACOS_WHEELS_AFFECTED = "1"
        elif changed_file.startswith("src/"):
            pass
        elif changed_file.startswith("java/") and not file.startswith("java/doc/"):
            RAY_CI_JAVA_AFFECTED = "1"
        elif changed_file.startswith("doc/"):
            # nothing is run but linting in this case
            pass
        # elif changed_file.startswith("examples/"):
        #     pass
        # elif changed_file.startswith("dev/"):
        #     pass
        # elif changed_file.startswith("docker/"):
        #     pass
        # elif changed_file.startswith("kubernetes/"):
        #     pass
        # elif changed_file.startswith("site/"):
        #     pass
        else:
            RAY_CI_TUNE_AFFECTED = "1"
            RAY_CI_RLLIB_AFFECTED = "1"
            RAY_CI_DOC_AFFECTED = "1"
            RAY_CI_JAVA_AFFECTED = "1"
            RAY_CI_PYTHON_AFFECTED = "1"
            RAY_CI_LINUX_WHEELS_AFFECTED = "1"
            RAY_CI_MACOS_WHEELS_AFFECTED = "1"

    print("export RAY_CI_TUNE_AFFECTED=\"%s\"" % RAY_CI_TUNE_AFFECTED)
    print("export RAY_CI_RLLIB_AFFECTED=\"%s\"" % RAY_CI_RLLIB_AFFECTED)
    print("export RAY_CI_DOC_AFFECTED=\"%s\"" % RAY_CI_DOC_AFFECTED)
    print("export RAY_CI_JAVA_AFFECTED=\"%s\"" % RAY_CI_JAVA_AFFECTED)
    print("export RAY_CI_PYTHON_AFFECTED=\"%s\"" % RAY_CI_PYTHON_AFFECTED)
    print("export RAY_CI_LINUX_WHEELS_AFFECTED=\"%s\"" % RAY_CI_LINUX_WHEELS_AFFECTED)
    print("export RAY_CI_MACOS_WHEELS_AFFECTED=\"%s\"" % RAY_CI_MACOS_WHEELS_AFFECTED)
