# Script used for checking changes for incremental testing cases
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import re
import subprocess
import sys
from pprint import pformat
import argparse


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

    command = ["git", "diff", "--name-only", commit_range, "--"]
    out = subprocess.check_output(command)
    return [s.strip() for s in out.decode().splitlines() if s is not None]


def is_pull_request():
    event_type = None

    for key in ["GITHUB_EVENT_NAME", "TRAVIS_EVENT_TYPE"]:
        event_type = os.getenv(key, event_type)

    if (os.environ.get("BUILDKITE")
            and os.environ.get("BUILDKITE_PULL_REQUEST") != "false"):
        event_type = "pull_request"

    return event_type == "pull_request"


def get_commit_range():
    commit_range = None

    if os.environ.get("TRAVIS"):
        commit_range = os.environ["TRAVIS_COMMIT_RANGE"]
    elif os.environ.get("GITHUB_EVENT_PATH"):
        with open(os.environ["GITHUB_EVENT_PATH"], "rb") as f:
            event = json.loads(f.read())
        base = event["pull_request"]["base"]["sha"]
        commit_range = "{}...{}".format(base, event.get("after", ""))
    elif os.environ.get("BUILDKITE"):
        commit_range = "{}...{}".format(
            os.environ["BUILDKITE_PULL_REQUEST_BASE_BRANCH"],
            os.environ["BUILDKITE_COMMIT"],
        )

    assert commit_range is not None
    return commit_range


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=str, help="json or envvars", default="envvars")
    args = parser.parse_args()

    RAY_CI_TUNE_AFFECTED = 0
    RAY_CI_SGD_AFFECTED = 0
    RAY_CI_ONLY_RLLIB_AFFECTED = 0  # Whether only RLlib is affected.
    RAY_CI_RLLIB_AFFECTED = 0  # Whether RLlib minimal tests should be run.
    RAY_CI_RLLIB_FULL_AFFECTED = 0  # Whether full RLlib tests should be run.
    RAY_CI_SERVE_AFFECTED = 0
    RAY_CI_JAVA_AFFECTED = 0
    RAY_CI_PYTHON_AFFECTED = 0
    RAY_CI_LINUX_WHEELS_AFFECTED = 0
    RAY_CI_MACOS_WHEELS_AFFECTED = 0
    RAY_CI_STREAMING_CPP_AFFECTED = 0
    RAY_CI_STREAMING_PYTHON_AFFECTED = 0
    RAY_CI_STREAMING_JAVA_AFFECTED = 0
    RAY_CI_DASHBOARD_AFFECTED = 0
    RAY_CI_DOCKER_AFFECTED = 0
    RAY_CI_DOC_AFFECTED = 0
    RAY_CI_PYTHON_DEPENDENCIES_AFFECTED = 0

    if is_pull_request():
        commit_range = get_commit_range()
        files = list_changed_files(commit_range)
        print(pformat(commit_range), file=sys.stderr)
        print(pformat(files), file=sys.stderr)

        skip_prefix_list = [
            "doc/", "examples/", "dev/", "kubernetes/", "site/"
        ]

        for changed_file in files:
            if changed_file.startswith("python/ray/tune"):
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_RLLIB_FULL_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/util/sgd"):
                RAY_CI_SGD_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif re.match("^(python/ray/)?rllib/", changed_file):
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_RLLIB_FULL_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/serve"):
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/dashboard"):
                RAY_CI_DASHBOARD_AFFECTED = 1
            elif changed_file.startswith("dashboard"):
                RAY_CI_DASHBOARD_AFFECTED = 1
            elif changed_file.startswith("python/"):
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_SGD_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                if changed_file.startswith("python/setup.py") or re.match(
                        ".*requirements.*\.txt", changed_file):
                    RAY_CI_PYTHON_DEPENDENCIES_AFFECTED = 1
            elif changed_file.startswith("java/"):
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
            elif changed_file.startswith("docker/"):
                RAY_CI_DOCKER_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
            elif changed_file.startswith("doc/") and changed_file.endswith(
                    ".py"):
                RAY_CI_DOC_AFFECTED = 1
            elif any(
                    changed_file.startswith(prefix)
                    for prefix in skip_prefix_list):
                # nothing is run but linting in these cases
                pass
            elif changed_file.endswith("build-docker-images.py"):
                RAY_CI_DOCKER_AFFECTED = 1
            elif changed_file.startswith("src/"):
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_SGD_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_CPP_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
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
                RAY_CI_SGD_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_STREAMING_CPP_AFFECTED = 1
                RAY_CI_STREAMING_PYTHON_AFFECTED = 1
                RAY_CI_STREAMING_JAVA_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
    else:
        RAY_CI_TUNE_AFFECTED = 1
        RAY_CI_SGD_AFFECTED = 1
        RAY_CI_RLLIB_AFFECTED = 1
        RAY_CI_RLLIB_FULL_AFFECTED = 1
        RAY_CI_SERVE_AFFECTED = 1
        RAY_CI_JAVA_AFFECTED = 1
        RAY_CI_PYTHON_AFFECTED = 1
        RAY_CI_DOC_AFFECTED = 1
        RAY_CI_LINUX_WHEELS_AFFECTED = 1
        RAY_CI_MACOS_WHEELS_AFFECTED = 1
        RAY_CI_STREAMING_CPP_AFFECTED = 1
        RAY_CI_STREAMING_PYTHON_AFFECTED = 1
        RAY_CI_STREAMING_JAVA_AFFECTED = 1
        RAY_CI_DASHBOARD_AFFECTED = 1

    if not RAY_CI_TUNE_AFFECTED and not RAY_CI_SERVE_AFFECTED and \
            not RAY_CI_JAVA_AFFECTED and not RAY_CI_PYTHON_AFFECTED and not \
            RAY_CI_STREAMING_CPP_AFFECTED and \
            not RAY_CI_STREAMING_PYTHON_AFFECTED and \
            not RAY_CI_STREAMING_JAVA_AFFECTED and \
            not RAY_CI_SGD_AFFECTED:
        RAY_CI_ONLY_RLLIB_AFFECTED = 1

    # Log the modified environment variables visible in console.
    output_string = " ".join([
        "RAY_CI_TUNE_AFFECTED={}".format(RAY_CI_TUNE_AFFECTED),
        "RAY_CI_SGD_AFFECTED={}".format(RAY_CI_SGD_AFFECTED),
        "RAY_CI_ONLY_RLLIB_AFFECTED={}".format(RAY_CI_ONLY_RLLIB_AFFECTED),
        "RAY_CI_RLLIB_AFFECTED={}".format(RAY_CI_RLLIB_AFFECTED),
        "RAY_CI_RLLIB_FULL_AFFECTED={}".format(RAY_CI_RLLIB_FULL_AFFECTED),
        "RAY_CI_SERVE_AFFECTED={}".format(RAY_CI_SERVE_AFFECTED),
        "RAY_CI_DASHBOARD_AFFECTED={}".format(RAY_CI_DASHBOARD_AFFECTED),
        "RAY_CI_DOC_AFFECTED={}".format(RAY_CI_DOC_AFFECTED),
        "RAY_CI_JAVA_AFFECTED={}".format(RAY_CI_JAVA_AFFECTED),
        "RAY_CI_PYTHON_AFFECTED={}".format(RAY_CI_PYTHON_AFFECTED),
        "RAY_CI_LINUX_WHEELS_AFFECTED={}".format(RAY_CI_LINUX_WHEELS_AFFECTED),
        "RAY_CI_MACOS_WHEELS_AFFECTED={}".format(RAY_CI_MACOS_WHEELS_AFFECTED),
        "RAY_CI_STREAMING_CPP_AFFECTED={}".format(
            RAY_CI_STREAMING_CPP_AFFECTED),
        "RAY_CI_STREAMING_PYTHON_AFFECTED={}".format(
            RAY_CI_STREAMING_PYTHON_AFFECTED),
        "RAY_CI_STREAMING_JAVA_AFFECTED={}".format(
            RAY_CI_STREAMING_JAVA_AFFECTED),
        "RAY_CI_DOCKER_AFFECTED={}".format(RAY_CI_DOCKER_AFFECTED),
        "RAY_CI_PYTHON_DEPENDENCIES_AFFECTED={}".format(
            RAY_CI_PYTHON_DEPENDENCIES_AFFECTED),
    ])

    # Debug purpose
    print(output_string, file=sys.stderr)

    # Used by buildkite log format
    if args.output.lower() == "json":
        pairs = [item.split("=") for item in output_string.split(" ")]
        affected_vars = [key for key, affected in pairs if affected == "1"]
        print(json.dumps(affected_vars))
    else:
        print(output_string)
