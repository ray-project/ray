# Script used for checking changes for incremental testing cases
from __future__ import absolute_import, division, print_function

import argparse
import json
import os
import re
import subprocess
import sys
from pprint import pformat
import traceback


# NOTE(simon): do not add type hint here because it's ran using python2 in CI.
def list_changed_files(commit_range):
    """Returns a list of names of files changed in the given commit range.

    The function works by opening a subprocess and running git. If an error
    occurs while running git, the script will abort.

    Args:
        commit_range: The commit range to diff, consisting of the two
            commit IDs separated by \"..\"

    Returns:
        list: List of changed files within the commit range
    """
    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH")
    if base_branch:
        pull_command = ["git", "fetch", "origin", base_branch]
        subprocess.check_call(pull_command)

    command = ["git", "diff", "--name-only", commit_range, "--"]
    out = subprocess.check_output(command)
    return [s.strip() for s in out.decode().splitlines() if s is not None]


def is_pull_request():
    event_type = None

    for key in ["GITHUB_EVENT_NAME", "TRAVIS_EVENT_TYPE"]:
        event_type = os.getenv(key, event_type)

    if (
        os.environ.get("BUILDKITE")
        and os.environ.get("BUILDKITE_PULL_REQUEST") != "false"
    ):
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
        commit_range = "origin/{}...{}".format(
            os.environ["BUILDKITE_PULL_REQUEST_BASE_BRANCH"],
            os.environ["BUILDKITE_COMMIT"],
        )

    assert commit_range is not None
    return commit_range


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, help="json or envvars", default="envvars")
    args = parser.parse_args()

    RAY_CI_BRANCH_BUILD = int(
        os.environ.get("BUILDKITE_PULL_REQUEST", "false") == "false"
    )
    RAY_CI_ML_AFFECTED = 0
    RAY_CI_TUNE_AFFECTED = 0
    RAY_CI_TRAIN_AFFECTED = 0
    # Whether only the most important (high-level) RLlib tests should be run.
    # Set to 1 for any changes to Ray Tune or python source files that are
    # NOT related to Serve, Dashboard or Train.
    RAY_CI_RLLIB_AFFECTED = 0
    # Whether all RLlib tests should be run.
    # Set to 1 only when a source file in `ray/rllib` has been changed.
    RAY_CI_RLLIB_DIRECTLY_AFFECTED = 0
    RAY_CI_SERVE_AFFECTED = 0
    RAY_CI_CORE_CPP_AFFECTED = 0
    RAY_CI_CPP_AFFECTED = 0
    RAY_CI_JAVA_AFFECTED = 0
    RAY_CI_PYTHON_AFFECTED = 0
    RAY_CI_LINUX_WHEELS_AFFECTED = 0
    RAY_CI_MACOS_WHEELS_AFFECTED = 0
    RAY_CI_DASHBOARD_AFFECTED = 0
    RAY_CI_DOCKER_AFFECTED = 0
    RAY_CI_DOC_AFFECTED = 0
    RAY_CI_PYTHON_DEPENDENCIES_AFFECTED = 0
    RAY_CI_TOOLS_AFFECTED = 0
    RAY_CI_DATA_AFFECTED = 0
    RAY_CI_WORKFLOW_AFFECTED = 0
    RAY_CI_RELEASE_TESTS_AFFECTED = 0
    RAY_CI_COMPILED_PYTHON_AFFECTED = 0

    if is_pull_request():
        commit_range = get_commit_range()
        files = list_changed_files(commit_range)
        print(pformat(commit_range), file=sys.stderr)
        print(pformat(files), file=sys.stderr)

        # Dry run py_dep_analysis.py to see which tests we would have run.
        try:
            import py_dep_analysis as pda

            graph = pda.build_dep_graph()
            rllib_tests = pda.list_rllib_tests()
            print("Total # of RLlib tests: ", len(rllib_tests), file=sys.stderr)

            impacted = {}
            for test in rllib_tests:
                for file in files:
                    if pda.test_depends_on_file(graph, test, file):
                        impacted[test[0]] = True

            print("RLlib tests impacted: ", len(impacted), file=sys.stderr)
            for test in impacted.keys():
                print("    ", test, file=sys.stderr)
        except Exception:
            print("Failed to dry run py_dep_analysis.py", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        # End of dry run.

        skip_prefix_list = [
            ".buildkite/",
            "doc/",
            "examples/",
            "dev/",
            "kubernetes/",
            "site/",
        ]

        for changed_file in files:
            if changed_file.startswith("python/ray/air"):
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/data"):
                RAY_CI_DATA_AFFECTED = 1
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/workflow"):
                RAY_CI_WORKFLOW_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/tune"):
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/train"):
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif re.match("^(python/ray/)?rllib/", changed_file):
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_RLLIB_DIRECTLY_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/serve"):
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/ray/dashboard"):
                RAY_CI_DASHBOARD_AFFECTED = 1
                # https://github.com/ray-project/ray/pull/15981
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("dashboard"):
                RAY_CI_DASHBOARD_AFFECTED = 1
                # https://github.com/ray-project/ray/pull/15981
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
            elif changed_file.startswith("python/"):
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_WORKFLOW_AFFECTED = 1
                RAY_CI_DATA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                # Python changes might impact cross language stack in Java.
                # Java also depends on Python CLI to manage processes.
                RAY_CI_JAVA_AFFECTED = 1
                if changed_file.startswith("python/setup.py") or re.match(
                    ".*requirements.*\.txt", changed_file
                ):
                    RAY_CI_PYTHON_DEPENDENCIES_AFFECTED = 1
                for compiled_extension in (".pxd", ".pyi", ".pyx", ".so"):
                    if changed_file.endswith(compiled_extension):
                        RAY_CI_COMPILED_PYTHON_AFFECTED = 1
                        break
            elif changed_file.startswith("java/"):
                RAY_CI_JAVA_AFFECTED = 1
            elif changed_file.startswith("cpp/"):
                RAY_CI_CPP_AFFECTED = 1
            elif changed_file.startswith("docker/"):
                RAY_CI_DOCKER_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
            elif changed_file.startswith("doc/"):
                if (
                    changed_file.endswith(".py")
                    or changed_file.endswith(".ipynb")
                    or changed_file.endswith("BUILD")
                ):
                    RAY_CI_DOC_AFFECTED = 1
                # Else, this affects only a rst file or so. In that case,
                # we pass, as the flag RAY_CI_DOC_AFFECTED is only
                # used to indicate that tests/examples should be run
                # (documentation will be built always)
            elif changed_file.startswith("release/"):
                if changed_file.startswith("release/ray_release"):
                    # Release test unit tests are ALWAYS RUN, so pass
                    pass
                elif not changed_file.endswith(".yaml") and not changed_file.endswith(
                    ".md"
                ):
                    # Do not run on config changes
                    RAY_CI_RELEASE_TESTS_AFFECTED = 1
            elif any(changed_file.startswith(prefix) for prefix in skip_prefix_list):
                # nothing is run but linting in these cases
                pass
            elif changed_file.startswith("ci/lint"):
                # Linter will always be run
                RAY_CI_TOOLS_AFFECTED = 1
            elif changed_file.startswith("ci/pipeline"):
                # These scripts are always run as part of the build process
                RAY_CI_TOOLS_AFFECTED = 1
            elif changed_file.endswith("build-docker-images.py"):
                RAY_CI_DOCKER_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_TOOLS_AFFECTED = 1
            elif changed_file.startswith("ci/run"):
                RAY_CI_TOOLS_AFFECTED = 1
            elif changed_file.startswith("src/"):
                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_CORE_CPP_AFFECTED = 1
                RAY_CI_CPP_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_RELEASE_TESTS_AFFECTED = 1
            else:
                print(
                    "Unhandled source code change: {changed_file}".format(
                        changed_file=changed_file
                    ),
                    file=sys.stderr,
                )

                RAY_CI_ML_AFFECTED = 1
                RAY_CI_TUNE_AFFECTED = 1
                RAY_CI_TRAIN_AFFECTED = 1
                RAY_CI_RLLIB_AFFECTED = 1
                RAY_CI_SERVE_AFFECTED = 1
                RAY_CI_CORE_CPP_AFFECTED = 1
                RAY_CI_CPP_AFFECTED = 1
                RAY_CI_JAVA_AFFECTED = 1
                RAY_CI_PYTHON_AFFECTED = 1
                RAY_CI_DOC_AFFECTED = 1
                RAY_CI_LINUX_WHEELS_AFFECTED = 1
                RAY_CI_MACOS_WHEELS_AFFECTED = 1
                RAY_CI_DASHBOARD_AFFECTED = 1
                RAY_CI_TOOLS_AFFECTED = 1
                RAY_CI_RELEASE_TESTS_AFFECTED = 1
                RAY_CI_COMPILED_PYTHON_AFFECTED = 1

    else:
        RAY_CI_ML_AFFECTED = 1
        RAY_CI_TUNE_AFFECTED = 1
        RAY_CI_TRAIN_AFFECTED = 1
        RAY_CI_RLLIB_AFFECTED = 1
        RAY_CI_RLLIB_DIRECTLY_AFFECTED = 1
        RAY_CI_SERVE_AFFECTED = 1
        RAY_CI_CPP_AFFECTED = 1
        RAY_CI_CORE_CPP_AFFECTED = 1
        RAY_CI_JAVA_AFFECTED = 1
        RAY_CI_PYTHON_AFFECTED = 1
        RAY_CI_DOC_AFFECTED = 1
        RAY_CI_LINUX_WHEELS_AFFECTED = 1
        RAY_CI_MACOS_WHEELS_AFFECTED = 1
        RAY_CI_DASHBOARD_AFFECTED = 1
        RAY_CI_TOOLS_AFFECTED = 1
        RAY_CI_WORKFLOW_AFFECTED = 1
        RAY_CI_DATA_AFFECTED = 1
        RAY_CI_RELEASE_TESTS_AFFECTED = 1
        RAY_CI_COMPILED_PYTHON_AFFECTED = 1

    # Log the modified environment variables visible in console.
    output_string = " ".join(
        [
            "RAY_CI_BRANCH_BUILD={}".format(RAY_CI_BRANCH_BUILD),
            "RAY_CI_ML_AFFECTED={}".format(RAY_CI_ML_AFFECTED),
            "RAY_CI_TUNE_AFFECTED={}".format(RAY_CI_TUNE_AFFECTED),
            "RAY_CI_TRAIN_AFFECTED={}".format(RAY_CI_TRAIN_AFFECTED),
            "RAY_CI_RLLIB_AFFECTED={}".format(RAY_CI_RLLIB_AFFECTED),
            "RAY_CI_RLLIB_DIRECTLY_AFFECTED={}".format(RAY_CI_RLLIB_DIRECTLY_AFFECTED),
            "RAY_CI_SERVE_AFFECTED={}".format(RAY_CI_SERVE_AFFECTED),
            "RAY_CI_DASHBOARD_AFFECTED={}".format(RAY_CI_DASHBOARD_AFFECTED),
            "RAY_CI_DOC_AFFECTED={}".format(RAY_CI_DOC_AFFECTED),
            "RAY_CI_CORE_CPP_AFFECTED={}".format(RAY_CI_CORE_CPP_AFFECTED),
            "RAY_CI_CPP_AFFECTED={}".format(RAY_CI_CPP_AFFECTED),
            "RAY_CI_JAVA_AFFECTED={}".format(RAY_CI_JAVA_AFFECTED),
            "RAY_CI_PYTHON_AFFECTED={}".format(RAY_CI_PYTHON_AFFECTED),
            "RAY_CI_LINUX_WHEELS_AFFECTED={}".format(RAY_CI_LINUX_WHEELS_AFFECTED),
            "RAY_CI_MACOS_WHEELS_AFFECTED={}".format(RAY_CI_MACOS_WHEELS_AFFECTED),
            "RAY_CI_DOCKER_AFFECTED={}".format(RAY_CI_DOCKER_AFFECTED),
            "RAY_CI_PYTHON_DEPENDENCIES_AFFECTED={}".format(
                RAY_CI_PYTHON_DEPENDENCIES_AFFECTED
            ),
            "RAY_CI_TOOLS_AFFECTED={}".format(RAY_CI_TOOLS_AFFECTED),
            "RAY_CI_WORKFLOW_AFFECTED={}".format(RAY_CI_WORKFLOW_AFFECTED),
            "RAY_CI_DATA_AFFECTED={}".format(RAY_CI_DATA_AFFECTED),
            "RAY_CI_RELEASE_TESTS_AFFECTED={}".format(RAY_CI_RELEASE_TESTS_AFFECTED),
            "RAY_CI_COMPILED_PYTHON_AFFECTED={}".format(
                RAY_CI_COMPILED_PYTHON_AFFECTED
            ),
        ]
    )

    # Debug purpose
    print(output_string, file=sys.stderr)

    # Used by buildkite log format
    if args.output.lower() == "json":
        pairs = [item.split("=") for item in output_string.split(" ")]
        affected_vars = [key for key, affected in pairs if affected == "1"]
        print(json.dumps(affected_vars))
    else:
        print(output_string)
